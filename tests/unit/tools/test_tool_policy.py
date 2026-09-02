"""工具策略中间件规格：未授权 fail-closed、授权放行 + 审计；token 预算熔断。"""

from __future__ import annotations

from typing import Any

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse, ToolCallRequest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.prebuilt.tool_node import ToolRuntime

from support.fakes import FakeRunRepository
from support.local_fake import LocalFakeChatModel
from kokoro_agent.tools.middleware import (
    TokenBudgetExceeded,
    TokenBudgetMiddleware,
    ToolPolicyMiddleware,
)


def _runtime() -> ToolRuntime[Any, Any]:
    return ToolRuntime(
        state=None,
        context=None,
        config={},
        stream_writer=lambda _chunk: None,
        tool_call_id="c1",
        store=None,
    )


class _Handler:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, request: ToolCallRequest) -> ToolMessage:
        self.calls += 1
        return ToolMessage(content="ok", tool_call_id="c1", name="lookup")


def _request(name: str) -> ToolCallRequest:
    return ToolCallRequest(
        tool_call={"name": name, "args": {}, "id": "c1", "type": "tool_call"},
        tool=None,
        state=None,
        runtime=_runtime(),
    )


async def test_authorized_tool_passes_through() -> None:
    middleware = ToolPolicyMiddleware(frozenset({"lookup"}))
    handler = _Handler()
    result = await middleware.awrap_tool_call(_request("lookup"), handler)
    assert handler.calls == 1
    assert isinstance(result, ToolMessage)
    assert result.status != "error"


async def test_unauthorized_tool_denied_without_handler() -> None:
    middleware = ToolPolicyMiddleware(frozenset({"lookup"}))
    handler = _Handler()
    result = await middleware.awrap_tool_call(_request("rm_rf"), handler)
    assert handler.calls == 0
    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert result.tool_call_id == "c1"
    assert result.name == "rm_rf"


async def test_audit_logged_for_authorized(caplog: pytest.LogCaptureFixture) -> None:
    middleware = ToolPolicyMiddleware(frozenset({"lookup"}))
    with caplog.at_level("INFO", logger="kokoro_agent.tools.middleware"):
        await middleware.awrap_tool_call(_request("lookup"), _Handler())
    assert any("audit" in r.message for r in caplog.records)


def _task_request(subagent_type: str) -> ToolCallRequest:
    return ToolCallRequest(
        tool_call={
            "name": "task",
            "args": {"description": "do it", "subagent_type": subagent_type},
            "id": "c1",
            "type": "tool_call",
        },
        tool=None,
        state=None,
        runtime=_runtime(),
    )


class _TaskHandler:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, request: ToolCallRequest) -> ToolMessage:
        self.calls += 1
        return ToolMessage(content="delegated", tool_call_id="c1", name="task")


async def test_delegation_deny_blocks_undeclared_including_general_purpose() -> None:
    middleware = ToolPolicyMiddleware(
        frozenset({"task"}), declared_subagents=frozenset({"researcher"}), subagent_create="deny"
    )
    handler = _TaskHandler()
    for undeclared in ("general-purpose", "ghost"):
        result = await middleware.awrap_tool_call(_task_request(undeclared), handler)
        assert isinstance(result, ToolMessage) and result.status == "error"
        assert "not allowed" in result.text
    assert handler.calls == 0


async def test_delegation_deny_allows_declared() -> None:
    middleware = ToolPolicyMiddleware(
        frozenset({"task"}), declared_subagents=frozenset({"researcher"}), subagent_create="deny"
    )
    handler = _TaskHandler()
    result = await middleware.awrap_tool_call(_task_request("researcher"), handler)
    assert isinstance(result, ToolMessage) and result.text == "delegated"
    assert handler.calls == 1


async def test_delegation_allow_passes_anything() -> None:
    middleware = ToolPolicyMiddleware(
        frozenset({"task"}), declared_subagents=frozenset(), subagent_create="allow"
    )
    handler = _TaskHandler()
    result = await middleware.awrap_tool_call(_task_request("general-purpose"), handler)
    assert isinstance(result, ToolMessage) and result.text == "delegated"


# --- token 预算熔断：跨段累计（store 背书），超限 fail-loud ---


def _model_request() -> ModelRequest:
    return ModelRequest(model=LocalFakeChatModel.with_script([]), messages=[])


def _model_response(total_tokens: int) -> ModelResponse:
    message = AIMessage(
        content="ok",
        usage_metadata={"input_tokens": total_tokens - 1, "output_tokens": 1, "total_tokens": total_tokens},
    )
    return ModelResponse(result=[message])


async def test_token_budget_allows_then_trips() -> None:
    store = FakeRunRepository()
    middleware = TokenBudgetMiddleware(budget=100, run_repository=store, run_id="r1")

    async def handler(_request: object) -> ModelResponse:
        return _model_response(60)

    first = await middleware.awrap_model_call(_model_request(), handler)
    assert isinstance(first, ModelResponse)
    with pytest.raises(TokenBudgetExceeded, match="budget"):
        await middleware.awrap_model_call(_model_request(), handler)


async def test_token_budget_survives_middleware_rebuild() -> None:
    # resume 重建 middleware：计数在 store，不清零。
    store = FakeRunRepository()
    first = TokenBudgetMiddleware(budget=100, run_repository=store, run_id="r1")

    async def handler(_request: object) -> ModelResponse:
        return _model_response(60)

    await first.awrap_model_call(_model_request(), handler)
    rebuilt = TokenBudgetMiddleware(budget=100, run_repository=store, run_id="r1")
    with pytest.raises(TokenBudgetExceeded):
        await rebuilt.awrap_model_call(_model_request(), handler)


async def test_token_budget_isolated_per_run() -> None:
    store = FakeRunRepository()
    a = TokenBudgetMiddleware(budget=100, run_repository=store, run_id="ra")
    b = TokenBudgetMiddleware(budget=100, run_repository=store, run_id="rb")

    async def handler(_request: object) -> ModelResponse:
        return _model_response(90)

    await a.awrap_model_call(_model_request(), handler)
    await b.awrap_model_call(_model_request(), handler)  # 各自 90，均不超
