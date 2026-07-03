"""工具策略中间件规格：未授权 fail-closed、授权放行 + 审计。"""

from __future__ import annotations

from typing import Any

import pytest
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolRuntime

from kokoro_agent.tools.middleware import ToolPolicyMiddleware


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
