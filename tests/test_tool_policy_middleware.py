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
        return ToolMessage(content="ok", tool_call_id="c1", name="ask_user")


def _request(name: str) -> ToolCallRequest:
    return ToolCallRequest(
        tool_call={"name": name, "args": {}, "id": "c1", "type": "tool_call"},
        tool=None,
        state=None,
        runtime=_runtime(),
    )


async def test_authorized_tool_passes_through() -> None:
    middleware = ToolPolicyMiddleware(frozenset({"ask_user"}))
    handler = _Handler()
    result = await middleware.awrap_tool_call(_request("ask_user"), handler)
    assert handler.calls == 1
    assert isinstance(result, ToolMessage)
    assert result.status != "error"


async def test_unauthorized_tool_denied_without_handler() -> None:
    middleware = ToolPolicyMiddleware(frozenset({"ask_user"}))
    handler = _Handler()
    result = await middleware.awrap_tool_call(_request("rm_rf"), handler)
    assert handler.calls == 0
    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert result.tool_call_id == "c1"
    assert result.name == "rm_rf"


async def test_audit_logged_for_authorized(caplog: pytest.LogCaptureFixture) -> None:
    middleware = ToolPolicyMiddleware(frozenset({"ask_user"}))
    with caplog.at_level("INFO", logger="kokoro_agent.tools.middleware"):
        await middleware.awrap_tool_call(_request("ask_user"), _Handler())
    assert any("audit" in r.message for r in caplog.records)
