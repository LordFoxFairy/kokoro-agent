"""Stable tools use current owner ports; retired deployment/YAML behavior is not a product API."""

from __future__ import annotations

# BaseTool.ainvoke is the upstream unparameterized test boundary.
# pyright: reportUnknownMemberType=false

import asyncio
from dataclasses import dataclass

import pytest
from langchain_core.callbacks import BaseCallbackHandler
from langgraph.errors import GraphInterrupt
from langchain_core.messages import ToolMessage
from langchain_core.tools import StructuredTool
from mcp.types import CallToolResult, TextContent
from pydantic import JsonValue
from langchain_mcp_adapters.callbacks import Callbacks

from kokoro_agent.clients.mcp import McpClientError, McpConnectionRef, McpTool
from kokoro_agent.mcp.tools import make_mcp_tools
from kokoro_agent.execution.events import tool_returned_payload
from kokoro_agent.domain.chat.projection import project_chat_fact
from kokoro_agent.application.chat.mappers import wire_epoch_millis_to_utc
from support.fakes import FakeRunRepository
from support.tool_journal import journal_request as _request, journal_middleware as _mw


class CurrentClient:
    def __init__(self) -> None:
        self.reads = 0
        self.calls = 0
        self.unavailable = False
        self.remote_error = False
        self.call_error: BaseException | None = None

    async def check(self, references: object) -> None:
        return None

    async def list_tools(self, reference: McpConnectionRef) -> tuple[McpTool, ...]:
        self.reads += 1
        if self.unavailable:
            raise McpClientError("upstream URL and secret must not escape")
        return (
            McpTool(
                name="ok",
                description="第一行说明\n第二行细节",
                input_schema={"type": "object"},
            ),
        )

    async def call_tool(
        self,
        reference: McpConnectionRef,
        tool: str,
        arguments: dict[str, JsonValue],
        *,
        tool_call_id: str,
        callbacks: Callbacks | None = None,
    ) -> CallToolResult:
        if self.call_error is not None:
            raise self.call_error
        if self.unavailable:
            raise McpClientError("upstream URL and secret must not escape")
        self.calls += 1
        assert tool_call_id == "c1"
        return CallToolResult(
            isError=self.remote_error,
            content=[
                TextContent(type="text", text="ok:" + str(arguments.get("text", "")))
            ],
        )


async def test_list_and_describe_are_lazy_current_and_schema_is_data() -> None:
    client = CurrentClient()
    tools = make_mcp_tools(
        (McpConnectionRef(connection_id="srv"),), client, run_id="rn"
    )
    assert client.reads == 0
    assert "srv/ok — 第一行说明" in await tools[0].ainvoke({})
    described = await tools[1].ainvoke({"server": "srv", "tool": "ok"})
    assert '"type": "object"' in described and "第二行细节" in described
    assert (
        client.reads == 2
    )  # No previous tools/list result becomes an authorization cache.
    assert "NOT_ALLOWED" in await tools[1].ainvoke({"server": "srv", "tool": "blocked"})
    assert "NOT_DECLARED" in await tools[1].ainvoke({"server": "other", "tool": "ok"})


async def test_journal_bound_call_roundtrip_replay_and_current_errors() -> None:
    client = CurrentClient()
    tools = make_mcp_tools(
        (McpConnectionRef(connection_id="srv"),), client, run_id="rn"
    )
    assert "JOURNAL_CALL_REQUIRED" in await tools[2].ainvoke(
        {"server": "srv", "tool": "ok"}
    )
    assert client.calls == 0
    store = FakeRunRepository()
    middleware = await _mw(store)

    async def handler(request: object) -> ToolMessage:
        output = await tools[2].ainvoke(
            {
                "type": "tool_call",
                "name": "mcp_call",
                "id": "c1",
                "args": {"server": "srv", "tool": "ok", "arguments": {"text": "hello"}},
            }
        )
        assert isinstance(output, ToolMessage)
        return output

    first = await middleware.awrap_tool_call(_request("mcp_call"), handler)
    assert isinstance(first, ToolMessage) and "ok:hello" in first.text
    await middleware.awrap_tool_call(_request("mcp_call"), handler)
    assert client.calls == 1
    client.unavailable = True
    assert "secret" not in await tools[0].ainvoke({})
    assert "CURRENT_READ_FAILED" in await tools[1].ainvoke(
        {"server": "srv", "tool": "ok"}
    )


def test_server_set_changes_do_not_change_stable_tool_schema() -> None:
    a = make_mcp_tools((), None, run_id="rn")
    b = make_mcp_tools(
        (McpConnectionRef(connection_id="srv"),), CurrentClient(), run_id="rn"
    )

    def exposed_schema(tool: StructuredTool) -> object:
        schema = tool.tool_call_schema
        return schema if isinstance(schema, dict) else schema.model_json_schema()

    assert [(t.name, t.description, exposed_schema(t)) for t in a] == [
        (t.name, t.description, exposed_schema(t)) for t in b
    ]
    with pytest.raises(McpClientError):
        make_mcp_tools((McpConnectionRef(connection_id="srv"),), None, run_id="rn")


async def test_empty_selection_does_not_read_or_connect() -> None:
    tools = make_mcp_tools((), None, run_id="rn")
    assert "没有可用" in await tools[0].ainvoke({})


async def test_remote_mcp_error_finishes_journal_as_failed() -> None:
    client = CurrentClient()
    client.remote_error = True
    tools = make_mcp_tools(
        (McpConnectionRef(connection_id="srv"),), client, run_id="rn"
    )
    store = FakeRunRepository()
    middleware = await _mw(store)

    async def handler(request: object) -> ToolMessage:
        output = await tools[2].ainvoke(
            {
                "type": "tool_call",
                "name": "mcp_call",
                "id": "c1",
                "args": {"server": "srv", "tool": "ok"},
            }
        )
        assert isinstance(output, ToolMessage)
        return output

    result = await middleware.awrap_tool_call(_request("mcp_call"), handler)
    assert isinstance(result, ToolMessage) and result.status == "error"
    assert store.tool_journal[("rn", "c1")]["status"] == "failed"


class _ToolEnds(BaseCallbackHandler):
    def __init__(self) -> None:
        self.outputs: list[object] = []

    def on_tool_end(self, output: object, **kwargs: object) -> None:
        self.outputs.append(output)


@dataclass
class _EndedCall:
    output: object
    tool_call_id: str = "c1"
    tool_name: str = "mcp_call"
    input: dict[str, object] | None = None
    error: str | None = None


@pytest.mark.parametrize(
    "remote_error,unavailable", [(False, False), (True, False), (False, True)]
)
async def test_official_callback_journal_and_safe_activity_agree(
    remote_error: bool, unavailable: bool
) -> None:
    import json

    client = CurrentClient()
    client.remote_error = remote_error
    client.unavailable = unavailable
    tool = make_mcp_tools(
        (McpConnectionRef(connection_id="srv"),), client, run_id="rn"
    )[2]
    store = FakeRunRepository()
    middleware = await _mw(store)
    ends = _ToolEnds()

    async def handler(request: object) -> ToolMessage:
        output = await tool.ainvoke(
            {
                "type": "tool_call",
                "name": "mcp_call",
                "id": "c1",
                "args": {
                    "server": "srv",
                    "tool": "ok",
                    "arguments": {"text": "diagnostic"},
                },
            },
            config={"callbacks": [ends]},
        )
        assert isinstance(output, ToolMessage)
        return output

    result = await middleware.awrap_tool_call(_request("mcp_call"), handler)
    assert isinstance(result, ToolMessage)
    assert len(ends.outputs) == 1
    observed = ends.outputs[0]
    assert isinstance(observed, ToolMessage)
    expected_error = remote_error or unavailable
    assert (
        observed.status == result.status == ("error" if expected_error else "success")
    )
    assert store.tool_journal[("rn", "c1")]["is_error"] is expected_error
    payload = tool_returned_payload(_EndedCall(observed))
    assert payload is not None and payload.is_error is expected_error
    projected = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        run_id="rn",
        source_index=1,
        created_at=wire_epoch_millis_to_utc(0),
        payload=payload,
        tool_display_code="tool.mcp.call",
    )
    assert projected is not None
    safe = json.loads(projected.event.payload_json)
    assert safe["status"] == ("failed" if expected_error else "completed")
    assert set(safe) == {
        "activity",
        "activity_id",
        "segment_id",
        "status",
        "display_code",
    }
    if unavailable:
        assert "MCP_TOOL_CALL_FAILED" in result.text and "secret" not in result.text
    else:
        assert "diagnostic" in result.text
    replay = await middleware.awrap_tool_call(_request("mcp_call"), handler)
    assert isinstance(replay, ToolMessage) and replay.status == result.status
    assert len(ends.outputs) == 1 and client.calls == (0 if unavailable else 1)


@pytest.mark.parametrize("error", [asyncio.CancelledError(), GraphInterrupt()])
async def test_official_mcp_cancel_and_pause_do_not_become_a_handled_result(
    error: BaseException,
) -> None:
    client = CurrentClient()
    client.call_error = error
    tool = make_mcp_tools(
        (McpConnectionRef(connection_id="srv"),), client, run_id="rn"
    )[2]
    store = FakeRunRepository()
    middleware = await _mw(store)
    ends = _ToolEnds()

    async def handler(request: object) -> ToolMessage:
        output = await tool.ainvoke(
            {
                "type": "tool_call",
                "name": "mcp_call",
                "id": "c1",
                "args": {"server": "srv", "tool": "ok"},
            },
            config={"callbacks": [ends]},
        )
        assert isinstance(output, ToolMessage)
        return output

    with pytest.raises(type(error)):
        await middleware.awrap_tool_call(_request("mcp_call"), handler)
    assert ends.outputs == []
    assert store.tool_journal[("rn", "c1")]["status"] == "started"
