"""Journal-bound invocation fixture shared by current MCP tool tests."""

from __future__ import annotations

from typing import Any
from langchain.agents.middleware.types import ToolCallRequest
from langgraph.prebuilt.tool_node import ToolRuntime

from kokoro_agent.tools.middleware import ToolEffectJournalMiddleware
from support.fakes import FakeRunRepository, request


def journal_request(name: str = "mcp_call", call_id: str = "c1") -> ToolCallRequest:
    runtime: ToolRuntime[Any, Any] = ToolRuntime(
        state=None,
        context=None,
        config={},
        stream_writer=lambda _chunk: None,
        tool_call_id=call_id,
        store=None,
    )
    return ToolCallRequest(
        tool_call={"name": name, "args": {}, "id": call_id, "type": "tool_call"},
        tool=None,
        state=None,
        runtime=runtime,
    )


async def journal_middleware(store: FakeRunRepository) -> ToolEffectJournalMiddleware:
    lease = store.current_lease("rn")
    if lease is None:
        lease = await store.try_claim(request("rn"))
    assert lease is not None
    return ToolEffectJournalMiddleware(run_repository=store, run_id="rn", lease=lease)
