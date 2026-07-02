"""LangChain tool-call middleware owned by Kokoro."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import AgentState
from langchain_core.messages import ToolCall
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

ToolArgs = Mapping[str, object]
ToolArgNormalizer = Callable[[ToolArgs], ToolArgs]


class ToolPolicyMiddleware(AgentMiddleware[AgentState[object], None, object]):
    """Central hook for deterministic tool-call policy before HIL."""

    def __init__(self, normalizers: Mapping[str, ToolArgNormalizer] | None = None) -> None:
        super().__init__()
        self._normalizers = dict(normalizers or {})

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[object]]],
    ) -> ToolMessage | Command[object]:
        normalizer = self._normalizers.get(request.tool_call["name"])
        if normalizer is not None:
            normalized_args = dict(normalizer(dict(request.tool_call["args"])))
            request = request.override(
                tool_call=ToolCall(
                    name=request.tool_call["name"],
                    args=normalized_args,
                    id=request.tool_call["id"],
                    type="tool_call",
                )
            )
        return await handler(request)


__all__ = ["ToolArgNormalizer", "ToolPolicyMiddleware"]
