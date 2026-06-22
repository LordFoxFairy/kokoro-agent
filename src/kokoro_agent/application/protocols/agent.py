"""application 消费的 agent 端口：上层依赖的强类型契约，infra 的 builder 负责实现。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Literal, Protocol, TypedDict

from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StreamEvent


class _UserMessage(TypedDict):
    role: Literal["user"]
    content: str


class AgentInvokeInput(TypedDict):
    messages: list[_UserMessage]


class EventStreamingAgent(Protocol):
    def astream_events(
        self,
        inp: AgentInvokeInput,
        *,
        version: str,
        config: RunnableConfig | None,
    ) -> AsyncIterator[StreamEvent]: ...
