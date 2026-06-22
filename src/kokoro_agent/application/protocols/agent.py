"""application 消费的 agent 端口：上层依赖的强类型契约，infra 的 builder 负责实现。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, TypedDict

from langchain_core.messages import BaseMessage
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StreamEvent


class AgentInvokeInput(TypedDict):
    # langgraph/deepagents 图入参形状：messages 直接用 LangChain message，不自建影子类型。
    messages: list[BaseMessage]


class EventStreamingAgent(Protocol):
    def astream_events(
        self,
        inp: AgentInvokeInput,
        *,
        version: str,
        config: RunnableConfig | None,
    ) -> AsyncIterator[StreamEvent]: ...
