"""application 消费的 agent 端口：上层依赖的强类型契约，infra 的 builder 负责实现。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from langchain_core.messages import BaseMessage
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StreamEvent


class EventStreamingAgent(Protocol):
    # inp 是 langgraph 图的 partial state 更新（messages 用 LangChain message）；
    # 与 AsyncRunner.ainvoke 同形，不另立命名信封。
    def astream_events(
        self,
        inp: dict[str, list[BaseMessage]],
        *,
        version: str,
        config: RunnableConfig | None,
    ) -> AsyncIterator[StreamEvent]: ...
