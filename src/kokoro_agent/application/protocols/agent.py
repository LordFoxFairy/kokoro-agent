"""application 消费的 agent 端口：上层依赖的强类型契约，infra 的 builder 负责实现。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StreamEvent


@runtime_checkable
class InvokableAgent(Protocol):
    """编译后 langgraph 图的窄契约：仅 invoke 路径需要的两个方法。"""

    def astream_events(
        self, payload: object, *, version: str, config: RunnableConfig
    ) -> AsyncIterator[StreamEvent]: ...

    async def aget_state(self, config: RunnableConfig) -> object: ...
