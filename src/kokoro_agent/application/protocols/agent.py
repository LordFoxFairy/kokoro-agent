"""application 消费的 agent 端口：v3 typed projections 的窄契约，infra builder 实现。"""

from __future__ import annotations

from collections.abc import AsyncIterable, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from langchain_core.messages import AIMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.stream import StreamTransformer
from langgraph.types import Interrupt


@runtime_checkable
class ModelStream(Protocol):
    """单次模型调用的 v3 流（AsyncChatModelStream）：原生 .text/.reasoning projection 取文本/推理与归属。"""

    namespace: list[str]
    node: str | None

    @property
    def message_id(self) -> str | None: ...
    @property
    def text(self) -> AsyncIterable[str]: ...
    @property
    def reasoning(self) -> AsyncIterable[str]: ...
    @property
    def output_message(self) -> AIMessage | None: ...


@runtime_checkable
class ToolCallInfo(Protocol):
    """transformer 映射工具事件所需的最小视图：起始稳定 + 终值。"""

    tool_call_id: str
    tool_name: str
    input: dict[str, object] | None
    output: object
    error: str | None


@runtime_checkable
class ToolCallView(ToolCallInfo, Protocol):
    """invoke 驱动消费额外所需：完成标志 + 输出增量流（ToolCallStream 全貌）。"""

    completed: bool
    output_deltas: AsyncIterable[object]


class _RunProjections(Protocol):
    """run 与子代理 run 共享的四投影（只读，单消费者）。"""

    @property
    def messages(self) -> AsyncIterable[ModelStream]: ...
    @property
    def tool_calls(self) -> AsyncIterable[ToolCallView]: ...
    @property
    def subagents(self) -> "AsyncIterable[SubagentRunStream]": ...
    @property
    def custom(self) -> AsyncIterable[object]: ...


@runtime_checkable
class SubagentInfo(Protocol):
    """transformer 映射子代理状态所需的最小身份：归属取 trigger_call_id。"""

    name: str | None
    trigger_call_id: str | None
    task_input: str | None
    status: str


@runtime_checkable
class SubagentRunStream(SubagentInfo, _RunProjections, Protocol):
    """递归子代理 run 流（AsyncSubagentRunStream）：身份 + 自带递归投影。"""


@runtime_checkable
class AgentRunStream(_RunProjections, Protocol):
    """顶层 v3 run 流（AsyncGraphRunStream）：投影 + 暂停查询 + async 上下文清退。"""

    async def interrupted(self) -> bool: ...
    async def __aenter__(self) -> AgentRunStream: ...
    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool | None: ...


@runtime_checkable
class StateView(Protocol):
    """checkpoint 快照的窄视图（StateSnapshot）：暂停信息取 typed interrupts，消息取 values。"""

    @property
    def values(self) -> Mapping[str, Any]: ...
    @property
    def interrupts(self) -> tuple[Interrupt, ...]: ...


@runtime_checkable
class InvokableAgent(Protocol):
    """编译后 langgraph 图的窄契约：v3 流入口 + 状态查询，挡住私有泛型泄漏。"""

    async def astream_events(
        self,
        payload: object,
        *,
        version: str,
        config: RunnableConfig,
        transformers: Sequence[type[StreamTransformer]],
    ) -> AgentRunStream: ...

    async def aget_state(self, config: RunnableConfig) -> StateView: ...
