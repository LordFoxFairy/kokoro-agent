"""构造层：把 langchain / deepagents 的 agent 与 runner 构造成强类型协议。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, Protocol, TypedDict

import deepagents
import langchain.agents

# deepagents 运行时导出 FilesystemPermission，但其类型表面省略了它。
from deepagents.middleware.filesystem import FilesystemPermission  # type: ignore[attr-defined]
from deepagents.middleware.subagents import SubAgent
from langchain_core.language_models import BaseChatModel
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StreamEvent
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver

# deepagents/langchain.agents 对外缺少完整类型声明；经包的 Any 视图取得构造函数，
# 使结果直接流入下方强类型 Protocol，免去 cast 与逐调用 type: ignore。
_deepagents: Any = deepagents
_langchain_agents: Any = langchain.agents
_build_deep_agent = _deepagents.create_deep_agent
_build_subagent = _langchain_agents.create_agent

__all__ = [
    "AgentInvokeInput",
    "AsyncRunner",
    "EventStreamingAgent",
    "FilesystemPermission",
    "make_deep_agent",
    "make_subagent_runner",
]


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


class AsyncRunner(Protocol):
    # 返回 object：runner 结果是进程内不透明对象，由调用方按需收窄。
    async def ainvoke(self, payload: dict[str, list[dict[str, str]]]) -> object: ...


def make_deep_agent(
    *,
    model: BaseChatModel,
    tools: Sequence[StructuredTool],
    system_prompt: str,
    subagents: Sequence[SubAgent],
    checkpointer: BaseCheckpointSaver[str] | None,
    permissions: Sequence[FilesystemPermission],
) -> EventStreamingAgent:
    agent: EventStreamingAgent = _build_deep_agent(
        model=model,
        tools=list(tools),
        system_prompt=system_prompt,
        subagents=list(subagents),
        checkpointer=checkpointer,
        permissions=list(permissions),
    )
    return agent


def make_subagent_runner(model: BaseChatModel, *, system_prompt: str, name: str) -> AsyncRunner:
    runner: AsyncRunner = _build_subagent(model, system_prompt=system_prompt, tools=[], name=name)
    return runner
