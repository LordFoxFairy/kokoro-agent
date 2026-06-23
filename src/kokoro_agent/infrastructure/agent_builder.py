"""构造层：把 langchain / deepagents 的 agent 与 runner 构造成强类型协议。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

import deepagents
import langchain.agents

# mypy resolves a stale deepagents lacking this re-exported symbol; pyright (venv) sees it fine.
from deepagents.middleware.filesystem import FilesystemPermission
from deepagents.middleware.subagents import CompiledSubAgent, SubAgent
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_core.runnables import Runnable
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver

from kokoro_agent.application.protocols.agent import InvokableAgent

# 框架返回的 CompiledStateGraph 结构上不匹配下方窄 Protocol（astream_events/ainvoke 签名更宽），
# 经包的 Any 视图取构造函数，使结果直接收敛到强类型 Protocol，免去逐调用的类型抑制。
_deepagents: Any = deepagents
_langchain_agents: Any = langchain.agents
_build_deep_agent = _deepagents.create_deep_agent
_build_subagent = _langchain_agents.create_agent

__all__ = [
    "AsyncRunner",
    "FilesystemPermission",
    "make_deep_agent",
    "make_subagent_runnable",
    "make_subagent_runner",
]


class AsyncRunner(Protocol):
    # 入参是 langgraph 图状态（messages 用 LangChain message）；结果是字符串键的进程内
    # 图状态（值为 BaseMessage 等不透明对象），由调用方按需收窄。
    async def ainvoke(self, payload: dict[str, list[BaseMessage]]) -> Mapping[str, object]: ...


def make_deep_agent(
    *,
    model: BaseChatModel,
    tools: Sequence[StructuredTool],
    system_prompt: str,
    subagents: Sequence[SubAgent | CompiledSubAgent],
    checkpointer: BaseCheckpointSaver[str] | None,
    permissions: Sequence[FilesystemPermission],
) -> InvokableAgent:
    agent: InvokableAgent = _build_deep_agent(
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


def make_subagent_runnable(
    model: BaseChatModel, *, system_prompt: str, name: str
) -> Runnable[dict[str, list[BaseMessage]], Mapping[str, object]]:
    runnable: Runnable[dict[str, list[BaseMessage]], Mapping[str, object]] = _build_subagent(
        model, system_prompt=system_prompt, tools=[], name=name
    )
    return runnable
