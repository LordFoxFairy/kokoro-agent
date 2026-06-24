"""构造层：把 langchain / deepagents 的 agent 与 runner 构造成强类型协议。"""
# 本文件是唯一的框架构造适配点：deepagents/langgraph v3 是 @beta、部分未类型化，构造函数的返回
# 类型在此对齐到本仓维护的强类型视图，未类型化噪音收口于此一处（上层全程强类型、零 ignore）。
# pyright: reportUnknownVariableType=false

from __future__ import annotations

from collections.abc import Mapping, Sequence

from deepagents import create_deep_agent
from deepagents.middleware.filesystem import FilesystemPermission
from deepagents.middleware.subagents import CompiledSubAgent, SubAgent
from langchain.agents import create_agent
from langchain.agents.middleware import InterruptOnConfig
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_core.runnables import Runnable
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver

from kokoro_agent.application.protocols.agent import InvokableAgent

__all__ = [
    "FilesystemPermission",
    "make_deep_agent",
    "make_subagent_runnable",
]


def make_deep_agent(
    *,
    model: BaseChatModel,
    tools: Sequence[StructuredTool],
    system_prompt: str,
    subagents: Sequence[SubAgent | CompiledSubAgent],
    checkpointer: BaseCheckpointSaver[str] | None,
    permissions: Sequence[FilesystemPermission],
    interrupt_on: Mapping[str, bool | InterruptOnConfig],
) -> InvokableAgent:
    # 唯一类型让步：deepagents/langgraph v3 streaming 是 @beta、部分未类型化，create_deep_agent
    # 返回的 CompiledStateGraph 其 astream_events overload 与本仓 InvokableAgent 视图不同源。
    # 我们基于 langchain/deepagents（永久地基），不为纯类型洁癖再叠抽象——一行局部 ignore 收口此
    # 框架边界，上层全程强类型；框架补全类型后即可移除。
    agent: InvokableAgent = create_deep_agent(  # type: ignore[assignment]
        model=model,
        tools=list(tools),
        system_prompt=system_prompt,
        subagents=list(subagents),
        checkpointer=checkpointer,
        permissions=list(permissions),
        interrupt_on=dict(interrupt_on),
    )
    return agent


def make_subagent_runnable(
    model: BaseChatModel, *, system_prompt: str, name: str
) -> Runnable[dict[str, list[BaseMessage]], Mapping[str, object]]:
    # 同上 @beta 边界让步：create_agent 返回 CompiledStateGraph，局部 ignore 收口到声明视图。
    runnable: Runnable[dict[str, list[BaseMessage]], Mapping[str, object]] = create_agent(  # type: ignore[assignment]
        model, system_prompt=system_prompt, tools=[], name=name
    )
    return runnable
