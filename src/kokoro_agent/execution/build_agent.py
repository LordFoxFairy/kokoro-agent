"""DeepAgents 装配：静态 import create_deep_agent，出口收窄为 InvokableAgent 端口。"""

# create_deep_agent 的签名含未解 ResponseT 泛型（上游 deepagents 边界）；本文件唯一职责就是包住它。
# pyright: reportUnknownVariableType=false

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypeGuard

from deepagents import create_deep_agent
from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware.filesystem import FilesystemPermission
from deepagents.middleware.subagents import SubAgent
from langchain.agents.middleware import AgentMiddleware, InterruptOnConfig
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore

from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.state import KokoroAgentState


def build_agent(
    *,
    model: BaseChatModel,
    tools: Sequence[BaseTool],
    system_prompt: str,
    subagents: Sequence[SubAgent],
    checkpointer: BaseCheckpointSaver[str] | None,
    permissions: Sequence[FilesystemPermission],
    interrupt_on: Mapping[str, bool | InterruptOnConfig],
    middleware: Sequence[AgentMiddleware] = (),
    backend: BackendProtocol | None = None,
    store: BaseStore | None = None,
    # Skills V2：deepagents 原生 SkillsMiddleware 源路径（渐进披露）；空=不挂。
    skills: Sequence[str] = (),
) -> InvokableAgent:
    # deepagents 返回泛型 CompiledStateGraph（含未定 ResponseT）：object 边界 + TypeGuard
    # 一次收窄为窄端口，私有泛型不外泄。
    agent: object = create_deep_agent(
        model=model,
        tools=list(tools),
        system_prompt=system_prompt,
        subagents=list(subagents),
        checkpointer=checkpointer,
        permissions=list(permissions),
        interrupt_on=dict(interrupt_on),
        middleware=list(middleware),
        backend=backend,
        store=store,
        skills=list(skills) if skills else None,
        # 身份乘 State 轴（scope 键）：随 input 进图、落 checkpoint（run/state.py 法则）。
        state_schema=KokoroAgentState,
    )
    if not _is_invokable_agent(agent):
        raise TypeError("create_deep_agent returned an object that does not satisfy InvokableAgent")
    return agent


def _is_invokable_agent(value: object) -> TypeGuard[InvokableAgent]:
    return callable(getattr(value, "astream_events", None)) and callable(
        getattr(value, "aget_state", None)
    )
