"""Agent 构建入口：把模型、工具、子代理、middleware 和 backend 组装成可运行 agent。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from importlib import import_module
from typing import TypeGuard

from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware.filesystem import FilesystemPermission
from deepagents.middleware.subagents import SubAgent
from langchain.agents.middleware import AgentMiddleware, InterruptOnConfig
from langchain.agents.middleware.types import AgentState
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver

from kokoro_agent.config import AppConfig, RuntimeSettings
from kokoro_agent.execution.prompts import SYSTEM_PROMPT
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.run.request import PermissionMode
from kokoro_agent.sandbox.backend import backend_from_settings
from kokoro_agent.subagents import subagent_definitions
from kokoro_agent.tools.middleware import ToolPolicyMiddleware
from kokoro_agent.tools.permissions import build_filesystem_permissions, build_interrupt_on
from kokoro_agent.tools.registry import BUILT_IN_TOOLS

__all__ = [
    "FilesystemPermission",
    "build_agent",
    "build_deep_agent",
]


def _load_callable(module_name: str, attr: str) -> Callable[..., object]:
    raw: object = getattr(import_module(module_name), attr)
    if not callable(raw):
        msg = f"{module_name}.{attr} is not callable"
        raise TypeError(msg)
    return raw


_CREATE_DEEP_AGENT = _load_callable("deepagents", "create_deep_agent")


def build_agent(
    model: BaseChatModel,
    permission_mode: PermissionMode,
    checkpointer: BaseCheckpointSaver[str] | None = None,
    runtime: RuntimeSettings | None = None,
) -> InvokableAgent:
    settings = runtime if runtime is not None else AppConfig.from_env().runtime
    # default 档通过原生 interrupt_on 做工具级审批，auto 档空映射跳过。
    return build_deep_agent(
        model=model,
        tools=BUILT_IN_TOOLS,
        system_prompt=SYSTEM_PROMPT,
        subagents=subagent_definitions(),
        checkpointer=checkpointer,
        permissions=build_filesystem_permissions(permission_mode),
        interrupt_on=build_interrupt_on(permission_mode),
        middleware=(ToolPolicyMiddleware(),),
        skills=settings.skills,
        memory=settings.memory,
        backend=backend_from_settings(settings),
    )


def build_deep_agent(
    *,
    model: BaseChatModel,
    tools: Sequence[StructuredTool],
    system_prompt: str,
    subagents: Sequence[SubAgent],
    checkpointer: BaseCheckpointSaver[str] | None,
    permissions: Sequence[FilesystemPermission],
    interrupt_on: Mapping[str, bool | InterruptOnConfig],
    middleware: Sequence[AgentMiddleware[AgentState[object], None, object]] = (),
    skills: Sequence[str] = (),
    memory: Sequence[str] = (),
    backend: BackendProtocol | None = None,
) -> InvokableAgent:
    agent = _CREATE_DEEP_AGENT(
        model=model,
        tools=list(tools),
        system_prompt=system_prompt,
        subagents=list(subagents),
        checkpointer=checkpointer,
        permissions=list(permissions),
        interrupt_on=dict(interrupt_on),
        middleware=list(middleware),
        skills=list(skills) or None,
        memory=list(memory) or None,
        backend=backend,
    )
    if not _is_invokable_agent(agent):
        msg = "create_deep_agent returned an object that does not satisfy InvokableAgent"
        raise TypeError(msg)
    return agent


def _is_invokable_agent(value: object) -> TypeGuard[InvokableAgent]:
    return callable(getattr(value, "astream_events", None)) and callable(
        getattr(value, "aget_state", None)
    )
