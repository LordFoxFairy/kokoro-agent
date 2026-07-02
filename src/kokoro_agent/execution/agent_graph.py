"""构造层：把 langchain / deepagents 的 agent 与 runner 构造成强类型协议。"""

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

from kokoro_agent.execution.protocols import InvokableAgent

__all__ = [
    "FilesystemPermission",
    "make_deep_agent",
]


def _load_callable(module_name: str, attr: str) -> Callable[..., object]:
    raw: object = getattr(import_module(module_name), attr)
    if not callable(raw):
        msg = f"{module_name}.{attr} is not callable"
        raise TypeError(msg)
    return raw


_CREATE_DEEP_AGENT = _load_callable("deepagents", "create_deep_agent")


def make_deep_agent(
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
