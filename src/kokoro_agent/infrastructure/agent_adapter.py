from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any, Literal, Protocol, TypedDict

import deepagents
import langchain.agents

# deepagents exports FilesystemPermission at runtime but omits it from its typed surface.
from deepagents.middleware.filesystem import FilesystemPermission  # type: ignore[attr-defined]
from deepagents.middleware.subagents import SubAgent
from langchain_core.language_models import BaseChatModel
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StreamEvent
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver

# The agent SDK is a real, under-typed boundary: reach its constructors through an
# Any view of the package so their results flow into the typed protocols below
# without a cast or a per-call ignore.
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
    "tool_coroutine",
    "tool_func",
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
    async def ainvoke(self, input: dict[str, list[dict[str, str]]]) -> object: ...


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


def tool_coroutine(tool: StructuredTool) -> Callable[..., Awaitable[str]] | None:
    return tool.coroutine


def tool_func(tool: StructuredTool) -> Callable[..., str] | None:
    return tool.func
