from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Literal, Protocol, TypedDict, cast

from deepagents import FilesystemPermission, create_deep_agent  # type: ignore[attr-defined]  # pyright: ignore[reportUnknownVariableType]
from langchain.agents import create_agent  # pyright: ignore[reportUnknownVariableType]
from langchain_core.language_models import BaseChatModel
from langchain_core.runnables.config import RunnableConfig
from langchain_core.runnables.schema import StreamEvent
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver

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
    subagents: Sequence[object],
    checkpointer: BaseCheckpointSaver[str] | None,
    permissions: Sequence[FilesystemPermission],
) -> EventStreamingAgent:
    # deepagents' create_deep_agent stub lags its runtime: `permissions` is accepted
    # at runtime but absent from the signature, and `subagents` is an invariant list.
    agent = create_deep_agent(  # type: ignore[call-arg]
        model=model,
        tools=list(tools),
        system_prompt=system_prompt,
        subagents=list(subagents),  # type: ignore[arg-type]
        checkpointer=checkpointer,
        permissions=list(permissions),
    )
    return cast("EventStreamingAgent", agent)


def make_subagent_runner(model: BaseChatModel, *, system_prompt: str, name: str) -> AsyncRunner:
    # create_agent returns a CompiledStateGraph whose .ainvoke is the runner we use.
    return cast("AsyncRunner", create_agent(model, system_prompt=system_prompt, tools=[], name=name))


def tool_coroutine(tool: StructuredTool) -> Callable[..., Awaitable[str]] | None:
    return tool.coroutine


def tool_func(tool: StructuredTool) -> Callable[..., str] | None:
    return tool.func
