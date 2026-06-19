from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

from kokoro_agent.domain.registered_subagent import SubagentSource

ToolScalar: TypeAlias = str | int | float | bool | None
TodoStatus: TypeAlias = Literal["pending", "in_progress", "completed"]


@dataclass(slots=True, frozen=True)
class TodoItem:
    content: str
    status: TodoStatus


@dataclass(slots=True, frozen=True)
class TodoUpdated:
    todos: tuple[TodoItem, ...]


@dataclass(slots=True, frozen=True)
class ToolInvoked:
    tool_id: str
    name: str
    args: dict[str, ToolScalar]


@dataclass(slots=True, frozen=True)
class ToolReturned:
    tool_id: str
    name: str
    result: str
    is_error: bool
    rejected: bool = False


@dataclass(slots=True, frozen=True)
class SubagentStarted:
    subagent_id: str
    name: str
    description: str
    subagent_type: str
    source: SubagentSource


@dataclass(slots=True, frozen=True)
class SubagentFinished:
    subagent_id: str
    name: str
    subagent_type: str
    source: SubagentSource


@dataclass(slots=True, frozen=True)
class ThinkingDelta:
    text: str


@dataclass(slots=True, frozen=True)
class TextStream:
    text: str


@dataclass(slots=True, frozen=True)
class TextFinal:
    text: str


StreamIntent: TypeAlias = (
    TodoUpdated
    | ToolInvoked
    | ToolReturned
    | SubagentStarted
    | SubagentFinished
    | ThinkingDelta
    | TextStream
    | TextFinal
)
