from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

TODO_TOOL_NAME = "write_todos"
SUBAGENT_TOOL_NAME = "task"
RUNTIME_SUBAGENT_TOOL_NAME = "agent"
TOOL_RESULT_MAX_CHARS = 8_000

ToolScalar: TypeAlias = str | int | float | bool | None
SubagentSource: TypeAlias = Literal["built-in", "config-custom", "runtime-custom"]
TodoStatus: TypeAlias = Literal["pending", "in_progress", "completed"]


@dataclass(slots=True, frozen=True)
class TodoItem:
    content: str
    status: TodoStatus


@dataclass(slots=True, frozen=True)
class EventHeader:
    event: str
    name: str
    run_id: str
    lc_agent_name: str


@dataclass(slots=True, frozen=True)
class ToolInput:
    args: dict[str, ToolScalar]
    todos: tuple[TodoItem, ...]
    subagent_type: str
    description: str
    name: str


@dataclass(slots=True, frozen=True)
class MessageParts:
    text: str
    reasoning: str


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
