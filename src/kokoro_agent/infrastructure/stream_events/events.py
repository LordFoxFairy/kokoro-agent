"""流事件的基础设施值对象与工具名常量（解析层内部使用）。"""

from __future__ import annotations

from dataclasses import dataclass

from kokoro_agent.domain.stream_intent import TodoItem, ToolScalar

TODO_TOOL_NAME = "write_todos"
SUBAGENT_TOOL_NAME = "task"
RUNTIME_SUBAGENT_TOOL_NAME = "agent"
TOOL_RESULT_MAX_CHARS = 8_000


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
