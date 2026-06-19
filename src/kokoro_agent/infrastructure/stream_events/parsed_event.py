"""LangChain 防腐层 DTO：从未类型化 StreamEvent 解析出的强类型值对象。"""

from __future__ import annotations

from dataclasses import dataclass

from kokoro_agent.domain.stream_intent import TodoItem, ToolScalar


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
