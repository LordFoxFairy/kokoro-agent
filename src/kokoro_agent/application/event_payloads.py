"""AgentEvent 各类载荷形状的单一来源：每种形状的键名在此独占，避免散落手写。"""

from __future__ import annotations

from pydantic import JsonValue

from kokoro_agent.domain.stream_intent import (
    SubagentFinished,
    SubagentStarted,
    TodoItem,
    ToolInvoked,
    ToolReturned,
)


def text_payload(segment_id: str, text: str) -> dict[str, JsonValue]:
    return {"segment_id": segment_id, "text": text}


def subagent_text_payload(segment_id: str, subagent_id: str, text: str) -> dict[str, JsonValue]:
    return {"segment_id": segment_id, "subagent_id": subagent_id, "text": text}


def todo_payload(todos: tuple[TodoItem, ...]) -> dict[str, JsonValue]:
    return {"todos": [{"content": todo.content, "status": todo.status} for todo in todos]}


def tool_invoked_payload(segment_id: str, tool: ToolInvoked) -> dict[str, JsonValue]:
    return {
        "segment_id": segment_id,
        "tool_id": tool.tool_id,
        "name": tool.name,
        "args": dict(tool.args),
    }


def tool_returned_payload(segment_id: str, tool: ToolReturned) -> dict[str, JsonValue]:
    # rejected 始终随载荷输出，对齐 agent_event 契约的 tool.returned 形状。
    return {
        "segment_id": segment_id,
        "tool_id": tool.tool_id,
        "name": tool.name,
        "result": tool.result,
        "is_error": tool.is_error,
        "rejected": tool.rejected,
    }


def subagent_started_payload(segment_id: str, sub: SubagentStarted) -> dict[str, JsonValue]:
    return {
        "segment_id": segment_id,
        "subagent_id": sub.subagent_id,
        "name": sub.name,
        "description": sub.description,
        "subagent_type": sub.subagent_type,
        "source": sub.source,
    }


def subagent_finished_payload(segment_id: str, sub: SubagentFinished) -> dict[str, JsonValue]:
    return {
        "segment_id": segment_id,
        "subagent_id": sub.subagent_id,
        "name": sub.name,
        "subagent_type": sub.subagent_type,
        "source": sub.source,
    }
