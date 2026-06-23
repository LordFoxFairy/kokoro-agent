"""唯一投影：把单个原生 StreamEvent 直接映射为 0+ AgentEvent（无状态纯函数）。"""

from collections.abc import Mapping
from typing import TypeGuard

from langchain_core.messages import BaseMessage
from langchain_core.runnables.schema import StreamEvent
from pydantic import JsonValue

from kokoro_agent.interfaces.envelope import AgentEvent
from kokoro_agent.application.projection.attribution import SubagentAttribution
from kokoro_agent.application.projection.reasoning_shim import message_text_and_reasoning
from kokoro_agent.infrastructure.constants import (
    RUNTIME_SUBAGENT_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
    TODO_TOOL_NAME,
)
from kokoro_agent.infrastructure.subagent.specs import subagent_source_for

TOOL_RESULT_MAX_CHARS = 8_000


def project(event: StreamEvent, attribution: SubagentAttribution, run_id: str) -> list[AgentEvent]:
    # 信封 run_id 恒为 kokoro run_id；event["run_id"] 是 LC 每-Runnable 随机 id，仅作 segment_id/tool_id。
    native_id = event["run_id"]
    match event["event"]:
        case "on_chat_model_stream":
            return _from_message(event, attribution, run_id, native_id, final=False)
        case "on_chat_model_end":
            return _from_message(event, attribution, run_id, native_id, final=True)
        case "on_tool_start":
            return _from_tool_start(event, attribution, run_id, native_id)
        case "on_tool_end":
            return _from_tool_end(event, attribution, run_id, native_id, is_error=False)
        case "on_tool_error":
            return _from_tool_end(event, attribution, run_id, native_id, is_error=True)
        case _:
            return []


def _ev(kind: str, run_id: str, payload: dict[str, JsonValue]) -> AgentEvent:
    return AgentEvent.model_validate({"kind": kind, "run_id": run_id, "payload": payload})


def _as_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _data(event: StreamEvent) -> Mapping[object, object]:
    # StreamEvent.data 声明为无泛型 dict；先收口到 object 再 isinstance。
    raw: object = event.get("data")
    return raw if _as_mapping(raw) else {}


def _from_message(
    event: StreamEvent,
    attribution: SubagentAttribution,
    run_id: str,
    native_id: str,
    *,
    final: bool,
) -> list[AgentEvent]:
    key = "output" if final else "chunk"
    message: object = _data(event).get(key)
    if not isinstance(message, BaseMessage):
        return []
    text, reasoning = message_text_and_reasoning(message)
    subagent_id = attribution.active_id(event)
    events: list[AgentEvent] = []
    if reasoning:
        events.append(_ev("thinking.delta", run_id, {"segment_id": native_id, "text": reasoning}))
    if text:
        events.append(_text_event(run_id, native_id, subagent_id, text, final=final))
    return events


def _text_event(
    run_id: str, native_id: str, subagent_id: str | None, text: str, *, final: bool
) -> AgentEvent:
    if subagent_id is not None:
        kind = "subagent.text.completed" if final else "subagent.text.delta"
        return _ev(
            kind, run_id, {"segment_id": native_id, "subagent_id": subagent_id, "text": text}
        )
    kind = "text.completed" if final else "text.delta"
    return _ev(kind, run_id, {"segment_id": native_id, "text": text})


def _tool_input(event: StreamEvent) -> Mapping[object, object]:
    raw: object = _data(event).get("input")
    return raw if _as_mapping(raw) else {}


def _str_field(source: Mapping[object, object], key: str) -> str:
    value = source.get(key)
    return value if isinstance(value, str) else ""


def _from_tool_start(
    event: StreamEvent, attribution: SubagentAttribution, run_id: str, native_id: str
) -> list[AgentEvent]:
    name = event["name"]
    tool_input = _tool_input(event)
    if name == TODO_TOOL_NAME:
        return [_ev("todo.updated", run_id, {"todos": _todos(tool_input)})]
    subagent = _subagent_identity(name, tool_input)
    if subagent is not None:
        sub_name, subagent_type, source = subagent
        attribution.started(native_id, sub_name)
        return [
            _ev(
                "subagent.started",
                run_id,
                {
                    "segment_id": native_id,
                    "subagent_id": native_id,
                    "name": sub_name,
                    "description": _str_field(tool_input, "description"),
                    "subagent_type": subagent_type,
                    "source": source,
                },
            )
        ]
    return [
        _ev(
            "tool.invoked",
            run_id,
            {
                "segment_id": native_id,
                "tool_id": native_id,
                "name": name,
                "args": _scalar_args(tool_input),
            },
        )
    ]


def _from_tool_end(
    event: StreamEvent,
    attribution: SubagentAttribution,
    run_id: str,
    native_id: str,
    *,
    is_error: bool,
) -> list[AgentEvent]:
    name = event["name"]
    tool_input = _tool_input(event)
    if name == TODO_TOOL_NAME:
        return []
    subagent = _subagent_identity(name, tool_input)
    if subagent is not None:
        sub_name, subagent_type, source = subagent
        attribution.finished(sub_name)
        return [
            _ev(
                "subagent.finished",
                run_id,
                {
                    "segment_id": native_id,
                    "subagent_id": native_id,
                    "name": sub_name,
                    "subagent_type": subagent_type,
                    "source": source,
                },
            )
        ]
    result = _truncated(_result_text(event, is_error=is_error))
    return [
        _ev(
            "tool.returned",
            run_id,
            {
                "segment_id": native_id,
                "tool_id": native_id,
                "name": name,
                "result": result,
                "is_error": is_error,
                # R1 阶段恒 False：HITL reject 语义在后续 R-approval 轮落地。
                "rejected": False,
            },
        )
    ]


def _subagent_identity(name: str, tool_input: Mapping[object, object]) -> tuple[str, str, str] | None:
    if name == SUBAGENT_TOOL_NAME:
        subagent_type = _str_field(tool_input, "subagent_type") or "subagent"
        return subagent_type, subagent_type, subagent_source_for(subagent_type)
    if name == RUNTIME_SUBAGENT_TOOL_NAME:
        runtime_name = _str_field(tool_input, "name") or "runtime-subagent"
        return runtime_name, runtime_name, "runtime-custom"
    return None


def _scalar_args(tool_input: Mapping[object, object]) -> dict[str, JsonValue]:
    # 仅 JSON 原生标量进入 args，复杂值在边界丢弃。
    args: dict[str, JsonValue] = {}
    for key, value in tool_input.items():
        if isinstance(key, str) and (value is None or isinstance(value, (str, int, float, bool))):
            args[key] = value
    return args


def _todos(tool_input: Mapping[object, object]) -> list[JsonValue]:
    raw: object = tool_input.get("todos")
    if not _is_object_list(raw):
        return []
    todos: list[JsonValue] = []
    for todo in raw:
        item: Mapping[object, object] = todo if _as_mapping(todo) else {}
        content: object = item.get("content")
        status: object = item.get("status")
        if isinstance(content, str) and status in ("pending", "in_progress", "completed"):
            todos.append({"content": content, "status": status})
    return todos


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _result_text(event: StreamEvent, *, is_error: bool) -> str:
    key = "error" if is_error else "output"
    value: object = _data(event).get(key)
    if isinstance(value, BaseMessage):
        return str(value.text)
    if isinstance(value, BaseException):
        return str(value) or type(value).__name__
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    return str(value)


def _truncated(result: str) -> str:
    if len(result) <= TOOL_RESULT_MAX_CHARS:
        return result
    return f"{result[:TOOL_RESULT_MAX_CHARS]}…（结果过长，事件流中已在 {TOOL_RESULT_MAX_CHARS} 字符处截断）"
