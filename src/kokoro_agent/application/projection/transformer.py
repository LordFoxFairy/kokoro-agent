"""唯一投影(ACL 流转换器)：把单个 LangChain StreamEvent 映射为 0+ 对外 AgentEvent。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeGuard

from langchain_core.messages import BaseMessage
from langchain_core.runnables.schema import StreamEvent
from pydantic import JsonValue, TypeAdapter

from kokoro_agent.application.projection.attribution import SubagentAttribution
from kokoro_agent.infrastructure.constants import (
    RUNTIME_SUBAGENT_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
    TODO_TOOL_NAME,
)
from kokoro_agent.infrastructure.subagent.specs import subagent_source_for
from kokoro_agent.interfaces.envelope import AgentEvent

TOOL_RESULT_MAX_CHARS = 8_000

# content_blocks 原生即 list[dict]，经 JsonValue 校验洗成 wire 安全载荷后透传，零结构改写。
_BLOCKS_ADAPTER: TypeAdapter[list[JsonValue]] = TypeAdapter(list[JsonValue])
_USAGE_KEYS = ("input_tokens", "output_tokens", "total_tokens")


def project(
    event: StreamEvent, attribution: SubagentAttribution, request_id: str
) -> list[AgentEvent]:
    # native_id 为 LC 每-Runnable 随机 id，仅作 segment_id/tool_id；信封 request_id 是 kokoro run_id。
    native_id = event["run_id"]
    match event["event"]:
        case "on_chat_model_stream":
            return _from_message(event, attribution, request_id, native_id, final=False)
        case "on_chat_model_end":
            return _from_message(event, attribution, request_id, native_id, final=True)
        case "on_tool_start":
            return _from_tool_start(event, attribution, request_id, native_id)
        case "on_tool_end":
            return _from_tool_end(event, attribution, request_id, native_id, is_error=False)
        case "on_tool_error":
            return _from_tool_end(event, attribution, request_id, native_id, is_error=True)
        case _:
            return []


def usage_delta(event: StreamEvent) -> dict[str, int]:
    # 守则 C：从 on_chat_model_end 的 usage_metadata 抽 token 增量，供 invoke 聚合进 agent_done。
    if event["event"] != "on_chat_model_end":
        return {}
    message: object = _data(event).get("output")
    if not isinstance(message, BaseMessage):
        return {}
    usage: object = getattr(message, "usage_metadata", None)
    if not _as_mapping(usage):
        return {}
    return {key: value for key in _USAGE_KEYS if isinstance(value := usage.get(key), int)}


def _ev(event: str, request_id: str, data: dict[str, JsonValue]) -> AgentEvent:
    return AgentEvent.model_validate({"event": event, "request_id": request_id, "data": data})


def _as_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _data(event: StreamEvent) -> Mapping[object, object]:
    # StreamEvent.data 声明为无泛型 dict；先收口到 object 再 isinstance。
    raw: object = event.get("data")
    return raw if _as_mapping(raw) else {}


def _from_message(
    event: StreamEvent,
    attribution: SubagentAttribution,
    request_id: str,
    native_id: str,
    *,
    final: bool,
) -> list[AgentEvent]:
    message: object = _data(event).get("output" if final else "chunk")
    if not isinstance(message, BaseMessage):
        return []
    content = _BLOCKS_ADAPTER.validate_python(message.content_blocks)
    if not content and not final:
        # 空增量块跳过；final 即使空内容也发，标记该 segment 结束。
        return []
    data: dict[str, JsonValue] = {"segment_id": native_id, "content": content, "final": final}
    subagent_id = attribution.active_id(event)
    if subagent_id is not None:
        data["subagent_id"] = subagent_id
    return [_ev("text_chunk", request_id, data)]


def _from_tool_start(
    event: StreamEvent, attribution: SubagentAttribution, request_id: str, native_id: str
) -> list[AgentEvent]:
    name = event["name"]
    tool_input = _tool_input(event)
    if name == TODO_TOOL_NAME:
        return [
            _ev(
                "agent_status",
                request_id,
                {"status": "todo_updated", "segment_id": native_id, "todos": _todos(tool_input)},
            )
        ]
    subagent = _subagent_identity(name, tool_input)
    if subagent is not None:
        sub_name, subagent_type, source = subagent
        attribution.started(native_id, sub_name)
        return [
            _ev(
                "agent_status",
                request_id,
                {
                    "status": "subagent_started",
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
            "tool_call_start",
            request_id,
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
    request_id: str,
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
                "agent_status",
                request_id,
                {
                    "status": "subagent_finished",
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
            "tool_call_end",
            request_id,
            {
                "segment_id": native_id,
                "tool_id": native_id,
                "name": name,
                "result": result,
                "is_error": is_error,
                # HITL reject 语义后续接入；当前自然返回恒 False。
                "rejected": False,
            },
        )
    ]


def _tool_input(event: StreamEvent) -> Mapping[object, object]:
    raw: object = _data(event).get("input")
    return raw if _as_mapping(raw) else {}


def _str_field(source: Mapping[object, object], key: str) -> str:
    value = source.get(key)
    return value if isinstance(value, str) else ""


def _subagent_identity(
    name: str, tool_input: Mapping[object, object]
) -> tuple[str, str, str] | None:
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
    value: object = _data(event).get("error" if is_error else "output")
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
