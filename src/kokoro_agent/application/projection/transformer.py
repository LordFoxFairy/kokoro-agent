"""ACL 投影映射：把 v3 typed projection 元素映射为对外 AgentEvent，归属取自结构而非状态。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeGuard

from langchain_core.messages import AIMessage
from pydantic import JsonValue, TypeAdapter, ValidationError

from kokoro_agent.application.protocols.agent import SubagentInfo, ToolCallInfo
from kokoro_agent.domain.registered_subagent import SubagentSource
from kokoro_agent.infrastructure.constants import (
    RUNTIME_SUBAGENT_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
)
from kokoro_agent.infrastructure.subagent.specs import subagent_source_for
from kokoro_agent.interfaces.envelope import AgentEvent

TOOL_RESULT_MAX_CHARS = 8_000
SUBAGENT_LAUNCH_NAMES = frozenset({SUBAGENT_TOOL_NAME, RUNTIME_SUBAGENT_TOOL_NAME})

_JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
_USAGE_KEYS = ("input_tokens", "output_tokens", "total_tokens")


def _ev(event: str, request_id: str, data: dict[str, JsonValue]) -> AgentEvent:
    return AgentEvent.model_validate({"event": event, "request_id": request_id, "data": data})


def _wash(value: object) -> JsonValue | None:
    # 经 JsonValue 校验洗成 wire 安全载荷；非 JSON 块(如携带 bytes 的多模态块)返回 None 跳过。
    try:
        return _JSON.validate_python(value)
    except ValidationError:
        return None



def stream_text_event(
    block: Mapping[str, object], *, segment_id: str, request_id: str, subagent_id: str | None
) -> AgentEvent | None:
    # v3 模型流的增量分块：仅 content-block-delta 携带可显示增量；tool_call 块走 tool_call_* 不混入文本。
    if block.get("event") != "content-block-delta":
        return None
    raw = block.get("delta")
    if _is_tool_block(raw):
        return None
    delta = _wash(raw)
    if delta is None:
        return None
    return _ev("text_chunk", request_id, _text_data(segment_id, [delta], subagent_id, final=False))


def final_text_event(
    message: AIMessage | None, *, segment_id: str, request_id: str, subagent_id: str | None
) -> AgentEvent | None:
    # 段终态：透传 output_message 规范多模态 content_blocks（剔除 tool_call 块）；无可显示内容则不发。
    blocks = message.content_blocks if message is not None else []
    content: list[JsonValue] = [
        washed
        for block in blocks
        if not _is_tool_block(block) and (washed := _wash(block)) is not None
    ]
    if not content:
        return None
    return _ev("text_chunk", request_id, _text_data(segment_id, content, subagent_id, final=True))


def usage_delta(message: AIMessage | None) -> dict[str, int]:
    # 守则C：从模型流终态消息抽 token 增量，供 invoke 聚合进 agent_done。
    usage = message.usage_metadata if message is not None else None
    if usage is None:
        return {}
    return {key: value for key in _USAGE_KEYS if isinstance(value := usage.get(key), int)}


def todo_event(tc: ToolCallInfo, *, request_id: str) -> AgentEvent:
    return _ev(
        "agent_status",
        request_id,
        {"status": "todo_updated", "segment_id": tc.tool_call_id, "todos": _todos(tc.input)},
    )


def tool_start_event(tc: ToolCallInfo, *, request_id: str) -> AgentEvent:
    return _ev(
        "tool_call_start",
        request_id,
        {
            "segment_id": tc.tool_call_id,
            "tool_id": tc.tool_call_id,
            "name": tc.tool_name,
            "args": _scalar_args(tc.input),
        },
    )


def tool_end_event(tc: ToolCallInfo, *, request_id: str) -> AgentEvent:
    is_error = tc.error is not None
    return _ev(
        "tool_call_end",
        request_id,
        {
            "segment_id": tc.tool_call_id,
            "tool_id": tc.tool_call_id,
            "name": tc.tool_name,
            "result": _truncate(_result_text(tc)),
            "is_error": is_error,
            # HITL reject 语义后续接入；自然返回恒 False。
            "rejected": False,
        },
    )


def subagent_started_event(sub: SubagentInfo, *, request_id: str) -> AgentEvent:
    name = sub.name or "subagent"
    return _ev(
        "agent_status",
        request_id,
        {
            "status": "subagent_started",
            "segment_id": sub.trigger_call_id or "",
            "subagent_id": sub.trigger_call_id or "",
            "name": name,
            "description": sub.task_input or "",
            "subagent_type": name,
            "source": _source_for(name),
        },
    )


def subagent_finished_event(sub: SubagentInfo, *, request_id: str) -> AgentEvent:
    name = sub.name or "subagent"
    return _ev(
        "agent_status",
        request_id,
        {
            "status": "subagent_finished",
            "segment_id": sub.trigger_call_id or "",
            "subagent_id": sub.trigger_call_id or "",
            "name": name,
            "subagent_type": name,
            "source": _source_for(name),
        },
    )


def custom_event(payload: object, *, request_id: str) -> AgentEvent | None:
    # 守则D：get_stream_writer() 派发的纯业务事件，洗净后挂 agent_status.data.custom 透传。
    washed = _wash(payload)
    if washed is None:
        return None
    return _ev("agent_status", request_id, {"status": "custom", "custom": washed})


def _text_data(
    segment_id: str, content: list[JsonValue], subagent_id: str | None, *, final: bool
) -> dict[str, JsonValue]:
    data: dict[str, JsonValue] = {"segment_id": segment_id, "content": content, "final": final}
    if subagent_id is not None:
        data["subagent_id"] = subagent_id
    return data


def _source_for(name: str) -> SubagentSource:
    # 未在内建/env catalog 的名即运行时注册 → runtime-custom（catalog 对未知名抛 ValueError）。
    try:
        return subagent_source_for(name)
    except ValueError:
        return "runtime-custom"


def _scalar_args(tool_input: Mapping[str, object] | None) -> dict[str, JsonValue]:
    # 仅 JSON 原生标量进入 args，复杂值在边界丢弃。
    args: dict[str, JsonValue] = {}
    for key, value in (tool_input or {}).items():
        if value is None or isinstance(value, (str, int, float, bool)):
            args[key] = value
    return args


def _todos(tool_input: Mapping[str, object] | None) -> list[JsonValue]:
    raw: object = (tool_input or {}).get("todos")
    if not _is_list(raw):
        return []
    todos: list[JsonValue] = []
    for todo in raw:
        if not _is_mapping(todo):
            continue
        content: object = todo.get("content")
        status: object = todo.get("status")
        if isinstance(content, str) and status in ("pending", "in_progress", "completed"):
            todos.append({"content": content, "status": status})
    return todos


def _is_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _is_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _is_tool_block(block: object) -> bool:
    # 内容块/增量中的工具调用类型（tool_call / tool_call_chunk）由 tool_call_* 事件承载，不进文本。
    return _is_mapping(block) and isinstance(kind := block.get("type"), str) and "tool_call" in kind


def _result_text(tc: ToolCallInfo) -> str:
    if tc.error is not None:
        return tc.error
    output = tc.output
    if output is None:
        return ""
    text = getattr(output, "text", None)
    return text if isinstance(text, str) else str(output)


def _truncate(result: str) -> str:
    if len(result) <= TOOL_RESULT_MAX_CHARS:
        return result
    return f"{result[:TOOL_RESULT_MAX_CHARS]}…（结果过长，事件流中已在 {TOOL_RESULT_MAX_CHARS} 字符处截断）"
