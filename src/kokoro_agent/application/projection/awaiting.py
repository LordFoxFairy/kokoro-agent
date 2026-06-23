"""HITL 暂停态投影：把 interrupt action_requests 对齐 AIMessage.tool_calls 取 tool_id。"""

from collections.abc import Mapping, Sequence
from typing import TypeGuard

from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.messages.tool import ToolCall
from pydantic import JsonValue

from kokoro_agent.interfaces.envelope import AgentEvent


def _is_object_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    # 未类型化值收口到 Mapping[object, object]，避免裸 isinstance 收窄成 Unknown。
    return isinstance(value, Mapping)


def awaiting_approval_events(
    messages: Sequence[BaseMessage],
    action_requests: Sequence[object],
    interrupt_on_names: frozenset[str],
    *,
    segment_id: str,
    request_id: str,
) -> list[AgentEvent]:
    last_ai = _last_ai_message(messages)
    if last_ai is None:
        return []
    # interrupt 命中子序列：按 interrupt_on 名集过滤 tool_calls，与 action_requests 同序对齐。
    pending = [tc for tc in last_ai.tool_calls if tc["name"] in interrupt_on_names]
    # 长度不等即 wiring bug：fail-loud 抛错，绝不静默截断丢审批信号。
    if len(pending) != len(action_requests):
        raise ValueError(
            f"awaiting 对齐失配: pending tool_calls={len(pending)} != "
            f"action_requests={len(action_requests)} (names={sorted(interrupt_on_names)})"
        )
    if not pending:
        return []
    items: list[JsonValue] = [
        {
            "tool_id": tool_call["id"] or "",
            "name": tool_call["name"],
            "args": _scalar_args(_request_args(request, tool_call)),
        }
        for tool_call, request in zip(pending, action_requests, strict=True)
    ]
    return [
        AgentEvent.model_validate(
            {
                "event": "agent_status",
                "request_id": request_id,
                "data": {
                    "status": "awaiting_approval",
                    "segment_id": segment_id,
                    "pending": items,
                },
            }
        )
    ]


def _last_ai_message(messages: Sequence[BaseMessage]) -> AIMessage | None:
    for message in reversed(messages):
        if isinstance(message, AIMessage):
            return message
    return None


def _request_args(request: object, tool_call: ToolCall) -> Mapping[object, object]:
    # action_request.args 缺失时回落到对齐的 tool_call.args（同序子序列保证语义一致）。
    if _is_object_mapping(request):
        args = request.get("args")
        if _is_object_mapping(args):
            return args
    raw_args: object = tool_call["args"]
    return raw_args if _is_object_mapping(raw_args) else {}


def _scalar_args(source: Mapping[object, object]) -> dict[str, JsonValue]:
    # 仅 JSON 原生标量进入 args，复杂值在边界丢弃（对齐 transformer._scalar_args）。
    args: dict[str, JsonValue] = {}
    for key, value in source.items():
        if isinstance(key, str) and (value is None or isinstance(value, (str, int, float, bool))):
            args[key] = value
    return args
