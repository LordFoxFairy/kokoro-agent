"""HITL 暂停态投影：把 typed ActionRequest 按序对齐 AIMessage.tool_calls 取 canonical tool_id。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from langchain.agents.middleware.human_in_the_loop import ActionRequest
from langchain_core.messages import AIMessage, BaseMessage
from pydantic import JsonValue

from kokoro_agent.interfaces.envelope import AgentEvent


def awaiting_approval_events(
    messages: Sequence[BaseMessage],
    action_requests: Sequence[ActionRequest],
    interrupt_on_names: frozenset[str],
    *,
    request_id: str,
) -> list[AgentEvent]:
    last_ai = next((m for m in reversed(messages) if isinstance(m, AIMessage)), None)
    if last_ai is None:
        return []
    # segment_id 取触发 interrupt 的那条 AIMessage id，与该段 text_chunk 同源对齐。
    segment_id = last_ai.id or ""
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
            "name": request["name"],
            "args": _scalar_args(request["args"]),
        }
        for tool_call, request in zip(pending, action_requests, strict=True)
    ]
    return [
        AgentEvent.model_validate(
            {
                "event": "agent_status",
                "request_id": request_id,
                "data": {"status": "awaiting_approval", "segment_id": segment_id, "pending": items},
            }
        )
    ]


def _scalar_args(args: dict[str, Any]) -> dict[str, JsonValue]:
    # 仅 JSON 原生标量进入 args，复杂值在 wire 边界丢弃（对齐 transformer._scalar_args）。
    return {
        key: value
        for key, value in args.items()
        if value is None or isinstance(value, (str, int, float, bool))
    }
