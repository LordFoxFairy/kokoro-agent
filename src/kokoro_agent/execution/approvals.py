"""HITL 投影：把暂停中的工具审批请求转成对外事件。"""

from __future__ import annotations

from collections.abc import Sequence

from langchain_core.messages import AIMessage, BaseMessage
from pydantic import BaseModel, ConfigDict

from kokoro_agent.run.json_payload import JsonObject
from kokoro_agent.run.events import AgentEvent, DecisionType, ToolAwaitingData
from kokoro_agent.tools.names import ASK_USER_TOOL_NAME


class ApprovalRequest(BaseModel):
    """应用层审批请求：只保留 Kokoro 事件需要的字段。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    args: JsonObject
    description: str
    allowed_decisions: list[DecisionType]


class _ApprovalAction(BaseModel):
    """HITL action_request 的窄解析模型。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    args: JsonObject
    description: str


class _ApprovalReviewConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    action_name: str
    allowed_decisions: list[DecisionType]


class _ApprovalInterrupt(BaseModel):
    """LangGraph interrupt.value 中 HumanInTheLoopMiddleware 写入的结构。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    action_requests: list[_ApprovalAction]
    review_configs: list[_ApprovalReviewConfig]


def tool_approval_requests(raw_interrupt_values: Sequence[object]) -> list[ApprovalRequest]:
    requests: list[ApprovalRequest] = []
    for raw in raw_interrupt_values:
        payload = _ApprovalInterrupt.model_validate(raw)
        config_by_name = {config.action_name: config for config in payload.review_configs}
        reviewed_names = set(config_by_name)
        missing = [request.name for request in payload.action_requests if request.name not in reviewed_names]
        if missing:
            raise ValueError(f"HITL review_configs missing action names: {sorted(missing)}")
        requests.extend(
            ApprovalRequest(
                name=request.name,
                args=request.args,
                description=request.description,
                allowed_decisions=config_by_name[request.name].allowed_decisions,
            )
            for request in payload.action_requests
        )
    return requests


def tool_approval_events(
    messages: Sequence[BaseMessage],
    approval_requests: Sequence[ApprovalRequest],
    approval_tool_names: frozenset[str],
    *,
    request_id: str,
) -> list[AgentEvent]:
    last_ai = next((m for m in reversed(messages) if isinstance(m, AIMessage)), None)
    if last_ai is None:
        return []
    # segment_id 取触发 interrupt 的那条 AIMessage id，与该段 text_chunk 同源对齐。
    segment_id = last_ai.id or ""
    # HITL 命中子序列：按需审批工具名过滤 tool_calls，与审批请求同序对齐。
    pending = [tc for tc in last_ai.tool_calls if tc["name"] in approval_tool_names]
    if len(pending) != len(approval_requests):
        raise ValueError(
            f"HITL 审批对齐失配: pending tool_calls={len(pending)} != "
            f"approval_requests={len(approval_requests)} (names={sorted(approval_tool_names)})"
        )
    if not pending:
        return []
    # 逐工具发顶层 tool_call_awaiting，与 tool_call_start/end 同层同 granularity（不再打包 pending 数组）。
    events: list[AgentEvent] = []
    for tool_call, request in zip(pending, approval_requests, strict=True):
        data: ToolAwaitingData = {
            "segment_id": segment_id,
            "tool_id": tool_call["id"] or "",
            "name": request.name,
            "args": dict(request.args),
            "description": request.description,
            "allowed_decisions": request.allowed_decisions,
            "kind": "ask_user" if request.name == ASK_USER_TOOL_NAME else "tool_approval",
            "editable": "edit" in request.allowed_decisions,
        }
        events.append(
            AgentEvent.model_validate(
                {"event": "tool_call_awaiting", "request_id": request_id, "data": data}
            )
        )
    return events
