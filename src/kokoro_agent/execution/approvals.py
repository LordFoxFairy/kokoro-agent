"""HITL 权威唯一实现：pending 集合、awaiting 事件、resume fail-loud 对齐、快照直发终态。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import AIMessage
from langgraph.types import Interrupt
from pydantic import BaseModel, ConfigDict, JsonValue

from kokoro_agent.contract import (
    AllowedDecision,
    ResumeDecision,
    ToolAwaitingApprovalPayload,
    ToolReturnedPayload,
)
from kokoro_agent.execution.protocols import StateView
from kokoro_agent.tools.names import ASK_USER_TOOL_NAME

_DEFAULT_REJECT_MESSAGE = "rejected by user"


@dataclass(frozen=True)
class PendingFrame:
    """同帧完整待批集合：segment 归属 + (tool_id, name) 按 interrupt 顺序。"""

    segment_id: str
    tools: tuple[tuple[str, str], ...]

    @property
    def tool_ids(self) -> list[str]:
        return [tool_id for tool_id, _name in self.tools]


def has_pending_interrupt(snapshot: StateView) -> bool:
    # StateSnapshot.interrupts 是 typed tuple：非空即有待审批暂停。
    return bool(snapshot.interrupts)


def pending_frame(snapshot: StateView, approval_tool_names: frozenset[str]) -> PendingFrame:
    # 触发 HITL 的 AIMessage 中命中审批工具名的子序列（与 langgraph HITL 同序）——全仓唯一实现。
    # LangGraph state values 为 Any 框架边界：messages 在此一次过滤为 typed AIMessage。
    raw: Any = snapshot.values.get("messages") or []
    last_ai = next((m for m in reversed(raw) if isinstance(m, AIMessage)), None)
    if last_ai is None:
        return PendingFrame("", ())
    tools = tuple(
        (tc["id"] or "", tc["name"]) for tc in last_ai.tool_calls if tc["name"] in approval_tool_names
    )
    return PendingFrame(last_ai.id or "", tools)


class ApprovalRequest(BaseModel):
    """审批请求的应用视图：只保留 wire 事件需要的字段。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    args: dict[str, JsonValue]
    description: str
    allowed_decisions: list[AllowedDecision]


class _ApprovalAction(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    args: dict[str, JsonValue]
    description: str


class _ApprovalReviewConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    action_name: str
    allowed_decisions: list[AllowedDecision]


class _ApprovalInterrupt(BaseModel):
    """LangGraph interrupt.value 中 HumanInTheLoopMiddleware 写入的结构。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    action_requests: list[_ApprovalAction]
    review_configs: list[_ApprovalReviewConfig]


def approval_requests(interrupts: tuple[Interrupt, ...]) -> list[ApprovalRequest]:
    requests: list[ApprovalRequest] = []
    for interrupt in interrupts:
        payload = _ApprovalInterrupt.model_validate(interrupt.value)
        config_by_name = {config.action_name: config for config in payload.review_configs}
        missing = [req.name for req in payload.action_requests if req.name not in config_by_name]
        if missing:
            raise ValueError(f"HITL review_configs missing action names: {sorted(missing)}")
        requests.extend(
            ApprovalRequest(
                name=req.name,
                args=req.args,
                description=req.description,
                allowed_decisions=config_by_name[req.name].allowed_decisions,
            )
            for req in payload.action_requests
        )
    return requests


def awaiting_payloads(
    snapshot: StateView, approval_tool_names: frozenset[str]
) -> list[ToolAwaitingApprovalPayload]:
    frame = pending_frame(snapshot, approval_tool_names)
    requests = approval_requests(snapshot.interrupts)
    # 对齐失配即 invariant 破裂：宁可 fail-loud 收口为 run.failed，不发错帧。
    if len(frame.tools) != len(requests):
        raise ValueError(
            f"HITL alignment mismatch: pending tool_calls={len(frame.tools)} != "
            f"approval_requests={len(requests)} (names={sorted(approval_tool_names)})"
        )
    pending_ids = frame.tool_ids
    payloads: list[ToolAwaitingApprovalPayload] = []
    for (tool_id, _name), request in zip(frame.tools, requests, strict=True):
        payloads.append(
            ToolAwaitingApprovalPayload(
                segment_id=frame.segment_id,
                tool_id=tool_id,
                name=request.name,
                args=request.args,
                description=request.description,
                allowed_decisions=request.allowed_decisions,
                kind="ask_user" if request.name == ASK_USER_TOOL_NAME else "tool_approval",
                editable="edit" in request.allowed_decisions,
                # 同帧完整待批集合进契约：web 暂存逻辑读契约字段而非内嵌 agent 算法。
                pending_tool_ids=pending_ids,
            )
        )
    return payloads


def align_decisions(
    decisions: Sequence[ResumeDecision], frame: PendingFrame
) -> list[ResumeDecision]:
    # 按 tool_id 重排到 pending 顺序（langgraph 按序匹配 decisions↔interrupt）；
    # 缺/多/重复/未知 tool_id 一律 fail-loud。
    by_id: dict[str, ResumeDecision] = {d.tool_id: d for d in decisions}
    if len(by_id) != len(decisions):
        raise ValueError("resume decisions contain duplicate tool_id")
    if set(by_id) != set(frame.tool_ids):
        raise ValueError(
            f"resume decisions {sorted(by_id)} != pending tools {sorted(frame.tool_ids)}"
        )
    name_by_id = dict(frame.tools)
    for decision in decisions:
        is_ask_user = name_by_id[decision.tool_id] == ASK_USER_TOOL_NAME
        # respond 是 ask_user 专属人工作答；普通审批工具只接 approve/edit/reject。双向越界即 fail-loud。
        if decision.type == "respond" and not is_ask_user:
            raise ValueError(f"respond decision not allowed for tool {decision.tool_id!r}")
        if decision.type != "respond" and is_ask_user:
            raise ValueError(f"ask_user tool {decision.tool_id!r} accepts only respond")
    return [by_id[tool_id] for tool_id in frame.tool_ids]


def resume_command_decisions(
    decisions: Sequence[ResumeDecision], frame: PendingFrame
) -> list[dict[str, JsonValue]]:
    # langgraph Decision 不含 tool_id（按序匹配）；契约词汇在此翻译为框架词汇。
    name_by_id = dict(frame.tools)
    out: list[dict[str, JsonValue]] = []
    for decision in decisions:
        if decision.type == "approve":
            if decision.args is None:
                out.append({"type": "approve"})
            else:
                # 带 args 的 approve 语义=按给定参数放行：框架侧即 edit。
                out.append(
                    {
                        "type": "edit",
                        "edited_action": {"name": name_by_id[decision.tool_id], "args": decision.args},
                    }
                )
        elif decision.type == "edit":
            out.append(
                {
                    "type": "edit",
                    "edited_action": {"name": name_by_id[decision.tool_id], "args": decision.args},
                }
            )
        elif decision.type == "reject":
            out.append({"type": "reject", "message": decision.reason or _DEFAULT_REJECT_MESSAGE})
        else:
            out.append({"type": "respond", "message": decision.response})
    return out


def resolution_payloads(
    decisions: Sequence[ResumeDecision], frame: PendingFrame
) -> list[ToolReturnedPayload]:
    # reject/respond 生成 synthetic ToolMessage 跳过 tool 节点 → 工具不经 v3 projection 浮现，
    # 由 resume 据快照+decision 直发 tool.returned（replay 安全）。
    name_by_id = dict(frame.tools)
    payloads: list[ToolReturnedPayload] = []
    for decision in decisions:
        if decision.type == "reject":
            reason = decision.reason or _DEFAULT_REJECT_MESSAGE
            payloads.append(
                ToolReturnedPayload(
                    segment_id=frame.segment_id,
                    tool_id=decision.tool_id,
                    name=name_by_id[decision.tool_id],
                    result=reason,
                    is_error=False,
                    rejected=True,
                    reject_reason=reason,
                )
            )
        elif decision.type == "respond":
            # respond=人工答复：done 态但带 responded 标记，回看者知结果是人填非工具产出。
            payloads.append(
                ToolReturnedPayload(
                    segment_id=frame.segment_id,
                    tool_id=decision.tool_id,
                    name=name_by_id[decision.tool_id],
                    result=decision.response,
                    is_error=False,
                    responded=True,
                )
            )
    return payloads
