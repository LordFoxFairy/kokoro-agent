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
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME

_DEFAULT_REJECT_MESSAGE = "rejected by user"
_REVIEW_KEY = "kokoro_result_review"
_REVIEW_DECISIONS: tuple[AllowedDecision, ...] = ("approve", "respond", "reject")


@dataclass(frozen=True)
class PendingFrame:
    """同帧完整待批集合：segment 归属 + (tool_id, name) 按 interrupt 顺序。"""

    segment_id: str
    tools: tuple[tuple[str, str], ...]

    @property
    def tool_ids(self) -> list[str]:
        return [tool_id for tool_id, _name in self.tools]


class ReviewEntry(BaseModel):
    """ToolResultReviewMiddleware interrupt 载荷：结果审核暂停的应用视图。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    tool_id: str
    name: str
    args: dict[str, JsonValue]
    result: str
    is_error: bool


def review_entries(interrupts: tuple[Interrupt, ...]) -> list[ReviewEntry] | None:
    """全帧 review 形状则返回条目；全帧 HIL 形状返回 None；混帧/多 review fail-loud。"""
    # Interrupt.value 是框架 Any 边界：先按形状分拣，再交 Pydantic 洗净。
    values: list[Any] = [i.value for i in interrupts]
    shaped = [isinstance(v, dict) and _REVIEW_KEY in v for v in values]
    if not any(shaped):
        return None
    if not all(shaped):
        raise ValueError("mixed approval/review interrupts in one frame is unsupported (V1)")
    if len(interrupts) != 1:
        raise ValueError("multiple result-review interrupts in one frame is unsupported (V1)")
    return [ReviewEntry.model_validate(values[0][_REVIEW_KEY])]


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


def review_frame(snapshot: StateView, entries: Sequence[ReviewEntry]) -> PendingFrame:
    # 审核帧归属段=触发帧的 AIMessage（与审批帧同源）；条目顺序即 interrupt 顺序。
    raw: Any = snapshot.values.get("messages") or []
    last_ai = next((m for m in reversed(raw) if isinstance(m, AIMessage)), None)
    segment = (last_ai.id or "") if last_ai is not None else ""
    return PendingFrame(segment, tuple((e.tool_id, e.name) for e in entries))


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
    entries = review_entries(snapshot.interrupts)
    if entries is not None:
        frame = review_frame(snapshot, entries)
        pending_ids = frame.tool_ids
        return [
            ToolAwaitingApprovalPayload(
                segment_id=frame.segment_id,
                tool_id=entry.tool_id,
                name=entry.name,
                args=entry.args,
                description=f"Tool result awaiting review: {entry.name}",
                allowed_decisions=list(_REVIEW_DECISIONS),
                kind="result_review",
                editable=False,
                pending_tool_ids=pending_ids,
                result=entry.result,
            )
            for entry in entries
        ]
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
                kind="ask_user_question" if request.name == ASK_USER_TOOL_NAME else "tool_approval",
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


def align_review_decisions(
    decisions: Sequence[ResumeDecision], frame: PendingFrame
) -> list[ResumeDecision]:
    # 与审批帧同一对齐纪律（缺/多/重复/未知 fail-loud）；决策集为 approve/respond/reject，
    # respond=人工替换结果（不绑 ask_user），edit 对已执行结果无意义。
    by_id: dict[str, ResumeDecision] = {d.tool_id: d for d in decisions}
    if len(by_id) != len(decisions):
        raise ValueError("resume decisions contain duplicate tool_id")
    if set(by_id) != set(frame.tool_ids):
        raise ValueError(
            f"resume decisions {sorted(by_id)} != pending review tools {sorted(frame.tool_ids)}"
        )
    for decision in decisions:
        if decision.type not in _REVIEW_DECISIONS:
            raise ValueError(
                f"decision {decision.type!r} not allowed for result review tool {decision.tool_id!r}"
            )
    return [by_id[tool_id] for tool_id in frame.tool_ids]


def review_resume_value(decisions: Sequence[ResumeDecision]) -> list[dict[str, JsonValue]]:
    # ToolResultReviewMiddleware 的 resume 契约：list[decision dict]，按 tool_id 自取。
    out: list[dict[str, JsonValue]] = []
    for decision in decisions:
        if decision.type == "approve":
            out.append({"tool_id": decision.tool_id, "type": "approve"})
        elif decision.type == "respond":
            out.append(
                {"tool_id": decision.tool_id, "type": "respond", "response": decision.response}
            )
        elif decision.type == "reject":
            out.append(
                {
                    "tool_id": decision.tool_id,
                    "type": "reject",
                    "reason": decision.reason or _DEFAULT_REJECT_MESSAGE,
                }
            )
        else:
            raise ValueError(f"decision {decision.type!r} not allowed for result review")
    return out


def review_resolution_payloads(
    decisions: Sequence[ResumeDecision],
    frame: PendingFrame,
    results: dict[str, tuple[str, bool]],
) -> list[ToolReturnedPayload]:
    # 审核工具的 returned 一律裁决后直发（投影侧被抑制）：approve=放行原结果，
    # respond=人工替换（responded 标记），reject=废弃（rejected 标记）。
    name_by_id = dict(frame.tools)
    payloads: list[ToolReturnedPayload] = []
    for decision in decisions:
        tool_id = decision.tool_id
        if decision.type == "approve":
            cached = results.get(tool_id)
            if cached is None:
                raise ValueError(f"no cached result for reviewed tool {tool_id!r}")
            payloads.append(
                ToolReturnedPayload(
                    segment_id=frame.segment_id,
                    tool_id=tool_id,
                    name=name_by_id[tool_id],
                    result=cached[0],
                    is_error=cached[1],
                    responded=True,
                )
            )
        elif decision.type == "respond":
            payloads.append(
                ToolReturnedPayload(
                    segment_id=frame.segment_id,
                    tool_id=tool_id,
                    name=name_by_id[tool_id],
                    result=decision.response,
                    is_error=False,
                    responded=True,
                )
            )
        elif decision.type == "reject":
            reason = decision.reason or _DEFAULT_REJECT_MESSAGE
            payloads.append(
                ToolReturnedPayload(
                    segment_id=frame.segment_id,
                    tool_id=tool_id,
                    name=name_by_id[tool_id],
                    result=reason,
                    is_error=False,
                    rejected=True,
                    reject_reason=reason,
                )
            )
    return payloads


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
