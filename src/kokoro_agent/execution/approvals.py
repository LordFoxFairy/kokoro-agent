"""HITL 权威唯一实现：pending 集合、awaiting 事件、resume fail-loud 对齐、快照直发终态。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import AIMessage
from langgraph.types import Interrupt
from pydantic import BaseModel, ConfigDict, JsonValue

from kokoro_agent.contract import (
    AllowedDecision,
    RejectDecision,
    ResumeDecision,
    SubmitDecision,
    ToolAwaitingApprovalPayload,
    ToolReturnedPayload,
)
from kokoro_agent.execution.events import clip_result
from kokoro_agent.execution.protocols import StateView
from kokoro_agent.hitl import INPUT_DECISIONS, REVIEW_DECISIONS, HumanRequest
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME

_DEFAULT_REJECT_MESSAGE = "rejected by user"


@dataclass(frozen=True)
class PendingFrame:
    """同帧完整待批集合：segment 归属 + (tool_id, name) 按 interrupt 顺序。

    nested=True：暂停源自子代理内部（deepagents 把 interrupt_on 下发进子图）——
    主图消息里没有对应 tool_call，tool_id 为合成稳定 id（interrupt.id 派生），
    segment 归属触发委派的 task 调用。"""

    segment_id: str
    tools: tuple[tuple[str, str], ...]
    nested: bool = False

    @property
    def tool_ids(self) -> list[str]:
        return [tool_id for tool_id, _name in self.tools]


class ReviewContext(BaseModel):
    """review 预设的 HumanRequest.context 约定字段：结果审核暂停的展示载荷。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    args: dict[str, JsonValue]
    result: str
    is_error: bool


class ReviewEntry(BaseModel):
    """结果审核暂停的应用视图：request_id 归位为 tool_id + context 展开。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    tool_id: str
    name: str
    args: dict[str, JsonValue]
    result: str
    is_error: bool


def review_entries(interrupts: tuple[Interrupt, ...]) -> list[ReviewEntry] | None:
    """全帧 review 预设则返回条目；全帧 approval 形态返回 None；混帧/多 review fail-loud。"""
    # Interrupt.value 是框架 Any 边界：HumanRequest.from_interrupt_value 反解本原语信封
    # （非本信封=langchain approval 形态返回 None，信封在但体不合法 fail-loud），再按 kind 分拣。
    requests = [HumanRequest.from_interrupt_value(i.value) for i in interrupts]
    is_review = [req is not None and req.kind == "review" for req in requests]
    if not any(is_review):
        return None
    if not all(is_review):
        raise ValueError("mixed approval/review interrupts in one frame is unsupported (V1)")
    if len(requests) != 1:
        raise ValueError("multiple result-review interrupts in one frame is unsupported (V1)")
    request = requests[0]
    if request is None:  # is_review[0] 已保证非空；显式收窄给类型检查器。
        raise ValueError("result review interrupt missing human request envelope")
    ctx = ReviewContext.model_validate(request.context)
    return [
        ReviewEntry(
            tool_id=request.request_id,
            name=ctx.name,
            args=ctx.args,
            result=ctx.result,
            is_error=ctx.is_error,
        )
    ]


class InputContext(BaseModel):
    """input 预设的 HumanRequest.context 约定字段：结构化请求的展示载荷。

    name=发起方展示名（如 MCP 工具名）；args=展示细节（elicitation message 等）；
    validation_error=上一次回灌不合法时的重问文案（缺席=首次请求）。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    args: dict[str, JsonValue] = {}
    validation_error: str | None = None


class InputEntry(BaseModel):
    """结构化请求的应用视图：request_id + 展示载荷 + 期待回应的 JSON Schema。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    request_id: str
    name: str
    args: dict[str, JsonValue]
    input_schema: dict[str, JsonValue] | None


def input_entries(interrupts: tuple[Interrupt, ...]) -> list[InputEntry] | None:
    """全帧 input 预设则返回条目；无 input 返回 None（让位审批路径）；混帧 fail-loud。"""
    # 同 review_entries：反解本原语信封按 kind 分拣，input 与其它 kind 混帧属 V1 不支持。
    requests = [HumanRequest.from_interrupt_value(i.value) for i in interrupts]
    is_input = [req is not None and req.kind == "input" for req in requests]
    if not any(is_input):
        return None
    if not all(is_input):
        raise ValueError("mixed input/other interrupts in one frame is unsupported (V1)")
    entries: list[InputEntry] = []
    for req in requests:
        if req is None:  # is_input 已保证非空；显式收窄给类型检查器。
            raise ValueError("input interrupt missing human request envelope")
        ctx = InputContext.model_validate(req.context)
        args = dict(ctx.args)
        if ctx.validation_error is not None:
            # 重问文案随 args 上 wire：web 的 schema 表单据此提示人重填。
            args["validation_error"] = ctx.validation_error
        entries.append(
            InputEntry(
                request_id=req.request_id,
                name=ctx.name,
                args=args,
                input_schema=req.response_schema,
            )
        )
    return entries


def input_frame(snapshot: StateView, entries: Sequence[InputEntry]) -> PendingFrame:
    # input 帧归属段=发起请求的工具调用所在 AIMessage（request_id=该工具 tool_id）；
    # 条目顺序即 interrupt 顺序。
    raw: Any = snapshot.values.get("messages") or []
    last_ai = next((m for m in reversed(raw) if isinstance(m, AIMessage)), None)
    segment = (last_ai.id or "") if last_ai is not None else ""
    return PendingFrame(segment, tuple((e.request_id, e.name) for e in entries))


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
    snapshot: StateView,
    approval_tool_names: frozenset[str],
    describe_tool: Callable[[str], str | None] = lambda _name: None,
) -> list[ToolAwaitingApprovalPayload]:
    # wire 只带数据：description=工具自述（装配侧注入查询）；查不到发空串，
    # 执行侧英文 interrupt 模板是调试语料、永不上 wire（展示文案归 web）。
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
                description=describe_tool(entry.name) or "",
                allowed_decisions=list(REVIEW_DECISIONS),
                kind="result_review",
                editable=False,
                pending_tool_ids=pending_ids,
                # awaiting 帧无 truncated 契约位：只作展示裁剪，完整结果留在缓存。
                result=clip_result(entry.result)[0],
            )
            for entry in entries
        ]
    inputs = input_entries(snapshot.interrupts)
    if inputs is not None:
        iframe = input_frame(snapshot, inputs)
        input_ids = iframe.tool_ids
        return [
            ToolAwaitingApprovalPayload(
                segment_id=iframe.segment_id,
                # 工具边界二者同值：pause 锚 request_id 承在 tool_id 槽（web 据此关联发起工具卡）。
                tool_id=entry.request_id,
                name=entry.name,
                args=entry.args,
                description=describe_tool(entry.name) or "",
                allowed_decisions=list(INPUT_DECISIONS),
                kind="input",
                editable=False,
                # schema 驱动表单的真源：web 按 JSON Schema 自动渲染回应控件。
                input_schema=entry.input_schema,
                pending_tool_ids=input_ids,
            )
            for entry in inputs
        ]
    frame, requests = approval_frame(snapshot, approval_tool_names)
    pending_ids = frame.tool_ids
    payloads: list[ToolAwaitingApprovalPayload] = []
    for (tool_id, _name), request in zip(frame.tools, requests, strict=True):
        payloads.append(
            ToolAwaitingApprovalPayload(
                segment_id=frame.segment_id,
                tool_id=tool_id,
                name=request.name,
                args=request.args,
                description=describe_tool(request.name) or "",
                allowed_decisions=request.allowed_decisions,
                kind="ask_user_question" if request.name == ASK_USER_TOOL_NAME else "tool_approval",
                editable="edit" in request.allowed_decisions,
                # 同帧完整待批集合进契约：web 暂存逻辑读契约字段而非内嵌 agent 算法。
                pending_tool_ids=pending_ids,
            )
        )
    return payloads


def approval_frame(
    snapshot: StateView, approval_tool_names: frozenset[str]
) -> tuple[PendingFrame, list[ApprovalRequest]]:
    """审批帧唯一构造点：主帧优先；主图无对应 tool_call 时回退嵌套帧（子代理内暂停）。"""
    frame = pending_frame(snapshot, approval_tool_names)
    requests = approval_requests(snapshot.interrupts)
    if not frame.tools and requests:
        frame = _nested_frame(snapshot)
    # 对齐失配即 invariant 破裂：宁可 fail-loud 收口为 run.failed，不发错帧。
    if len(frame.tools) != len(requests):
        raise ValueError(
            f"HITL alignment mismatch: pending tool_calls={len(frame.tools)} != "
            f"approval_requests={len(requests)} (names={sorted(approval_tool_names)})"
        )
    return frame, requests


def _nested_frame(snapshot: StateView) -> PendingFrame:
    # 合成 tool_id 必须跨快照重读稳定（resume 重建帧要与已发 awaiting 对齐）：由
    # interrupt.id（langgraph 稳定哈希）+ 序号派生；segment 归属触发委派的 task 调用。
    raw: Any = snapshot.values.get("messages") or []
    last_ai = next((m for m in reversed(raw) if isinstance(m, AIMessage)), None)
    segment_id = ""
    if last_ai is not None:
        task_call = next((tc for tc in last_ai.tool_calls if tc["name"] == SUBAGENT_TOOL_NAME), None)
        segment_id = (task_call["id"] if task_call else None) or last_ai.id or ""
    tools: list[tuple[str, str]] = []
    for interrupt in snapshot.interrupts:
        payload = _ApprovalInterrupt.model_validate(interrupt.value)
        for index, request in enumerate(payload.action_requests):
            tools.append((f"{interrupt.id}:{index}", request.name))
    return PendingFrame(segment_id, tuple(tools), nested=True)


def _decision_id(decision: ResumeDecision) -> str:
    # 帧锚统一取值：submit 用 request_id，其余四型用 tool_id（工具边界场景二者同值）。
    # 审批/审核路径不会收到 submit（走 input 分支），此处仅为消解 union 扩张后的类型歧义。
    if isinstance(decision, SubmitDecision):
        return decision.request_id
    return decision.tool_id


def align_decisions(
    decisions: Sequence[ResumeDecision], frame: PendingFrame
) -> list[ResumeDecision]:
    # 按 tool_id 重排到 pending 顺序（langgraph 按序匹配 decisions↔interrupt）；
    # 缺/多/重复/未知 tool_id 一律 fail-loud。
    by_id: dict[str, ResumeDecision] = {_decision_id(d): d for d in decisions}
    if len(by_id) != len(decisions):
        raise ValueError("resume decisions contain duplicate tool_id")
    if set(by_id) != set(frame.tool_ids):
        raise ValueError(
            f"resume decisions {sorted(by_id)} != pending tools {sorted(frame.tool_ids)}"
        )
    name_by_id = dict(frame.tools)
    for decision in decisions:
        is_ask_user = name_by_id[_decision_id(decision)] == ASK_USER_TOOL_NAME
        # respond 是 ask_user 专属人工作答；普通审批工具只接 approve/edit/reject。双向越界即 fail-loud。
        if decision.type == "respond" and not is_ask_user:
            raise ValueError(f"respond decision not allowed for tool {decision.tool_id!r}")
        if decision.type != "respond" and is_ask_user:
            raise ValueError(f"ask_user tool {_decision_id(decision)!r} accepts only respond")
    return [by_id[tool_id] for tool_id in frame.tool_ids]


def align_review_decisions(
    decisions: Sequence[ResumeDecision], frame: PendingFrame
) -> list[ResumeDecision]:
    # 与审批帧同一对齐纪律（缺/多/重复/未知 fail-loud）；决策集为 approve/respond/reject，
    # respond=人工替换结果（不绑 ask_user），edit 对已执行结果无意义。
    by_id: dict[str, ResumeDecision] = {_decision_id(d): d for d in decisions}
    if len(by_id) != len(decisions):
        raise ValueError("resume decisions contain duplicate tool_id")
    if set(by_id) != set(frame.tool_ids):
        raise ValueError(
            f"resume decisions {sorted(by_id)} != pending review tools {sorted(frame.tool_ids)}"
        )
    for decision in decisions:
        if decision.type not in REVIEW_DECISIONS:
            raise ValueError(
                f"decision {decision.type!r} not allowed for result review tool "
                f"{_decision_id(decision)!r}"
            )
    return [by_id[tool_id] for tool_id in frame.tool_ids]


def align_input_decisions(
    decisions: Sequence[ResumeDecision], frame: PendingFrame
) -> list[ResumeDecision]:
    # 与审批/审核帧同一对齐纪律（缺/多/重复/未知 fail-loud）；决策集为 submit/reject。
    by_id: dict[str, ResumeDecision] = {}
    for decision in decisions:
        if decision.type not in INPUT_DECISIONS:
            raise ValueError(f"decision {decision.type!r} not allowed for input request")
        # submit 用 request_id，reject 沿用 tool_id 槽（工具边界二者同值）。
        by_id[_decision_id(decision)] = decision
    if len(by_id) != len(decisions):
        raise ValueError("resume decisions contain duplicate request_id")
    if set(by_id) != set(frame.tool_ids):
        raise ValueError(
            f"resume decisions {sorted(by_id)} != pending input requests {sorted(frame.tool_ids)}"
        )
    return [by_id[request_id] for request_id in frame.tool_ids]


def submit_resume_value(decisions: Sequence[ResumeDecision]) -> list[dict[str, JsonValue]]:
    # kind=input 的 resume 契约：list[{request_id, type, value?/reason?}]，request_input 按 request_id 自取。
    out: list[dict[str, JsonValue]] = []
    for decision in decisions:
        if isinstance(decision, SubmitDecision):
            out.append(
                {"request_id": decision.request_id, "type": "submit", "value": decision.value}
            )
        elif isinstance(decision, RejectDecision):
            out.append(
                {"request_id": decision.tool_id, "type": "reject", "reason": decision.reason}
            )
        else:
            raise ValueError(f"decision {decision.type!r} not allowed for input request")
    return out


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
        tool_id = _decision_id(decision)
        if decision.type == "approve":
            cached = results.get(tool_id)
            if cached is None:
                raise ValueError(f"no cached result for reviewed tool {tool_id!r}")
            result, truncated = clip_result(cached[0])
            payloads.append(
                ToolReturnedPayload(
                    segment_id=frame.segment_id,
                    tool_id=tool_id,
                    name=name_by_id[tool_id],
                    result=result,
                    is_error=cached[1],
                    truncated=True if truncated else None,
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


def nested_approved_payloads(
    decisions: Sequence[ResumeDecision], frame: PendingFrame
) -> list[ToolReturnedPayload]:
    """嵌套帧的 approve/edit 收尾直发：子代理内工具无投影通道，占位文案闭合审批卡。"""
    name_by_id = dict(frame.tools)
    return [
        ToolReturnedPayload(
            segment_id=frame.segment_id,
            tool_id=_decision_id(decision),
            name=name_by_id[_decision_id(decision)],
            result="已批准，已在子代理内执行。",
            is_error=False,
        )
        for decision in decisions
        if decision.type in ("approve", "edit")
    ]


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
        elif decision.type == "respond":
            out.append({"type": "respond", "message": decision.response})
        else:
            # submit 不经审批帧的 langgraph resume（走 input 分支的 submit_resume_value）。
            raise ValueError(f"decision {decision.type!r} not allowed for approval resume")
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
