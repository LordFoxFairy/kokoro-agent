# GENERATED — DO NOT EDIT. Kokoro Root authority: contract/spec/events.yaml
# Root materialization (run from Kokoro Root): uv run --locked python contract/generate.py --output-root OUTPUT_ROOT
# Consumer regeneration (run from Kokoro Root): node contract/generate.mjs --consumer CONSUMER --source-root ROOT --output-repository CONSUMER_REPOSITORY
from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, JsonValue, StringConstraints, TypeAdapter

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]

def _trimmed_reference(value: str) -> str:
    if value.strip() != value:
        raise ValueError("reference must not have surrounding whitespace")
    return value

Sha256Str = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Reference = Annotated[str, StringConstraints(min_length=1, max_length=256),
    AfterValidator(_trimmed_reference)]
NonNegInt = Annotated[int, Field(ge=0)]
PositiveInt = Annotated[int, Field(gt=0)]

TodoStatus = Literal["pending", "in_progress", "completed"]
PlanAction = Literal["accept", "reject"]
AllowedDecision = Literal["approve", "edit", "reject", "respond", "submit"]
AwaitingKind = Literal["tool_approval", "ask_user_question", "result_review", "input"]
SubagentSource = Literal["built-in", "config-custom", "runtime-custom"]
ControlReceiptStatus = Literal["persisted", "applied"]
RunCompletedStatus = Literal["completed", "cancelled"]
RunErrorCode = Literal["token_budget_exceeded", "recursion_limit_exceeded", "assembly_failed", "enqueue_failed", "dispatch_exhausted", "contract_incompatible", "internal_error"]


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class Todo(StrictModel):
    content: NonEmptyStr
    status: TodoStatus


class TokenUsage(StrictModel):
    input_tokens: int
    output_tokens: int


class Risk(StrictModel):
    level: NonEmptyStr
    source: NonEmptyStr
    reason: NonEmptyStr


class PlanStep(StrictModel):
    step_ref: NonEmptyStr
    label: NonEmptyStr
    status: TodoStatus


class PlanProposal(StrictModel):
    summary: NonEmptyStr
    steps: list[PlanStep]
    allowed_actions: list[PlanAction]


class RunStartedPayload(StrictModel):
    pass


class ThinkingDeltaPayload(StrictModel):
    segment_id: NonEmptyStr
    delta: str


class MessageDeltaPayload(StrictModel):
    segment_id: NonEmptyStr
    # 流上文本恒为 assistant，无 role 字段；角色由 segment 归属决定。
    delta: str


class MessageCompletedPayload(StrictModel):
    segment_id: NonEmptyStr
    content: str


class ToolInvokedPayload(StrictModel):
    segment_id: NonEmptyStr
    tool_id: NonEmptyStr
    name: NonEmptyStr
    args: dict[str, JsonValue]


class ToolOutputDeltaPayload(StrictModel):
    segment_id: NonEmptyStr
    tool_id: NonEmptyStr
    name: NonEmptyStr
    # 长执行工具的增量输出（如 execute）；每工具累计上限同 result 护栏，超限静默停发（终值仍走 tool.returned）。
    delta: str


class ToolAwaitingApprovalPayload(StrictModel):
    segment_id: NonEmptyStr
    tool_id: NonEmptyStr
    name: NonEmptyStr
    args: dict[str, JsonValue]
    description: str
    allowed_decisions: list[AllowedDecision]
    kind: AwaitingKind
    # 面向 web 的风险摘要，非权限判断真源。
    risk: Risk | None = None
    editable: bool
    input_schema: dict[str, JsonValue] | None = None
    # 同帧完整待批 tool_id 列表；HITL『凑齐才提交』契约依据，web 读契约而非内嵌算法。
    pending_tool_ids: list[NonEmptyStr]
    # 仅 kind=result_review 时存在：待人工审核的已执行结果（payload 列表尾缀 ? = 该 kind 局部可选）。
    result: str | None = None


class ToolReturnedPayload(StrictModel):
    segment_id: NonEmptyStr
    tool_id: NonEmptyStr
    name: NonEmptyStr
    result: str
    # 严格必填 fail-loud：生产端始终发送；缺失即报错，绝不用默认 false 掩盖真失败。
    is_error: bool
    # wire 展示层截断标记：缺席=结果完整，true=已截断（完整结果在工作区文件，预览经 files 端点取）。
    truncated: bool | None = None
    rejected: bool | None = None
    reject_reason: str | None = None
    responded: bool | None = None
    summary: dict[str, JsonValue] | None = None


class TodoUpdatedPayload(StrictModel):
    todos: list[Todo]


class PlanProposedPayload(StrictModel):
    segment_id: NonEmptyStr
    # 计划 owner 的唯一身份；必须逐字等于真实 LangGraph tool_call_id，Session resume 用它回填 decision.tool_id。
    owner_ref: NonEmptyStr
    # V1 immutable proposal 恒为 1；修订必须创建新的 propose_plan tool call/owner_ref，禁止原位覆盖。
    owner_version: PositiveInt
    # 专用计划提案；与 todo.updated 的内部进度清单语义严格分离，禁止互相推断。
    proposal: PlanProposal


class SubagentStartedPayload(StrictModel):
    segment_id: NonEmptyStr
    subagent_id: NonEmptyStr
    name: NonEmptyStr
    description: str
    subagent_type: NonEmptyStr
    source: SubagentSource


class SubagentFinishedPayload(StrictModel):
    segment_id: NonEmptyStr
    subagent_id: NonEmptyStr
    name: NonEmptyStr
    subagent_type: NonEmptyStr
    source: SubagentSource
    failed: bool | None = None
    error: str | None = None


class SubagentThinkingDeltaPayload(StrictModel):
    segment_id: NonEmptyStr
    subagent_id: NonEmptyStr
    delta: str


class SubagentTextDeltaPayload(StrictModel):
    segment_id: NonEmptyStr
    subagent_id: NonEmptyStr
    text: str


class SubagentTextCompletedPayload(StrictModel):
    segment_id: NonEmptyStr
    subagent_id: NonEmptyStr
    text: str


class SubagentToolInvokedPayload(StrictModel):
    segment_id: NonEmptyStr
    subagent_id: NonEmptyStr
    tool_id: NonEmptyStr
    name: NonEmptyStr
    # 子代理内工具过程可见性通道；HITL 审批仍走主通道嵌套帧，无输出增量通道（终值走 returned）。
    args: dict[str, JsonValue]


class SubagentToolReturnedPayload(StrictModel):
    segment_id: NonEmptyStr
    subagent_id: NonEmptyStr
    tool_id: NonEmptyStr
    name: NonEmptyStr
    result: str
    is_error: bool
    # 同 tool.returned.truncated：缺席=结果完整。
    truncated: bool | None = None


class DeliveryCreatedPayload(StrictModel):
    path: NonEmptyStr
    title: NonEmptyStr
    mime: NonEmptyStr
    size: int
    # 成果冻结键：deliveries/<namespace>/<content_hash> 内容寻址,永不漂移；由 deliver 工具归档时计算,emitter 在 tool.returned 后追发本事件。
    content_hash: NonEmptyStr
    note: str | None = None


class RunControlReceiptPayload(StrictModel):
    decision_id: NonEmptyStr
    control_status: ControlReceiptStatus


class RunOwnerCompletedPayload(StrictModel):
    execution_context_anchor: Reference
    execution_context_digest: Sha256Str
    owner_revision: PositiveInt


class RunCompletedPayload(StrictModel):
    status: RunCompletedStatus
    # agent 认真算的用量全链路贯通；无用量时为 null。
    token_usage: TokenUsage | None = None


class RunFailedPayload(StrictModel):
    # 三层错误语义：code=稳定错误码（web 按码本地化的键，闭集枚举）；error_kind=诊断用异常类名（观测/排障，不作展示）；message=人读原文（未知码/未译码的兜底展示，绝不裸露 key）。
    code: RunErrorCode
    error_kind: NonEmptyStr
    message: NonEmptyStr


class RunStarted(StrictModel):
    kind: Literal["run.started"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: RunStartedPayload


class ThinkingDelta(StrictModel):
    kind: Literal["thinking.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: ThinkingDeltaPayload


class MessageDelta(StrictModel):
    kind: Literal["message.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: MessageDeltaPayload


class MessageCompleted(StrictModel):
    kind: Literal["message.completed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: MessageCompletedPayload


class ToolInvoked(StrictModel):
    kind: Literal["tool.invoked"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: ToolInvokedPayload


class ToolOutputDelta(StrictModel):
    kind: Literal["tool.output.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: ToolOutputDeltaPayload


class ToolAwaitingApproval(StrictModel):
    kind: Literal["tool.awaiting_approval"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: ToolAwaitingApprovalPayload


class ToolReturned(StrictModel):
    kind: Literal["tool.returned"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: ToolReturnedPayload


class TodoUpdated(StrictModel):
    kind: Literal["todo.updated"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: TodoUpdatedPayload


class PlanProposed(StrictModel):
    kind: Literal["plan.proposed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: PlanProposedPayload


class SubagentStarted(StrictModel):
    kind: Literal["subagent.started"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: SubagentStartedPayload


class SubagentFinished(StrictModel):
    kind: Literal["subagent.finished"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: SubagentFinishedPayload


class SubagentThinkingDelta(StrictModel):
    kind: Literal["subagent.thinking.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: SubagentThinkingDeltaPayload


class SubagentTextDelta(StrictModel):
    kind: Literal["subagent.text.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: SubagentTextDeltaPayload


class SubagentTextCompleted(StrictModel):
    kind: Literal["subagent.text.completed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: SubagentTextCompletedPayload


class SubagentToolInvoked(StrictModel):
    kind: Literal["subagent.tool.invoked"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: SubagentToolInvokedPayload


class SubagentToolReturned(StrictModel):
    kind: Literal["subagent.tool.returned"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: SubagentToolReturnedPayload


class DeliveryCreated(StrictModel):
    kind: Literal["delivery.created"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: DeliveryCreatedPayload


class RunControlReceipt(StrictModel):
    kind: Literal["run.control.receipt"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: RunControlReceiptPayload


class RunOwnerCompleted(StrictModel):
    kind: Literal["run.owner.completed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: RunOwnerCompletedPayload


class RunCompleted(StrictModel):
    kind: Literal["run.completed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: RunCompletedPayload


class RunFailed(StrictModel):
    kind: Literal["run.failed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    # R4 durable 身份位:critical 帧必带(per-run 连续 seq 从 1 起),live 帧缺席。
    durable_seq: Annotated[int, Field(ge=1)] | None = None
    event_id: NonEmptyStr | None = None
    payload: RunFailedPayload


AgentEvent = Annotated[
    Union[
        RunStarted,
        ThinkingDelta,
        MessageDelta,
        MessageCompleted,
        ToolInvoked,
        ToolOutputDelta,
        ToolAwaitingApproval,
        ToolReturned,
        TodoUpdated,
        PlanProposed,
        SubagentStarted,
        SubagentFinished,
        SubagentThinkingDelta,
        SubagentTextDelta,
        SubagentTextCompleted,
        SubagentToolInvoked,
        SubagentToolReturned,
        DeliveryCreated,
        RunControlReceipt,
        RunOwnerCompleted,
        RunCompleted,
        RunFailed,
    ],
    Field(discriminator="kind"),
]

agent_event_adapter: TypeAdapter[AgentEvent] = TypeAdapter(AgentEvent)
