# Agent-owned event protocol. Keep changes within this repository.
from __future__ import annotations

from typing import Annotated, Literal, Union, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    TypeAdapter,
    model_validator,
)

from kokoro_agent.protocol.run_failure_generated import RunFailedPayload

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
NonNegInt = Annotated[int, Field(ge=0)]

TodoStatus = Literal["pending", "in_progress", "completed"]
AllowedDecision = Literal["approve", "edit", "reject", "respond", "submit"]
AwaitingKind = Literal["tool_approval", "ask_user_question", "result_review", "input"]
SubagentSource = Literal["built-in", "config-custom", "runtime-custom"]
ControlReceiptStatus = Literal["persisted", "applied"]
RunCompletedStatus = Literal["completed", "cancelled"]
ArtifactKind = Literal[
    "document", "code", "image", "audio", "video", "data", "archive", "other"
]


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
    tool_call_id: NonEmptyStr
    artifact_id: NonEmptyStr
    asset_id: NonEmptyStr
    artifact_kind: ArtifactKind
    path: NonEmptyStr
    title: NonEmptyStr
    mime: NonEmptyStr
    size: int
    # 内容摘要由 deliver 读取 workspace 字节时计算；Storage 完成 Artifact 发布后，
    # emitter 在 tool.returned 后追发本事件。
    content_hash: NonEmptyStr
    note: str | None = None


class RunControlReceiptPayload(StrictModel):
    command_id: NonEmptyStr
    control_status: ControlReceiptStatus


class RunCompletedPayload(StrictModel):
    status: RunCompletedStatus
    # agent 认真算的用量全链路贯通；无用量时为 null。
    token_usage: TokenUsage | None = None


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
        SubagentStarted,
        SubagentFinished,
        SubagentThinkingDelta,
        SubagentTextDelta,
        SubagentTextCompleted,
        SubagentToolInvoked,
        SubagentToolReturned,
        DeliveryCreated,
        RunControlReceipt,
        RunCompleted,
        RunFailed,
    ],
    Field(discriminator="kind"),
]

agent_event_adapter: TypeAdapter[AgentEvent] = TypeAdapter(AgentEvent)


class InteractionValidation(StrictModel):
    code: Literal["json_schema_invalid"]
    instance_path: list[str | int]


class InteractionDisplay(StrictModel):
    name: NonEmptyStr
    description: str
    editable: bool
    input_schema: dict[str, JsonValue]
    result_preview: str | None = None
    truncated: bool | None = None
    source: str | None = None

    @model_validator(mode="after")
    def validate_preview(self) -> Self:
        if self.result_preview is not None and (
            self.truncated is None or not self.source
        ):
            raise ValueError("result preview requires truncation and source")
        return self


class InteractionItem(StrictModel):
    item_id: NonEmptyStr
    request_id: NonEmptyStr
    kind: AwaitingKind
    allowed_decisions: Annotated[list[AllowedDecision], Field(min_length=1)]
    display: InteractionDisplay
    validation: InteractionValidation | None = None

    @model_validator(mode="after")
    def unique_decisions(self) -> Self:
        if len(set(self.allowed_decisions)) != len(self.allowed_decisions):
            raise ValueError("duplicate allowed decision")
        return self


class InteractionGroup(StrictModel):
    group_id: NonEmptyStr
    items: Annotated[list[InteractionItem], Field(min_length=1)]


class InteractionActionResult(StrictModel):
    command_id: NonEmptyStr
    pause_revision: Annotated[int, Field(ge=1)]
    kind: Literal[
        "accepted", "native_consumed", "validation_failed", "unknown", "cancelled"
    ]


class ChatInteractionState(StrictModel):
    """Candidate full replacement source, not an execution/consumption proof."""

    interaction_revision: Annotated[int, Field(ge=1)]
    pause_revision: Annotated[int, Field(ge=0)]
    pause_ref: NonEmptyStr | None
    phase: Literal["active", "waiting", "resuming", "terminal"]
    groups: list[InteractionGroup]
    action_result: InteractionActionResult | None

    @model_validator(mode="after")
    def complete_collection(self) -> Self:
        if self.pause_revision > self.interaction_revision:
            raise ValueError("pause revision exceeds interaction revision")
        if (self.pause_revision == 0) != (self.pause_ref is None):
            raise ValueError("pause identity is incomplete")
        waiting = self.phase in {"waiting", "resuming"}
        if waiting != bool(self.groups) or (waiting and self.pause_revision == 0):
            raise ValueError("phase and complete collection disagree")
        action = self.action_result
        if action is not None:
            if action.pause_revision > self.pause_revision:
                raise ValueError("action references a future pause")
            if self.phase == "resuming" and (
                action.kind not in {"accepted", "unknown"}
                or action.pause_revision != self.pause_revision
            ):
                raise ValueError("resuming action disagrees with original round")
            if (
                action.kind == "validation_failed"
                and self.phase == "waiting"
                and action.pause_revision >= self.pause_revision
            ):
                raise ValueError("validation result requires a new pause round")
        elif self.phase == "resuming":
            raise ValueError("resuming requires an accepted action")
        group_ids = [group.group_id for group in self.groups]
        item_ids = [item.item_id for group in self.groups for item in group.items]
        if len(set(group_ids)) != len(group_ids) or len(set(item_ids)) != len(item_ids):
            raise ValueError("duplicate group or item")
        return self
