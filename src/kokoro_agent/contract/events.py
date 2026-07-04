# GENERATED — DO NOT EDIT. Source: contract/spec/events.yaml
# Regenerate: python3 contract/generate.py
from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, TypeAdapter

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
NonNegInt = Annotated[int, Field(ge=0)]

TodoStatus = Literal["pending", "in_progress", "completed"]
AllowedDecision = Literal["approve", "edit", "reject", "respond"]
AwaitingKind = Literal["tool_approval", "ask_user_question", "result_review"]
SubagentSource = Literal["built-in", "config-custom", "runtime-custom"]
RunCompletedStatus = Literal["completed", "cancelled"]


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
    # wire 展示层截断标记：缺席=结果完整，true=已截断（完整结果在后端，canvas 预览 P1 经 artifact_ref 取）。
    truncated: bool | None = None
    rejected: bool | None = None
    reject_reason: str | None = None
    responded: bool | None = None
    # 大结果落 artifact，SSE 只带引用；P1 生产者。
    artifact_ref: NonEmptyStr | None = None
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


class RunCompletedPayload(StrictModel):
    status: RunCompletedStatus
    # agent 认真算的用量全链路贯通；无用量时为 null。
    token_usage: TokenUsage | None = None


class RunFailedPayload(StrictModel):
    error_kind: NonEmptyStr
    message: NonEmptyStr


class RunStarted(StrictModel):
    kind: Literal["run.started"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: RunStartedPayload


class ThinkingDelta(StrictModel):
    kind: Literal["thinking.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: ThinkingDeltaPayload


class MessageDelta(StrictModel):
    kind: Literal["message.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: MessageDeltaPayload


class MessageCompleted(StrictModel):
    kind: Literal["message.completed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: MessageCompletedPayload


class ToolInvoked(StrictModel):
    kind: Literal["tool.invoked"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: ToolInvokedPayload


class ToolOutputDelta(StrictModel):
    kind: Literal["tool.output.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: ToolOutputDeltaPayload


class ToolAwaitingApproval(StrictModel):
    kind: Literal["tool.awaiting_approval"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: ToolAwaitingApprovalPayload


class ToolReturned(StrictModel):
    kind: Literal["tool.returned"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: ToolReturnedPayload


class TodoUpdated(StrictModel):
    kind: Literal["todo.updated"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: TodoUpdatedPayload


class SubagentStarted(StrictModel):
    kind: Literal["subagent.started"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: SubagentStartedPayload


class SubagentFinished(StrictModel):
    kind: Literal["subagent.finished"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: SubagentFinishedPayload


class SubagentThinkingDelta(StrictModel):
    kind: Literal["subagent.thinking.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: SubagentThinkingDeltaPayload


class SubagentTextDelta(StrictModel):
    kind: Literal["subagent.text.delta"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: SubagentTextDeltaPayload


class SubagentTextCompleted(StrictModel):
    kind: Literal["subagent.text.completed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: SubagentTextCompletedPayload


class RunCompleted(StrictModel):
    kind: Literal["run.completed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
    payload: RunCompletedPayload


class RunFailed(StrictModel):
    kind: Literal["run.failed"]
    run_id: NonEmptyStr
    index: NonNegInt
    timestamp: int
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
        RunCompleted,
        RunFailed,
    ],
    Field(discriminator="kind"),
]

agent_event_adapter: TypeAdapter[AgentEvent] = TypeAdapter(AgentEvent)
