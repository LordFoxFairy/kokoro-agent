# GENERATED — DO NOT EDIT. Source: Root contract protobuf descriptor
# Regenerate: uv run python scripts/sync_interaction_v2_contract.py --contract-root <path>
from __future__ import annotations

from datetime import datetime
from enum import IntEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class DurableExecutionEvidenceKindV2(IntEnum):
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_UNSPECIFIED = 0
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_STARTED = 1
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_INTERACTION_GROUP_REVISION = 2
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_OWNER_COMPLETED = 3
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_COMPLETED = 4
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_FAILED = 5

class InteractionKindV2(IntEnum):
    INTERACTION_KIND_V2_UNSPECIFIED = 0
    INTERACTION_KIND_V2_APPROVAL = 1
    INTERACTION_KIND_V2_QUESTION = 2
    INTERACTION_KIND_V2_STRUCTURED_INPUT = 3
    INTERACTION_KIND_V2_RESULT_REVIEW = 4
    INTERACTION_KIND_V2_PLAN = 5

class InteractionRevisionStateV2(IntEnum):
    INTERACTION_REVISION_STATE_V2_UNSPECIFIED = 0
    INTERACTION_REVISION_STATE_V2_PENDING = 1
    INTERACTION_REVISION_STATE_V2_RESUME_PERSISTED = 2
    INTERACTION_REVISION_STATE_V2_APPLYING = 3
    INTERACTION_REVISION_STATE_V2_APPLIED = 4
    INTERACTION_REVISION_STATE_V2_OUTCOME_UNKNOWN = 5
    INTERACTION_REVISION_STATE_V2_RESOLVED = 6
    INTERACTION_REVISION_STATE_V2_SUPERSEDED_BY_REVISION = 7
    INTERACTION_REVISION_STATE_V2_CANCELED = 8
    INTERACTION_REVISION_STATE_V2_CLOSED_BY_TERMINAL = 9

class InteractionDecisionKindV2(IntEnum):
    INTERACTION_DECISION_KIND_V2_UNSPECIFIED = 0
    INTERACTION_DECISION_KIND_V2_APPROVE = 1
    INTERACTION_DECISION_KIND_V2_EDIT = 2
    INTERACTION_DECISION_KIND_V2_REJECT = 3
    INTERACTION_DECISION_KIND_V2_RESPOND = 4
    INTERACTION_DECISION_KIND_V2_SUBMIT = 5

class PlanStepStatusV2(IntEnum):
    PLAN_STEP_STATUS_V2_UNSPECIFIED = 0
    PLAN_STEP_STATUS_V2_PENDING = 1
    PLAN_STEP_STATUS_V2_IN_PROGRESS = 2
    PLAN_STEP_STATUS_V2_COMPLETED = 3

class RunCompletedEvidenceStatusV2(IntEnum):
    RUN_COMPLETED_EVIDENCE_STATUS_V2_UNSPECIFIED = 0
    RUN_COMPLETED_EVIDENCE_STATUS_V2_COMPLETED = 1
    RUN_COMPLETED_EVIDENCE_STATUS_V2_CANCELLED = 2

class SubagentProgressStatusV2(IntEnum):
    SUBAGENT_PROGRESS_STATUS_V2_UNSPECIFIED = 0
    SUBAGENT_PROGRESS_STATUS_V2_PENDING = 1
    SUBAGENT_PROGRESS_STATUS_V2_RUNNING = 2
    SUBAGENT_PROGRESS_STATUS_V2_COMPLETED = 3
    SUBAGENT_PROGRESS_STATUS_V2_FAILED = 4
    SUBAGENT_PROGRESS_STATUS_V2_CANCELED = 5

class NoticeSeverityV2(IntEnum):
    NOTICE_SEVERITY_V2_UNSPECIFIED = 0
    NOTICE_SEVERITY_V2_INFO = 1
    NOTICE_SEVERITY_V2_WARNING = 2

class OutputRetryClassV2(IntEnum):
    OUTPUT_RETRY_CLASS_V2_UNSPECIFIED = 0
    OUTPUT_RETRY_CLASS_V2_NEVER = 1
    OUTPUT_RETRY_CLASS_V2_IMMEDIATE = 2
    OUTPUT_RETRY_CLASS_V2_AFTER_DELAY = 3
    OUTPUT_RETRY_CLASS_V2_AFTER_USER_ACTION = 4
    OUTPUT_RETRY_CLASS_V2_RECONCILE_RECEIPT = 5

class DurableExecutionCanonicalPayloadV2(StrictModel):
    run_started: RunStartedEvidenceV2 | None = None
    interaction_group_revision: InteractionGroupRevisionEvidenceV2 | None = None
    run_owner_completed: RunOwnerCompletedEvidenceV2 | None = None
    run_completed: RunCompletedEvidenceV2 | None = None
    run_failed: RunFailedEvidenceV2 | None = None

    @model_validator(mode="after")
    def _validate_payload(self) -> Self:
        fields = ('run_started', 'interaction_group_revision', 'run_owner_completed', 'run_completed', 'run_failed',)
        if sum(getattr(self, name) is not None for name in fields) != 1:
            raise ValueError("exactly one payload arm is required")
        return self

class RunStartedEvidenceV2(StrictModel):
    pass

class InteractionOwnerRevisionRefV2(StrictModel):
    interaction_owner_ref: str = Field(min_length=1, max_length=256)
    owner_revision: int = Field(gt=0)

class InteractionRiskSummaryV2(StrictModel):
    level: str = Field(min_length=1, max_length=64)
    source: str = Field(min_length=1, max_length=128)
    reason: str = Field(min_length=1)

class SafeInteractionPromptV2(StrictModel):
    title: str = Field(min_length=1)
    description: str = ""
    risk: InteractionRiskSummaryV2 | None = None
    safe_request_json: bytes | None = None
    input_schema_ref: str | None = None
    safe_input_schema_json: bytes | None = None
    safe_validation_error: str | None = None
    deadline: datetime | None = None

class ApprovalPresentationV2(StrictModel):
    prompt: SafeInteractionPromptV2
    allowed_decisions: list[InteractionDecisionKindV2] = Field(default_factory=list[InteractionDecisionKindV2], min_length=1, max_length=3)

class QuestionPresentationV2(StrictModel):
    prompt: SafeInteractionPromptV2
    allowed_decisions: list[InteractionDecisionKindV2] = Field(default_factory=list[InteractionDecisionKindV2], min_length=1, max_length=2)

class StructuredInputPresentationV2(StrictModel):
    prompt: SafeInteractionPromptV2
    allowed_decisions: list[InteractionDecisionKindV2] = Field(default_factory=list[InteractionDecisionKindV2], min_length=1, max_length=2)

class ResultReviewPresentationV2(StrictModel):
    prompt: SafeInteractionPromptV2
    safe_result_preview: str = ""
    allowed_decisions: list[InteractionDecisionKindV2] = Field(default_factory=list[InteractionDecisionKindV2], min_length=1, max_length=3)

class PlanStepV2(StrictModel):
    step_ref: str = Field(min_length=1, max_length=256)
    label: str = Field(min_length=1)
    status: PlanStepStatusV2

class PlanPresentationV2(StrictModel):
    summary: str = Field(min_length=1)
    steps: list[PlanStepV2] = Field(default_factory=list[PlanStepV2], max_length=256)
    allowed_decisions: list[InteractionDecisionKindV2] = Field(default_factory=list[InteractionDecisionKindV2], min_length=1, max_length=2)
    deadline: datetime | None = None

class InteractionPresentationV2(StrictModel):
    approval: ApprovalPresentationV2 | None = None
    question: QuestionPresentationV2 | None = None
    structured_input: StructuredInputPresentationV2 | None = None
    result_review: ResultReviewPresentationV2 | None = None
    plan: PlanPresentationV2 | None = None

    @model_validator(mode="after")
    def _validate_presentation(self) -> Self:
        fields = ('approval', 'question', 'structured_input', 'result_review', 'plan',)
        if sum(getattr(self, name) is not None for name in fields) != 1:
            raise ValueError("exactly one presentation arm is required")
        return self

class InteractionOwnerRevisionEvidenceV2(StrictModel):
    interaction_owner_ref: str = Field(min_length=1, max_length=256)
    owner_revision: int = Field(gt=0)
    projection_event_ref: str = Field(min_length=1, max_length=256)
    predecessor_projection_event_ref: str | None = None
    predecessor_evidence_sha256: str | None = None
    projection_payload_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    interaction_kind: InteractionKindV2
    application_request_ref: str = Field(min_length=1, max_length=256)
    decision_group_ref: str = Field(min_length=1, max_length=256)
    decision_group_revision: int = Field(gt=0)
    group_member_ordinal: int = Field(gt=0, le=64)
    required_owner_revision_refs: list[InteractionOwnerRevisionRefV2] = Field(default_factory=list[InteractionOwnerRevisionRefV2], min_length=1, max_length=64)
    pending_frame_digest: str = Field(pattern='^[0-9a-f]{64}$')
    presentation: InteractionPresentationV2
    state: InteractionRevisionStateV2

class InteractionGroupRevisionEvidenceV2(StrictModel):
    decision_group_ref: str = Field(min_length=1, max_length=256)
    decision_group_revision: int = Field(gt=0)
    group_projection_ref: str = Field(min_length=1, max_length=256)
    pending_frame_digest: str = Field(pattern='^[0-9a-f]{64}$')
    member_vector_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    members: list[InteractionOwnerRevisionEvidenceV2] = Field(default_factory=list[InteractionOwnerRevisionEvidenceV2], min_length=1, max_length=64)

class RunOwnerCompletedEvidenceV2(StrictModel):
    execution_context_anchor: str = Field(min_length=1, max_length=256)
    execution_context_digest: str = Field(pattern='^[0-9a-f]{64}$')
    owner_revision: int = Field(gt=0)

class TokenUsageEvidenceV2(StrictModel):
    input_tokens: int = 0
    output_tokens: int = 0

class RunCompletedEvidenceV2(StrictModel):
    status: RunCompletedEvidenceStatusV2
    token_usage: TokenUsageEvidenceV2 | None = None
    output_high_watermark: int = 0
    output_digest_sha256: str = Field(pattern='^[0-9a-f]{64}$')

class RunFailedEvidenceV2(StrictModel):
    code: str = Field(min_length=1, max_length=64)
    error_kind: str = Field(min_length=1, max_length=128)
    message: str = ""
    output_high_watermark: int = 0
    output_digest_sha256: str = Field(pattern='^[0-9a-f]{64}$')

class DurableOutputPayloadV2(StrictModel):
    text_delta: TextDeltaOutputV2 | None = None
    text_snapshot: TextSnapshotOutputV2 | None = None
    safe_reasoning_summary: SafeReasoningSummaryOutputV2 | None = None
    tool_started: ToolStartedOutputV2 | None = None
    tool_finished: ToolFinishedOutputV2 | None = None
    plan_progress: PlanProgressOutputV2 | None = None
    subagent_progress: SubagentProgressOutputV2 | None = None
    media_operation_reference: MediaOperationReferenceOutputV2 | None = None
    notice: NoticeOutputV2 | None = None
    error: ErrorOutputV2 | None = None

    @model_validator(mode="after")
    def _validate_payload(self) -> Self:
        fields = ('text_delta', 'text_snapshot', 'safe_reasoning_summary', 'tool_started', 'tool_finished', 'plan_progress', 'subagent_progress', 'media_operation_reference', 'notice', 'error',)
        if sum(getattr(self, name) is not None for name in fields) != 1:
            raise ValueError("exactly one payload arm is required")
        return self

class TextDeltaOutputV2(StrictModel):
    part_ref: str = Field(min_length=1, max_length=256)
    delta: str = Field(min_length=1)

class TextSnapshotOutputV2(StrictModel):
    part_ref: str = Field(min_length=1, max_length=256)
    text: str = ""
    replaces_through_output_seq: int = 0

class SafeReasoningSummaryOutputV2(StrictModel):
    part_ref: str = Field(min_length=1, max_length=256)
    safe_summary: str = Field(min_length=1)

class ToolStartedOutputV2(StrictModel):
    tool_call_ref: str = Field(min_length=1, max_length=256)
    tool_label: str = Field(min_length=1, max_length=256)
    redacted_input_summary_json: bytes | None = None

class ToolFinishedOutputV2(StrictModel):
    tool_call_ref: str = Field(min_length=1, max_length=256)
    safe_result_preview: str = ""
    is_error: bool = False
    truncated: bool = False

class PlanProgressOutputV2(StrictModel):
    plan_ref: str = Field(min_length=1, max_length=256)
    safe_summary: str = Field(min_length=1)
    steps: list[PlanStepV2] = Field(default_factory=list[PlanStepV2], max_length=256)

class SubagentProgressOutputV2(StrictModel):
    subagent_ref: str = Field(min_length=1, max_length=256)
    status: SubagentProgressStatusV2
    safe_summary: str | None = None

class MediaOperationReferenceOutputV2(StrictModel):
    stable_output_slot_ref: str = Field(min_length=1, max_length=256)
    agent_media_command_ref: str = Field(min_length=1, max_length=256)
    operation_ref: str = Field(min_length=1, max_length=256)

class NoticeOutputV2(StrictModel):
    notice_ref: str = Field(min_length=1, max_length=256)
    code: str = Field(min_length=1, max_length=64)
    message: str = ""
    severity: NoticeSeverityV2
    retry_class: OutputRetryClassV2 | None = None

class ErrorOutputV2(StrictModel):
    error_ref: str = Field(min_length=1, max_length=256)
    code: str = Field(min_length=1, max_length=64)
    message: str = ""
    retry_class: OutputRetryClassV2

class DurableOutputRecordV2(StrictModel):
    output_ref: str = Field(min_length=1, max_length=256)
    output_version: int
    run_id: str = Field(min_length=1, max_length=128)
    output_seq: int = Field(gt=0)
    canonical_payload: bytes = Field(min_length=1, max_length=65536)
    payload_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    recorded_at: datetime
    producer_instance_ref: str = Field(min_length=1, max_length=256)
    producer_generation: int = Field(gt=0)

class PullDurableOutputRecordsRequest(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)
    after_output_seq: int = 0
    page_size: int = Field(gt=0, le=64)

class PullDurableOutputRecordsResponse(StrictModel):
    records: list[DurableOutputRecordV2] = Field(default_factory=list[DurableOutputRecordV2], max_length=64)
    next_after_output_seq: int | None = None
    has_more: bool = False

class DurableExecutionEvidenceV2(StrictModel):
    evidence_ref: str = Field(min_length=1, max_length=256)
    evidence_version: int
    run_id: str = Field(min_length=1, max_length=128)
    durable_seq: int = Field(gt=0)
    event_id: str = Field(min_length=1, max_length=256)
    kind: DurableExecutionEvidenceKindV2
    canonical_payload: bytes = Field(min_length=1, max_length=1048576)
    payload_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    recorded_at: datetime
    producer_instance_ref: str = Field(min_length=1, max_length=256)
    producer_generation: int = Field(gt=0)
    dispatch_id: str = Field(min_length=1, max_length=128)
    assistant_message_id: str = Field(min_length=1, max_length=128)
    evidence_sha256: str = Field(pattern='^[0-9a-f]{64}$')
    interaction_protocol_release_epoch: str = Field(min_length=1, max_length=256)

class PullDurableExecutionEvidenceRequest(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)
    after_durable_seq: int = 0
    page_size: int = Field(gt=0, le=256)

class PullDurableExecutionEvidenceResponse(StrictModel):
    evidence: list[DurableExecutionEvidenceV2] = Field(default_factory=list[DurableExecutionEvidenceV2], max_length=256)
    next_after_durable_seq: int | None = None
    has_more: bool = False

class GetDurableExecutionEvidenceRequest(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)
    evidence_ref: str = Field(min_length=1, max_length=256)

class DurableExecutionEvidenceNotFoundV2(StrictModel):
    pass

class GetDurableExecutionEvidenceResponse(StrictModel):
    evidence: DurableExecutionEvidenceV2 | None = None
    not_found: DurableExecutionEvidenceNotFoundV2 | None = None

    @model_validator(mode="after")
    def _validate_outcome(self) -> Self:
        fields = ('evidence', 'not_found',)
        if sum(getattr(self, name) is not None for name in fields) != 1:
            raise ValueError("exactly one outcome arm is required")
        return self

class GetRunDurableCheckpointRequest(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)

class GetRunDurableCheckpointResponse(StrictModel):
    evidence: DurableExecutionEvidenceV2 | None = None
    not_found: DurableExecutionEvidenceNotFoundV2 | None = None

    @model_validator(mode="after")
    def _validate_outcome(self) -> Self:
        fields = ('evidence', 'not_found',)
        if sum(getattr(self, name) is not None for name in fields) != 1:
            raise ValueError("exactly one outcome arm is required")
        return self

for _model in (DurableExecutionCanonicalPayloadV2, RunStartedEvidenceV2, InteractionOwnerRevisionRefV2, InteractionRiskSummaryV2, SafeInteractionPromptV2, ApprovalPresentationV2, QuestionPresentationV2, StructuredInputPresentationV2, ResultReviewPresentationV2, PlanStepV2, PlanPresentationV2, InteractionPresentationV2, InteractionOwnerRevisionEvidenceV2, InteractionGroupRevisionEvidenceV2, RunOwnerCompletedEvidenceV2, TokenUsageEvidenceV2, RunCompletedEvidenceV2, RunFailedEvidenceV2, DurableOutputPayloadV2, TextDeltaOutputV2, TextSnapshotOutputV2, SafeReasoningSummaryOutputV2, ToolStartedOutputV2, ToolFinishedOutputV2, PlanProgressOutputV2, SubagentProgressOutputV2, MediaOperationReferenceOutputV2, NoticeOutputV2, ErrorOutputV2, DurableOutputRecordV2, PullDurableOutputRecordsRequest, PullDurableOutputRecordsResponse, DurableExecutionEvidenceV2, PullDurableExecutionEvidenceRequest, PullDurableExecutionEvidenceResponse, GetDurableExecutionEvidenceRequest, DurableExecutionEvidenceNotFoundV2, GetDurableExecutionEvidenceResponse, GetRunDurableCheckpointRequest, GetRunDurableCheckpointResponse,):
    _model.model_rebuild()

del _model
