import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DurableExecutionEvidenceKindV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_UNSPECIFIED: _ClassVar[DurableExecutionEvidenceKindV2]
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_STARTED: _ClassVar[DurableExecutionEvidenceKindV2]
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_INTERACTION_GROUP_REVISION: _ClassVar[DurableExecutionEvidenceKindV2]
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_OWNER_COMPLETED: _ClassVar[DurableExecutionEvidenceKindV2]
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_COMPLETED: _ClassVar[DurableExecutionEvidenceKindV2]
    DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_FAILED: _ClassVar[DurableExecutionEvidenceKindV2]

class InteractionKindV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERACTION_KIND_V2_UNSPECIFIED: _ClassVar[InteractionKindV2]
    INTERACTION_KIND_V2_APPROVAL: _ClassVar[InteractionKindV2]
    INTERACTION_KIND_V2_QUESTION: _ClassVar[InteractionKindV2]
    INTERACTION_KIND_V2_STRUCTURED_INPUT: _ClassVar[InteractionKindV2]
    INTERACTION_KIND_V2_RESULT_REVIEW: _ClassVar[InteractionKindV2]
    INTERACTION_KIND_V2_PLAN: _ClassVar[InteractionKindV2]

class InteractionRevisionStateV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERACTION_REVISION_STATE_V2_UNSPECIFIED: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_PENDING: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_RESUME_PERSISTED: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_APPLYING: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_APPLIED: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_OUTCOME_UNKNOWN: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_RESOLVED: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_SUPERSEDED_BY_REVISION: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_CANCELED: _ClassVar[InteractionRevisionStateV2]
    INTERACTION_REVISION_STATE_V2_CLOSED_BY_TERMINAL: _ClassVar[InteractionRevisionStateV2]

class InteractionDecisionKindV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERACTION_DECISION_KIND_V2_UNSPECIFIED: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_APPROVE: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_EDIT: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_REJECT: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_RESPOND: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_SUBMIT: _ClassVar[InteractionDecisionKindV2]

class PlanStepStatusV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLAN_STEP_STATUS_V2_UNSPECIFIED: _ClassVar[PlanStepStatusV2]
    PLAN_STEP_STATUS_V2_PENDING: _ClassVar[PlanStepStatusV2]
    PLAN_STEP_STATUS_V2_IN_PROGRESS: _ClassVar[PlanStepStatusV2]
    PLAN_STEP_STATUS_V2_COMPLETED: _ClassVar[PlanStepStatusV2]

class RunCompletedEvidenceStatusV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_COMPLETED_EVIDENCE_STATUS_V2_UNSPECIFIED: _ClassVar[RunCompletedEvidenceStatusV2]
    RUN_COMPLETED_EVIDENCE_STATUS_V2_COMPLETED: _ClassVar[RunCompletedEvidenceStatusV2]
    RUN_COMPLETED_EVIDENCE_STATUS_V2_CANCELLED: _ClassVar[RunCompletedEvidenceStatusV2]

class SubagentProgressStatusV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SUBAGENT_PROGRESS_STATUS_V2_UNSPECIFIED: _ClassVar[SubagentProgressStatusV2]
    SUBAGENT_PROGRESS_STATUS_V2_PENDING: _ClassVar[SubagentProgressStatusV2]
    SUBAGENT_PROGRESS_STATUS_V2_RUNNING: _ClassVar[SubagentProgressStatusV2]
    SUBAGENT_PROGRESS_STATUS_V2_COMPLETED: _ClassVar[SubagentProgressStatusV2]
    SUBAGENT_PROGRESS_STATUS_V2_FAILED: _ClassVar[SubagentProgressStatusV2]
    SUBAGENT_PROGRESS_STATUS_V2_CANCELED: _ClassVar[SubagentProgressStatusV2]

class NoticeSeverityV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTICE_SEVERITY_V2_UNSPECIFIED: _ClassVar[NoticeSeverityV2]
    NOTICE_SEVERITY_V2_INFO: _ClassVar[NoticeSeverityV2]
    NOTICE_SEVERITY_V2_WARNING: _ClassVar[NoticeSeverityV2]

class OutputRetryClassV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTPUT_RETRY_CLASS_V2_UNSPECIFIED: _ClassVar[OutputRetryClassV2]
    OUTPUT_RETRY_CLASS_V2_NEVER: _ClassVar[OutputRetryClassV2]
    OUTPUT_RETRY_CLASS_V2_IMMEDIATE: _ClassVar[OutputRetryClassV2]
    OUTPUT_RETRY_CLASS_V2_AFTER_DELAY: _ClassVar[OutputRetryClassV2]
    OUTPUT_RETRY_CLASS_V2_AFTER_USER_ACTION: _ClassVar[OutputRetryClassV2]
    OUTPUT_RETRY_CLASS_V2_RECONCILE_RECEIPT: _ClassVar[OutputRetryClassV2]
DURABLE_EXECUTION_EVIDENCE_KIND_V2_UNSPECIFIED: DurableExecutionEvidenceKindV2
DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_STARTED: DurableExecutionEvidenceKindV2
DURABLE_EXECUTION_EVIDENCE_KIND_V2_INTERACTION_GROUP_REVISION: DurableExecutionEvidenceKindV2
DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_OWNER_COMPLETED: DurableExecutionEvidenceKindV2
DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_COMPLETED: DurableExecutionEvidenceKindV2
DURABLE_EXECUTION_EVIDENCE_KIND_V2_RUN_FAILED: DurableExecutionEvidenceKindV2
INTERACTION_KIND_V2_UNSPECIFIED: InteractionKindV2
INTERACTION_KIND_V2_APPROVAL: InteractionKindV2
INTERACTION_KIND_V2_QUESTION: InteractionKindV2
INTERACTION_KIND_V2_STRUCTURED_INPUT: InteractionKindV2
INTERACTION_KIND_V2_RESULT_REVIEW: InteractionKindV2
INTERACTION_KIND_V2_PLAN: InteractionKindV2
INTERACTION_REVISION_STATE_V2_UNSPECIFIED: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_PENDING: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_RESUME_PERSISTED: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_APPLYING: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_APPLIED: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_OUTCOME_UNKNOWN: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_RESOLVED: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_SUPERSEDED_BY_REVISION: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_CANCELED: InteractionRevisionStateV2
INTERACTION_REVISION_STATE_V2_CLOSED_BY_TERMINAL: InteractionRevisionStateV2
INTERACTION_DECISION_KIND_V2_UNSPECIFIED: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_APPROVE: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_EDIT: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_REJECT: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_RESPOND: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_SUBMIT: InteractionDecisionKindV2
PLAN_STEP_STATUS_V2_UNSPECIFIED: PlanStepStatusV2
PLAN_STEP_STATUS_V2_PENDING: PlanStepStatusV2
PLAN_STEP_STATUS_V2_IN_PROGRESS: PlanStepStatusV2
PLAN_STEP_STATUS_V2_COMPLETED: PlanStepStatusV2
RUN_COMPLETED_EVIDENCE_STATUS_V2_UNSPECIFIED: RunCompletedEvidenceStatusV2
RUN_COMPLETED_EVIDENCE_STATUS_V2_COMPLETED: RunCompletedEvidenceStatusV2
RUN_COMPLETED_EVIDENCE_STATUS_V2_CANCELLED: RunCompletedEvidenceStatusV2
SUBAGENT_PROGRESS_STATUS_V2_UNSPECIFIED: SubagentProgressStatusV2
SUBAGENT_PROGRESS_STATUS_V2_PENDING: SubagentProgressStatusV2
SUBAGENT_PROGRESS_STATUS_V2_RUNNING: SubagentProgressStatusV2
SUBAGENT_PROGRESS_STATUS_V2_COMPLETED: SubagentProgressStatusV2
SUBAGENT_PROGRESS_STATUS_V2_FAILED: SubagentProgressStatusV2
SUBAGENT_PROGRESS_STATUS_V2_CANCELED: SubagentProgressStatusV2
NOTICE_SEVERITY_V2_UNSPECIFIED: NoticeSeverityV2
NOTICE_SEVERITY_V2_INFO: NoticeSeverityV2
NOTICE_SEVERITY_V2_WARNING: NoticeSeverityV2
OUTPUT_RETRY_CLASS_V2_UNSPECIFIED: OutputRetryClassV2
OUTPUT_RETRY_CLASS_V2_NEVER: OutputRetryClassV2
OUTPUT_RETRY_CLASS_V2_IMMEDIATE: OutputRetryClassV2
OUTPUT_RETRY_CLASS_V2_AFTER_DELAY: OutputRetryClassV2
OUTPUT_RETRY_CLASS_V2_AFTER_USER_ACTION: OutputRetryClassV2
OUTPUT_RETRY_CLASS_V2_RECONCILE_RECEIPT: OutputRetryClassV2

class DurableExecutionCanonicalPayloadV2(_message.Message):
    __slots__ = ("run_started", "interaction_group_revision", "run_owner_completed", "run_completed", "run_failed")
    RUN_STARTED_FIELD_NUMBER: _ClassVar[int]
    INTERACTION_GROUP_REVISION_FIELD_NUMBER: _ClassVar[int]
    RUN_OWNER_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    RUN_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    RUN_FAILED_FIELD_NUMBER: _ClassVar[int]
    run_started: RunStartedEvidenceV2
    interaction_group_revision: InteractionGroupRevisionEvidenceV2
    run_owner_completed: RunOwnerCompletedEvidenceV2
    run_completed: RunCompletedEvidenceV2
    run_failed: RunFailedEvidenceV2
    def __init__(self, run_started: _Optional[_Union[RunStartedEvidenceV2, _Mapping]] = ..., interaction_group_revision: _Optional[_Union[InteractionGroupRevisionEvidenceV2, _Mapping]] = ..., run_owner_completed: _Optional[_Union[RunOwnerCompletedEvidenceV2, _Mapping]] = ..., run_completed: _Optional[_Union[RunCompletedEvidenceV2, _Mapping]] = ..., run_failed: _Optional[_Union[RunFailedEvidenceV2, _Mapping]] = ...) -> None: ...

class RunStartedEvidenceV2(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class InteractionOwnerRevisionRefV2(_message.Message):
    __slots__ = ("interaction_owner_ref", "owner_revision")
    INTERACTION_OWNER_REF_FIELD_NUMBER: _ClassVar[int]
    OWNER_REVISION_FIELD_NUMBER: _ClassVar[int]
    interaction_owner_ref: str
    owner_revision: int
    def __init__(self, interaction_owner_ref: _Optional[str] = ..., owner_revision: _Optional[int] = ...) -> None: ...

class InteractionRiskSummaryV2(_message.Message):
    __slots__ = ("level", "source", "reason")
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    level: str
    source: str
    reason: str
    def __init__(self, level: _Optional[str] = ..., source: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class SafeInteractionPromptV2(_message.Message):
    __slots__ = ("title", "description", "risk", "safe_request_json", "input_schema_ref", "safe_input_schema_json", "safe_validation_error", "deadline")
    TITLE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    RISK_FIELD_NUMBER: _ClassVar[int]
    SAFE_REQUEST_JSON_FIELD_NUMBER: _ClassVar[int]
    INPUT_SCHEMA_REF_FIELD_NUMBER: _ClassVar[int]
    SAFE_INPUT_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    SAFE_VALIDATION_ERROR_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    title: str
    description: str
    risk: InteractionRiskSummaryV2
    safe_request_json: bytes
    input_schema_ref: str
    safe_input_schema_json: bytes
    safe_validation_error: str
    deadline: _timestamp_pb2.Timestamp
    def __init__(self, title: _Optional[str] = ..., description: _Optional[str] = ..., risk: _Optional[_Union[InteractionRiskSummaryV2, _Mapping]] = ..., safe_request_json: _Optional[bytes] = ..., input_schema_ref: _Optional[str] = ..., safe_input_schema_json: _Optional[bytes] = ..., safe_validation_error: _Optional[str] = ..., deadline: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ApprovalPresentationV2(_message.Message):
    __slots__ = ("prompt", "allowed_decisions")
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    prompt: SafeInteractionPromptV2
    allowed_decisions: _containers.RepeatedScalarFieldContainer[InteractionDecisionKindV2]
    def __init__(self, prompt: _Optional[_Union[SafeInteractionPromptV2, _Mapping]] = ..., allowed_decisions: _Optional[_Iterable[_Union[InteractionDecisionKindV2, str]]] = ...) -> None: ...

class QuestionPresentationV2(_message.Message):
    __slots__ = ("prompt", "allowed_decisions")
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    prompt: SafeInteractionPromptV2
    allowed_decisions: _containers.RepeatedScalarFieldContainer[InteractionDecisionKindV2]
    def __init__(self, prompt: _Optional[_Union[SafeInteractionPromptV2, _Mapping]] = ..., allowed_decisions: _Optional[_Iterable[_Union[InteractionDecisionKindV2, str]]] = ...) -> None: ...

class StructuredInputPresentationV2(_message.Message):
    __slots__ = ("prompt", "allowed_decisions")
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    prompt: SafeInteractionPromptV2
    allowed_decisions: _containers.RepeatedScalarFieldContainer[InteractionDecisionKindV2]
    def __init__(self, prompt: _Optional[_Union[SafeInteractionPromptV2, _Mapping]] = ..., allowed_decisions: _Optional[_Iterable[_Union[InteractionDecisionKindV2, str]]] = ...) -> None: ...

class ResultReviewPresentationV2(_message.Message):
    __slots__ = ("prompt", "safe_result_preview", "allowed_decisions")
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    SAFE_RESULT_PREVIEW_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    prompt: SafeInteractionPromptV2
    safe_result_preview: str
    allowed_decisions: _containers.RepeatedScalarFieldContainer[InteractionDecisionKindV2]
    def __init__(self, prompt: _Optional[_Union[SafeInteractionPromptV2, _Mapping]] = ..., safe_result_preview: _Optional[str] = ..., allowed_decisions: _Optional[_Iterable[_Union[InteractionDecisionKindV2, str]]] = ...) -> None: ...

class PlanStepV2(_message.Message):
    __slots__ = ("step_ref", "label", "status")
    STEP_REF_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    step_ref: str
    label: str
    status: PlanStepStatusV2
    def __init__(self, step_ref: _Optional[str] = ..., label: _Optional[str] = ..., status: _Optional[_Union[PlanStepStatusV2, str]] = ...) -> None: ...

class PlanPresentationV2(_message.Message):
    __slots__ = ("summary", "steps", "allowed_decisions", "deadline")
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    summary: str
    steps: _containers.RepeatedCompositeFieldContainer[PlanStepV2]
    allowed_decisions: _containers.RepeatedScalarFieldContainer[InteractionDecisionKindV2]
    deadline: _timestamp_pb2.Timestamp
    def __init__(self, summary: _Optional[str] = ..., steps: _Optional[_Iterable[_Union[PlanStepV2, _Mapping]]] = ..., allowed_decisions: _Optional[_Iterable[_Union[InteractionDecisionKindV2, str]]] = ..., deadline: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InteractionPresentationV2(_message.Message):
    __slots__ = ("approval", "question", "structured_input", "result_review", "plan")
    APPROVAL_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    STRUCTURED_INPUT_FIELD_NUMBER: _ClassVar[int]
    RESULT_REVIEW_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    approval: ApprovalPresentationV2
    question: QuestionPresentationV2
    structured_input: StructuredInputPresentationV2
    result_review: ResultReviewPresentationV2
    plan: PlanPresentationV2
    def __init__(self, approval: _Optional[_Union[ApprovalPresentationV2, _Mapping]] = ..., question: _Optional[_Union[QuestionPresentationV2, _Mapping]] = ..., structured_input: _Optional[_Union[StructuredInputPresentationV2, _Mapping]] = ..., result_review: _Optional[_Union[ResultReviewPresentationV2, _Mapping]] = ..., plan: _Optional[_Union[PlanPresentationV2, _Mapping]] = ...) -> None: ...

class InteractionOwnerRevisionEvidenceV2(_message.Message):
    __slots__ = ("interaction_owner_ref", "owner_revision", "projection_event_ref", "predecessor_projection_event_ref", "predecessor_evidence_sha256", "projection_payload_sha256", "interaction_kind", "application_request_ref", "decision_group_ref", "decision_group_revision", "group_member_ordinal", "required_owner_revision_refs", "pending_frame_digest", "presentation", "state")
    INTERACTION_OWNER_REF_FIELD_NUMBER: _ClassVar[int]
    OWNER_REVISION_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_EVENT_REF_FIELD_NUMBER: _ClassVar[int]
    PREDECESSOR_PROJECTION_EVENT_REF_FIELD_NUMBER: _ClassVar[int]
    PREDECESSOR_EVIDENCE_SHA256_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    INTERACTION_KIND_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_REQUEST_REF_FIELD_NUMBER: _ClassVar[int]
    DECISION_GROUP_REF_FIELD_NUMBER: _ClassVar[int]
    DECISION_GROUP_REVISION_FIELD_NUMBER: _ClassVar[int]
    GROUP_MEMBER_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_OWNER_REVISION_REFS_FIELD_NUMBER: _ClassVar[int]
    PENDING_FRAME_DIGEST_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    interaction_owner_ref: str
    owner_revision: int
    projection_event_ref: str
    predecessor_projection_event_ref: str
    predecessor_evidence_sha256: str
    projection_payload_sha256: str
    interaction_kind: InteractionKindV2
    application_request_ref: str
    decision_group_ref: str
    decision_group_revision: int
    group_member_ordinal: int
    required_owner_revision_refs: _containers.RepeatedCompositeFieldContainer[InteractionOwnerRevisionRefV2]
    pending_frame_digest: str
    presentation: InteractionPresentationV2
    state: InteractionRevisionStateV2
    def __init__(self, interaction_owner_ref: _Optional[str] = ..., owner_revision: _Optional[int] = ..., projection_event_ref: _Optional[str] = ..., predecessor_projection_event_ref: _Optional[str] = ..., predecessor_evidence_sha256: _Optional[str] = ..., projection_payload_sha256: _Optional[str] = ..., interaction_kind: _Optional[_Union[InteractionKindV2, str]] = ..., application_request_ref: _Optional[str] = ..., decision_group_ref: _Optional[str] = ..., decision_group_revision: _Optional[int] = ..., group_member_ordinal: _Optional[int] = ..., required_owner_revision_refs: _Optional[_Iterable[_Union[InteractionOwnerRevisionRefV2, _Mapping]]] = ..., pending_frame_digest: _Optional[str] = ..., presentation: _Optional[_Union[InteractionPresentationV2, _Mapping]] = ..., state: _Optional[_Union[InteractionRevisionStateV2, str]] = ...) -> None: ...

class InteractionGroupRevisionEvidenceV2(_message.Message):
    __slots__ = ("decision_group_ref", "decision_group_revision", "group_projection_ref", "pending_frame_digest", "member_vector_sha256", "members")
    DECISION_GROUP_REF_FIELD_NUMBER: _ClassVar[int]
    DECISION_GROUP_REVISION_FIELD_NUMBER: _ClassVar[int]
    GROUP_PROJECTION_REF_FIELD_NUMBER: _ClassVar[int]
    PENDING_FRAME_DIGEST_FIELD_NUMBER: _ClassVar[int]
    MEMBER_VECTOR_SHA256_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    decision_group_ref: str
    decision_group_revision: int
    group_projection_ref: str
    pending_frame_digest: str
    member_vector_sha256: str
    members: _containers.RepeatedCompositeFieldContainer[InteractionOwnerRevisionEvidenceV2]
    def __init__(self, decision_group_ref: _Optional[str] = ..., decision_group_revision: _Optional[int] = ..., group_projection_ref: _Optional[str] = ..., pending_frame_digest: _Optional[str] = ..., member_vector_sha256: _Optional[str] = ..., members: _Optional[_Iterable[_Union[InteractionOwnerRevisionEvidenceV2, _Mapping]]] = ...) -> None: ...

class RunOwnerCompletedEvidenceV2(_message.Message):
    __slots__ = ("execution_context_anchor", "execution_context_digest", "owner_revision")
    EXECUTION_CONTEXT_ANCHOR_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_CONTEXT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    OWNER_REVISION_FIELD_NUMBER: _ClassVar[int]
    execution_context_anchor: str
    execution_context_digest: str
    owner_revision: int
    def __init__(self, execution_context_anchor: _Optional[str] = ..., execution_context_digest: _Optional[str] = ..., owner_revision: _Optional[int] = ...) -> None: ...

class TokenUsageEvidenceV2(_message.Message):
    __slots__ = ("input_tokens", "output_tokens")
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    input_tokens: int
    output_tokens: int
    def __init__(self, input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ...) -> None: ...

class RunCompletedEvidenceV2(_message.Message):
    __slots__ = ("status", "token_usage", "output_high_watermark", "output_digest_sha256")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOKEN_USAGE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_HIGH_WATERMARK_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DIGEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    status: RunCompletedEvidenceStatusV2
    token_usage: TokenUsageEvidenceV2
    output_high_watermark: int
    output_digest_sha256: str
    def __init__(self, status: _Optional[_Union[RunCompletedEvidenceStatusV2, str]] = ..., token_usage: _Optional[_Union[TokenUsageEvidenceV2, _Mapping]] = ..., output_high_watermark: _Optional[int] = ..., output_digest_sha256: _Optional[str] = ...) -> None: ...

class RunFailedEvidenceV2(_message.Message):
    __slots__ = ("code", "error_kind", "message", "output_high_watermark", "output_digest_sha256")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_KIND_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_HIGH_WATERMARK_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DIGEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    code: str
    error_kind: str
    message: str
    output_high_watermark: int
    output_digest_sha256: str
    def __init__(self, code: _Optional[str] = ..., error_kind: _Optional[str] = ..., message: _Optional[str] = ..., output_high_watermark: _Optional[int] = ..., output_digest_sha256: _Optional[str] = ...) -> None: ...

class DurableOutputPayloadV2(_message.Message):
    __slots__ = ("text_delta", "text_snapshot", "safe_reasoning_summary", "tool_started", "tool_finished", "plan_progress", "subagent_progress", "media_operation_reference", "notice", "error")
    TEXT_DELTA_FIELD_NUMBER: _ClassVar[int]
    TEXT_SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    SAFE_REASONING_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    TOOL_STARTED_FIELD_NUMBER: _ClassVar[int]
    TOOL_FINISHED_FIELD_NUMBER: _ClassVar[int]
    PLAN_PROGRESS_FIELD_NUMBER: _ClassVar[int]
    SUBAGENT_PROGRESS_FIELD_NUMBER: _ClassVar[int]
    MEDIA_OPERATION_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    NOTICE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    text_delta: TextDeltaOutputV2
    text_snapshot: TextSnapshotOutputV2
    safe_reasoning_summary: SafeReasoningSummaryOutputV2
    tool_started: ToolStartedOutputV2
    tool_finished: ToolFinishedOutputV2
    plan_progress: PlanProgressOutputV2
    subagent_progress: SubagentProgressOutputV2
    media_operation_reference: MediaOperationReferenceOutputV2
    notice: NoticeOutputV2
    error: ErrorOutputV2
    def __init__(self, text_delta: _Optional[_Union[TextDeltaOutputV2, _Mapping]] = ..., text_snapshot: _Optional[_Union[TextSnapshotOutputV2, _Mapping]] = ..., safe_reasoning_summary: _Optional[_Union[SafeReasoningSummaryOutputV2, _Mapping]] = ..., tool_started: _Optional[_Union[ToolStartedOutputV2, _Mapping]] = ..., tool_finished: _Optional[_Union[ToolFinishedOutputV2, _Mapping]] = ..., plan_progress: _Optional[_Union[PlanProgressOutputV2, _Mapping]] = ..., subagent_progress: _Optional[_Union[SubagentProgressOutputV2, _Mapping]] = ..., media_operation_reference: _Optional[_Union[MediaOperationReferenceOutputV2, _Mapping]] = ..., notice: _Optional[_Union[NoticeOutputV2, _Mapping]] = ..., error: _Optional[_Union[ErrorOutputV2, _Mapping]] = ...) -> None: ...

class TextDeltaOutputV2(_message.Message):
    __slots__ = ("part_ref", "delta")
    PART_REF_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    part_ref: str
    delta: str
    def __init__(self, part_ref: _Optional[str] = ..., delta: _Optional[str] = ...) -> None: ...

class TextSnapshotOutputV2(_message.Message):
    __slots__ = ("part_ref", "text", "replaces_through_output_seq")
    PART_REF_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    REPLACES_THROUGH_OUTPUT_SEQ_FIELD_NUMBER: _ClassVar[int]
    part_ref: str
    text: str
    replaces_through_output_seq: int
    def __init__(self, part_ref: _Optional[str] = ..., text: _Optional[str] = ..., replaces_through_output_seq: _Optional[int] = ...) -> None: ...

class SafeReasoningSummaryOutputV2(_message.Message):
    __slots__ = ("part_ref", "safe_summary")
    PART_REF_FIELD_NUMBER: _ClassVar[int]
    SAFE_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    part_ref: str
    safe_summary: str
    def __init__(self, part_ref: _Optional[str] = ..., safe_summary: _Optional[str] = ...) -> None: ...

class ToolStartedOutputV2(_message.Message):
    __slots__ = ("tool_call_ref", "tool_label", "redacted_input_summary_json")
    TOOL_CALL_REF_FIELD_NUMBER: _ClassVar[int]
    TOOL_LABEL_FIELD_NUMBER: _ClassVar[int]
    REDACTED_INPUT_SUMMARY_JSON_FIELD_NUMBER: _ClassVar[int]
    tool_call_ref: str
    tool_label: str
    redacted_input_summary_json: bytes
    def __init__(self, tool_call_ref: _Optional[str] = ..., tool_label: _Optional[str] = ..., redacted_input_summary_json: _Optional[bytes] = ...) -> None: ...

class ToolFinishedOutputV2(_message.Message):
    __slots__ = ("tool_call_ref", "safe_result_preview", "is_error", "truncated")
    TOOL_CALL_REF_FIELD_NUMBER: _ClassVar[int]
    SAFE_RESULT_PREVIEW_FIELD_NUMBER: _ClassVar[int]
    IS_ERROR_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    tool_call_ref: str
    safe_result_preview: str
    is_error: bool
    truncated: bool
    def __init__(self, tool_call_ref: _Optional[str] = ..., safe_result_preview: _Optional[str] = ..., is_error: _Optional[bool] = ..., truncated: _Optional[bool] = ...) -> None: ...

class PlanProgressOutputV2(_message.Message):
    __slots__ = ("plan_ref", "safe_summary", "steps")
    PLAN_REF_FIELD_NUMBER: _ClassVar[int]
    SAFE_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    plan_ref: str
    safe_summary: str
    steps: _containers.RepeatedCompositeFieldContainer[PlanStepV2]
    def __init__(self, plan_ref: _Optional[str] = ..., safe_summary: _Optional[str] = ..., steps: _Optional[_Iterable[_Union[PlanStepV2, _Mapping]]] = ...) -> None: ...

class SubagentProgressOutputV2(_message.Message):
    __slots__ = ("subagent_ref", "status", "safe_summary")
    SUBAGENT_REF_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SAFE_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    subagent_ref: str
    status: SubagentProgressStatusV2
    safe_summary: str
    def __init__(self, subagent_ref: _Optional[str] = ..., status: _Optional[_Union[SubagentProgressStatusV2, str]] = ..., safe_summary: _Optional[str] = ...) -> None: ...

class MediaOperationReferenceOutputV2(_message.Message):
    __slots__ = ("stable_output_slot_ref", "agent_media_command_ref", "operation_ref")
    STABLE_OUTPUT_SLOT_REF_FIELD_NUMBER: _ClassVar[int]
    AGENT_MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    OPERATION_REF_FIELD_NUMBER: _ClassVar[int]
    stable_output_slot_ref: str
    agent_media_command_ref: str
    operation_ref: str
    def __init__(self, stable_output_slot_ref: _Optional[str] = ..., agent_media_command_ref: _Optional[str] = ..., operation_ref: _Optional[str] = ...) -> None: ...

class NoticeOutputV2(_message.Message):
    __slots__ = ("notice_ref", "code", "message", "severity", "retry_class")
    NOTICE_REF_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    RETRY_CLASS_FIELD_NUMBER: _ClassVar[int]
    notice_ref: str
    code: str
    message: str
    severity: NoticeSeverityV2
    retry_class: OutputRetryClassV2
    def __init__(self, notice_ref: _Optional[str] = ..., code: _Optional[str] = ..., message: _Optional[str] = ..., severity: _Optional[_Union[NoticeSeverityV2, str]] = ..., retry_class: _Optional[_Union[OutputRetryClassV2, str]] = ...) -> None: ...

class ErrorOutputV2(_message.Message):
    __slots__ = ("error_ref", "code", "message", "retry_class")
    ERROR_REF_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRY_CLASS_FIELD_NUMBER: _ClassVar[int]
    error_ref: str
    code: str
    message: str
    retry_class: OutputRetryClassV2
    def __init__(self, error_ref: _Optional[str] = ..., code: _Optional[str] = ..., message: _Optional[str] = ..., retry_class: _Optional[_Union[OutputRetryClassV2, str]] = ...) -> None: ...

class DurableOutputRecordV2(_message.Message):
    __slots__ = ("output_ref", "output_version", "run_id", "output_seq", "canonical_payload", "payload_sha256", "recorded_at", "producer_instance_ref", "producer_generation")
    OUTPUT_REF_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_SEQ_FIELD_NUMBER: _ClassVar[int]
    CANONICAL_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_INSTANCE_REF_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_GENERATION_FIELD_NUMBER: _ClassVar[int]
    output_ref: str
    output_version: int
    run_id: str
    output_seq: int
    canonical_payload: bytes
    payload_sha256: str
    recorded_at: _timestamp_pb2.Timestamp
    producer_instance_ref: str
    producer_generation: int
    def __init__(self, output_ref: _Optional[str] = ..., output_version: _Optional[int] = ..., run_id: _Optional[str] = ..., output_seq: _Optional[int] = ..., canonical_payload: _Optional[bytes] = ..., payload_sha256: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., producer_instance_ref: _Optional[str] = ..., producer_generation: _Optional[int] = ...) -> None: ...

class PullDurableOutputRecordsRequest(_message.Message):
    __slots__ = ("run_id", "after_output_seq", "page_size")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_OUTPUT_SEQ_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    after_output_seq: int
    page_size: int
    def __init__(self, run_id: _Optional[str] = ..., after_output_seq: _Optional[int] = ..., page_size: _Optional[int] = ...) -> None: ...

class PullDurableOutputRecordsResponse(_message.Message):
    __slots__ = ("records", "next_after_output_seq", "has_more")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    NEXT_AFTER_OUTPUT_SEQ_FIELD_NUMBER: _ClassVar[int]
    HAS_MORE_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[DurableOutputRecordV2]
    next_after_output_seq: int
    has_more: bool
    def __init__(self, records: _Optional[_Iterable[_Union[DurableOutputRecordV2, _Mapping]]] = ..., next_after_output_seq: _Optional[int] = ..., has_more: _Optional[bool] = ...) -> None: ...

class DurableExecutionEvidenceV2(_message.Message):
    __slots__ = ("evidence_ref", "evidence_version", "run_id", "durable_seq", "event_id", "kind", "canonical_payload", "payload_sha256", "recorded_at", "producer_instance_ref", "producer_generation", "dispatch_id", "assistant_message_id", "evidence_sha256", "interaction_protocol_release_epoch")
    EVIDENCE_REF_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    DURABLE_SEQ_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    CANONICAL_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_INSTANCE_REF_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_GENERATION_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_ID_FIELD_NUMBER: _ClassVar[int]
    ASSISTANT_MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_SHA256_FIELD_NUMBER: _ClassVar[int]
    INTERACTION_PROTOCOL_RELEASE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    evidence_ref: str
    evidence_version: int
    run_id: str
    durable_seq: int
    event_id: str
    kind: DurableExecutionEvidenceKindV2
    canonical_payload: bytes
    payload_sha256: str
    recorded_at: _timestamp_pb2.Timestamp
    producer_instance_ref: str
    producer_generation: int
    dispatch_id: str
    assistant_message_id: str
    evidence_sha256: str
    interaction_protocol_release_epoch: str
    def __init__(self, evidence_ref: _Optional[str] = ..., evidence_version: _Optional[int] = ..., run_id: _Optional[str] = ..., durable_seq: _Optional[int] = ..., event_id: _Optional[str] = ..., kind: _Optional[_Union[DurableExecutionEvidenceKindV2, str]] = ..., canonical_payload: _Optional[bytes] = ..., payload_sha256: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., producer_instance_ref: _Optional[str] = ..., producer_generation: _Optional[int] = ..., dispatch_id: _Optional[str] = ..., assistant_message_id: _Optional[str] = ..., evidence_sha256: _Optional[str] = ..., interaction_protocol_release_epoch: _Optional[str] = ...) -> None: ...

class PullDurableExecutionEvidenceRequest(_message.Message):
    __slots__ = ("run_id", "after_durable_seq", "page_size")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_DURABLE_SEQ_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    after_durable_seq: int
    page_size: int
    def __init__(self, run_id: _Optional[str] = ..., after_durable_seq: _Optional[int] = ..., page_size: _Optional[int] = ...) -> None: ...

class PullDurableExecutionEvidenceResponse(_message.Message):
    __slots__ = ("evidence", "next_after_durable_seq", "has_more")
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    NEXT_AFTER_DURABLE_SEQ_FIELD_NUMBER: _ClassVar[int]
    HAS_MORE_FIELD_NUMBER: _ClassVar[int]
    evidence: _containers.RepeatedCompositeFieldContainer[DurableExecutionEvidenceV2]
    next_after_durable_seq: int
    has_more: bool
    def __init__(self, evidence: _Optional[_Iterable[_Union[DurableExecutionEvidenceV2, _Mapping]]] = ..., next_after_durable_seq: _Optional[int] = ..., has_more: _Optional[bool] = ...) -> None: ...

class GetDurableExecutionEvidenceRequest(_message.Message):
    __slots__ = ("run_id", "evidence_ref")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_REF_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    evidence_ref: str
    def __init__(self, run_id: _Optional[str] = ..., evidence_ref: _Optional[str] = ...) -> None: ...

class DurableExecutionEvidenceNotFoundV2(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetDurableExecutionEvidenceResponse(_message.Message):
    __slots__ = ("evidence", "not_found")
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    evidence: DurableExecutionEvidenceV2
    not_found: DurableExecutionEvidenceNotFoundV2
    def __init__(self, evidence: _Optional[_Union[DurableExecutionEvidenceV2, _Mapping]] = ..., not_found: _Optional[_Union[DurableExecutionEvidenceNotFoundV2, _Mapping]] = ...) -> None: ...

class GetRunDurableCheckpointRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunDurableCheckpointResponse(_message.Message):
    __slots__ = ("evidence", "not_found")
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    evidence: DurableExecutionEvidenceV2
    not_found: DurableExecutionEvidenceNotFoundV2
    def __init__(self, evidence: _Optional[_Union[DurableExecutionEvidenceV2, _Mapping]] = ..., not_found: _Optional[_Union[DurableExecutionEvidenceNotFoundV2, _Mapping]] = ...) -> None: ...
