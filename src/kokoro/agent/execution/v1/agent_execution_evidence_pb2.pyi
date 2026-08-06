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

class DurableExecutionEvidenceKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DURABLE_EXECUTION_EVIDENCE_KIND_UNSPECIFIED: _ClassVar[DurableExecutionEvidenceKind]
    DURABLE_EXECUTION_EVIDENCE_KIND_RUN_STARTED: _ClassVar[DurableExecutionEvidenceKind]
    DURABLE_EXECUTION_EVIDENCE_KIND_ACTION_OWNER: _ClassVar[DurableExecutionEvidenceKind]
    DURABLE_EXECUTION_EVIDENCE_KIND_PLAN_OWNER: _ClassVar[DurableExecutionEvidenceKind]
    DURABLE_EXECUTION_EVIDENCE_KIND_RUN_OWNER_COMPLETED: _ClassVar[DurableExecutionEvidenceKind]
    DURABLE_EXECUTION_EVIDENCE_KIND_RUN_COMPLETED: _ClassVar[DurableExecutionEvidenceKind]
    DURABLE_EXECUTION_EVIDENCE_KIND_RUN_FAILED: _ClassVar[DurableExecutionEvidenceKind]

class ActionAwaitingKindV1(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACTION_AWAITING_KIND_V1_UNSPECIFIED: _ClassVar[ActionAwaitingKindV1]
    ACTION_AWAITING_KIND_V1_TOOL_APPROVAL: _ClassVar[ActionAwaitingKindV1]
    ACTION_AWAITING_KIND_V1_ASK_USER_QUESTION: _ClassVar[ActionAwaitingKindV1]
    ACTION_AWAITING_KIND_V1_RESULT_REVIEW: _ClassVar[ActionAwaitingKindV1]
    ACTION_AWAITING_KIND_V1_INPUT: _ClassVar[ActionAwaitingKindV1]

class ActionDecisionV1(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACTION_DECISION_V1_UNSPECIFIED: _ClassVar[ActionDecisionV1]
    ACTION_DECISION_V1_APPROVE: _ClassVar[ActionDecisionV1]
    ACTION_DECISION_V1_EDIT: _ClassVar[ActionDecisionV1]
    ACTION_DECISION_V1_REJECT: _ClassVar[ActionDecisionV1]
    ACTION_DECISION_V1_RESPOND: _ClassVar[ActionDecisionV1]
    ACTION_DECISION_V1_SUBMIT: _ClassVar[ActionDecisionV1]

class PlanStepStatusV1(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLAN_STEP_STATUS_V1_UNSPECIFIED: _ClassVar[PlanStepStatusV1]
    PLAN_STEP_STATUS_V1_PENDING: _ClassVar[PlanStepStatusV1]
    PLAN_STEP_STATUS_V1_IN_PROGRESS: _ClassVar[PlanStepStatusV1]
    PLAN_STEP_STATUS_V1_COMPLETED: _ClassVar[PlanStepStatusV1]

class PlanDecisionV1(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLAN_DECISION_V1_UNSPECIFIED: _ClassVar[PlanDecisionV1]
    PLAN_DECISION_V1_ACCEPT: _ClassVar[PlanDecisionV1]
    PLAN_DECISION_V1_REJECT: _ClassVar[PlanDecisionV1]

class RunCompletedEvidenceStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_COMPLETED_EVIDENCE_STATUS_UNSPECIFIED: _ClassVar[RunCompletedEvidenceStatus]
    RUN_COMPLETED_EVIDENCE_STATUS_COMPLETED: _ClassVar[RunCompletedEvidenceStatus]
    RUN_COMPLETED_EVIDENCE_STATUS_CANCELLED: _ClassVar[RunCompletedEvidenceStatus]

class SubagentProgressStatusV1(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SUBAGENT_PROGRESS_STATUS_V1_UNSPECIFIED: _ClassVar[SubagentProgressStatusV1]
    SUBAGENT_PROGRESS_STATUS_V1_PENDING: _ClassVar[SubagentProgressStatusV1]
    SUBAGENT_PROGRESS_STATUS_V1_RUNNING: _ClassVar[SubagentProgressStatusV1]
    SUBAGENT_PROGRESS_STATUS_V1_COMPLETED: _ClassVar[SubagentProgressStatusV1]
    SUBAGENT_PROGRESS_STATUS_V1_FAILED: _ClassVar[SubagentProgressStatusV1]
    SUBAGENT_PROGRESS_STATUS_V1_CANCELED: _ClassVar[SubagentProgressStatusV1]

class NoticeSeverityV1(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTICE_SEVERITY_V1_UNSPECIFIED: _ClassVar[NoticeSeverityV1]
    NOTICE_SEVERITY_V1_INFO: _ClassVar[NoticeSeverityV1]
    NOTICE_SEVERITY_V1_WARNING: _ClassVar[NoticeSeverityV1]

class OutputRetryClassV1(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTPUT_RETRY_CLASS_V1_UNSPECIFIED: _ClassVar[OutputRetryClassV1]
    OUTPUT_RETRY_CLASS_V1_NEVER: _ClassVar[OutputRetryClassV1]
    OUTPUT_RETRY_CLASS_V1_IMMEDIATE: _ClassVar[OutputRetryClassV1]
    OUTPUT_RETRY_CLASS_V1_AFTER_DELAY: _ClassVar[OutputRetryClassV1]
    OUTPUT_RETRY_CLASS_V1_AFTER_USER_ACTION: _ClassVar[OutputRetryClassV1]
    OUTPUT_RETRY_CLASS_V1_RECONCILE_RECEIPT: _ClassVar[OutputRetryClassV1]
DURABLE_EXECUTION_EVIDENCE_KIND_UNSPECIFIED: DurableExecutionEvidenceKind
DURABLE_EXECUTION_EVIDENCE_KIND_RUN_STARTED: DurableExecutionEvidenceKind
DURABLE_EXECUTION_EVIDENCE_KIND_ACTION_OWNER: DurableExecutionEvidenceKind
DURABLE_EXECUTION_EVIDENCE_KIND_PLAN_OWNER: DurableExecutionEvidenceKind
DURABLE_EXECUTION_EVIDENCE_KIND_RUN_OWNER_COMPLETED: DurableExecutionEvidenceKind
DURABLE_EXECUTION_EVIDENCE_KIND_RUN_COMPLETED: DurableExecutionEvidenceKind
DURABLE_EXECUTION_EVIDENCE_KIND_RUN_FAILED: DurableExecutionEvidenceKind
ACTION_AWAITING_KIND_V1_UNSPECIFIED: ActionAwaitingKindV1
ACTION_AWAITING_KIND_V1_TOOL_APPROVAL: ActionAwaitingKindV1
ACTION_AWAITING_KIND_V1_ASK_USER_QUESTION: ActionAwaitingKindV1
ACTION_AWAITING_KIND_V1_RESULT_REVIEW: ActionAwaitingKindV1
ACTION_AWAITING_KIND_V1_INPUT: ActionAwaitingKindV1
ACTION_DECISION_V1_UNSPECIFIED: ActionDecisionV1
ACTION_DECISION_V1_APPROVE: ActionDecisionV1
ACTION_DECISION_V1_EDIT: ActionDecisionV1
ACTION_DECISION_V1_REJECT: ActionDecisionV1
ACTION_DECISION_V1_RESPOND: ActionDecisionV1
ACTION_DECISION_V1_SUBMIT: ActionDecisionV1
PLAN_STEP_STATUS_V1_UNSPECIFIED: PlanStepStatusV1
PLAN_STEP_STATUS_V1_PENDING: PlanStepStatusV1
PLAN_STEP_STATUS_V1_IN_PROGRESS: PlanStepStatusV1
PLAN_STEP_STATUS_V1_COMPLETED: PlanStepStatusV1
PLAN_DECISION_V1_UNSPECIFIED: PlanDecisionV1
PLAN_DECISION_V1_ACCEPT: PlanDecisionV1
PLAN_DECISION_V1_REJECT: PlanDecisionV1
RUN_COMPLETED_EVIDENCE_STATUS_UNSPECIFIED: RunCompletedEvidenceStatus
RUN_COMPLETED_EVIDENCE_STATUS_COMPLETED: RunCompletedEvidenceStatus
RUN_COMPLETED_EVIDENCE_STATUS_CANCELLED: RunCompletedEvidenceStatus
SUBAGENT_PROGRESS_STATUS_V1_UNSPECIFIED: SubagentProgressStatusV1
SUBAGENT_PROGRESS_STATUS_V1_PENDING: SubagentProgressStatusV1
SUBAGENT_PROGRESS_STATUS_V1_RUNNING: SubagentProgressStatusV1
SUBAGENT_PROGRESS_STATUS_V1_COMPLETED: SubagentProgressStatusV1
SUBAGENT_PROGRESS_STATUS_V1_FAILED: SubagentProgressStatusV1
SUBAGENT_PROGRESS_STATUS_V1_CANCELED: SubagentProgressStatusV1
NOTICE_SEVERITY_V1_UNSPECIFIED: NoticeSeverityV1
NOTICE_SEVERITY_V1_INFO: NoticeSeverityV1
NOTICE_SEVERITY_V1_WARNING: NoticeSeverityV1
OUTPUT_RETRY_CLASS_V1_UNSPECIFIED: OutputRetryClassV1
OUTPUT_RETRY_CLASS_V1_NEVER: OutputRetryClassV1
OUTPUT_RETRY_CLASS_V1_IMMEDIATE: OutputRetryClassV1
OUTPUT_RETRY_CLASS_V1_AFTER_DELAY: OutputRetryClassV1
OUTPUT_RETRY_CLASS_V1_AFTER_USER_ACTION: OutputRetryClassV1
OUTPUT_RETRY_CLASS_V1_RECONCILE_RECEIPT: OutputRetryClassV1

class DurableExecutionCanonicalPayloadV1(_message.Message):
    __slots__ = ("run_started", "action_owner", "plan_owner", "run_owner_completed", "run_completed", "run_failed")
    RUN_STARTED_FIELD_NUMBER: _ClassVar[int]
    ACTION_OWNER_FIELD_NUMBER: _ClassVar[int]
    PLAN_OWNER_FIELD_NUMBER: _ClassVar[int]
    RUN_OWNER_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    RUN_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    RUN_FAILED_FIELD_NUMBER: _ClassVar[int]
    run_started: RunStartedEvidenceV1
    action_owner: ActionOwnerEvidenceV1
    plan_owner: PlanOwnerEvidenceV1
    run_owner_completed: RunOwnerCompletedEvidenceV1
    run_completed: RunCompletedEvidenceV1
    run_failed: RunFailedEvidenceV1
    def __init__(self, run_started: _Optional[_Union[RunStartedEvidenceV1, _Mapping]] = ..., action_owner: _Optional[_Union[ActionOwnerEvidenceV1, _Mapping]] = ..., plan_owner: _Optional[_Union[PlanOwnerEvidenceV1, _Mapping]] = ..., run_owner_completed: _Optional[_Union[RunOwnerCompletedEvidenceV1, _Mapping]] = ..., run_completed: _Optional[_Union[RunCompletedEvidenceV1, _Mapping]] = ..., run_failed: _Optional[_Union[RunFailedEvidenceV1, _Mapping]] = ...) -> None: ...

class RunStartedEvidenceV1(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ActionRiskSummaryV1(_message.Message):
    __slots__ = ("level", "source", "reason")
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    level: str
    source: str
    reason: str
    def __init__(self, level: _Optional[str] = ..., source: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class ActionOwnerEvidenceV1(_message.Message):
    __slots__ = ("owner_ref", "owner_version", "segment_id", "action_name", "awaiting_kind", "action_payload_sha256", "description", "allowed_decisions", "pending_owner_refs", "editable", "risk", "safe_request_json", "input_schema_ref", "safe_input_schema_json", "safe_result_preview")
    OWNER_REF_FIELD_NUMBER: _ClassVar[int]
    OWNER_VERSION_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_NAME_FIELD_NUMBER: _ClassVar[int]
    AWAITING_KIND_FIELD_NUMBER: _ClassVar[int]
    ACTION_PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    PENDING_OWNER_REFS_FIELD_NUMBER: _ClassVar[int]
    EDITABLE_FIELD_NUMBER: _ClassVar[int]
    RISK_FIELD_NUMBER: _ClassVar[int]
    SAFE_REQUEST_JSON_FIELD_NUMBER: _ClassVar[int]
    INPUT_SCHEMA_REF_FIELD_NUMBER: _ClassVar[int]
    SAFE_INPUT_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    SAFE_RESULT_PREVIEW_FIELD_NUMBER: _ClassVar[int]
    owner_ref: str
    owner_version: int
    segment_id: str
    action_name: str
    awaiting_kind: ActionAwaitingKindV1
    action_payload_sha256: str
    description: str
    allowed_decisions: _containers.RepeatedScalarFieldContainer[ActionDecisionV1]
    pending_owner_refs: _containers.RepeatedScalarFieldContainer[str]
    editable: bool
    risk: ActionRiskSummaryV1
    safe_request_json: bytes
    input_schema_ref: str
    safe_input_schema_json: bytes
    safe_result_preview: str
    def __init__(self, owner_ref: _Optional[str] = ..., owner_version: _Optional[int] = ..., segment_id: _Optional[str] = ..., action_name: _Optional[str] = ..., awaiting_kind: _Optional[_Union[ActionAwaitingKindV1, str]] = ..., action_payload_sha256: _Optional[str] = ..., description: _Optional[str] = ..., allowed_decisions: _Optional[_Iterable[_Union[ActionDecisionV1, str]]] = ..., pending_owner_refs: _Optional[_Iterable[str]] = ..., editable: _Optional[bool] = ..., risk: _Optional[_Union[ActionRiskSummaryV1, _Mapping]] = ..., safe_request_json: _Optional[bytes] = ..., input_schema_ref: _Optional[str] = ..., safe_input_schema_json: _Optional[bytes] = ..., safe_result_preview: _Optional[str] = ...) -> None: ...

class PlanStepV1(_message.Message):
    __slots__ = ("step_ref", "label", "status")
    STEP_REF_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    step_ref: str
    label: str
    status: PlanStepStatusV1
    def __init__(self, step_ref: _Optional[str] = ..., label: _Optional[str] = ..., status: _Optional[_Union[PlanStepStatusV1, str]] = ...) -> None: ...

class PlanOwnerEvidenceV1(_message.Message):
    __slots__ = ("owner_ref", "owner_version", "segment_id", "proposal_payload_sha256", "summary", "steps", "allowed_decisions")
    OWNER_REF_FIELD_NUMBER: _ClassVar[int]
    OWNER_VERSION_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    PROPOSAL_PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    owner_ref: str
    owner_version: int
    segment_id: str
    proposal_payload_sha256: str
    summary: str
    steps: _containers.RepeatedCompositeFieldContainer[PlanStepV1]
    allowed_decisions: _containers.RepeatedScalarFieldContainer[PlanDecisionV1]
    def __init__(self, owner_ref: _Optional[str] = ..., owner_version: _Optional[int] = ..., segment_id: _Optional[str] = ..., proposal_payload_sha256: _Optional[str] = ..., summary: _Optional[str] = ..., steps: _Optional[_Iterable[_Union[PlanStepV1, _Mapping]]] = ..., allowed_decisions: _Optional[_Iterable[_Union[PlanDecisionV1, str]]] = ...) -> None: ...

class RunOwnerCompletedEvidenceV1(_message.Message):
    __slots__ = ("execution_context_anchor", "execution_context_digest", "owner_revision")
    EXECUTION_CONTEXT_ANCHOR_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_CONTEXT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    OWNER_REVISION_FIELD_NUMBER: _ClassVar[int]
    execution_context_anchor: str
    execution_context_digest: str
    owner_revision: int
    def __init__(self, execution_context_anchor: _Optional[str] = ..., execution_context_digest: _Optional[str] = ..., owner_revision: _Optional[int] = ...) -> None: ...

class RunCompletedEvidenceV1(_message.Message):
    __slots__ = ("status", "token_usage", "output_high_watermark", "output_digest_sha256")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOKEN_USAGE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_HIGH_WATERMARK_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DIGEST_SHA256_FIELD_NUMBER: _ClassVar[int]
    status: RunCompletedEvidenceStatus
    token_usage: TokenUsageEvidenceV1
    output_high_watermark: int
    output_digest_sha256: str
    def __init__(self, status: _Optional[_Union[RunCompletedEvidenceStatus, str]] = ..., token_usage: _Optional[_Union[TokenUsageEvidenceV1, _Mapping]] = ..., output_high_watermark: _Optional[int] = ..., output_digest_sha256: _Optional[str] = ...) -> None: ...

class TokenUsageEvidenceV1(_message.Message):
    __slots__ = ("input_tokens", "output_tokens")
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    input_tokens: int
    output_tokens: int
    def __init__(self, input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ...) -> None: ...

class RunFailedEvidenceV1(_message.Message):
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

class DurableOutputPayloadV1(_message.Message):
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
    text_delta: TextDeltaOutputV1
    text_snapshot: TextSnapshotOutputV1
    safe_reasoning_summary: SafeReasoningSummaryOutputV1
    tool_started: ToolStartedOutputV1
    tool_finished: ToolFinishedOutputV1
    plan_progress: PlanProgressOutputV1
    subagent_progress: SubagentProgressOutputV1
    media_operation_reference: MediaOperationReferenceOutputV1
    notice: NoticeOutputV1
    error: ErrorOutputV1
    def __init__(self, text_delta: _Optional[_Union[TextDeltaOutputV1, _Mapping]] = ..., text_snapshot: _Optional[_Union[TextSnapshotOutputV1, _Mapping]] = ..., safe_reasoning_summary: _Optional[_Union[SafeReasoningSummaryOutputV1, _Mapping]] = ..., tool_started: _Optional[_Union[ToolStartedOutputV1, _Mapping]] = ..., tool_finished: _Optional[_Union[ToolFinishedOutputV1, _Mapping]] = ..., plan_progress: _Optional[_Union[PlanProgressOutputV1, _Mapping]] = ..., subagent_progress: _Optional[_Union[SubagentProgressOutputV1, _Mapping]] = ..., media_operation_reference: _Optional[_Union[MediaOperationReferenceOutputV1, _Mapping]] = ..., notice: _Optional[_Union[NoticeOutputV1, _Mapping]] = ..., error: _Optional[_Union[ErrorOutputV1, _Mapping]] = ...) -> None: ...

class TextDeltaOutputV1(_message.Message):
    __slots__ = ("part_ref", "delta")
    PART_REF_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    part_ref: str
    delta: str
    def __init__(self, part_ref: _Optional[str] = ..., delta: _Optional[str] = ...) -> None: ...

class TextSnapshotOutputV1(_message.Message):
    __slots__ = ("part_ref", "text", "replaces_through_output_seq")
    PART_REF_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    REPLACES_THROUGH_OUTPUT_SEQ_FIELD_NUMBER: _ClassVar[int]
    part_ref: str
    text: str
    replaces_through_output_seq: int
    def __init__(self, part_ref: _Optional[str] = ..., text: _Optional[str] = ..., replaces_through_output_seq: _Optional[int] = ...) -> None: ...

class SafeReasoningSummaryOutputV1(_message.Message):
    __slots__ = ("part_ref", "safe_summary")
    PART_REF_FIELD_NUMBER: _ClassVar[int]
    SAFE_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    part_ref: str
    safe_summary: str
    def __init__(self, part_ref: _Optional[str] = ..., safe_summary: _Optional[str] = ...) -> None: ...

class ToolStartedOutputV1(_message.Message):
    __slots__ = ("tool_call_id", "tool_label", "redacted_input_summary_json")
    TOOL_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    TOOL_LABEL_FIELD_NUMBER: _ClassVar[int]
    REDACTED_INPUT_SUMMARY_JSON_FIELD_NUMBER: _ClassVar[int]
    tool_call_id: str
    tool_label: str
    redacted_input_summary_json: bytes
    def __init__(self, tool_call_id: _Optional[str] = ..., tool_label: _Optional[str] = ..., redacted_input_summary_json: _Optional[bytes] = ...) -> None: ...

class ToolFinishedOutputV1(_message.Message):
    __slots__ = ("tool_call_id", "safe_result_preview", "is_error", "truncated")
    TOOL_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    SAFE_RESULT_PREVIEW_FIELD_NUMBER: _ClassVar[int]
    IS_ERROR_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    tool_call_id: str
    safe_result_preview: str
    is_error: bool
    truncated: bool
    def __init__(self, tool_call_id: _Optional[str] = ..., safe_result_preview: _Optional[str] = ..., is_error: _Optional[bool] = ..., truncated: _Optional[bool] = ...) -> None: ...

class PlanProgressOutputV1(_message.Message):
    __slots__ = ("plan_ref", "safe_summary", "steps")
    PLAN_REF_FIELD_NUMBER: _ClassVar[int]
    SAFE_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    plan_ref: str
    safe_summary: str
    steps: _containers.RepeatedCompositeFieldContainer[PlanStepV1]
    def __init__(self, plan_ref: _Optional[str] = ..., safe_summary: _Optional[str] = ..., steps: _Optional[_Iterable[_Union[PlanStepV1, _Mapping]]] = ...) -> None: ...

class SubagentProgressOutputV1(_message.Message):
    __slots__ = ("subagent_ref", "status", "safe_summary")
    SUBAGENT_REF_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SAFE_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    subagent_ref: str
    status: SubagentProgressStatusV1
    safe_summary: str
    def __init__(self, subagent_ref: _Optional[str] = ..., status: _Optional[_Union[SubagentProgressStatusV1, str]] = ..., safe_summary: _Optional[str] = ...) -> None: ...

class MediaOperationReferenceOutputV1(_message.Message):
    __slots__ = ("stable_output_slot_ref", "agent_media_command_ref", "operation_ref")
    STABLE_OUTPUT_SLOT_REF_FIELD_NUMBER: _ClassVar[int]
    AGENT_MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    OPERATION_REF_FIELD_NUMBER: _ClassVar[int]
    stable_output_slot_ref: str
    agent_media_command_ref: str
    operation_ref: str
    def __init__(self, stable_output_slot_ref: _Optional[str] = ..., agent_media_command_ref: _Optional[str] = ..., operation_ref: _Optional[str] = ...) -> None: ...

class NoticeOutputV1(_message.Message):
    __slots__ = ("notice_ref", "code", "message", "severity", "retry_class")
    NOTICE_REF_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    RETRY_CLASS_FIELD_NUMBER: _ClassVar[int]
    notice_ref: str
    code: str
    message: str
    severity: NoticeSeverityV1
    retry_class: OutputRetryClassV1
    def __init__(self, notice_ref: _Optional[str] = ..., code: _Optional[str] = ..., message: _Optional[str] = ..., severity: _Optional[_Union[NoticeSeverityV1, str]] = ..., retry_class: _Optional[_Union[OutputRetryClassV1, str]] = ...) -> None: ...

class ErrorOutputV1(_message.Message):
    __slots__ = ("error_ref", "code", "message", "retry_class")
    ERROR_REF_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRY_CLASS_FIELD_NUMBER: _ClassVar[int]
    error_ref: str
    code: str
    message: str
    retry_class: OutputRetryClassV1
    def __init__(self, error_ref: _Optional[str] = ..., code: _Optional[str] = ..., message: _Optional[str] = ..., retry_class: _Optional[_Union[OutputRetryClassV1, str]] = ...) -> None: ...

class DurableOutputRecord(_message.Message):
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
    records: _containers.RepeatedCompositeFieldContainer[DurableOutputRecord]
    next_after_output_seq: int
    has_more: bool
    def __init__(self, records: _Optional[_Iterable[_Union[DurableOutputRecord, _Mapping]]] = ..., next_after_output_seq: _Optional[int] = ..., has_more: _Optional[bool] = ...) -> None: ...

class DurableExecutionEvidence(_message.Message):
    __slots__ = ("evidence_ref", "evidence_version", "run_id", "durable_seq", "event_id", "kind", "canonical_payload", "payload_sha256", "recorded_at", "producer_instance_ref", "producer_generation")
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
    evidence_ref: str
    evidence_version: int
    run_id: str
    durable_seq: int
    event_id: str
    kind: DurableExecutionEvidenceKind
    canonical_payload: bytes
    payload_sha256: str
    recorded_at: _timestamp_pb2.Timestamp
    producer_instance_ref: str
    producer_generation: int
    def __init__(self, evidence_ref: _Optional[str] = ..., evidence_version: _Optional[int] = ..., run_id: _Optional[str] = ..., durable_seq: _Optional[int] = ..., event_id: _Optional[str] = ..., kind: _Optional[_Union[DurableExecutionEvidenceKind, str]] = ..., canonical_payload: _Optional[bytes] = ..., payload_sha256: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., producer_instance_ref: _Optional[str] = ..., producer_generation: _Optional[int] = ...) -> None: ...

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
    evidence: _containers.RepeatedCompositeFieldContainer[DurableExecutionEvidence]
    next_after_durable_seq: int
    has_more: bool
    def __init__(self, evidence: _Optional[_Iterable[_Union[DurableExecutionEvidence, _Mapping]]] = ..., next_after_durable_seq: _Optional[int] = ..., has_more: _Optional[bool] = ...) -> None: ...

class GetDurableExecutionEvidenceRequest(_message.Message):
    __slots__ = ("run_id", "evidence_ref")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_REF_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    evidence_ref: str
    def __init__(self, run_id: _Optional[str] = ..., evidence_ref: _Optional[str] = ...) -> None: ...

class DurableExecutionEvidenceNotFound(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetDurableExecutionEvidenceResponse(_message.Message):
    __slots__ = ("evidence", "not_found")
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    evidence: DurableExecutionEvidence
    not_found: DurableExecutionEvidenceNotFound
    def __init__(self, evidence: _Optional[_Union[DurableExecutionEvidence, _Mapping]] = ..., not_found: _Optional[_Union[DurableExecutionEvidenceNotFound, _Mapping]] = ...) -> None: ...

class GetRunDurableCheckpointRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunDurableCheckpointResponse(_message.Message):
    __slots__ = ("evidence", "not_found")
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    evidence: DurableExecutionEvidence
    not_found: DurableExecutionEvidenceNotFound
    def __init__(self, evidence: _Optional[_Union[DurableExecutionEvidence, _Mapping]] = ..., not_found: _Optional[_Union[DurableExecutionEvidenceNotFound, _Mapping]] = ...) -> None: ...
