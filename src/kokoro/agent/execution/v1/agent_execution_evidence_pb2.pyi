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

class DurableExecutionCanonicalPayloadV1(_message.Message):
    __slots__ = ()
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
    __slots__ = ()
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    level: str
    source: str
    reason: str
    def __init__(self, level: _Optional[str] = ..., source: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class ActionOwnerEvidenceV1(_message.Message):
    __slots__ = ()
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
    __slots__ = ()
    STEP_REF_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    step_ref: str
    label: str
    status: PlanStepStatusV1
    def __init__(self, step_ref: _Optional[str] = ..., label: _Optional[str] = ..., status: _Optional[_Union[PlanStepStatusV1, str]] = ...) -> None: ...

class PlanOwnerEvidenceV1(_message.Message):
    __slots__ = ()
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
    __slots__ = ()
    EXECUTION_CONTEXT_ANCHOR_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_CONTEXT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    OWNER_REVISION_FIELD_NUMBER: _ClassVar[int]
    execution_context_anchor: str
    execution_context_digest: str
    owner_revision: int
    def __init__(self, execution_context_anchor: _Optional[str] = ..., execution_context_digest: _Optional[str] = ..., owner_revision: _Optional[int] = ...) -> None: ...

class RunCompletedEvidenceV1(_message.Message):
    __slots__ = ()
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOKEN_USAGE_FIELD_NUMBER: _ClassVar[int]
    status: RunCompletedEvidenceStatus
    token_usage: TokenUsageEvidenceV1
    def __init__(self, status: _Optional[_Union[RunCompletedEvidenceStatus, str]] = ..., token_usage: _Optional[_Union[TokenUsageEvidenceV1, _Mapping]] = ...) -> None: ...

class TokenUsageEvidenceV1(_message.Message):
    __slots__ = ()
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    input_tokens: int
    output_tokens: int
    def __init__(self, input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ...) -> None: ...

class RunFailedEvidenceV1(_message.Message):
    __slots__ = ()
    CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_KIND_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: str
    error_kind: str
    message: str
    def __init__(self, code: _Optional[str] = ..., error_kind: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class DurableExecutionEvidence(_message.Message):
    __slots__ = ()
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
    __slots__ = ()
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_DURABLE_SEQ_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    after_durable_seq: int
    page_size: int
    def __init__(self, run_id: _Optional[str] = ..., after_durable_seq: _Optional[int] = ..., page_size: _Optional[int] = ...) -> None: ...

class PullDurableExecutionEvidenceResponse(_message.Message):
    __slots__ = ()
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    NEXT_AFTER_DURABLE_SEQ_FIELD_NUMBER: _ClassVar[int]
    HAS_MORE_FIELD_NUMBER: _ClassVar[int]
    evidence: _containers.RepeatedCompositeFieldContainer[DurableExecutionEvidence]
    next_after_durable_seq: int
    has_more: bool
    def __init__(self, evidence: _Optional[_Iterable[_Union[DurableExecutionEvidence, _Mapping]]] = ..., next_after_durable_seq: _Optional[int] = ..., has_more: _Optional[bool] = ...) -> None: ...

class GetDurableExecutionEvidenceRequest(_message.Message):
    __slots__ = ()
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_REF_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    evidence_ref: str
    def __init__(self, run_id: _Optional[str] = ..., evidence_ref: _Optional[str] = ...) -> None: ...

class DurableExecutionEvidenceNotFound(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetDurableExecutionEvidenceResponse(_message.Message):
    __slots__ = ()
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    evidence: DurableExecutionEvidence
    not_found: DurableExecutionEvidenceNotFound
    def __init__(self, evidence: _Optional[_Union[DurableExecutionEvidence, _Mapping]] = ..., not_found: _Optional[_Union[DurableExecutionEvidenceNotFound, _Mapping]] = ...) -> None: ...

class GetRunDurableCheckpointRequest(_message.Message):
    __slots__ = ()
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunDurableCheckpointResponse(_message.Message):
    __slots__ = ()
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    evidence: DurableExecutionEvidence
    not_found: DurableExecutionEvidenceNotFound
    def __init__(self, evidence: _Optional[_Union[DurableExecutionEvidence, _Mapping]] = ..., not_found: _Optional[_Union[DurableExecutionEvidenceNotFound, _Mapping]] = ...) -> None: ...
