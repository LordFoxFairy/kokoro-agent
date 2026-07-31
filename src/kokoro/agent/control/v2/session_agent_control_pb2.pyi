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

class InteractionDecisionKindV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERACTION_DECISION_KIND_V2_UNSPECIFIED: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_APPROVE: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_EDIT: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_REJECT: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_RESPOND: _ClassVar[InteractionDecisionKindV2]
    INTERACTION_DECISION_KIND_V2_SUBMIT: _ClassVar[InteractionDecisionKindV2]

class DecisionDataClassificationV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DECISION_DATA_CLASSIFICATION_V2_UNSPECIFIED: _ClassVar[DecisionDataClassificationV2]
    DECISION_DATA_CLASSIFICATION_V2_INTERNAL: _ClassVar[DecisionDataClassificationV2]
    DECISION_DATA_CLASSIFICATION_V2_CONFIDENTIAL: _ClassVar[DecisionDataClassificationV2]
    DECISION_DATA_CLASSIFICATION_V2_RESTRICTED: _ClassVar[DecisionDataClassificationV2]

class RunResumeReceiptStatusV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_RESUME_RECEIPT_STATUS_V2_UNSPECIFIED: _ClassVar[RunResumeReceiptStatusV2]
    RUN_RESUME_RECEIPT_STATUS_V2_PERSISTED: _ClassVar[RunResumeReceiptStatusV2]
    RUN_RESUME_RECEIPT_STATUS_V2_APPLYING: _ClassVar[RunResumeReceiptStatusV2]
    RUN_RESUME_RECEIPT_STATUS_V2_APPLIED: _ClassVar[RunResumeReceiptStatusV2]
    RUN_RESUME_RECEIPT_STATUS_V2_SUPERSEDED: _ClassVar[RunResumeReceiptStatusV2]
    RUN_RESUME_RECEIPT_STATUS_V2_REJECTED: _ClassVar[RunResumeReceiptStatusV2]
    RUN_RESUME_RECEIPT_STATUS_V2_OUTCOME_UNKNOWN: _ClassVar[RunResumeReceiptStatusV2]
    RUN_RESUME_RECEIPT_STATUS_V2_CLOSED_BY_TERMINAL: _ClassVar[RunResumeReceiptStatusV2]
INTERACTION_DECISION_KIND_V2_UNSPECIFIED: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_APPROVE: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_EDIT: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_REJECT: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_RESPOND: InteractionDecisionKindV2
INTERACTION_DECISION_KIND_V2_SUBMIT: InteractionDecisionKindV2
DECISION_DATA_CLASSIFICATION_V2_UNSPECIFIED: DecisionDataClassificationV2
DECISION_DATA_CLASSIFICATION_V2_INTERNAL: DecisionDataClassificationV2
DECISION_DATA_CLASSIFICATION_V2_CONFIDENTIAL: DecisionDataClassificationV2
DECISION_DATA_CLASSIFICATION_V2_RESTRICTED: DecisionDataClassificationV2
RUN_RESUME_RECEIPT_STATUS_V2_UNSPECIFIED: RunResumeReceiptStatusV2
RUN_RESUME_RECEIPT_STATUS_V2_PERSISTED: RunResumeReceiptStatusV2
RUN_RESUME_RECEIPT_STATUS_V2_APPLYING: RunResumeReceiptStatusV2
RUN_RESUME_RECEIPT_STATUS_V2_APPLIED: RunResumeReceiptStatusV2
RUN_RESUME_RECEIPT_STATUS_V2_SUPERSEDED: RunResumeReceiptStatusV2
RUN_RESUME_RECEIPT_STATUS_V2_REJECTED: RunResumeReceiptStatusV2
RUN_RESUME_RECEIPT_STATUS_V2_OUTCOME_UNKNOWN: RunResumeReceiptStatusV2
RUN_RESUME_RECEIPT_STATUS_V2_CLOSED_BY_TERMINAL: RunResumeReceiptStatusV2

class EncryptedDecisionValueV2(_message.Message):
    __slots__ = ("encryption_key_handle", "ciphertext", "ciphertext_sha256", "nonce", "associated_data_sha256", "classification")
    ENCRYPTION_KEY_HANDLE_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_SHA256_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_DATA_SHA256_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    encryption_key_handle: str
    ciphertext: bytes
    ciphertext_sha256: str
    nonce: bytes
    associated_data_sha256: str
    classification: DecisionDataClassificationV2
    def __init__(self, encryption_key_handle: _Optional[str] = ..., ciphertext: _Optional[bytes] = ..., ciphertext_sha256: _Optional[str] = ..., nonce: _Optional[bytes] = ..., associated_data_sha256: _Optional[str] = ..., classification: _Optional[_Union[DecisionDataClassificationV2, str]] = ...) -> None: ...

class ApproveDecisionV2(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class EditDecisionV2(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: EncryptedDecisionValueV2
    def __init__(self, value: _Optional[_Union[EncryptedDecisionValueV2, _Mapping]] = ...) -> None: ...

class RejectDecisionV2(_message.Message):
    __slots__ = ("safe_reason",)
    SAFE_REASON_FIELD_NUMBER: _ClassVar[int]
    safe_reason: str
    def __init__(self, safe_reason: _Optional[str] = ...) -> None: ...

class RespondDecisionV2(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: EncryptedDecisionValueV2
    def __init__(self, value: _Optional[_Union[EncryptedDecisionValueV2, _Mapping]] = ...) -> None: ...

class SubmitDecisionV2(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: EncryptedDecisionValueV2
    def __init__(self, value: _Optional[_Union[EncryptedDecisionValueV2, _Mapping]] = ...) -> None: ...

class InteractionDecisionPayloadV2(_message.Message):
    __slots__ = ("approve", "edit", "reject", "respond", "submit")
    APPROVE_FIELD_NUMBER: _ClassVar[int]
    EDIT_FIELD_NUMBER: _ClassVar[int]
    REJECT_FIELD_NUMBER: _ClassVar[int]
    RESPOND_FIELD_NUMBER: _ClassVar[int]
    SUBMIT_FIELD_NUMBER: _ClassVar[int]
    approve: ApproveDecisionV2
    edit: EditDecisionV2
    reject: RejectDecisionV2
    respond: RespondDecisionV2
    submit: SubmitDecisionV2
    def __init__(self, approve: _Optional[_Union[ApproveDecisionV2, _Mapping]] = ..., edit: _Optional[_Union[EditDecisionV2, _Mapping]] = ..., reject: _Optional[_Union[RejectDecisionV2, _Mapping]] = ..., respond: _Optional[_Union[RespondDecisionV2, _Mapping]] = ..., submit: _Optional[_Union[SubmitDecisionV2, _Mapping]] = ...) -> None: ...

class RunResumeDecisionV2(_message.Message):
    __slots__ = ("interaction_owner_ref", "owner_revision", "projection_event_ref", "application_request_ref", "group_member_ordinal", "decision_receipt_ref", "decision_payload_sha256", "kind", "decision")
    INTERACTION_OWNER_REF_FIELD_NUMBER: _ClassVar[int]
    OWNER_REVISION_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_EVENT_REF_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_REQUEST_REF_FIELD_NUMBER: _ClassVar[int]
    GROUP_MEMBER_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    DECISION_RECEIPT_REF_FIELD_NUMBER: _ClassVar[int]
    DECISION_PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    interaction_owner_ref: str
    owner_revision: int
    projection_event_ref: str
    application_request_ref: str
    group_member_ordinal: int
    decision_receipt_ref: str
    decision_payload_sha256: str
    kind: InteractionDecisionKindV2
    decision: InteractionDecisionPayloadV2
    def __init__(self, interaction_owner_ref: _Optional[str] = ..., owner_revision: _Optional[int] = ..., projection_event_ref: _Optional[str] = ..., application_request_ref: _Optional[str] = ..., group_member_ordinal: _Optional[int] = ..., decision_receipt_ref: _Optional[str] = ..., decision_payload_sha256: _Optional[str] = ..., kind: _Optional[_Union[InteractionDecisionKindV2, str]] = ..., decision: _Optional[_Union[InteractionDecisionPayloadV2, _Mapping]] = ...) -> None: ...

class RunResumePayloadV2(_message.Message):
    __slots__ = ("run_id", "resume_ref", "pending_frame_digest", "decision_group_ref", "decision_group_revision", "decisions")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RESUME_REF_FIELD_NUMBER: _ClassVar[int]
    PENDING_FRAME_DIGEST_FIELD_NUMBER: _ClassVar[int]
    DECISION_GROUP_REF_FIELD_NUMBER: _ClassVar[int]
    DECISION_GROUP_REVISION_FIELD_NUMBER: _ClassVar[int]
    DECISIONS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    resume_ref: str
    pending_frame_digest: str
    decision_group_ref: str
    decision_group_revision: int
    decisions: _containers.RepeatedCompositeFieldContainer[RunResumeDecisionV2]
    def __init__(self, run_id: _Optional[str] = ..., resume_ref: _Optional[str] = ..., pending_frame_digest: _Optional[str] = ..., decision_group_ref: _Optional[str] = ..., decision_group_revision: _Optional[int] = ..., decisions: _Optional[_Iterable[_Union[RunResumeDecisionV2, _Mapping]]] = ...) -> None: ...

class RunResumeV2(_message.Message):
    __slots__ = ("payload", "request_digest", "interaction_protocol_release_epoch")
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    INTERACTION_PROTOCOL_RELEASE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    payload: RunResumePayloadV2
    request_digest: str
    interaction_protocol_release_epoch: str
    def __init__(self, payload: _Optional[_Union[RunResumePayloadV2, _Mapping]] = ..., request_digest: _Optional[str] = ..., interaction_protocol_release_epoch: _Optional[str] = ...) -> None: ...

class RunResumeReceiptEventV2(_message.Message):
    __slots__ = ("run_id", "resume_ref", "resume_receipt_ref", "resume_receipt_event_ref", "resume_receipt_revision", "predecessor_receipt_event_ref", "predecessor_receipt_event_sha256", "request_digest", "receipt_event_sha256", "status", "disposition_proof_ref", "disposition_proof_sha256", "application_proof_ref", "application_proof_sha256", "applied_checkpoint_ref", "terminal_evidence_ref", "safe_code", "recorded_at", "producer_instance_ref", "producer_generation", "interaction_protocol_release_epoch")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RESUME_REF_FIELD_NUMBER: _ClassVar[int]
    RESUME_RECEIPT_REF_FIELD_NUMBER: _ClassVar[int]
    RESUME_RECEIPT_EVENT_REF_FIELD_NUMBER: _ClassVar[int]
    RESUME_RECEIPT_REVISION_FIELD_NUMBER: _ClassVar[int]
    PREDECESSOR_RECEIPT_EVENT_REF_FIELD_NUMBER: _ClassVar[int]
    PREDECESSOR_RECEIPT_EVENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_EVENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DISPOSITION_PROOF_REF_FIELD_NUMBER: _ClassVar[int]
    DISPOSITION_PROOF_SHA256_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_PROOF_REF_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_PROOF_SHA256_FIELD_NUMBER: _ClassVar[int]
    APPLIED_CHECKPOINT_REF_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_EVIDENCE_REF_FIELD_NUMBER: _ClassVar[int]
    SAFE_CODE_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_INSTANCE_REF_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_GENERATION_FIELD_NUMBER: _ClassVar[int]
    INTERACTION_PROTOCOL_RELEASE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    resume_ref: str
    resume_receipt_ref: str
    resume_receipt_event_ref: str
    resume_receipt_revision: int
    predecessor_receipt_event_ref: str
    predecessor_receipt_event_sha256: str
    request_digest: str
    receipt_event_sha256: str
    status: RunResumeReceiptStatusV2
    disposition_proof_ref: str
    disposition_proof_sha256: str
    application_proof_ref: str
    application_proof_sha256: str
    applied_checkpoint_ref: str
    terminal_evidence_ref: str
    safe_code: str
    recorded_at: _timestamp_pb2.Timestamp
    producer_instance_ref: str
    producer_generation: int
    interaction_protocol_release_epoch: str
    def __init__(self, run_id: _Optional[str] = ..., resume_ref: _Optional[str] = ..., resume_receipt_ref: _Optional[str] = ..., resume_receipt_event_ref: _Optional[str] = ..., resume_receipt_revision: _Optional[int] = ..., predecessor_receipt_event_ref: _Optional[str] = ..., predecessor_receipt_event_sha256: _Optional[str] = ..., request_digest: _Optional[str] = ..., receipt_event_sha256: _Optional[str] = ..., status: _Optional[_Union[RunResumeReceiptStatusV2, str]] = ..., disposition_proof_ref: _Optional[str] = ..., disposition_proof_sha256: _Optional[str] = ..., application_proof_ref: _Optional[str] = ..., application_proof_sha256: _Optional[str] = ..., applied_checkpoint_ref: _Optional[str] = ..., terminal_evidence_ref: _Optional[str] = ..., safe_code: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., producer_instance_ref: _Optional[str] = ..., producer_generation: _Optional[int] = ..., interaction_protocol_release_epoch: _Optional[str] = ...) -> None: ...

class GetRunResumeReceiptEventsRequest(_message.Message):
    __slots__ = ("run_id", "resume_ref", "after_receipt_revision", "page_size")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RESUME_REF_FIELD_NUMBER: _ClassVar[int]
    AFTER_RECEIPT_REVISION_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    resume_ref: str
    after_receipt_revision: int
    page_size: int
    def __init__(self, run_id: _Optional[str] = ..., resume_ref: _Optional[str] = ..., after_receipt_revision: _Optional[int] = ..., page_size: _Optional[int] = ...) -> None: ...

class GetRunResumeReceiptEventsResponse(_message.Message):
    __slots__ = ("resume_receipt_ref", "current_head_revision", "current_head_event_ref", "current_head_event_sha256", "events", "next_after_receipt_revision", "has_more", "returned_after_receipt_revision", "run_id", "resume_ref")
    RESUME_RECEIPT_REF_FIELD_NUMBER: _ClassVar[int]
    CURRENT_HEAD_REVISION_FIELD_NUMBER: _ClassVar[int]
    CURRENT_HEAD_EVENT_REF_FIELD_NUMBER: _ClassVar[int]
    CURRENT_HEAD_EVENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_AFTER_RECEIPT_REVISION_FIELD_NUMBER: _ClassVar[int]
    HAS_MORE_FIELD_NUMBER: _ClassVar[int]
    RETURNED_AFTER_RECEIPT_REVISION_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RESUME_REF_FIELD_NUMBER: _ClassVar[int]
    resume_receipt_ref: str
    current_head_revision: int
    current_head_event_ref: str
    current_head_event_sha256: str
    events: _containers.RepeatedCompositeFieldContainer[RunResumeReceiptEventV2]
    next_after_receipt_revision: int
    has_more: bool
    returned_after_receipt_revision: int
    run_id: str
    resume_ref: str
    def __init__(self, resume_receipt_ref: _Optional[str] = ..., current_head_revision: _Optional[int] = ..., current_head_event_ref: _Optional[str] = ..., current_head_event_sha256: _Optional[str] = ..., events: _Optional[_Iterable[_Union[RunResumeReceiptEventV2, _Mapping]]] = ..., next_after_receipt_revision: _Optional[int] = ..., has_more: _Optional[bool] = ..., returned_after_receipt_revision: _Optional[int] = ..., run_id: _Optional[str] = ..., resume_ref: _Optional[str] = ...) -> None: ...
