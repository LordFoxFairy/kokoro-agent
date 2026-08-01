import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from kokoro.common.v2 import command_envelope_pb2 as _command_envelope_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PresentationTerminalDisposition(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRESENTATION_TERMINAL_DISPOSITION_UNSPECIFIED: _ClassVar[PresentationTerminalDisposition]
    PRESENTATION_TERMINAL_DISPOSITION_COMPLETED: _ClassVar[PresentationTerminalDisposition]
    PRESENTATION_TERMINAL_DISPOSITION_FAILED: _ClassVar[PresentationTerminalDisposition]
    PRESENTATION_TERMINAL_DISPOSITION_CANCELED: _ClassVar[PresentationTerminalDisposition]

class PresentationRejectionClass(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRESENTATION_REJECTION_CLASS_UNSPECIFIED: _ClassVar[PresentationRejectionClass]
    PRESENTATION_REJECTION_CLASS_PERMANENT: _ClassVar[PresentationRejectionClass]

class PresentationTransientErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRESENTATION_TRANSIENT_ERROR_KIND_UNSPECIFIED: _ClassVar[PresentationTransientErrorKind]
    PRESENTATION_TRANSIENT_ERROR_KIND_UNAVAILABLE: _ClassVar[PresentationTransientErrorKind]
    PRESENTATION_TRANSIENT_ERROR_KIND_DEADLINE_EXCEEDED: _ClassVar[PresentationTransientErrorKind]
    PRESENTATION_TRANSIENT_ERROR_KIND_CONCURRENT_UPDATE: _ClassVar[PresentationTransientErrorKind]
    PRESENTATION_TRANSIENT_ERROR_KIND_SNAPSHOT_NOT_READY: _ClassVar[PresentationTransientErrorKind]

class PresentationPermanentErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRESENTATION_PERMANENT_ERROR_KIND_UNSPECIFIED: _ClassVar[PresentationPermanentErrorKind]
    PRESENTATION_PERMANENT_ERROR_KIND_UNAUTHENTICATED_CONSUMER: _ClassVar[PresentationPermanentErrorKind]
    PRESENTATION_PERMANENT_ERROR_KIND_PRODUCER_FENCED: _ClassVar[PresentationPermanentErrorKind]
    PRESENTATION_PERMANENT_ERROR_KIND_SNAPSHOT_MISMATCH: _ClassVar[PresentationPermanentErrorKind]
    PRESENTATION_PERMANENT_ERROR_KIND_SEQUENCE_GAP: _ClassVar[PresentationPermanentErrorKind]
    PRESENTATION_PERMANENT_ERROR_KIND_IDEMPOTENCY_CONFLICT: _ClassVar[PresentationPermanentErrorKind]
    PRESENTATION_PERMANENT_ERROR_KIND_QUARANTINED_GAP: _ClassVar[PresentationPermanentErrorKind]
PRESENTATION_TERMINAL_DISPOSITION_UNSPECIFIED: PresentationTerminalDisposition
PRESENTATION_TERMINAL_DISPOSITION_COMPLETED: PresentationTerminalDisposition
PRESENTATION_TERMINAL_DISPOSITION_FAILED: PresentationTerminalDisposition
PRESENTATION_TERMINAL_DISPOSITION_CANCELED: PresentationTerminalDisposition
PRESENTATION_REJECTION_CLASS_UNSPECIFIED: PresentationRejectionClass
PRESENTATION_REJECTION_CLASS_PERMANENT: PresentationRejectionClass
PRESENTATION_TRANSIENT_ERROR_KIND_UNSPECIFIED: PresentationTransientErrorKind
PRESENTATION_TRANSIENT_ERROR_KIND_UNAVAILABLE: PresentationTransientErrorKind
PRESENTATION_TRANSIENT_ERROR_KIND_DEADLINE_EXCEEDED: PresentationTransientErrorKind
PRESENTATION_TRANSIENT_ERROR_KIND_CONCURRENT_UPDATE: PresentationTransientErrorKind
PRESENTATION_TRANSIENT_ERROR_KIND_SNAPSHOT_NOT_READY: PresentationTransientErrorKind
PRESENTATION_PERMANENT_ERROR_KIND_UNSPECIFIED: PresentationPermanentErrorKind
PRESENTATION_PERMANENT_ERROR_KIND_UNAUTHENTICATED_CONSUMER: PresentationPermanentErrorKind
PRESENTATION_PERMANENT_ERROR_KIND_PRODUCER_FENCED: PresentationPermanentErrorKind
PRESENTATION_PERMANENT_ERROR_KIND_SNAPSHOT_MISMATCH: PresentationPermanentErrorKind
PRESENTATION_PERMANENT_ERROR_KIND_SEQUENCE_GAP: PresentationPermanentErrorKind
PRESENTATION_PERMANENT_ERROR_KIND_IDEMPOTENCY_CONFLICT: PresentationPermanentErrorKind
PRESENTATION_PERMANENT_ERROR_KIND_QUARANTINED_GAP: PresentationPermanentErrorKind

class CheckActiveRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CheckActiveResponse(_message.Message):
    __slots__ = ("contract_revision",)
    CONTRACT_REVISION_FIELD_NUMBER: _ClassVar[int]
    contract_revision: str
    def __init__(self, contract_revision: _Optional[str] = ...) -> None: ...

class PresentationProducerFence(_message.Message):
    __slots__ = ("producer_instance_ref", "producer_generation", "producer_fence_digest")
    PRODUCER_INSTANCE_REF_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_GENERATION_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FENCE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    producer_instance_ref: str
    producer_generation: int
    producer_fence_digest: str
    def __init__(self, producer_instance_ref: _Optional[str] = ..., producer_generation: _Optional[int] = ..., producer_fence_digest: _Optional[str] = ...) -> None: ...

class PresentationProducerFenceDigestPayload(_message.Message):
    __slots__ = ("producer_instance_ref", "producer_generation")
    PRODUCER_INSTANCE_REF_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_GENERATION_FIELD_NUMBER: _ClassVar[int]
    producer_instance_ref: str
    producer_generation: int
    def __init__(self, producer_instance_ref: _Optional[str] = ..., producer_generation: _Optional[int] = ...) -> None: ...

class PresentationRecordChainGenesisDigestPayload(_message.Message):
    __slots__ = ("run_id", "producer")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: PresentationProducerFence
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ...) -> None: ...

class PresentationCandidateRecordDigestPayload(_message.Message):
    __slots__ = ("run_id", "presentation_ref", "previous_presentation_seq", "presentation_seq", "envelope_digest", "candidate_ref", "candidate_digest", "recorded_at", "producer", "previous_record_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_REF_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    ENVELOPE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_REF_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    presentation_ref: str
    previous_presentation_seq: int
    presentation_seq: int
    envelope_digest: str
    candidate_ref: str
    candidate_digest: str
    recorded_at: _timestamp_pb2.Timestamp
    producer: PresentationProducerFence
    previous_record_digest: str
    def __init__(self, run_id: _Optional[str] = ..., presentation_ref: _Optional[str] = ..., previous_presentation_seq: _Optional[int] = ..., presentation_seq: _Optional[int] = ..., envelope_digest: _Optional[str] = ..., candidate_ref: _Optional[str] = ..., candidate_digest: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., previous_record_digest: _Optional[str] = ...) -> None: ...

class PresentationSnapshotHeadDigestPayload(_message.Message):
    __slots__ = ("run_id", "producer", "snapshot_through_presentation_seq", "snapshot_head_record_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_THROUGH_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: PresentationProducerFence
    snapshot_through_presentation_seq: int
    snapshot_head_record_digest: str
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., snapshot_through_presentation_seq: _Optional[int] = ..., snapshot_head_record_digest: _Optional[str] = ...) -> None: ...

class PresentationTerminalSeal(_message.Message):
    __slots__ = ("sealed_through_presentation_seq", "sealed_head_record_digest", "terminal_evidence_ref", "terminal_evidence_payload_digest", "terminal_disposition", "sealed_at")
    SEALED_THROUGH_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    SEALED_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_EVIDENCE_REF_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_EVIDENCE_PAYLOAD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_DISPOSITION_FIELD_NUMBER: _ClassVar[int]
    SEALED_AT_FIELD_NUMBER: _ClassVar[int]
    sealed_through_presentation_seq: int
    sealed_head_record_digest: str
    terminal_evidence_ref: str
    terminal_evidence_payload_digest: str
    terminal_disposition: PresentationTerminalDisposition
    sealed_at: _timestamp_pb2.Timestamp
    def __init__(self, sealed_through_presentation_seq: _Optional[int] = ..., sealed_head_record_digest: _Optional[str] = ..., terminal_evidence_ref: _Optional[str] = ..., terminal_evidence_payload_digest: _Optional[str] = ..., terminal_disposition: _Optional[_Union[PresentationTerminalDisposition, str]] = ..., sealed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PresentationDeliveryStatusDigestPayload(_message.Message):
    __slots__ = ("run_id", "producer", "acknowledged_through_presentation_seq", "status_revision", "quarantine", "last_command", "updated_at", "acknowledged_head_record_digest", "terminal_seal")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_THROUGH_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    STATUS_REVISION_FIELD_NUMBER: _ClassVar[int]
    QUARANTINE_FIELD_NUMBER: _ClassVar[int]
    LAST_COMMAND_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_SEAL_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: PresentationProducerFence
    acknowledged_through_presentation_seq: int
    status_revision: int
    quarantine: PresentationQuarantineStatus
    last_command: _command_envelope_pb2.CommandIdentityV2
    updated_at: _timestamp_pb2.Timestamp
    acknowledged_head_record_digest: str
    terminal_seal: PresentationTerminalSeal
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., acknowledged_through_presentation_seq: _Optional[int] = ..., status_revision: _Optional[int] = ..., quarantine: _Optional[_Union[PresentationQuarantineStatus, _Mapping]] = ..., last_command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., acknowledged_head_record_digest: _Optional[str] = ..., terminal_seal: _Optional[_Union[PresentationTerminalSeal, _Mapping]] = ...) -> None: ...

class PresentationTransientErrorDetail(_message.Message):
    __slots__ = ("kind", "retryable", "retry_after_milliseconds", "correlation_ref")
    KIND_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_MILLISECONDS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_REF_FIELD_NUMBER: _ClassVar[int]
    kind: PresentationTransientErrorKind
    retryable: bool
    retry_after_milliseconds: int
    correlation_ref: str
    def __init__(self, kind: _Optional[_Union[PresentationTransientErrorKind, str]] = ..., retryable: _Optional[bool] = ..., retry_after_milliseconds: _Optional[int] = ..., correlation_ref: _Optional[str] = ...) -> None: ...

class PresentationPermanentErrorDetail(_message.Message):
    __slots__ = ("kind", "retryable", "correlation_ref")
    KIND_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_REF_FIELD_NUMBER: _ClassVar[int]
    kind: PresentationPermanentErrorKind
    retryable: bool
    correlation_ref: str
    def __init__(self, kind: _Optional[_Union[PresentationPermanentErrorKind, str]] = ..., retryable: _Optional[bool] = ..., correlation_ref: _Optional[str] = ...) -> None: ...

class PresentationCandidateRecord(_message.Message):
    __slots__ = ("presentation_ref", "previous_presentation_seq", "presentation_seq", "envelope_bytes", "envelope_digest", "candidate_ref", "candidate_digest", "recorded_at", "producer", "previous_record_digest", "record_digest")
    PRESENTATION_REF_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    ENVELOPE_BYTES_FIELD_NUMBER: _ClassVar[int]
    ENVELOPE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_REF_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    presentation_ref: str
    previous_presentation_seq: int
    presentation_seq: int
    envelope_bytes: bytes
    envelope_digest: str
    candidate_ref: str
    candidate_digest: str
    recorded_at: _timestamp_pb2.Timestamp
    producer: PresentationProducerFence
    previous_record_digest: str
    record_digest: str
    def __init__(self, presentation_ref: _Optional[str] = ..., previous_presentation_seq: _Optional[int] = ..., presentation_seq: _Optional[int] = ..., envelope_bytes: _Optional[bytes] = ..., envelope_digest: _Optional[str] = ..., candidate_ref: _Optional[str] = ..., candidate_digest: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., previous_record_digest: _Optional[str] = ..., record_digest: _Optional[str] = ...) -> None: ...

class PresentationQuarantineStatus(_message.Message):
    __slots__ = ("presentation_seq", "presentation_ref", "candidate_ref", "rejection_class", "reason_code", "session_rejection_digest", "quarantined_at")
    PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_REF_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_REF_FIELD_NUMBER: _ClassVar[int]
    REJECTION_CLASS_FIELD_NUMBER: _ClassVar[int]
    REASON_CODE_FIELD_NUMBER: _ClassVar[int]
    SESSION_REJECTION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    QUARANTINED_AT_FIELD_NUMBER: _ClassVar[int]
    presentation_seq: int
    presentation_ref: str
    candidate_ref: str
    rejection_class: PresentationRejectionClass
    reason_code: str
    session_rejection_digest: str
    quarantined_at: _timestamp_pb2.Timestamp
    def __init__(self, presentation_seq: _Optional[int] = ..., presentation_ref: _Optional[str] = ..., candidate_ref: _Optional[str] = ..., rejection_class: _Optional[_Union[PresentationRejectionClass, str]] = ..., reason_code: _Optional[str] = ..., session_rejection_digest: _Optional[str] = ..., quarantined_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PresentationDeliveryStatus(_message.Message):
    __slots__ = ("run_id", "producer", "acknowledged_through_presentation_seq", "status_revision", "quarantine", "last_command", "updated_at", "status_digest", "acknowledged_head_record_digest", "terminal_seal")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_THROUGH_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    STATUS_REVISION_FIELD_NUMBER: _ClassVar[int]
    QUARANTINE_FIELD_NUMBER: _ClassVar[int]
    LAST_COMMAND_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_DIGEST_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_SEAL_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: PresentationProducerFence
    acknowledged_through_presentation_seq: int
    status_revision: int
    quarantine: PresentationQuarantineStatus
    last_command: _command_envelope_pb2.CommandIdentityV2
    updated_at: _timestamp_pb2.Timestamp
    status_digest: str
    acknowledged_head_record_digest: str
    terminal_seal: PresentationTerminalSeal
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., acknowledged_through_presentation_seq: _Optional[int] = ..., status_revision: _Optional[int] = ..., quarantine: _Optional[_Union[PresentationQuarantineStatus, _Mapping]] = ..., last_command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status_digest: _Optional[str] = ..., acknowledged_head_record_digest: _Optional[str] = ..., terminal_seal: _Optional[_Union[PresentationTerminalSeal, _Mapping]] = ...) -> None: ...

class PullCandidateBatchesRequest(_message.Message):
    __slots__ = ("run_id", "producer", "after_presentation_seq", "page_size", "snapshot_through_presentation_seq")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    AFTER_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_THROUGH_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: PresentationProducerFence
    after_presentation_seq: int
    page_size: int
    snapshot_through_presentation_seq: int
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., after_presentation_seq: _Optional[int] = ..., page_size: _Optional[int] = ..., snapshot_through_presentation_seq: _Optional[int] = ...) -> None: ...

class PullCandidateBatchesResponse(_message.Message):
    __slots__ = ("run_id", "producer", "page_after_presentation_seq", "snapshot_through_presentation_seq", "records", "next_after_presentation_seq", "has_more", "delivery_status", "snapshot_head_digest", "snapshot_head_record_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    PAGE_AFTER_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_THROUGH_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    NEXT_AFTER_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    HAS_MORE_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_STATUS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HEAD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: PresentationProducerFence
    page_after_presentation_seq: int
    snapshot_through_presentation_seq: int
    records: _containers.RepeatedCompositeFieldContainer[PresentationCandidateRecord]
    next_after_presentation_seq: int
    has_more: bool
    delivery_status: PresentationDeliveryStatus
    snapshot_head_digest: str
    snapshot_head_record_digest: str
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., page_after_presentation_seq: _Optional[int] = ..., snapshot_through_presentation_seq: _Optional[int] = ..., records: _Optional[_Iterable[_Union[PresentationCandidateRecord, _Mapping]]] = ..., next_after_presentation_seq: _Optional[int] = ..., has_more: _Optional[bool] = ..., delivery_status: _Optional[_Union[PresentationDeliveryStatus, _Mapping]] = ..., snapshot_head_digest: _Optional[str] = ..., snapshot_head_record_digest: _Optional[str] = ...) -> None: ...

class CandidateAdmissionReceipt(_message.Message):
    __slots__ = ("previous_presentation_seq", "presentation_seq", "presentation_ref", "record_digest", "candidate_ref", "candidate_digest", "session_admission_receipt_ref", "session_effect_digest")
    PREVIOUS_PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_REF_FIELD_NUMBER: _ClassVar[int]
    RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_REF_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SESSION_ADMISSION_RECEIPT_REF_FIELD_NUMBER: _ClassVar[int]
    SESSION_EFFECT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    previous_presentation_seq: int
    presentation_seq: int
    presentation_ref: str
    record_digest: str
    candidate_ref: str
    candidate_digest: str
    session_admission_receipt_ref: str
    session_effect_digest: str
    def __init__(self, previous_presentation_seq: _Optional[int] = ..., presentation_seq: _Optional[int] = ..., presentation_ref: _Optional[str] = ..., record_digest: _Optional[str] = ..., candidate_ref: _Optional[str] = ..., candidate_digest: _Optional[str] = ..., session_admission_receipt_ref: _Optional[str] = ..., session_effect_digest: _Optional[str] = ...) -> None: ...

class AcknowledgeCandidateAdmissionsEffect(_message.Message):
    __slots__ = ("run_id", "producer", "expected_acknowledged_through", "expected_status_revision", "idempotency_ref", "receipts", "effect_digest_domain", "effect_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_ACKNOWLEDGED_THROUGH_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_STATUS_REVISION_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_REF_FIELD_NUMBER: _ClassVar[int]
    RECEIPTS_FIELD_NUMBER: _ClassVar[int]
    EFFECT_DIGEST_DOMAIN_FIELD_NUMBER: _ClassVar[int]
    EFFECT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: PresentationProducerFence
    expected_acknowledged_through: int
    expected_status_revision: int
    idempotency_ref: str
    receipts: _containers.RepeatedCompositeFieldContainer[CandidateAdmissionReceipt]
    effect_digest_domain: str
    effect_digest: str
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., expected_acknowledged_through: _Optional[int] = ..., expected_status_revision: _Optional[int] = ..., idempotency_ref: _Optional[str] = ..., receipts: _Optional[_Iterable[_Union[CandidateAdmissionReceipt, _Mapping]]] = ..., effect_digest_domain: _Optional[str] = ..., effect_digest: _Optional[str] = ...) -> None: ...

class AcknowledgeCandidateAdmissionsRequest(_message.Message):
    __slots__ = ("command", "effect")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    command: _command_envelope_pb2.CommandIdentityV2
    effect: AcknowledgeCandidateAdmissionsEffect
    def __init__(self, command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ..., effect: _Optional[_Union[AcknowledgeCandidateAdmissionsEffect, _Mapping]] = ...) -> None: ...

class AcknowledgeCandidateAdmissionsResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: PresentationDeliveryStatus
    def __init__(self, status: _Optional[_Union[PresentationDeliveryStatus, _Mapping]] = ...) -> None: ...

class QuarantineCandidateAdmissionEffect(_message.Message):
    __slots__ = ("run_id", "producer", "expected_acknowledged_through", "expected_status_revision", "idempotency_ref", "presentation_seq", "presentation_ref", "record_digest", "candidate_ref", "candidate_digest", "rejection_class", "reason_code", "session_rejection_digest", "effect_digest_domain", "effect_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_ACKNOWLEDGED_THROUGH_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_STATUS_REVISION_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_REF_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_SEQ_FIELD_NUMBER: _ClassVar[int]
    PRESENTATION_REF_FIELD_NUMBER: _ClassVar[int]
    RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_REF_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    REJECTION_CLASS_FIELD_NUMBER: _ClassVar[int]
    REASON_CODE_FIELD_NUMBER: _ClassVar[int]
    SESSION_REJECTION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    EFFECT_DIGEST_DOMAIN_FIELD_NUMBER: _ClassVar[int]
    EFFECT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: PresentationProducerFence
    expected_acknowledged_through: int
    expected_status_revision: int
    idempotency_ref: str
    presentation_seq: int
    presentation_ref: str
    record_digest: str
    candidate_ref: str
    candidate_digest: str
    rejection_class: PresentationRejectionClass
    reason_code: str
    session_rejection_digest: str
    effect_digest_domain: str
    effect_digest: str
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., expected_acknowledged_through: _Optional[int] = ..., expected_status_revision: _Optional[int] = ..., idempotency_ref: _Optional[str] = ..., presentation_seq: _Optional[int] = ..., presentation_ref: _Optional[str] = ..., record_digest: _Optional[str] = ..., candidate_ref: _Optional[str] = ..., candidate_digest: _Optional[str] = ..., rejection_class: _Optional[_Union[PresentationRejectionClass, str]] = ..., reason_code: _Optional[str] = ..., session_rejection_digest: _Optional[str] = ..., effect_digest_domain: _Optional[str] = ..., effect_digest: _Optional[str] = ...) -> None: ...

class QuarantineCandidateAdmissionRequest(_message.Message):
    __slots__ = ("command", "effect")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    command: _command_envelope_pb2.CommandIdentityV2
    effect: QuarantineCandidateAdmissionEffect
    def __init__(self, command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ..., effect: _Optional[_Union[QuarantineCandidateAdmissionEffect, _Mapping]] = ...) -> None: ...

class QuarantineCandidateAdmissionResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: PresentationDeliveryStatus
    def __init__(self, status: _Optional[_Union[PresentationDeliveryStatus, _Mapping]] = ...) -> None: ...

class GetDeliveryStatusRequest(_message.Message):
    __slots__ = ("producer", "run_id", "original_command")
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_COMMAND_FIELD_NUMBER: _ClassVar[int]
    producer: PresentationProducerFence
    run_id: str
    original_command: _command_envelope_pb2.CommandIdentityV2
    def __init__(self, producer: _Optional[_Union[PresentationProducerFence, _Mapping]] = ..., run_id: _Optional[str] = ..., original_command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ...) -> None: ...

class GetDeliveryStatusResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: PresentationDeliveryStatus
    def __init__(self, status: _Optional[_Union[PresentationDeliveryStatus, _Mapping]] = ...) -> None: ...
