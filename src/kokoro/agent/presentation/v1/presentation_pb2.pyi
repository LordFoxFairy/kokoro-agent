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

class TerminalDisposition(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TERMINAL_DISPOSITION_UNSPECIFIED: _ClassVar[TerminalDisposition]
    TERMINAL_DISPOSITION_COMPLETED: _ClassVar[TerminalDisposition]
    TERMINAL_DISPOSITION_FAILED: _ClassVar[TerminalDisposition]
    TERMINAL_DISPOSITION_CANCELED: _ClassVar[TerminalDisposition]

class RejectionClass(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REJECTION_CLASS_UNSPECIFIED: _ClassVar[RejectionClass]
    REJECTION_CLASS_PERMANENT: _ClassVar[RejectionClass]

class TransientErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRANSIENT_ERROR_KIND_UNSPECIFIED: _ClassVar[TransientErrorKind]
    TRANSIENT_ERROR_KIND_UNAVAILABLE: _ClassVar[TransientErrorKind]
    TRANSIENT_ERROR_KIND_DEADLINE_EXCEEDED: _ClassVar[TransientErrorKind]
    TRANSIENT_ERROR_KIND_CONCURRENT_UPDATE: _ClassVar[TransientErrorKind]
    TRANSIENT_ERROR_KIND_SNAPSHOT_NOT_READY: _ClassVar[TransientErrorKind]

class PermanentErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PERMANENT_ERROR_KIND_UNSPECIFIED: _ClassVar[PermanentErrorKind]
    PERMANENT_ERROR_KIND_UNAUTHENTICATED_CONSUMER: _ClassVar[PermanentErrorKind]
    PERMANENT_ERROR_KIND_PRODUCER_FENCED: _ClassVar[PermanentErrorKind]
    PERMANENT_ERROR_KIND_SNAPSHOT_MISMATCH: _ClassVar[PermanentErrorKind]
    PERMANENT_ERROR_KIND_SEQUENCE_GAP: _ClassVar[PermanentErrorKind]
    PERMANENT_ERROR_KIND_IDEMPOTENCY_CONFLICT: _ClassVar[PermanentErrorKind]
    PERMANENT_ERROR_KIND_QUARANTINED_GAP: _ClassVar[PermanentErrorKind]
TERMINAL_DISPOSITION_UNSPECIFIED: TerminalDisposition
TERMINAL_DISPOSITION_COMPLETED: TerminalDisposition
TERMINAL_DISPOSITION_FAILED: TerminalDisposition
TERMINAL_DISPOSITION_CANCELED: TerminalDisposition
REJECTION_CLASS_UNSPECIFIED: RejectionClass
REJECTION_CLASS_PERMANENT: RejectionClass
TRANSIENT_ERROR_KIND_UNSPECIFIED: TransientErrorKind
TRANSIENT_ERROR_KIND_UNAVAILABLE: TransientErrorKind
TRANSIENT_ERROR_KIND_DEADLINE_EXCEEDED: TransientErrorKind
TRANSIENT_ERROR_KIND_CONCURRENT_UPDATE: TransientErrorKind
TRANSIENT_ERROR_KIND_SNAPSHOT_NOT_READY: TransientErrorKind
PERMANENT_ERROR_KIND_UNSPECIFIED: PermanentErrorKind
PERMANENT_ERROR_KIND_UNAUTHENTICATED_CONSUMER: PermanentErrorKind
PERMANENT_ERROR_KIND_PRODUCER_FENCED: PermanentErrorKind
PERMANENT_ERROR_KIND_SNAPSHOT_MISMATCH: PermanentErrorKind
PERMANENT_ERROR_KIND_SEQUENCE_GAP: PermanentErrorKind
PERMANENT_ERROR_KIND_IDEMPOTENCY_CONFLICT: PermanentErrorKind
PERMANENT_ERROR_KIND_QUARANTINED_GAP: PermanentErrorKind

class CheckActiveRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CheckActiveResponse(_message.Message):
    __slots__ = ("contract_revision",)
    CONTRACT_REVISION_FIELD_NUMBER: _ClassVar[int]
    contract_revision: str
    def __init__(self, contract_revision: _Optional[str] = ...) -> None: ...

class ProducerFence(_message.Message):
    __slots__ = ("producer_instance_ref", "producer_generation", "producer_fence_digest")
    PRODUCER_INSTANCE_REF_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_GENERATION_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FENCE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    producer_instance_ref: str
    producer_generation: int
    producer_fence_digest: str
    def __init__(self, producer_instance_ref: _Optional[str] = ..., producer_generation: _Optional[int] = ..., producer_fence_digest: _Optional[str] = ...) -> None: ...

class ProducerFenceDigestPayload(_message.Message):
    __slots__ = ("producer_instance_ref", "producer_generation")
    PRODUCER_INSTANCE_REF_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_GENERATION_FIELD_NUMBER: _ClassVar[int]
    producer_instance_ref: str
    producer_generation: int
    def __init__(self, producer_instance_ref: _Optional[str] = ..., producer_generation: _Optional[int] = ...) -> None: ...

class RecordChainGenesisDigestPayload(_message.Message):
    __slots__ = ("run_id", "producer")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: ProducerFence
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ...) -> None: ...

class DeliveryRecordDigestPayload(_message.Message):
    __slots__ = ("run_id", "record_ref", "previous_delivery_seq", "delivery_seq", "envelope_digest", "submission_ref", "submission_digest", "recorded_at", "producer", "previous_record_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RECORD_REF_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    ENVELOPE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_REF_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    record_ref: str
    previous_delivery_seq: int
    delivery_seq: int
    envelope_digest: str
    submission_ref: str
    submission_digest: str
    recorded_at: _timestamp_pb2.Timestamp
    producer: ProducerFence
    previous_record_digest: str
    def __init__(self, run_id: _Optional[str] = ..., record_ref: _Optional[str] = ..., previous_delivery_seq: _Optional[int] = ..., delivery_seq: _Optional[int] = ..., envelope_digest: _Optional[str] = ..., submission_ref: _Optional[str] = ..., submission_digest: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., previous_record_digest: _Optional[str] = ...) -> None: ...

class DeliveryHeadDigestPayload(_message.Message):
    __slots__ = ("run_id", "producer", "snapshot_through_delivery_seq", "snapshot_head_record_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_THROUGH_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: ProducerFence
    snapshot_through_delivery_seq: int
    snapshot_head_record_digest: str
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., snapshot_through_delivery_seq: _Optional[int] = ..., snapshot_head_record_digest: _Optional[str] = ...) -> None: ...

class TerminalSeal(_message.Message):
    __slots__ = ("sealed_through_delivery_seq", "sealed_head_record_digest", "terminal_evidence_ref", "terminal_evidence_payload_digest", "terminal_disposition", "sealed_at")
    SEALED_THROUGH_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    SEALED_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_EVIDENCE_REF_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_EVIDENCE_PAYLOAD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_DISPOSITION_FIELD_NUMBER: _ClassVar[int]
    SEALED_AT_FIELD_NUMBER: _ClassVar[int]
    sealed_through_delivery_seq: int
    sealed_head_record_digest: str
    terminal_evidence_ref: str
    terminal_evidence_payload_digest: str
    terminal_disposition: TerminalDisposition
    sealed_at: _timestamp_pb2.Timestamp
    def __init__(self, sealed_through_delivery_seq: _Optional[int] = ..., sealed_head_record_digest: _Optional[str] = ..., terminal_evidence_ref: _Optional[str] = ..., terminal_evidence_payload_digest: _Optional[str] = ..., terminal_disposition: _Optional[_Union[TerminalDisposition, str]] = ..., sealed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DeliveryStatusDigestPayload(_message.Message):
    __slots__ = ("run_id", "producer", "acknowledged_through_delivery_seq", "status_revision", "quarantine", "last_command", "updated_at", "acknowledged_head_record_digest", "terminal_seal")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_THROUGH_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    STATUS_REVISION_FIELD_NUMBER: _ClassVar[int]
    QUARANTINE_FIELD_NUMBER: _ClassVar[int]
    LAST_COMMAND_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_SEAL_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: ProducerFence
    acknowledged_through_delivery_seq: int
    status_revision: int
    quarantine: QuarantineStatus
    last_command: _command_envelope_pb2.CommandIdentityV2
    updated_at: _timestamp_pb2.Timestamp
    acknowledged_head_record_digest: str
    terminal_seal: TerminalSeal
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., acknowledged_through_delivery_seq: _Optional[int] = ..., status_revision: _Optional[int] = ..., quarantine: _Optional[_Union[QuarantineStatus, _Mapping]] = ..., last_command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., acknowledged_head_record_digest: _Optional[str] = ..., terminal_seal: _Optional[_Union[TerminalSeal, _Mapping]] = ...) -> None: ...

class TransientErrorDetail(_message.Message):
    __slots__ = ("kind", "retryable", "retry_after_milliseconds", "correlation_ref")
    KIND_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_MILLISECONDS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_REF_FIELD_NUMBER: _ClassVar[int]
    kind: TransientErrorKind
    retryable: bool
    retry_after_milliseconds: int
    correlation_ref: str
    def __init__(self, kind: _Optional[_Union[TransientErrorKind, str]] = ..., retryable: _Optional[bool] = ..., retry_after_milliseconds: _Optional[int] = ..., correlation_ref: _Optional[str] = ...) -> None: ...

class PermanentErrorDetail(_message.Message):
    __slots__ = ("kind", "retryable", "correlation_ref")
    KIND_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_REF_FIELD_NUMBER: _ClassVar[int]
    kind: PermanentErrorKind
    retryable: bool
    correlation_ref: str
    def __init__(self, kind: _Optional[_Union[PermanentErrorKind, str]] = ..., retryable: _Optional[bool] = ..., correlation_ref: _Optional[str] = ...) -> None: ...

class DeliveryRecord(_message.Message):
    __slots__ = ("record_ref", "previous_delivery_seq", "delivery_seq", "envelope_bytes", "envelope_digest", "submission_ref", "submission_digest", "recorded_at", "producer", "previous_record_digest", "record_digest")
    RECORD_REF_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    ENVELOPE_BYTES_FIELD_NUMBER: _ClassVar[int]
    ENVELOPE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_REF_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    record_ref: str
    previous_delivery_seq: int
    delivery_seq: int
    envelope_bytes: bytes
    envelope_digest: str
    submission_ref: str
    submission_digest: str
    recorded_at: _timestamp_pb2.Timestamp
    producer: ProducerFence
    previous_record_digest: str
    record_digest: str
    def __init__(self, record_ref: _Optional[str] = ..., previous_delivery_seq: _Optional[int] = ..., delivery_seq: _Optional[int] = ..., envelope_bytes: _Optional[bytes] = ..., envelope_digest: _Optional[str] = ..., submission_ref: _Optional[str] = ..., submission_digest: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., previous_record_digest: _Optional[str] = ..., record_digest: _Optional[str] = ...) -> None: ...

class QuarantineStatus(_message.Message):
    __slots__ = ("delivery_seq", "record_ref", "submission_ref", "rejection_class", "reason_code", "session_rejection_digest", "quarantined_at")
    DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    RECORD_REF_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_REF_FIELD_NUMBER: _ClassVar[int]
    REJECTION_CLASS_FIELD_NUMBER: _ClassVar[int]
    REASON_CODE_FIELD_NUMBER: _ClassVar[int]
    SESSION_REJECTION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    QUARANTINED_AT_FIELD_NUMBER: _ClassVar[int]
    delivery_seq: int
    record_ref: str
    submission_ref: str
    rejection_class: RejectionClass
    reason_code: str
    session_rejection_digest: str
    quarantined_at: _timestamp_pb2.Timestamp
    def __init__(self, delivery_seq: _Optional[int] = ..., record_ref: _Optional[str] = ..., submission_ref: _Optional[str] = ..., rejection_class: _Optional[_Union[RejectionClass, str]] = ..., reason_code: _Optional[str] = ..., session_rejection_digest: _Optional[str] = ..., quarantined_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DeliveryStatus(_message.Message):
    __slots__ = ("run_id", "producer", "acknowledged_through_delivery_seq", "status_revision", "quarantine", "last_command", "updated_at", "status_digest", "acknowledged_head_record_digest", "terminal_seal")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_THROUGH_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    STATUS_REVISION_FIELD_NUMBER: _ClassVar[int]
    QUARANTINE_FIELD_NUMBER: _ClassVar[int]
    LAST_COMMAND_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_DIGEST_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_SEAL_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: ProducerFence
    acknowledged_through_delivery_seq: int
    status_revision: int
    quarantine: QuarantineStatus
    last_command: _command_envelope_pb2.CommandIdentityV2
    updated_at: _timestamp_pb2.Timestamp
    status_digest: str
    acknowledged_head_record_digest: str
    terminal_seal: TerminalSeal
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., acknowledged_through_delivery_seq: _Optional[int] = ..., status_revision: _Optional[int] = ..., quarantine: _Optional[_Union[QuarantineStatus, _Mapping]] = ..., last_command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status_digest: _Optional[str] = ..., acknowledged_head_record_digest: _Optional[str] = ..., terminal_seal: _Optional[_Union[TerminalSeal, _Mapping]] = ...) -> None: ...

class PullRecordsRequest(_message.Message):
    __slots__ = ("run_id", "producer", "after_delivery_seq", "page_size", "snapshot_through_delivery_seq")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    AFTER_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_THROUGH_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: ProducerFence
    after_delivery_seq: int
    page_size: int
    snapshot_through_delivery_seq: int
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., after_delivery_seq: _Optional[int] = ..., page_size: _Optional[int] = ..., snapshot_through_delivery_seq: _Optional[int] = ...) -> None: ...

class PullRecordsResponse(_message.Message):
    __slots__ = ("run_id", "producer", "page_after_delivery_seq", "snapshot_through_delivery_seq", "records", "next_after_delivery_seq", "has_more", "delivery_status", "snapshot_head_digest", "snapshot_head_record_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    PAGE_AFTER_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_THROUGH_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    NEXT_AFTER_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    HAS_MORE_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_STATUS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HEAD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HEAD_RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: ProducerFence
    page_after_delivery_seq: int
    snapshot_through_delivery_seq: int
    records: _containers.RepeatedCompositeFieldContainer[DeliveryRecord]
    next_after_delivery_seq: int
    has_more: bool
    delivery_status: DeliveryStatus
    snapshot_head_digest: str
    snapshot_head_record_digest: str
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., page_after_delivery_seq: _Optional[int] = ..., snapshot_through_delivery_seq: _Optional[int] = ..., records: _Optional[_Iterable[_Union[DeliveryRecord, _Mapping]]] = ..., next_after_delivery_seq: _Optional[int] = ..., has_more: _Optional[bool] = ..., delivery_status: _Optional[_Union[DeliveryStatus, _Mapping]] = ..., snapshot_head_digest: _Optional[str] = ..., snapshot_head_record_digest: _Optional[str] = ...) -> None: ...

class AdmissionReceipt(_message.Message):
    __slots__ = ("previous_delivery_seq", "delivery_seq", "record_ref", "record_digest", "submission_ref", "submission_digest", "session_admission_receipt_ref", "session_effect_digest")
    PREVIOUS_DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    RECORD_REF_FIELD_NUMBER: _ClassVar[int]
    RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_REF_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SESSION_ADMISSION_RECEIPT_REF_FIELD_NUMBER: _ClassVar[int]
    SESSION_EFFECT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    previous_delivery_seq: int
    delivery_seq: int
    record_ref: str
    record_digest: str
    submission_ref: str
    submission_digest: str
    session_admission_receipt_ref: str
    session_effect_digest: str
    def __init__(self, previous_delivery_seq: _Optional[int] = ..., delivery_seq: _Optional[int] = ..., record_ref: _Optional[str] = ..., record_digest: _Optional[str] = ..., submission_ref: _Optional[str] = ..., submission_digest: _Optional[str] = ..., session_admission_receipt_ref: _Optional[str] = ..., session_effect_digest: _Optional[str] = ...) -> None: ...

class AcknowledgeAdmissionsEffect(_message.Message):
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
    producer: ProducerFence
    expected_acknowledged_through: int
    expected_status_revision: int
    idempotency_ref: str
    receipts: _containers.RepeatedCompositeFieldContainer[AdmissionReceipt]
    effect_digest_domain: str
    effect_digest: str
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., expected_acknowledged_through: _Optional[int] = ..., expected_status_revision: _Optional[int] = ..., idempotency_ref: _Optional[str] = ..., receipts: _Optional[_Iterable[_Union[AdmissionReceipt, _Mapping]]] = ..., effect_digest_domain: _Optional[str] = ..., effect_digest: _Optional[str] = ...) -> None: ...

class AcknowledgeAdmissionsRequest(_message.Message):
    __slots__ = ("command", "effect")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    command: _command_envelope_pb2.CommandIdentityV2
    effect: AcknowledgeAdmissionsEffect
    def __init__(self, command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ..., effect: _Optional[_Union[AcknowledgeAdmissionsEffect, _Mapping]] = ...) -> None: ...

class AcknowledgeAdmissionsResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: DeliveryStatus
    def __init__(self, status: _Optional[_Union[DeliveryStatus, _Mapping]] = ...) -> None: ...

class QuarantineSubmissionEffect(_message.Message):
    __slots__ = ("run_id", "producer", "expected_acknowledged_through", "expected_status_revision", "idempotency_ref", "delivery_seq", "record_ref", "record_digest", "submission_ref", "submission_digest", "rejection_class", "reason_code", "session_rejection_digest", "effect_digest_domain", "effect_digest")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_ACKNOWLEDGED_THROUGH_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_STATUS_REVISION_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_REF_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_SEQ_FIELD_NUMBER: _ClassVar[int]
    RECORD_REF_FIELD_NUMBER: _ClassVar[int]
    RECORD_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_REF_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    REJECTION_CLASS_FIELD_NUMBER: _ClassVar[int]
    REASON_CODE_FIELD_NUMBER: _ClassVar[int]
    SESSION_REJECTION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    EFFECT_DIGEST_DOMAIN_FIELD_NUMBER: _ClassVar[int]
    EFFECT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    producer: ProducerFence
    expected_acknowledged_through: int
    expected_status_revision: int
    idempotency_ref: str
    delivery_seq: int
    record_ref: str
    record_digest: str
    submission_ref: str
    submission_digest: str
    rejection_class: RejectionClass
    reason_code: str
    session_rejection_digest: str
    effect_digest_domain: str
    effect_digest: str
    def __init__(self, run_id: _Optional[str] = ..., producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., expected_acknowledged_through: _Optional[int] = ..., expected_status_revision: _Optional[int] = ..., idempotency_ref: _Optional[str] = ..., delivery_seq: _Optional[int] = ..., record_ref: _Optional[str] = ..., record_digest: _Optional[str] = ..., submission_ref: _Optional[str] = ..., submission_digest: _Optional[str] = ..., rejection_class: _Optional[_Union[RejectionClass, str]] = ..., reason_code: _Optional[str] = ..., session_rejection_digest: _Optional[str] = ..., effect_digest_domain: _Optional[str] = ..., effect_digest: _Optional[str] = ...) -> None: ...

class QuarantineSubmissionRequest(_message.Message):
    __slots__ = ("command", "effect")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    command: _command_envelope_pb2.CommandIdentityV2
    effect: QuarantineSubmissionEffect
    def __init__(self, command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ..., effect: _Optional[_Union[QuarantineSubmissionEffect, _Mapping]] = ...) -> None: ...

class QuarantineSubmissionResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: DeliveryStatus
    def __init__(self, status: _Optional[_Union[DeliveryStatus, _Mapping]] = ...) -> None: ...

class GetDeliveryStatusRequest(_message.Message):
    __slots__ = ("producer", "run_id", "original_command")
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_COMMAND_FIELD_NUMBER: _ClassVar[int]
    producer: ProducerFence
    run_id: str
    original_command: _command_envelope_pb2.CommandIdentityV2
    def __init__(self, producer: _Optional[_Union[ProducerFence, _Mapping]] = ..., run_id: _Optional[str] = ..., original_command: _Optional[_Union[_command_envelope_pb2.CommandIdentityV2, _Mapping]] = ...) -> None: ...

class GetDeliveryStatusResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: DeliveryStatus
    def __init__(self, status: _Optional[_Union[DeliveryStatus, _Mapping]] = ...) -> None: ...
