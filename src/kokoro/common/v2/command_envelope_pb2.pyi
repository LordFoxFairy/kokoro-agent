import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from kokoro.common.v1 import error_pb2 as _error_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CommandDigestAlgorithmV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMMAND_DIGEST_ALGORITHM_V2_UNSPECIFIED: _ClassVar[CommandDigestAlgorithmV2]
    COMMAND_DIGEST_ALGORITHM_V2_SHA256_COMMAND_ENVELOPE: _ClassVar[CommandDigestAlgorithmV2]

class OperatorAssuranceLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPERATOR_ASSURANCE_LEVEL_UNSPECIFIED: _ClassVar[OperatorAssuranceLevel]
    OPERATOR_ASSURANCE_LEVEL_PASSWORD: _ClassVar[OperatorAssuranceLevel]
    OPERATOR_ASSURANCE_LEVEL_MFA: _ClassVar[OperatorAssuranceLevel]
    OPERATOR_ASSURANCE_LEVEL_PHISHING_RESISTANT: _ClassVar[OperatorAssuranceLevel]

class CommandReceiptStateV2(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMMAND_RECEIPT_STATE_V2_UNSPECIFIED: _ClassVar[CommandReceiptStateV2]
    COMMAND_RECEIPT_STATE_V2_ACCEPTED: _ClassVar[CommandReceiptStateV2]
    COMMAND_RECEIPT_STATE_V2_COMMITTED: _ClassVar[CommandReceiptStateV2]
    COMMAND_RECEIPT_STATE_V2_REJECTED: _ClassVar[CommandReceiptStateV2]
    COMMAND_RECEIPT_STATE_V2_OUTCOME_UNKNOWN: _ClassVar[CommandReceiptStateV2]
COMMAND_DIGEST_ALGORITHM_V2_UNSPECIFIED: CommandDigestAlgorithmV2
COMMAND_DIGEST_ALGORITHM_V2_SHA256_COMMAND_ENVELOPE: CommandDigestAlgorithmV2
OPERATOR_ASSURANCE_LEVEL_UNSPECIFIED: OperatorAssuranceLevel
OPERATOR_ASSURANCE_LEVEL_PASSWORD: OperatorAssuranceLevel
OPERATOR_ASSURANCE_LEVEL_MFA: OperatorAssuranceLevel
OPERATOR_ASSURANCE_LEVEL_PHISHING_RESISTANT: OperatorAssuranceLevel
COMMAND_RECEIPT_STATE_V2_UNSPECIFIED: CommandReceiptStateV2
COMMAND_RECEIPT_STATE_V2_ACCEPTED: CommandReceiptStateV2
COMMAND_RECEIPT_STATE_V2_COMMITTED: CommandReceiptStateV2
COMMAND_RECEIPT_STATE_V2_REJECTED: CommandReceiptStateV2
COMMAND_RECEIPT_STATE_V2_OUTCOME_UNKNOWN: CommandReceiptStateV2

class CanonicalTypedProtobufV2(_message.Message):
    __slots__ = ("type_name", "known_field_protobuf")
    TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    KNOWN_FIELD_PROTOBUF_FIELD_NUMBER: _ClassVar[int]
    type_name: str
    known_field_protobuf: bytes
    def __init__(self, type_name: _Optional[str] = ..., known_field_protobuf: _Optional[bytes] = ...) -> None: ...

class CanonicalSecurityEpochV2(_message.Message):
    __slots__ = ("axis", "value")
    AXIS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    axis: str
    value: int
    def __init__(self, axis: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...

class CanonicalCommandTrustAxesV2(_message.Message):
    __slots__ = ("workload_identity_ref", "audience", "environment", "region", "site_ref", "actor_ref", "actor_session_ref", "managed_device_ref", "security_epochs", "actor_generation", "assurance_level", "factor_classes", "authenticated_at", "step_up_at", "operator_attestation_ref", "operator_attestation_digest")
    WORKLOAD_IDENTITY_REF_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    SITE_REF_FIELD_NUMBER: _ClassVar[int]
    ACTOR_REF_FIELD_NUMBER: _ClassVar[int]
    ACTOR_SESSION_REF_FIELD_NUMBER: _ClassVar[int]
    MANAGED_DEVICE_REF_FIELD_NUMBER: _ClassVar[int]
    SECURITY_EPOCHS_FIELD_NUMBER: _ClassVar[int]
    ACTOR_GENERATION_FIELD_NUMBER: _ClassVar[int]
    ASSURANCE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    FACTOR_CLASSES_FIELD_NUMBER: _ClassVar[int]
    AUTHENTICATED_AT_FIELD_NUMBER: _ClassVar[int]
    STEP_UP_AT_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_ATTESTATION_REF_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_ATTESTATION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    workload_identity_ref: str
    audience: str
    environment: str
    region: str
    site_ref: str
    actor_ref: str
    actor_session_ref: str
    managed_device_ref: str
    security_epochs: _containers.RepeatedCompositeFieldContainer[CanonicalSecurityEpochV2]
    actor_generation: int
    assurance_level: OperatorAssuranceLevel
    factor_classes: _containers.RepeatedScalarFieldContainer[str]
    authenticated_at: _timestamp_pb2.Timestamp
    step_up_at: _timestamp_pb2.Timestamp
    operator_attestation_ref: str
    operator_attestation_digest: str
    def __init__(self, workload_identity_ref: _Optional[str] = ..., audience: _Optional[str] = ..., environment: _Optional[str] = ..., region: _Optional[str] = ..., site_ref: _Optional[str] = ..., actor_ref: _Optional[str] = ..., actor_session_ref: _Optional[str] = ..., managed_device_ref: _Optional[str] = ..., security_epochs: _Optional[_Iterable[_Union[CanonicalSecurityEpochV2, _Mapping]]] = ..., actor_generation: _Optional[int] = ..., assurance_level: _Optional[_Union[OperatorAssuranceLevel, str]] = ..., factor_classes: _Optional[_Iterable[str]] = ..., authenticated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., step_up_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., operator_attestation_ref: _Optional[str] = ..., operator_attestation_digest: _Optional[str] = ...) -> None: ...

class CanonicalCommandEnvelopeV2(_message.Message):
    __slots__ = ("contract_version", "operation", "trust", "scope", "target_refs", "effect")
    CONTRACT_VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    TRUST_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_REFS_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    contract_version: str
    operation: str
    trust: CanonicalCommandTrustAxesV2
    scope: CanonicalTypedProtobufV2
    target_refs: _containers.RepeatedScalarFieldContainer[str]
    effect: CanonicalTypedProtobufV2
    def __init__(self, contract_version: _Optional[str] = ..., operation: _Optional[str] = ..., trust: _Optional[_Union[CanonicalCommandTrustAxesV2, _Mapping]] = ..., scope: _Optional[_Union[CanonicalTypedProtobufV2, _Mapping]] = ..., target_refs: _Optional[_Iterable[str]] = ..., effect: _Optional[_Union[CanonicalTypedProtobufV2, _Mapping]] = ...) -> None: ...

class CommandIdentityV2(_message.Message):
    __slots__ = ("command_id", "idempotency_key", "digest_algorithm", "request_digest")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    DIGEST_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    idempotency_key: str
    digest_algorithm: CommandDigestAlgorithmV2
    request_digest: str
    def __init__(self, command_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., digest_algorithm: _Optional[_Union[CommandDigestAlgorithmV2, str]] = ..., request_digest: _Optional[str] = ...) -> None: ...

class CommandReceiptV2(_message.Message):
    __slots__ = ("identity", "operation", "state", "recorded_at", "error")
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    identity: CommandIdentityV2
    operation: str
    state: CommandReceiptStateV2
    recorded_at: _timestamp_pb2.Timestamp
    error: _error_pb2.KokoroErrorDetail
    def __init__(self, identity: _Optional[_Union[CommandIdentityV2, _Mapping]] = ..., operation: _Optional[str] = ..., state: _Optional[_Union[CommandReceiptStateV2, str]] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., error: _Optional[_Union[_error_pb2.KokoroErrorDetail, _Mapping]] = ...) -> None: ...
