import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from kokoro.platform.media.v1 import media_canonical_pb2 as _media_canonical_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MediaOperationState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MEDIA_OPERATION_STATE_UNSPECIFIED: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_ADMISSION_PENDING: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_AUTHORIZED: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_QUEUED: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_ACTIVE: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_FINALIZING: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_CANCEL_REQUESTED: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_RECONCILING: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_COMPLETED: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_PARTIAL: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_FAILED: _ClassVar[MediaOperationState]
    MEDIA_OPERATION_STATE_CANCELED: _ClassVar[MediaOperationState]

class MediaOperationOutcomeClass(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MEDIA_OPERATION_OUTCOME_CLASS_UNSPECIFIED: _ClassVar[MediaOperationOutcomeClass]
    MEDIA_OPERATION_OUTCOME_CLASS_CANONICAL: _ClassVar[MediaOperationOutcomeClass]
    MEDIA_OPERATION_OUTCOME_CLASS_IRRECONCILABLE: _ClassVar[MediaOperationOutcomeClass]

class MediaCandidateState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MEDIA_CANDIDATE_STATE_UNSPECIFIED: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_ALLOCATED: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_PRODUCING: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_OUTPUT_RECEIVED: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_VALIDATING: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_READY: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_RESTRICTED: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_FAILED: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_UNKNOWN: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_CANCEL_REQUESTED: _ClassVar[MediaCandidateState]
    MEDIA_CANDIDATE_STATE_CANCELED: _ClassVar[MediaCandidateState]

class MediaRuntimeErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MEDIA_RUNTIME_ERROR_CODE_UNSPECIFIED: _ClassVar[MediaRuntimeErrorCode]
    MEDIA_RUNTIME_ERROR_CODE_ACCESS_DENIED: _ClassVar[MediaRuntimeErrorCode]
    MEDIA_RUNTIME_ERROR_CODE_ACCESS_EXPIRED: _ClassVar[MediaRuntimeErrorCode]
    MEDIA_RUNTIME_ERROR_CODE_IDEMPOTENCY_CONFLICT: _ClassVar[MediaRuntimeErrorCode]
    MEDIA_RUNTIME_ERROR_CODE_OPERATION_NOT_FOUND: _ClassVar[MediaRuntimeErrorCode]
    MEDIA_RUNTIME_ERROR_CODE_OPERATION_VERSION_CONFLICT: _ClassVar[MediaRuntimeErrorCode]
    MEDIA_RUNTIME_ERROR_CODE_PROJECTION_BINDING_REJECTED: _ClassVar[MediaRuntimeErrorCode]
    MEDIA_RUNTIME_ERROR_CODE_POLICY_REJECTED: _ClassVar[MediaRuntimeErrorCode]
    MEDIA_RUNTIME_ERROR_CODE_OUTCOME_UNKNOWN: _ClassVar[MediaRuntimeErrorCode]

class MediaCommandRecoveryAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MEDIA_COMMAND_RECOVERY_ACTION_UNSPECIFIED: _ClassVar[MediaCommandRecoveryAction]
    MEDIA_COMMAND_RECOVERY_ACTION_GET_OPERATION: _ClassVar[MediaCommandRecoveryAction]
    MEDIA_COMMAND_RECOVERY_ACTION_RECOVER_COMMAND: _ClassVar[MediaCommandRecoveryAction]
    MEDIA_COMMAND_RECOVERY_ACTION_CONTACT_SUPPORT: _ClassVar[MediaCommandRecoveryAction]
MEDIA_OPERATION_STATE_UNSPECIFIED: MediaOperationState
MEDIA_OPERATION_STATE_ADMISSION_PENDING: MediaOperationState
MEDIA_OPERATION_STATE_AUTHORIZED: MediaOperationState
MEDIA_OPERATION_STATE_QUEUED: MediaOperationState
MEDIA_OPERATION_STATE_ACTIVE: MediaOperationState
MEDIA_OPERATION_STATE_FINALIZING: MediaOperationState
MEDIA_OPERATION_STATE_CANCEL_REQUESTED: MediaOperationState
MEDIA_OPERATION_STATE_RECONCILING: MediaOperationState
MEDIA_OPERATION_STATE_COMPLETED: MediaOperationState
MEDIA_OPERATION_STATE_PARTIAL: MediaOperationState
MEDIA_OPERATION_STATE_FAILED: MediaOperationState
MEDIA_OPERATION_STATE_CANCELED: MediaOperationState
MEDIA_OPERATION_OUTCOME_CLASS_UNSPECIFIED: MediaOperationOutcomeClass
MEDIA_OPERATION_OUTCOME_CLASS_CANONICAL: MediaOperationOutcomeClass
MEDIA_OPERATION_OUTCOME_CLASS_IRRECONCILABLE: MediaOperationOutcomeClass
MEDIA_CANDIDATE_STATE_UNSPECIFIED: MediaCandidateState
MEDIA_CANDIDATE_STATE_ALLOCATED: MediaCandidateState
MEDIA_CANDIDATE_STATE_PRODUCING: MediaCandidateState
MEDIA_CANDIDATE_STATE_OUTPUT_RECEIVED: MediaCandidateState
MEDIA_CANDIDATE_STATE_VALIDATING: MediaCandidateState
MEDIA_CANDIDATE_STATE_READY: MediaCandidateState
MEDIA_CANDIDATE_STATE_RESTRICTED: MediaCandidateState
MEDIA_CANDIDATE_STATE_FAILED: MediaCandidateState
MEDIA_CANDIDATE_STATE_UNKNOWN: MediaCandidateState
MEDIA_CANDIDATE_STATE_CANCEL_REQUESTED: MediaCandidateState
MEDIA_CANDIDATE_STATE_CANCELED: MediaCandidateState
MEDIA_RUNTIME_ERROR_CODE_UNSPECIFIED: MediaRuntimeErrorCode
MEDIA_RUNTIME_ERROR_CODE_ACCESS_DENIED: MediaRuntimeErrorCode
MEDIA_RUNTIME_ERROR_CODE_ACCESS_EXPIRED: MediaRuntimeErrorCode
MEDIA_RUNTIME_ERROR_CODE_IDEMPOTENCY_CONFLICT: MediaRuntimeErrorCode
MEDIA_RUNTIME_ERROR_CODE_OPERATION_NOT_FOUND: MediaRuntimeErrorCode
MEDIA_RUNTIME_ERROR_CODE_OPERATION_VERSION_CONFLICT: MediaRuntimeErrorCode
MEDIA_RUNTIME_ERROR_CODE_PROJECTION_BINDING_REJECTED: MediaRuntimeErrorCode
MEDIA_RUNTIME_ERROR_CODE_POLICY_REJECTED: MediaRuntimeErrorCode
MEDIA_RUNTIME_ERROR_CODE_OUTCOME_UNKNOWN: MediaRuntimeErrorCode
MEDIA_COMMAND_RECOVERY_ACTION_UNSPECIFIED: MediaCommandRecoveryAction
MEDIA_COMMAND_RECOVERY_ACTION_GET_OPERATION: MediaCommandRecoveryAction
MEDIA_COMMAND_RECOVERY_ACTION_RECOVER_COMMAND: MediaCommandRecoveryAction
MEDIA_COMMAND_RECOVERY_ACTION_CONTACT_SUPPORT: MediaCommandRecoveryAction

class AgentImageIntentV1(_message.Message):
    __slots__ = ("prompt_intent", "aspect_ratio", "candidate_count", "output_format")
    PROMPT_INTENT_FIELD_NUMBER: _ClassVar[int]
    ASPECT_RATIO_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_COUNT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FORMAT_FIELD_NUMBER: _ClassVar[int]
    prompt_intent: str
    aspect_ratio: _media_canonical_pb2.CanonicalImageAspectRatio
    candidate_count: int
    output_format: _media_canonical_pb2.CanonicalImageOutputFormat
    def __init__(self, prompt_intent: _Optional[str] = ..., aspect_ratio: _Optional[_Union[_media_canonical_pb2.CanonicalImageAspectRatio, str]] = ..., candidate_count: _Optional[int] = ..., output_format: _Optional[_Union[_media_canonical_pb2.CanonicalImageOutputFormat, str]] = ...) -> None: ...

class AgentImageSubmissionFingerprintInputV1(_message.Message):
    __slots__ = ("stable_output_slot_ref", "image_intent")
    STABLE_OUTPUT_SLOT_REF_FIELD_NUMBER: _ClassVar[int]
    IMAGE_INTENT_FIELD_NUMBER: _ClassVar[int]
    stable_output_slot_ref: str
    image_intent: AgentImageIntentV1
    def __init__(self, stable_output_slot_ref: _Optional[str] = ..., image_intent: _Optional[_Union[AgentImageIntentV1, _Mapping]] = ...) -> None: ...

class CreateAgentImageOperationRequest(_message.Message):
    __slots__ = ("media_access_handle", "media_projection_reservation_handle", "stable_output_slot_ref", "agent_media_command_ref", "caller_request_fingerprint", "image_intent")
    MEDIA_ACCESS_HANDLE_FIELD_NUMBER: _ClassVar[int]
    MEDIA_PROJECTION_RESERVATION_HANDLE_FIELD_NUMBER: _ClassVar[int]
    STABLE_OUTPUT_SLOT_REF_FIELD_NUMBER: _ClassVar[int]
    AGENT_MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    CALLER_REQUEST_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    IMAGE_INTENT_FIELD_NUMBER: _ClassVar[int]
    media_access_handle: str
    media_projection_reservation_handle: str
    stable_output_slot_ref: str
    agent_media_command_ref: str
    caller_request_fingerprint: str
    image_intent: AgentImageIntentV1
    def __init__(self, media_access_handle: _Optional[str] = ..., media_projection_reservation_handle: _Optional[str] = ..., stable_output_slot_ref: _Optional[str] = ..., agent_media_command_ref: _Optional[str] = ..., caller_request_fingerprint: _Optional[str] = ..., image_intent: _Optional[_Union[AgentImageIntentV1, _Mapping]] = ...) -> None: ...

class AgentMediaCandidateView(_message.Message):
    __slots__ = ("candidate_ref", "owner_version", "state", "artifact_version_handle")
    CANDIDATE_REF_FIELD_NUMBER: _ClassVar[int]
    OWNER_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_VERSION_HANDLE_FIELD_NUMBER: _ClassVar[int]
    candidate_ref: str
    owner_version: int
    state: MediaCandidateState
    artifact_version_handle: str
    def __init__(self, candidate_ref: _Optional[str] = ..., owner_version: _Optional[int] = ..., state: _Optional[_Union[MediaCandidateState, str]] = ..., artifact_version_handle: _Optional[str] = ...) -> None: ...

class AgentMediaOperationView(_message.Message):
    __slots__ = ("media_operation_handle", "operation_ref", "owner_version", "state", "outcome_class", "safe_progress_bps", "candidates", "cost_projection_ref", "cost_projection_owner_version", "observed_at")
    MEDIA_OPERATION_HANDLE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_REF_FIELD_NUMBER: _ClassVar[int]
    OWNER_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CLASS_FIELD_NUMBER: _ClassVar[int]
    SAFE_PROGRESS_BPS_FIELD_NUMBER: _ClassVar[int]
    CANDIDATES_FIELD_NUMBER: _ClassVar[int]
    COST_PROJECTION_REF_FIELD_NUMBER: _ClassVar[int]
    COST_PROJECTION_OWNER_VERSION_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    media_operation_handle: str
    operation_ref: str
    owner_version: int
    state: MediaOperationState
    outcome_class: MediaOperationOutcomeClass
    safe_progress_bps: int
    candidates: _containers.RepeatedCompositeFieldContainer[AgentMediaCandidateView]
    cost_projection_ref: str
    cost_projection_owner_version: int
    observed_at: _timestamp_pb2.Timestamp
    def __init__(self, media_operation_handle: _Optional[str] = ..., operation_ref: _Optional[str] = ..., owner_version: _Optional[int] = ..., state: _Optional[_Union[MediaOperationState, str]] = ..., outcome_class: _Optional[_Union[MediaOperationOutcomeClass, str]] = ..., safe_progress_bps: _Optional[int] = ..., candidates: _Optional[_Iterable[_Union[AgentMediaCandidateView, _Mapping]]] = ..., cost_projection_ref: _Optional[str] = ..., cost_projection_owner_version: _Optional[int] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class MediaRuntimeError(_message.Message):
    __slots__ = ("code", "safe_message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SAFE_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: MediaRuntimeErrorCode
    safe_message: str
    def __init__(self, code: _Optional[_Union[MediaRuntimeErrorCode, str]] = ..., safe_message: _Optional[str] = ...) -> None: ...

class SubmitMediaCommandAccepted(_message.Message):
    __slots__ = ("media_command_ref", "caller_request_fingerprint", "operation_ref", "receipt_version", "recorded_at", "recovery_action")
    MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    CALLER_REQUEST_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_REF_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_ACTION_FIELD_NUMBER: _ClassVar[int]
    media_command_ref: str
    caller_request_fingerprint: str
    operation_ref: str
    receipt_version: int
    recorded_at: _timestamp_pb2.Timestamp
    recovery_action: MediaCommandRecoveryAction
    def __init__(self, media_command_ref: _Optional[str] = ..., caller_request_fingerprint: _Optional[str] = ..., operation_ref: _Optional[str] = ..., receipt_version: _Optional[int] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., recovery_action: _Optional[_Union[MediaCommandRecoveryAction, str]] = ...) -> None: ...

class SubmitMediaCommandRejected(_message.Message):
    __slots__ = ("media_command_ref", "caller_request_fingerprint", "error", "receipt_version", "recorded_at")
    MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    CALLER_REQUEST_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    media_command_ref: str
    caller_request_fingerprint: str
    error: MediaRuntimeError
    receipt_version: int
    recorded_at: _timestamp_pb2.Timestamp
    def __init__(self, media_command_ref: _Optional[str] = ..., caller_request_fingerprint: _Optional[str] = ..., error: _Optional[_Union[MediaRuntimeError, _Mapping]] = ..., receipt_version: _Optional[int] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SubmitMediaCommandOutcomeUnknown(_message.Message):
    __slots__ = ("media_command_ref", "caller_request_fingerprint", "error", "receipt_version", "recorded_at", "recovery_action")
    MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    CALLER_REQUEST_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_ACTION_FIELD_NUMBER: _ClassVar[int]
    media_command_ref: str
    caller_request_fingerprint: str
    error: MediaRuntimeError
    receipt_version: int
    recorded_at: _timestamp_pb2.Timestamp
    recovery_action: MediaCommandRecoveryAction
    def __init__(self, media_command_ref: _Optional[str] = ..., caller_request_fingerprint: _Optional[str] = ..., error: _Optional[_Union[MediaRuntimeError, _Mapping]] = ..., receipt_version: _Optional[int] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., recovery_action: _Optional[_Union[MediaCommandRecoveryAction, str]] = ...) -> None: ...

class CancelMediaCommandAccepted(_message.Message):
    __slots__ = ("media_command_ref", "operation_ref", "receipt_version", "recorded_at", "recovery_action")
    MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    OPERATION_REF_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_ACTION_FIELD_NUMBER: _ClassVar[int]
    media_command_ref: str
    operation_ref: str
    receipt_version: int
    recorded_at: _timestamp_pb2.Timestamp
    recovery_action: MediaCommandRecoveryAction
    def __init__(self, media_command_ref: _Optional[str] = ..., operation_ref: _Optional[str] = ..., receipt_version: _Optional[int] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., recovery_action: _Optional[_Union[MediaCommandRecoveryAction, str]] = ...) -> None: ...

class CancelMediaCommandRejected(_message.Message):
    __slots__ = ("media_command_ref", "error", "receipt_version", "recorded_at")
    MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    media_command_ref: str
    error: MediaRuntimeError
    receipt_version: int
    recorded_at: _timestamp_pb2.Timestamp
    def __init__(self, media_command_ref: _Optional[str] = ..., error: _Optional[_Union[MediaRuntimeError, _Mapping]] = ..., receipt_version: _Optional[int] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CancelMediaCommandOutcomeUnknown(_message.Message):
    __slots__ = ("media_command_ref", "operation_ref", "error", "receipt_version", "recorded_at", "recovery_action")
    MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    OPERATION_REF_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_ACTION_FIELD_NUMBER: _ClassVar[int]
    media_command_ref: str
    operation_ref: str
    error: MediaRuntimeError
    receipt_version: int
    recorded_at: _timestamp_pb2.Timestamp
    recovery_action: MediaCommandRecoveryAction
    def __init__(self, media_command_ref: _Optional[str] = ..., operation_ref: _Optional[str] = ..., error: _Optional[_Union[MediaRuntimeError, _Mapping]] = ..., receipt_version: _Optional[int] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., recovery_action: _Optional[_Union[MediaCommandRecoveryAction, str]] = ...) -> None: ...

class MediaCommandReceipt(_message.Message):
    __slots__ = ("submit_accepted", "submit_rejected", "submit_outcome_unknown", "cancel_accepted", "cancel_rejected", "cancel_outcome_unknown")
    SUBMIT_ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    SUBMIT_REJECTED_FIELD_NUMBER: _ClassVar[int]
    SUBMIT_OUTCOME_UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    CANCEL_ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    CANCEL_REJECTED_FIELD_NUMBER: _ClassVar[int]
    CANCEL_OUTCOME_UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    submit_accepted: SubmitMediaCommandAccepted
    submit_rejected: SubmitMediaCommandRejected
    submit_outcome_unknown: SubmitMediaCommandOutcomeUnknown
    cancel_accepted: CancelMediaCommandAccepted
    cancel_rejected: CancelMediaCommandRejected
    cancel_outcome_unknown: CancelMediaCommandOutcomeUnknown
    def __init__(self, submit_accepted: _Optional[_Union[SubmitMediaCommandAccepted, _Mapping]] = ..., submit_rejected: _Optional[_Union[SubmitMediaCommandRejected, _Mapping]] = ..., submit_outcome_unknown: _Optional[_Union[SubmitMediaCommandOutcomeUnknown, _Mapping]] = ..., cancel_accepted: _Optional[_Union[CancelMediaCommandAccepted, _Mapping]] = ..., cancel_rejected: _Optional[_Union[CancelMediaCommandRejected, _Mapping]] = ..., cancel_outcome_unknown: _Optional[_Union[CancelMediaCommandOutcomeUnknown, _Mapping]] = ...) -> None: ...

class CreateAgentImageOperationResponse(_message.Message):
    __slots__ = ("receipt", "operation")
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    receipt: MediaCommandReceipt
    operation: AgentMediaOperationView
    def __init__(self, receipt: _Optional[_Union[MediaCommandReceipt, _Mapping]] = ..., operation: _Optional[_Union[AgentMediaOperationView, _Mapping]] = ...) -> None: ...

class CancelAgentMediaOperationRequest(_message.Message):
    __slots__ = ("media_access_handle", "operation_ref", "cancel_command_ref", "expected_operation_version")
    MEDIA_ACCESS_HANDLE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_REF_FIELD_NUMBER: _ClassVar[int]
    CANCEL_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_OPERATION_VERSION_FIELD_NUMBER: _ClassVar[int]
    media_access_handle: str
    operation_ref: str
    cancel_command_ref: str
    expected_operation_version: int
    def __init__(self, media_access_handle: _Optional[str] = ..., operation_ref: _Optional[str] = ..., cancel_command_ref: _Optional[str] = ..., expected_operation_version: _Optional[int] = ...) -> None: ...

class CancelAgentMediaOperationResponse(_message.Message):
    __slots__ = ("receipt", "operation")
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    receipt: MediaCommandReceipt
    operation: AgentMediaOperationView
    def __init__(self, receipt: _Optional[_Union[MediaCommandReceipt, _Mapping]] = ..., operation: _Optional[_Union[AgentMediaOperationView, _Mapping]] = ...) -> None: ...

class RecoverMediaOperationByCommandRequest(_message.Message):
    __slots__ = ("media_access_handle", "media_command_ref")
    MEDIA_ACCESS_HANDLE_FIELD_NUMBER: _ClassVar[int]
    MEDIA_COMMAND_REF_FIELD_NUMBER: _ClassVar[int]
    media_access_handle: str
    media_command_ref: str
    def __init__(self, media_access_handle: _Optional[str] = ..., media_command_ref: _Optional[str] = ...) -> None: ...

class RecoverMediaOperationByCommandResponse(_message.Message):
    __slots__ = ("receipt", "operation")
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    receipt: MediaCommandReceipt
    operation: AgentMediaOperationView
    def __init__(self, receipt: _Optional[_Union[MediaCommandReceipt, _Mapping]] = ..., operation: _Optional[_Union[AgentMediaOperationView, _Mapping]] = ...) -> None: ...

class GetAgentMediaOperationRequest(_message.Message):
    __slots__ = ("media_access_handle", "operation_ref")
    MEDIA_ACCESS_HANDLE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_REF_FIELD_NUMBER: _ClassVar[int]
    media_access_handle: str
    operation_ref: str
    def __init__(self, media_access_handle: _Optional[str] = ..., operation_ref: _Optional[str] = ...) -> None: ...

class GetAgentMediaOperationResponse(_message.Message):
    __slots__ = ("operation",)
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    operation: AgentMediaOperationView
    def __init__(self, operation: _Optional[_Union[AgentMediaOperationView, _Mapping]] = ...) -> None: ...
