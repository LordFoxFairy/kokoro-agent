import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from kokoro.common.v1 import error_pb2 as _error_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CommandDigestAlgorithm(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMMAND_DIGEST_ALGORITHM_UNSPECIFIED: _ClassVar[CommandDigestAlgorithm]
    COMMAND_DIGEST_ALGORITHM_SHA256_PROTOBUF_V1: _ClassVar[CommandDigestAlgorithm]

class CommandReceiptState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMMAND_RECEIPT_STATE_UNSPECIFIED: _ClassVar[CommandReceiptState]
    COMMAND_RECEIPT_STATE_ACCEPTED: _ClassVar[CommandReceiptState]
    COMMAND_RECEIPT_STATE_COMMITTED: _ClassVar[CommandReceiptState]
    COMMAND_RECEIPT_STATE_REJECTED: _ClassVar[CommandReceiptState]
    COMMAND_RECEIPT_STATE_OUTCOME_UNKNOWN: _ClassVar[CommandReceiptState]
COMMAND_DIGEST_ALGORITHM_UNSPECIFIED: CommandDigestAlgorithm
COMMAND_DIGEST_ALGORITHM_SHA256_PROTOBUF_V1: CommandDigestAlgorithm
COMMAND_RECEIPT_STATE_UNSPECIFIED: CommandReceiptState
COMMAND_RECEIPT_STATE_ACCEPTED: CommandReceiptState
COMMAND_RECEIPT_STATE_COMMITTED: CommandReceiptState
COMMAND_RECEIPT_STATE_REJECTED: CommandReceiptState
COMMAND_RECEIPT_STATE_OUTCOME_UNKNOWN: CommandReceiptState

class CommandIdentity(_message.Message):
    __slots__ = ("command_id", "idempotency_key", "digest_algorithm", "request_digest")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    DIGEST_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    idempotency_key: str
    digest_algorithm: CommandDigestAlgorithm
    request_digest: str
    def __init__(self, command_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., digest_algorithm: _Optional[_Union[CommandDigestAlgorithm, str]] = ..., request_digest: _Optional[str] = ...) -> None: ...

class CommandReceipt(_message.Message):
    __slots__ = ("identity", "operation", "state", "recorded_at", "error")
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    identity: CommandIdentity
    operation: str
    state: CommandReceiptState
    recorded_at: _timestamp_pb2.Timestamp
    error: _error_pb2.KokoroErrorDetail
    def __init__(self, identity: _Optional[_Union[CommandIdentity, _Mapping]] = ..., operation: _Optional[str] = ..., state: _Optional[_Union[CommandReceiptState, str]] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., error: _Optional[_Union[_error_pb2.KokoroErrorDetail, _Mapping]] = ...) -> None: ...
