from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RetryClass(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RETRY_CLASS_UNSPECIFIED: _ClassVar[RetryClass]
    RETRY_CLASS_NEVER: _ClassVar[RetryClass]
    RETRY_CLASS_AFTER_DELAY: _ClassVar[RetryClass]
    RETRY_CLASS_SAME_IDENTITY: _ClassVar[RetryClass]
    RETRY_CLASS_RECONCILE_RECEIPT: _ClassVar[RetryClass]
RETRY_CLASS_UNSPECIFIED: RetryClass
RETRY_CLASS_NEVER: RetryClass
RETRY_CLASS_AFTER_DELAY: RetryClass
RETRY_CLASS_SAME_IDENTITY: RetryClass
RETRY_CLASS_RECONCILE_RECEIPT: RetryClass

class KokoroErrorDetail(_message.Message):
    __slots__ = ("domain_code", "retry_class", "request_id", "correlation_id", "safe_message", "retry_after_seconds", "receipt_ref", "required_contract_version")
    DOMAIN_CODE_FIELD_NUMBER: _ClassVar[int]
    RETRY_CLASS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    SAFE_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_SECONDS_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_REF_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_CONTRACT_VERSION_FIELD_NUMBER: _ClassVar[int]
    domain_code: str
    retry_class: RetryClass
    request_id: str
    correlation_id: str
    safe_message: str
    retry_after_seconds: int
    receipt_ref: str
    required_contract_version: str
    def __init__(self, domain_code: _Optional[str] = ..., retry_class: _Optional[_Union[RetryClass, str]] = ..., request_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., safe_message: _Optional[str] = ..., retry_after_seconds: _Optional[int] = ..., receipt_ref: _Optional[str] = ..., required_contract_version: _Optional[str] = ...) -> None: ...
