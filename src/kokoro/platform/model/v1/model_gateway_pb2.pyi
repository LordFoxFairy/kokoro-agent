from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ModelMessageRole(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODEL_MESSAGE_ROLE_UNSPECIFIED: _ClassVar[ModelMessageRole]
    MODEL_MESSAGE_ROLE_SYSTEM: _ClassVar[ModelMessageRole]
    MODEL_MESSAGE_ROLE_USER: _ClassVar[ModelMessageRole]
    MODEL_MESSAGE_ROLE_ASSISTANT: _ClassVar[ModelMessageRole]
    MODEL_MESSAGE_ROLE_TOOL: _ClassVar[ModelMessageRole]

class ModelToolChoice(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODEL_TOOL_CHOICE_UNSPECIFIED: _ClassVar[ModelToolChoice]
    MODEL_TOOL_CHOICE_AUTO: _ClassVar[ModelToolChoice]
    MODEL_TOOL_CHOICE_NONE: _ClassVar[ModelToolChoice]
    MODEL_TOOL_CHOICE_REQUIRED: _ClassVar[ModelToolChoice]
MODEL_MESSAGE_ROLE_UNSPECIFIED: ModelMessageRole
MODEL_MESSAGE_ROLE_SYSTEM: ModelMessageRole
MODEL_MESSAGE_ROLE_USER: ModelMessageRole
MODEL_MESSAGE_ROLE_ASSISTANT: ModelMessageRole
MODEL_MESSAGE_ROLE_TOOL: ModelMessageRole
MODEL_TOOL_CHOICE_UNSPECIFIED: ModelToolChoice
MODEL_TOOL_CHOICE_AUTO: ModelToolChoice
MODEL_TOOL_CHOICE_NONE: ModelToolChoice
MODEL_TOOL_CHOICE_REQUIRED: ModelToolChoice

class ModelToolCall(_message.Message):
    __slots__ = ("id", "name", "arguments_json")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGUMENTS_JSON_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    arguments_json: bytes
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., arguments_json: _Optional[bytes] = ...) -> None: ...

class ModelMessage(_message.Message):
    __slots__ = ("role", "content", "tool_calls", "tool_call_id", "name")
    ROLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    TOOL_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    role: ModelMessageRole
    content: str
    tool_calls: _containers.RepeatedCompositeFieldContainer[ModelToolCall]
    tool_call_id: str
    name: str
    def __init__(self, role: _Optional[_Union[ModelMessageRole, str]] = ..., content: _Optional[str] = ..., tool_calls: _Optional[_Iterable[_Union[ModelToolCall, _Mapping]]] = ..., tool_call_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class ModelToolDefinition(_message.Message):
    __slots__ = ("name", "description", "input_schema_json")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    INPUT_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    input_schema_json: bytes
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., input_schema_json: _Optional[bytes] = ...) -> None: ...

class ChatCompletionRequest(_message.Message):
    __slots__ = ("protocol", "model", "messages", "max_output_tokens", "tools", "tool_choice", "required_tool_name")
    PROTOCOL_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    MAX_OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    TOOLS_FIELD_NUMBER: _ClassVar[int]
    TOOL_CHOICE_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_TOOL_NAME_FIELD_NUMBER: _ClassVar[int]
    protocol: str
    model: str
    messages: _containers.RepeatedCompositeFieldContainer[ModelMessage]
    max_output_tokens: int
    tools: _containers.RepeatedCompositeFieldContainer[ModelToolDefinition]
    tool_choice: ModelToolChoice
    required_tool_name: str
    def __init__(self, protocol: _Optional[str] = ..., model: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[ModelMessage, _Mapping]]] = ..., max_output_tokens: _Optional[int] = ..., tools: _Optional[_Iterable[_Union[ModelToolDefinition, _Mapping]]] = ..., tool_choice: _Optional[_Union[ModelToolChoice, str]] = ..., required_tool_name: _Optional[str] = ...) -> None: ...

class InvokeModelRequest(_message.Message):
    __slots__ = ("model_authorization_handle", "logical_call_ref", "attempt_ref", "producer_context", "producer_generation", "request")
    MODEL_AUTHORIZATION_HANDLE_FIELD_NUMBER: _ClassVar[int]
    LOGICAL_CALL_REF_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_REF_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_GENERATION_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    model_authorization_handle: str
    logical_call_ref: str
    attempt_ref: str
    producer_context: str
    producer_generation: int
    request: ChatCompletionRequest
    def __init__(self, model_authorization_handle: _Optional[str] = ..., logical_call_ref: _Optional[str] = ..., attempt_ref: _Optional[str] = ..., producer_context: _Optional[str] = ..., producer_generation: _Optional[int] = ..., request: _Optional[_Union[ChatCompletionRequest, _Mapping]] = ...) -> None: ...

class ModelUsage(_message.Message):
    __slots__ = ("input_tokens", "output_tokens")
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    input_tokens: int
    output_tokens: int
    def __init__(self, input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ...) -> None: ...

class ModelCompleted(_message.Message):
    __slots__ = ("response_id", "content", "tool_calls", "reasoning_content", "finish_reason", "usage")
    RESPONSE_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    REASONING_CONTENT_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    response_id: str
    content: str
    tool_calls: _containers.RepeatedCompositeFieldContainer[ModelToolCall]
    reasoning_content: str
    finish_reason: str
    usage: ModelUsage
    def __init__(self, response_id: _Optional[str] = ..., content: _Optional[str] = ..., tool_calls: _Optional[_Iterable[_Union[ModelToolCall, _Mapping]]] = ..., reasoning_content: _Optional[str] = ..., finish_reason: _Optional[str] = ..., usage: _Optional[_Union[ModelUsage, _Mapping]] = ...) -> None: ...

class ModelFailed(_message.Message):
    __slots__ = ("code", "retryable")
    CODE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    code: str
    retryable: bool
    def __init__(self, code: _Optional[str] = ..., retryable: _Optional[bool] = ...) -> None: ...

class ModelOutcomeUnknown(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class InvokeModelResponse(_message.Message):
    __slots__ = ("invocation_ref", "attempt_ref", "replayed", "completed", "failed", "outcome_unknown")
    INVOCATION_REF_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_REF_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    invocation_ref: str
    attempt_ref: str
    replayed: bool
    completed: ModelCompleted
    failed: ModelFailed
    outcome_unknown: ModelOutcomeUnknown
    def __init__(self, invocation_ref: _Optional[str] = ..., attempt_ref: _Optional[str] = ..., replayed: _Optional[bool] = ..., completed: _Optional[_Union[ModelCompleted, _Mapping]] = ..., failed: _Optional[_Union[ModelFailed, _Mapping]] = ..., outcome_unknown: _Optional[_Union[ModelOutcomeUnknown, _Mapping]] = ...) -> None: ...
