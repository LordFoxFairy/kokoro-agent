# LangChain leaves convert_to_openai_tool/content partly unknown, while Pydantic's
# generated subclass initializer is not represented in BaseChatModel's overloads.
# Keep those upstream typing gaps confined to this adapter module.
# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportCallIssue=false

"""LangChain chat-model adapter for the private Platform Model Gateway.

GA owns conversation/tool orchestration.  Platform owns model authorization,
provider dispatch and usage settlement.  This adapter deliberately performs one
ConnectRPC call and never retries a provider effect itself.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Protocol, TypeGuard
from urllib.parse import urlsplit, urlunsplit

import pyqwest
from connectrpc.errors import ConnectError
from langchain_core.callbacks.manager import (
    AsyncCallbackManagerForLLMRun,
    CallbackManagerForLLMRun,
)
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolCall,
    ToolMessage,
    UsageMetadata,
)
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_core.runnables import Runnable, RunnableConfig, ensure_config
from langchain_core.tools import BaseTool
from langchain_core.utils.function_calling import convert_to_openai_tool
from pydantic import ConfigDict, PrivateAttr

from kokoro.platform.model.v1 import model_gateway_pb2 as gateway_pb
from kokoro.platform.model.v1.model_gateway_connect import (
    ModelGatewayServiceClient,
    ModelGatewayServiceClientSync,
)
from kokoro_agent.model.streaming import (
    ModelStreamOutcomeUnknown,
    ModelStreamProtocolError,
    ModelStreamRejected,
    ModelStreamTransportError,
    aiter_verified_model_stream,
    iter_verified_model_stream,
)

_stream_checkpoint_namespace: ContextVar[str | None] = ContextVar(
    "model_gateway_stream_checkpoint_namespace",
    default=None,
)


class AsyncModelGatewayClient(Protocol):
    async def invoke_model(
        self,
        request: gateway_pb.InvokeModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> gateway_pb.InvokeModelResponse: ...

    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[gateway_pb.StreamModelResponse]: ...


class SyncModelGatewayClient(Protocol):
    def invoke_model(
        self,
        request: gateway_pb.InvokeModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> gateway_pb.InvokeModelResponse: ...

    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> Iterator[gateway_pb.StreamModelResponse]: ...


class ModelGatewayError(RuntimeError):
    """Safe base exception: provider response bodies never enter the exception."""


class ModelGatewayRejected(ModelGatewayError):
    def __init__(self, *, code: str, retryable: bool) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


class ModelGatewayOutcomeUnknown(ModelGatewayError):
    """The exact attempt may be replayed/reconciled; a new provider effect is forbidden."""

    def __init__(self, *, invocation_ref: str, attempt_ref: str) -> None:
        super().__init__("MODEL_GATEWAY_OUTCOME_UNKNOWN")
        self.invocation_ref = invocation_ref
        self.attempt_ref = attempt_ref


class ModelGatewayUnavailable(ModelGatewayError):
    def __init__(self, *, rpc_code: str) -> None:
        super().__init__("MODEL_GATEWAY_UNAVAILABLE")
        self.rpc_code = rpc_code


class PlatformModelGatewayChatModel(BaseChatModel):
    """LangChain adapter backed only by verified Platform unary/stream RPCs."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True, extra="forbid")

    model_name: str
    authorization_handle: str
    run_id: str
    producer_generation: int
    maximum_output_tokens: int
    timeout_ms: int

    _async_client: AsyncModelGatewayClient = PrivateAttr()
    _sync_client: SyncModelGatewayClient = PrivateAttr()

    def __init__(
        self,
        *,
        model_name: str,
        authorization_handle: str,
        run_id: str,
        producer_generation: int,
        maximum_output_tokens: int,
        timeout_ms: int,
        gateway_url: str | None = None,
        ca_file: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
        async_client: AsyncModelGatewayClient | None = None,
        sync_client: SyncModelGatewayClient | None = None,
    ) -> None:
        super().__init__(
            model_name=model_name,
            authorization_handle=authorization_handle,
            run_id=run_id,
            producer_generation=producer_generation,
            maximum_output_tokens=maximum_output_tokens,
            timeout_ms=timeout_ms,
        )
        if async_client is None or sync_client is None:
            if gateway_url is None or ca_file is None or cert_file is None or key_file is None:
                raise ValueError("MODEL_GATEWAY_MTLS_CONFIGURATION_REQUIRED")
            address = _gateway_address(gateway_url)
            ca = _tls_file(ca_file, "MODEL_GATEWAY_CA_FILE_INVALID")
            cert = _tls_file(cert_file, "MODEL_GATEWAY_CERT_FILE_INVALID")
            key = _tls_file(key_file, "MODEL_GATEWAY_KEY_FILE_INVALID")
            async_http = pyqwest.Client(
                pyqwest.HTTPTransport(
                    tls_ca_cert=ca,
                    tls_include_system_certs=False,
                    tls_key=key,
                    tls_cert=cert,
                    http_version=pyqwest.HTTPVersion.HTTP2,
                    enable_cookie_store=False,
                )
            )
            sync_http = pyqwest.SyncClient(
                pyqwest.SyncHTTPTransport(
                    tls_ca_cert=ca,
                    tls_include_system_certs=False,
                    tls_key=key,
                    tls_cert=cert,
                    http_version=pyqwest.HTTPVersion.HTTP2,
                    enable_cookie_store=False,
                )
            )
            async_client = ModelGatewayServiceClient(
                address,
                timeout_ms=timeout_ms,
                read_max_bytes=9 * 1024 * 1024,
                http_client=async_http,
            )
            sync_client = ModelGatewayServiceClientSync(
                address,
                timeout_ms=timeout_ms,
                read_max_bytes=9 * 1024 * 1024,
                http_client=sync_http,
            )
        self._async_client = async_client
        self._sync_client = sync_client

    @property
    def _llm_type(self) -> str:
        return "kokoro-platform-model-gateway"

    @property
    def _identifying_params(self) -> Mapping[str, Any]:
        return {"model_name": self.model_name}

    def bind_tools(
        self,
        tools: Sequence[dict[str, Any] | type | Callable[..., Any] | BaseTool],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        strict = kwargs.pop("strict", None)
        if strict is not None and not isinstance(strict, bool):
            raise ValueError("MODEL_GATEWAY_TOOL_STRICT_INVALID")
        formatted = [convert_to_openai_tool(tool, strict=strict) for tool in tools]
        return self.bind(tools=formatted, tool_choice=tool_choice, **kwargs)

    def stream(
        self,
        input: LanguageModelInput,
        config: RunnableConfig | None = None,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> Iterator[AIMessageChunk]:
        resolved_config = ensure_config(config)
        token = _stream_checkpoint_namespace.set(
            _checkpoint_namespace_from_config(resolved_config)
        )
        try:
            yield from super().stream(
                input,
                config=resolved_config,
                stop=stop,
                **kwargs,
            )
        finally:
            _stream_checkpoint_namespace.reset(token)

    async def astream(
        self,
        input: LanguageModelInput,
        config: RunnableConfig | None = None,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[AIMessageChunk]:
        resolved_config = ensure_config(config)
        token = _stream_checkpoint_namespace.set(
            _checkpoint_namespace_from_config(resolved_config)
        )
        try:
            async for chunk in super().astream(
                input,
                config=resolved_config,
                stop=stop,
                **kwargs,
            ):
                yield chunk
        finally:
            _stream_checkpoint_namespace.reset(token)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        request = self._request(messages, stop, run_manager, kwargs)
        try:
            response = self._sync_client.invoke_model(request, timeout_ms=self.timeout_ms)
        except ConnectError as error:
            raise ModelGatewayUnavailable(rpc_code=error.code.name) from None
        except Exception as error:
            raise ModelGatewayUnavailable(rpc_code=type(error).__name__) from None
        return self._result(response, expected_attempt_ref=request.attempt_ref)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        request = self._request(messages, stop, run_manager, kwargs)
        try:
            response = await self._async_client.invoke_model(request, timeout_ms=self.timeout_ms)
        except ConnectError as error:
            raise ModelGatewayUnavailable(rpc_code=error.code.name) from None
        except Exception as error:
            raise ModelGatewayUnavailable(rpc_code=type(error).__name__) from None
        return self._result(response, expected_attempt_ref=request.attempt_ref)

    def _stream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> Iterator[ChatGenerationChunk]:
        request = self._request(messages, stop, run_manager, kwargs)
        try:
            for chunk in iter_verified_model_stream(
                self._sync_client,
                request,
                timeout_ms=self.timeout_ms,
            ):
                if run_manager is not None:
                    run_manager.on_llm_new_token(chunk.text, chunk=chunk)
                yield chunk
        except ModelStreamRejected as error:
            raise ModelGatewayRejected(code=error.code, retryable=error.retryable) from None
        except ModelStreamOutcomeUnknown as error:
            raise ModelGatewayOutcomeUnknown(
                invocation_ref=error.invocation_ref,
                attempt_ref=error.attempt_ref,
            ) from None
        except ModelStreamTransportError as error:
            raise ModelGatewayUnavailable(rpc_code=error.rpc_code) from None
        except ModelStreamProtocolError:
            raise ModelGatewayUnavailable(rpc_code="INVALID_STREAM") from None
        except Exception as error:
            raise ModelGatewayUnavailable(rpc_code=type(error).__name__) from None

    async def _astream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        request = self._request(messages, stop, run_manager, kwargs)
        try:
            async for chunk in aiter_verified_model_stream(
                self._async_client,
                request,
                timeout_ms=self.timeout_ms,
            ):
                if run_manager is not None:
                    await run_manager.on_llm_new_token(chunk.text, chunk=chunk)
                yield chunk
        except ModelStreamRejected as error:
            raise ModelGatewayRejected(code=error.code, retryable=error.retryable) from None
        except ModelStreamOutcomeUnknown as error:
            raise ModelGatewayOutcomeUnknown(
                invocation_ref=error.invocation_ref,
                attempt_ref=error.attempt_ref,
            ) from None
        except ModelStreamTransportError as error:
            raise ModelGatewayUnavailable(rpc_code=error.rpc_code) from None
        except ModelStreamProtocolError:
            raise ModelGatewayUnavailable(rpc_code="INVALID_STREAM") from None
        except Exception as error:
            raise ModelGatewayUnavailable(rpc_code=type(error).__name__) from None

    def _request(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None,
        run_manager: CallbackManagerForLLMRun | AsyncCallbackManagerForLLMRun | None,
        kwargs: Mapping[str, Any],
    ) -> gateway_pb.InvokeModelRequest:
        if stop:
            raise ValueError("MODEL_GATEWAY_STOP_SEQUENCES_UNSUPPORTED")
        checkpoint_namespace = _checkpoint_namespace(run_manager)
        logical_call_ref = _stable_reference(
            "model-call", "kokoro.ga.model-call.v1", self.run_id, checkpoint_namespace
        )
        attempt_ref = _stable_reference(
            "model-attempt",
            "kokoro.ga.model-attempt.v1",
            logical_call_ref,
            str(self.producer_generation),
        )
        tools, tool_choice, required_tool_name = _tools(kwargs)
        request_kwargs: dict[str, Any] = {
            "protocol": "openai.chat.completions.v1",
            "model": self.model_name,
            "messages": [_message(message) for message in messages],
            "max_output_tokens": self.maximum_output_tokens,
            "tools": tools,
            "tool_choice": tool_choice,
        }
        if required_tool_name is not None:
            request_kwargs["required_tool_name"] = required_tool_name
        return gateway_pb.InvokeModelRequest(
            model_authorization_handle=self.authorization_handle,
            logical_call_ref=logical_call_ref,
            attempt_ref=attempt_ref,
            producer_context=_stable_reference("ga-run", "kokoro.ga.run.v1", self.run_id),
            producer_generation=self.producer_generation,
            request=gateway_pb.ChatCompletionRequest(**request_kwargs),
        )

    def _result(
        self,
        response: gateway_pb.InvokeModelResponse,
        *,
        expected_attempt_ref: str,
    ) -> ChatResult:
        if response.attempt_ref != expected_attempt_ref:
            raise ModelGatewayUnavailable(rpc_code="INVALID_RESPONSE")
        outcome = response.WhichOneof("outcome")
        if outcome == "outcome_unknown":
            raise ModelGatewayOutcomeUnknown(
                invocation_ref=response.invocation_ref,
                attempt_ref=response.attempt_ref,
            )
        if outcome == "failed":
            raise ModelGatewayRejected(
                code=response.failed.code,
                retryable=response.failed.retryable,
            )
        if outcome != "completed":
            raise ModelGatewayUnavailable(rpc_code="INVALID_RESPONSE")
        completed = response.completed
        tool_calls: list[ToolCall] = [
            ToolCall(
                id=call.id,
                name=call.name,
                args=_parse_canonical_object(call.arguments_json),
            )
            for call in completed.tool_calls
        ]
        usage: UsageMetadata | None = None
        if completed.HasField("usage"):
            input_tokens = completed.usage.input_tokens
            output_tokens = completed.usage.output_tokens
            usage = {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens,
            }
        additional: dict[str, Any] = {}
        if completed.HasField("reasoning_content"):
            additional["reasoning_content"] = completed.reasoning_content
        metadata: dict[str, Any] = {
            "model_name": self.model_name,
            "model_gateway_invocation_ref": response.invocation_ref,
            "model_gateway_attempt_ref": response.attempt_ref,
            "model_gateway_replayed": response.replayed,
        }
        if completed.HasField("finish_reason"):
            metadata["finish_reason"] = completed.finish_reason
        message = AIMessage(
            content=completed.content,
            id=completed.response_id,
            tool_calls=tool_calls,
            additional_kwargs=additional,
            response_metadata=metadata,
            usage_metadata=usage,
        )
        return ChatResult(generations=[ChatGeneration(message=message)])


def _message(message: BaseMessage) -> gateway_pb.ModelMessage:
    content: object = message.content
    if not isinstance(content, str):
        raise ValueError("MODEL_GATEWAY_NON_TEXT_MESSAGE_UNSUPPORTED")
    fields: dict[str, Any] = {"content": content}
    if message.name is not None:
        fields["name"] = message.name
    if isinstance(message, SystemMessage):
        fields["role"] = gateway_pb.MODEL_MESSAGE_ROLE_SYSTEM
    elif isinstance(message, HumanMessage):
        fields["role"] = gateway_pb.MODEL_MESSAGE_ROLE_USER
    elif isinstance(message, AIMessage):
        if message.invalid_tool_calls:
            raise ValueError("MODEL_GATEWAY_INVALID_TOOL_CALL_HISTORY")
        fields["role"] = gateway_pb.MODEL_MESSAGE_ROLE_ASSISTANT
        fields["tool_calls"] = [
            gateway_pb.ModelToolCall(
                id=call["id"],
                name=call["name"],
                arguments_json=_canonical_json_object(call["args"]),
            )
            for call in message.tool_calls
        ]
    elif isinstance(message, ToolMessage):
        fields["role"] = gateway_pb.MODEL_MESSAGE_ROLE_TOOL
        fields["tool_call_id"] = message.tool_call_id
    else:
        raise ValueError(f"MODEL_GATEWAY_MESSAGE_TYPE_UNSUPPORTED:{message.type}")
    return gateway_pb.ModelMessage(**fields)


def _tools(
    kwargs: Mapping[str, Any],
) -> tuple[list[gateway_pb.ModelToolDefinition], int, str | None]:
    unknown = set(kwargs) - {"tools", "tool_choice"}
    if unknown:
        raise ValueError("MODEL_GATEWAY_INVOCATION_OPTIONS_UNSUPPORTED")
    raw_tools_value: object = kwargs.get("tools", [])
    if not _is_object_list(raw_tools_value):
        raise ValueError("MODEL_GATEWAY_TOOLS_INVALID")
    tools: list[gateway_pb.ModelToolDefinition] = []
    names: set[str] = set()
    for raw in raw_tools_value:
        if not _is_string_object_dict(raw):
            raise ValueError("MODEL_GATEWAY_TOOL_INVALID")
        if raw.get("type") != "function":
            raise ValueError("MODEL_GATEWAY_TOOL_INVALID")
        function = raw.get("function")
        if not _is_string_object_dict(function):
            raise ValueError("MODEL_GATEWAY_TOOL_INVALID")
        name = function.get("name")
        description = function.get("description", "")
        parameters = function.get("parameters", {})
        if not isinstance(name, str) or not isinstance(description, str) or name in names:
            raise ValueError("MODEL_GATEWAY_TOOL_INVALID")
        names.add(name)
        tools.append(
            gateway_pb.ModelToolDefinition(
                name=name,
                description=description,
                input_schema_json=_canonical_json_object(parameters),
            )
        )
    choice: object = kwargs.get("tool_choice")
    if not tools:
        if choice not in (None, "none"):
            raise ValueError("MODEL_GATEWAY_TOOL_CHOICE_INVALID")
        return tools, gateway_pb.MODEL_TOOL_CHOICE_NONE, None
    if choice in (None, "auto"):
        return tools, gateway_pb.MODEL_TOOL_CHOICE_AUTO, None
    if choice == "none":
        return tools, gateway_pb.MODEL_TOOL_CHOICE_NONE, None
    if choice in ("any", "required"):
        return tools, gateway_pb.MODEL_TOOL_CHOICE_REQUIRED, None
    required_name: object = choice
    if _is_string_object_dict(choice):
        function = choice.get("function")
        required_name = function.get("name") if _is_string_object_dict(function) else None
    if isinstance(required_name, str) and required_name in names:
        return tools, gateway_pb.MODEL_TOOL_CHOICE_REQUIRED, required_name
    raise ValueError("MODEL_GATEWAY_TOOL_CHOICE_INVALID")


def _checkpoint_namespace(
    manager: CallbackManagerForLLMRun | AsyncCallbackManagerForLLMRun | None,
) -> str:
    if manager is not None:
        value: object = manager.metadata.get("langgraph_checkpoint_ns")
    else:
        value = _stream_checkpoint_namespace.get()
    return _require_checkpoint_namespace(value)


def _checkpoint_namespace_from_config(config: RunnableConfig) -> str:
    metadata = config.get("metadata")
    value = (
        metadata.get("langgraph_checkpoint_ns")
        if isinstance(metadata, Mapping)
        else None
    )
    return _require_checkpoint_namespace(value)


def _require_checkpoint_namespace(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("MODEL_GATEWAY_STABLE_CALL_IDENTITY_REQUIRED")
    return value


def _stable_reference(prefix: str, domain: str, *parts: str) -> str:
    digest = hashlib.sha256()
    digest.update(domain.encode())
    for part in parts:
        digest.update(b"\0")
        digest.update(part.encode())
    return f"{prefix}:sha256:{digest.hexdigest()}"


def _canonical_json_object(value: object) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        parsed = json.loads(encoded)
    except (TypeError, ValueError, UnicodeError) as error:
        raise ValueError("MODEL_GATEWAY_JSON_OBJECT_INVALID") from error
    if not isinstance(parsed, dict):
        raise ValueError("MODEL_GATEWAY_JSON_OBJECT_INVALID")
    return encoded


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _is_string_object_dict(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict) and all(isinstance(key, str) for key in value)


def _parse_canonical_object(value: bytes) -> dict[str, Any]:
    try:
        text = value.decode("utf-8")
        parsed: object = json.loads(text)
    except (UnicodeError, ValueError) as error:
        raise ModelGatewayUnavailable(rpc_code="INVALID_RESPONSE") from error
    if not _is_string_object_dict(parsed):
        raise ModelGatewayUnavailable(rpc_code="INVALID_RESPONSE")
    if _canonical_json_object(parsed) != value:
        raise ModelGatewayUnavailable(rpc_code="INVALID_RESPONSE")
    return {key: item for key, item in parsed.items()}


def _gateway_address(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
    ):
        raise ValueError("MODEL_GATEWAY_URL_INVALID")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _tls_file(value: str, code: str) -> bytes:
    path = Path(value)
    if not path.is_absolute() or not path.is_file():
        raise ValueError(code)
    material = path.read_bytes()
    if not material or len(material) > 256 * 1024:
        raise ValueError(code)
    return material
