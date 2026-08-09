"""Verified resumable consumer for Platform Model Gateway stream frames.

The digest preimage is byte-equivalent to Root's generated
`model-stream-frame-digest.ts`. A reconnect always reuses the same immutable
`InvokeModelRequest` and advances only `after_sequence`; it can never dispatch a
new logical attempt.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from time import monotonic_ns
from typing import Protocol

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from langchain_core.messages import AIMessageChunk
from langchain_core.messages.ai import UsageMetadata
from langchain_core.messages.tool import tool_call_chunk
from langchain_core.outputs import ChatGenerationChunk
from pydantic import JsonValue, TypeAdapter

from kokoro.platform.model.v1 import model_gateway_pb2 as gateway_pb

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TOOL_NAME = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_ZERO_DIGEST = "0" * 64
_MAX_SEQUENCE = 65_536
_MAX_PAYLOAD_BYTES = 12 * 1024 * 1024
_MAX_OUTPUT_BYTES = 8 * 1024 * 1024
_MAX_TOOL_ARGUMENT_BYTES = 1024 * 1024
_MAX_RECONNECTS = 3
_NANOSECONDS_PER_MILLISECOND = 1_000_000
_DOMAIN = b"kokoro.platform.model.stream-frame.v1"
_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])
_RETRYABLE_RPC_CODES = frozenset(
    {
        Code.ABORTED,
        Code.DEADLINE_EXCEEDED,
        Code.INTERNAL,
        Code.RESOURCE_EXHAUSTED,
        Code.UNAVAILABLE,
        Code.UNKNOWN,
    }
)


class AsyncModelStreamClient(Protocol):
    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[gateway_pb.StreamModelResponse]: ...


class SyncModelStreamClient(Protocol):
    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> Iterator[gateway_pb.StreamModelResponse]: ...


class ModelStreamError(RuntimeError):
    """Safe stream error; remote payload material is never included."""


class ModelStreamProtocolError(ModelStreamError):
    def __init__(self) -> None:
        super().__init__("MODEL_GATEWAY_STREAM_INVALID")


class ModelStreamTransportError(ModelStreamError):
    def __init__(self, rpc_code: str) -> None:
        super().__init__("MODEL_GATEWAY_STREAM_UNAVAILABLE")
        self.rpc_code = rpc_code


class ModelStreamRejected(ModelStreamError):
    def __init__(self, code: str, retryable: bool) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


class ModelStreamOutcomeUnknown(ModelStreamError):
    def __init__(self, invocation_ref: str, attempt_ref: str) -> None:
        super().__init__("MODEL_GATEWAY_OUTCOME_UNKNOWN")
        self.invocation_ref = invocation_ref
        self.attempt_ref = attempt_ref


def _remaining_timeout_ms(deadline_ns: int) -> int:
    remaining_ms = (deadline_ns - monotonic_ns()) // _NANOSECONDS_PER_MILLISECOND
    if remaining_ms <= 0:
        raise ModelStreamTransportError("DEADLINE_EXCEEDED")
    return remaining_ms


@dataclass(slots=True)
class _ToolAggregate:
    id: str | None = None
    name: str | None = None
    arguments: bytes = b""


class ModelStreamVerifier:
    """Stateful identity, hash-chain, terminal, and semantic aggregate verifier."""

    def __init__(self, attempt_ref: str) -> None:
        if not _reference(attempt_ref, 256):
            raise ValueError("MODEL_GATEWAY_STREAM_ATTEMPT_REF_INVALID")
        self._attempt_ref = attempt_ref
        self._invocation_ref: str | None = None
        self._sequence = 0
        self._frame_digest: str = _ZERO_DIGEST
        self._accepted = False
        self._terminal = False
        self._content: list[str] = []
        self._reasoning: list[str] = []
        self._tools: dict[int, _ToolAggregate] = {}
        self._output_bytes = 0

    @property
    def after_sequence(self) -> int:
        return self._sequence

    @property
    def terminal(self) -> bool:
        return self._terminal

    def consume(self, frame: gateway_pb.StreamModelResponse) -> ChatGenerationChunk | None:
        if self._terminal:
            raise ModelStreamProtocolError()
        kind = frame.WhichOneof("payload")
        if kind is None:
            raise ModelStreamProtocolError()
        invocation_ref = frame.invocation_ref
        if (
            not _reference(invocation_ref, 256)
            or frame.attempt_ref != self._attempt_ref
            or frame.sequence != self._sequence + 1
            or frame.sequence > _MAX_SEQUENCE
            or frame.previous_frame_digest != self._frame_digest
            or _DIGEST.fullmatch(frame.frame_digest) is None
        ):
            raise ModelStreamProtocolError()
        if self._invocation_ref is None:
            self._invocation_ref = invocation_ref
        elif invocation_ref != self._invocation_ref:
            raise ModelStreamProtocolError()
        payload_bytes = model_stream_payload_bytes(frame)
        expected = model_stream_frame_digest(
            invocation_ref=invocation_ref,
            attempt_ref=frame.attempt_ref,
            sequence=frame.sequence,
            previous_frame_digest=frame.previous_frame_digest,
            payload_kind=kind,
            payload_bytes=payload_bytes,
        )
        if frame.frame_digest != expected:
            raise ModelStreamProtocolError()

        self._sequence = frame.sequence
        self._frame_digest = frame.frame_digest
        if kind == "accepted":
            if self._accepted or self._sequence != 1:
                raise ModelStreamProtocolError()
            self._accepted = True
            return None
        if not self._accepted:
            raise ModelStreamProtocolError()
        if kind == "content_delta":
            return self._content_delta(frame.content_delta.content)
        if kind == "reasoning_delta":
            return self._reasoning_delta(frame.reasoning_delta.content)
        if kind == "tool_call_delta":
            return self._tool_delta(frame.tool_call_delta)
        if kind == "completed":
            return self._completed(frame.completed)
        if kind == "failed":
            self._terminal = True
            if not _reference(frame.failed.code, 128):
                raise ModelStreamProtocolError()
            raise ModelStreamRejected(frame.failed.code, frame.failed.retryable)
        if kind == "outcome_unknown":
            self._terminal = True
            raise ModelStreamOutcomeUnknown(self._invocation_ref, self._attempt_ref)
        raise ModelStreamProtocolError()

    def finish(self) -> None:
        if not self._terminal:
            raise ModelStreamProtocolError()

    def _content_delta(self, content: str) -> ChatGenerationChunk:
        size = _utf8_length(content)
        if not 1 <= size <= 16_384:
            raise ModelStreamProtocolError()
        self._add_output_bytes(size)
        self._content.append(content)
        return ChatGenerationChunk(message=AIMessageChunk(content=content))

    def _reasoning_delta(self, content: str) -> ChatGenerationChunk:
        size = _utf8_length(content)
        if not 1 <= size <= 16_384:
            raise ModelStreamProtocolError()
        self._add_output_bytes(size)
        self._reasoning.append(content)
        return ChatGenerationChunk(
            message=AIMessageChunk(content="", additional_kwargs={"reasoning_content": content})
        )

    def _tool_delta(self, delta: gateway_pb.ModelToolCallDelta) -> ChatGenerationChunk:
        index = delta.tool_index
        if index > 127 or len(delta.arguments_json_fragment) > 16_384:
            raise ModelStreamProtocolError()
        aggregate = self._tools.setdefault(index, _ToolAggregate())
        identifier = delta.id if delta.HasField("id") else None
        name = delta.name if delta.HasField("name") else None
        if identifier is None and name is None and len(delta.arguments_json_fragment) == 0:
            raise ModelStreamProtocolError()
        if identifier is not None:
            if not _reference(identifier, 256) or aggregate.id not in (None, identifier):
                raise ModelStreamProtocolError()
            aggregate.id = identifier
        if name is not None:
            if _TOOL_NAME.fullmatch(name) is None or aggregate.name not in (None, name):
                raise ModelStreamProtocolError()
            aggregate.name = name
        arguments = aggregate.arguments + bytes(delta.arguments_json_fragment)
        if len(arguments) > _MAX_TOOL_ARGUMENT_BYTES:
            raise ModelStreamProtocolError()
        aggregate.arguments = arguments
        self._add_output_bytes(len(delta.arguments_json_fragment))
        try:
            fragment = bytes(delta.arguments_json_fragment).decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ModelStreamProtocolError() from error
        return ChatGenerationChunk(
            message=AIMessageChunk(
                content="",
                tool_call_chunks=[
                    tool_call_chunk(
                        name=name,
                        args=fragment,
                        id=identifier,
                        index=index,
                    )
                ],
            )
        )

    def _completed(self, completed: gateway_pb.ModelCompleted) -> ChatGenerationChunk:
        if not _reference(completed.response_id, 256):
            raise ModelStreamProtocolError()
        if completed.content != "".join(self._content):
            raise ModelStreamProtocolError()
        reasoning = "".join(self._reasoning)
        if (reasoning != "") != completed.HasField("reasoning_content"):
            raise ModelStreamProtocolError()
        if reasoning and completed.reasoning_content != reasoning:
            raise ModelStreamProtocolError()
        if len(completed.tool_calls) != len(self._tools):
            raise ModelStreamProtocolError()
        for index, call in enumerate(completed.tool_calls):
            aggregate = self._tools.get(index)
            if (
                aggregate is None
                or aggregate.id != call.id
                or aggregate.name != call.name
                or aggregate.arguments != bytes(call.arguments_json)
            ):
                raise ModelStreamProtocolError()
            _parse_canonical_object(call.arguments_json)
        metadata: dict[str, object] = {
            "model_gateway_invocation_ref": self._invocation_ref or "",
            "model_gateway_attempt_ref": self._attempt_ref,
        }
        generation_info: dict[str, object] | None = None
        if completed.HasField("finish_reason"):
            if not _reference(completed.finish_reason, 64):
                raise ModelStreamProtocolError()
            metadata["finish_reason"] = completed.finish_reason
            generation_info = {"finish_reason": completed.finish_reason}
        usage: UsageMetadata | None = None
        if completed.HasField("usage"):
            input_tokens = completed.usage.input_tokens
            output_tokens = completed.usage.output_tokens
            usage = UsageMetadata(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=input_tokens + output_tokens,
            )
        self._terminal = True
        return ChatGenerationChunk(
            message=AIMessageChunk(
                content="",
                id=completed.response_id,
                response_metadata=metadata,
                usage_metadata=usage,
                chunk_position="last",
            ),
            generation_info=generation_info,
        )

    def _add_output_bytes(self, size: int) -> None:
        self._output_bytes += size
        if self._output_bytes > _MAX_OUTPUT_BYTES:
            raise ModelStreamProtocolError()


def iter_verified_model_stream(
    client: SyncModelStreamClient,
    invocation: gateway_pb.InvokeModelRequest,
    *,
    timeout_ms: int,
) -> Iterator[ChatGenerationChunk]:
    verifier = ModelStreamVerifier(invocation.attempt_ref)
    reconnects = 0
    deadline_ns = monotonic_ns() + timeout_ms * _NANOSECONDS_PER_MILLISECOND
    remaining_timeout_ms = timeout_ms
    while not verifier.terminal:
        request = gateway_pb.StreamModelRequest(
            invocation=invocation,
            after_sequence=verifier.after_sequence,
        )
        try:
            for frame in client.stream_model(request, timeout_ms=remaining_timeout_ms):
                chunk = verifier.consume(frame)
                if chunk is not None:
                    yield chunk
            if verifier.terminal:
                break
            reconnects += 1
            if reconnects > _MAX_RECONNECTS:
                raise ModelStreamTransportError("PREMATURE_EOF")
        except ConnectError as error:
            if verifier.terminal:
                break
            if error.code not in _RETRYABLE_RPC_CODES:
                raise ModelStreamTransportError(error.code.name) from None
            reconnects += 1
            if reconnects > _MAX_RECONNECTS:
                raise ModelStreamTransportError(error.code.name) from None
        remaining_timeout_ms = _remaining_timeout_ms(deadline_ns)
    verifier.finish()


async def aiter_verified_model_stream(
    client: AsyncModelStreamClient,
    invocation: gateway_pb.InvokeModelRequest,
    *,
    timeout_ms: int,
) -> AsyncIterator[ChatGenerationChunk]:
    verifier = ModelStreamVerifier(invocation.attempt_ref)
    reconnects = 0
    deadline_ns = monotonic_ns() + timeout_ms * _NANOSECONDS_PER_MILLISECOND
    remaining_timeout_ms = timeout_ms
    while not verifier.terminal:
        request = gateway_pb.StreamModelRequest(
            invocation=invocation,
            after_sequence=verifier.after_sequence,
        )
        try:
            async for frame in client.stream_model(
                request,
                timeout_ms=remaining_timeout_ms,
            ):
                chunk = verifier.consume(frame)
                if chunk is not None:
                    yield chunk
            if verifier.terminal:
                break
            reconnects += 1
            if reconnects > _MAX_RECONNECTS:
                raise ModelStreamTransportError("PREMATURE_EOF")
        except ConnectError as error:
            if verifier.terminal:
                break
            if error.code not in _RETRYABLE_RPC_CODES:
                raise ModelStreamTransportError(error.code.name) from None
            reconnects += 1
            if reconnects > _MAX_RECONNECTS:
                raise ModelStreamTransportError(error.code.name) from None
        remaining_timeout_ms = _remaining_timeout_ms(deadline_ns)
    verifier.finish()


def model_stream_frame_digest(
    *,
    invocation_ref: str,
    attempt_ref: str,
    sequence: int,
    previous_frame_digest: str,
    payload_kind: str,
    payload_bytes: bytes,
) -> str:
    """Root generated helper's exact length-prefixed SHA-256 algorithm."""
    if (
        not _reference(invocation_ref, 256)
        or not _reference(attempt_ref, 256)
        or not 1 <= sequence <= _MAX_SEQUENCE
        or _DIGEST.fullmatch(previous_frame_digest) is None
        or payload_kind
        not in {
            "accepted",
            "content_delta",
            "reasoning_delta",
            "tool_call_delta",
            "completed",
            "failed",
            "outcome_unknown",
        }
        or not 1 <= len(payload_bytes) <= _MAX_PAYLOAD_BYTES
    ):
        raise ValueError("MODEL_STREAM_FRAME_DIGEST_INPUT_INVALID")
    digest = hashlib.sha256()
    digest.update(_DOMAIN)
    for field in (
        invocation_ref.encode("utf-8"),
        attempt_ref.encode("utf-8"),
        _uint64(sequence),
        previous_frame_digest.encode("utf-8"),
        payload_kind.encode("utf-8"),
        payload_bytes,
    ):
        digest.update(_uint64(len(field)))
        digest.update(field)
    return digest.hexdigest()


def model_stream_payload_bytes(frame: gateway_pb.StreamModelResponse) -> bytes:
    try:
        kind = frame.WhichOneof("payload")
        if kind == "accepted":
            value: object = {"kind": "accepted"}
        elif kind == "content_delta":
            value = {"kind": "content_delta", "content": frame.content_delta.content}
        elif kind == "reasoning_delta":
            value = {"kind": "reasoning_delta", "content": frame.reasoning_delta.content}
        elif kind == "tool_call_delta":
            delta = frame.tool_call_delta
            value = {
                "kind": "tool_call_delta",
                "toolIndex": delta.tool_index,
                **({"id": delta.id} if delta.HasField("id") else {}),
                **({"name": delta.name} if delta.HasField("name") else {}),
                "argumentsJsonFragment": _base64url(bytes(delta.arguments_json_fragment)),
            }
        elif kind == "completed":
            value = {
                "kind": "completed",
                "responseBody": _base64url(_completed_response_body(frame.completed)),
            }
        elif kind == "failed":
            value = {
                "kind": "failed",
                "responseBody": _base64url(_failed_response_body(frame.failed)),
            }
        elif kind == "outcome_unknown":
            value = {"kind": "outcome_unknown"}
        else:
            raise ModelStreamProtocolError()
        payload = _canonical_json(value)
    except ModelStreamProtocolError:
        raise
    except (UnicodeError, TypeError, ValueError) as error:
        raise ModelStreamProtocolError() from error
    if not 1 <= len(payload) <= _MAX_PAYLOAD_BYTES:
        raise ModelStreamProtocolError()
    return payload


def _completed_response_body(completed: gateway_pb.ModelCompleted) -> bytes:
    try:
        message: dict[str, object] = {
            "role": "assistant",
            "content": completed.content,
        }
        if completed.HasField("reasoning_content"):
            message["reasoning_content"] = completed.reasoning_content
        if completed.tool_calls:
            message["tool_calls"] = [
                {
                    "id": call.id,
                    "type": "function",
                    "function": {
                        "name": call.name,
                        "arguments": bytes(call.arguments_json).decode(
                            "utf-8", errors="strict"
                        ),
                    },
                }
                for call in completed.tool_calls
            ]
        choice: dict[str, object] = {"index": 0, "message": message}
        if completed.HasField("finish_reason"):
            choice["finish_reason"] = completed.finish_reason
        safe: dict[str, object] = {"id": completed.response_id, "choices": [choice]}
        if completed.HasField("usage"):
            safe["usage"] = {
                "prompt_tokens": completed.usage.input_tokens,
                "completion_tokens": completed.usage.output_tokens,
            }
        return _canonical_json(safe)
    except (UnicodeError, TypeError, ValueError) as error:
        raise ModelStreamProtocolError() from error


def _failed_response_body(failed: gateway_pb.ModelFailed) -> bytes:
    return _canonical_json(
        {"error": {"code": failed.code, "retryable": failed.retryable}}
    )


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _parse_canonical_object(value: bytes) -> None:
    try:
        parsed: object = json.loads(value.decode("utf-8", errors="strict"))
    except (UnicodeError, ValueError) as error:
        raise ModelStreamProtocolError() from error
    if not isinstance(parsed, dict) or _canonical_json(_JSON_OBJECT.validate_python(parsed)) != value:
        raise ModelStreamProtocolError()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _uint64(value: int) -> bytes:
    if not 0 <= value <= 18_446_744_073_709_551_615:
        raise ValueError("MODEL_STREAM_FRAME_DIGEST_INPUT_INVALID")
    return value.to_bytes(8, byteorder="big", signed=False)


def _reference(value: str, maximum: int) -> bool:
    return 1 <= len(value) <= maximum and value.strip() == value and _utf8_length(value) > 0


def _utf8_length(value: str) -> int:
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return -1
