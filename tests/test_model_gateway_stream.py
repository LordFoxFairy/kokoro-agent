"""Model Gateway stream integrity, aggregation, terminal, and resume semantics."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import TypeAlias, cast

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from langchain_core.messages import AIMessageChunk
from langchain_core.messages.ai import UsageMetadata

from kokoro.platform.model.v1 import model_gateway_pb2 as gateway_pb
from kokoro_agent.model.streaming import (
    ModelStreamOutcomeUnknown,
    ModelStreamProtocolError,
    ModelStreamRejected,
    ModelStreamTransportError,
    ModelStreamVerifier,
    aiter_verified_model_stream,
    iter_verified_model_stream,
    model_stream_frame_digest,
    model_stream_payload_bytes,
)

INVOCATION = "invocation-1"
ATTEMPT = "attempt-1"
ZERO = "0" * 64

_Payload: TypeAlias = (
    gateway_pb.ModelAccepted
    | gateway_pb.ModelContentDelta
    | gateway_pb.ModelReasoningDelta
    | gateway_pb.ModelToolCallDelta
    | gateway_pb.ModelCompleted
    | gateway_pb.ModelFailed
    | gateway_pb.ModelOutcomeUnknown
)


def _request() -> gateway_pb.InvokeModelRequest:
    return gateway_pb.InvokeModelRequest(
        model_authorization_handle=f"model-authorization:sha256:{'a' * 64}",
        logical_call_ref="logical-1",
        attempt_ref=ATTEMPT,
        producer_context="producer-1",
        producer_generation=1,
        request=gateway_pb.ChatCompletionRequest(
            protocol="openai.chat.completions.v1",
            model="chat-primary",
            messages=[
                gateway_pb.ModelMessage(
                    role=gateway_pb.MODEL_MESSAGE_ROLE_USER,
                    content="hello",
                )
            ],
            max_output_tokens=100,
            tool_choice=gateway_pb.MODEL_TOOL_CHOICE_NONE,
        ),
    )


def _frame(
    sequence: int,
    previous: str,
    payload: _Payload,
    *,
    invocation_ref: str = INVOCATION,
    attempt_ref: str = ATTEMPT,
) -> gateway_pb.StreamModelResponse:
    frame = gateway_pb.StreamModelResponse(
        invocation_ref=invocation_ref,
        attempt_ref=attempt_ref,
        sequence=sequence,
        previous_frame_digest=previous,
    )
    if isinstance(payload, gateway_pb.ModelAccepted):
        frame.accepted.CopyFrom(payload)
    elif isinstance(payload, gateway_pb.ModelContentDelta):
        frame.content_delta.CopyFrom(payload)
    elif isinstance(payload, gateway_pb.ModelReasoningDelta):
        frame.reasoning_delta.CopyFrom(payload)
    elif isinstance(payload, gateway_pb.ModelToolCallDelta):
        frame.tool_call_delta.CopyFrom(payload)
    elif isinstance(payload, gateway_pb.ModelCompleted):
        frame.completed.CopyFrom(payload)
    elif isinstance(payload, gateway_pb.ModelFailed):
        frame.failed.CopyFrom(payload)
    else:
        frame.outcome_unknown.CopyFrom(payload)
    kind = frame.WhichOneof("payload")
    assert kind is not None
    frame.frame_digest = model_stream_frame_digest(
        invocation_ref=frame.invocation_ref,
        attempt_ref=frame.attempt_ref,
        sequence=frame.sequence,
        previous_frame_digest=frame.previous_frame_digest,
        payload_kind=kind,
        payload_bytes=model_stream_payload_bytes(frame),
    )
    return frame


def _happy_frames() -> list[gateway_pb.StreamModelResponse]:
    frames: list[gateway_pb.StreamModelResponse] = []

    def append(payload: _Payload) -> None:
        previous = ZERO if not frames else frames[-1].frame_digest
        frames.append(_frame(len(frames) + 1, previous, payload))

    append(gateway_pb.ModelAccepted())
    append(gateway_pb.ModelContentDelta(content="hello "))
    append(gateway_pb.ModelReasoningDelta(content="think"))
    append(
        gateway_pb.ModelToolCallDelta(
            tool_index=0,
            id="call-1",
            name="search",
            arguments_json_fragment=b'{"q":',
        )
    )
    append(
        gateway_pb.ModelToolCallDelta(
            tool_index=0,
            arguments_json_fragment=b'"x"}',
        )
    )
    append(
        gateway_pb.ModelCompleted(
            response_id="response-1",
            content="hello ",
            reasoning_content="think",
            tool_calls=[
                gateway_pb.ModelToolCall(
                    id="call-1",
                    name="search",
                    arguments_json=b'{"q":"x"}',
                )
            ],
            finish_reason="tool_calls",
            usage=gateway_pb.ModelUsage(input_tokens=5, output_tokens=7),
        )
    )
    return frames


def test_digest_matches_root_typescript_vector() -> None:
    assert model_stream_frame_digest(
        invocation_ref="invocation-1",
        attempt_ref="attempt-1",
        sequence=2,
        previous_frame_digest="a" * 64,
        payload_kind="content_delta",
        payload_bytes=b'{"content":"hello","kind":"content_delta"}',
    ) == "35e1371a5849871291ac777885fe6b9fbfeabfe7489282424a01425cbbfce411"


def test_completed_payload_matches_platform_typescript_vector() -> None:
    frame = gateway_pb.StreamModelResponse(
        invocation_ref=INVOCATION,
        attempt_ref=ATTEMPT,
        sequence=6,
        previous_frame_digest="a" * 64,
        completed=_happy_frames()[-1].completed,
    )
    payload = model_stream_payload_bytes(frame)
    assert model_stream_frame_digest(
        invocation_ref=frame.invocation_ref,
        attempt_ref=frame.attempt_ref,
        sequence=frame.sequence,
        previous_frame_digest=frame.previous_frame_digest,
        payload_kind="completed",
        payload_bytes=payload,
    ) == "f5ef1504c0af4ebe39e65dcec5d8a871f0545ab496cf5ee6176b619be9d4a35c"


def test_verifier_aggregates_and_rejects_frame_after_terminal() -> None:
    verifier = ModelStreamVerifier(ATTEMPT)
    chunks = [chunk for frame in _happy_frames() if (chunk := verifier.consume(frame))]

    assert verifier.terminal is True
    assert verifier.after_sequence == 6
    assert "".join(chunk.text for chunk in chunks) == "hello "
    messages = [cast(AIMessageChunk, chunk.message) for chunk in chunks]
    assert cast(dict[str, object], getattr(messages[1], "additional_kwargs")) == {
        "reasoning_content": "think"
    }
    assert cast(
        list[dict[str, object]], getattr(messages[2], "tool_call_chunks")
    )[0]["args"] == '{"q":'
    assert cast(
        list[dict[str, object]], getattr(messages[3], "tool_call_chunks")
    )[0]["args"] == '"x"}'
    assert cast(dict[str, object], getattr(messages[-1], "response_metadata")) == {
        "model_gateway_invocation_ref": INVOCATION,
        "model_gateway_attempt_ref": ATTEMPT,
        "finish_reason": "tool_calls",
    }
    assert cast(UsageMetadata, getattr(messages[-1], "usage_metadata")) == {
        "input_tokens": 5,
        "output_tokens": 7,
        "total_tokens": 12,
    }
    with pytest.raises(ModelStreamProtocolError):
        verifier.consume(
            _frame(
                7,
                _happy_frames()[-1].frame_digest,
                gateway_pb.ModelContentDelta(content="late"),
            )
        )


@pytest.mark.parametrize(
    "defect", ["invocation", "attempt", "sequence", "previous", "digest"]
)
def test_verifier_rejects_chain_defects_even_when_rehashed(defect: str) -> None:
    verifier = ModelStreamVerifier(ATTEMPT)
    accepted = _happy_frames()[0]
    verifier.consume(accepted)
    invocation = "invocation-2" if defect == "invocation" else INVOCATION
    attempt = "attempt-2" if defect == "attempt" else ATTEMPT
    sequence = 3 if defect == "sequence" else 2
    previous = "b" * 64 if defect == "previous" else accepted.frame_digest
    frame = _frame(
        sequence,
        previous,
        gateway_pb.ModelContentDelta(content="x"),
        invocation_ref=invocation,
        attempt_ref=attempt,
    )
    if defect == "digest":
        frame.frame_digest = "f" * 64
    with pytest.raises(ModelStreamProtocolError):
        verifier.consume(frame)


def test_terminal_must_match_accumulated_deltas() -> None:
    frames = _happy_frames()
    terminal = frames[-1]
    terminal.completed.content = "different"
    terminal.frame_digest = model_stream_frame_digest(
        invocation_ref=terminal.invocation_ref,
        attempt_ref=terminal.attempt_ref,
        sequence=terminal.sequence,
        previous_frame_digest=terminal.previous_frame_digest,
        payload_kind="completed",
        payload_bytes=model_stream_payload_bytes(terminal),
    )
    verifier = ModelStreamVerifier(ATTEMPT)
    for frame in frames[:-1]:
        verifier.consume(frame)
    with pytest.raises(ModelStreamProtocolError):
        verifier.consume(terminal)


@pytest.mark.parametrize("terminal", ["failed", "outcome_unknown"])
def test_non_success_terminal_is_verified_and_never_returned_as_content(
    terminal: str,
) -> None:
    accepted = _frame(1, ZERO, gateway_pb.ModelAccepted())
    payload: _Payload
    expected: type[Exception]
    if terminal == "failed":
        payload = gateway_pb.ModelFailed(code="MODEL_BUSY", retryable=True)
        expected = ModelStreamRejected
    else:
        payload = gateway_pb.ModelOutcomeUnknown()
        expected = ModelStreamOutcomeUnknown
    frame = _frame(2, accepted.frame_digest, payload)
    verifier = ModelStreamVerifier(ATTEMPT)
    verifier.consume(accepted)
    with pytest.raises(expected):
        verifier.consume(frame)
    assert verifier.terminal is True


def test_empty_tool_delta_is_not_a_valid_stream_event() -> None:
    accepted = _frame(1, ZERO, gateway_pb.ModelAccepted())
    empty = _frame(
        2,
        accepted.frame_digest,
        gateway_pb.ModelToolCallDelta(tool_index=0),
    )
    verifier = ModelStreamVerifier(ATTEMPT)
    verifier.consume(accepted)
    with pytest.raises(ModelStreamProtocolError):
        verifier.consume(empty)


class DisconnectingAsyncClient:
    def __init__(self, frames: list[gateway_pb.StreamModelResponse]) -> None:
        self.frames = frames
        self.requests: list[gateway_pb.StreamModelRequest] = []

    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[gateway_pb.StreamModelResponse]:
        del timeout_ms
        self.requests.append(request)

        async def run() -> AsyncIterator[gateway_pb.StreamModelResponse]:
            if request.after_sequence == 0:
                yield self.frames[0]
                yield self.frames[1]
                raise ConnectError(Code.UNAVAILABLE, "disconnected")
            assert request.after_sequence == 2
            for frame in self.frames[2:]:
                yield frame

        return run()


async def test_disconnect_resumes_same_attempt_after_verified_sequence() -> None:
    client = DisconnectingAsyncClient(_happy_frames())
    invocation = _request()
    original = invocation.SerializeToString(deterministic=True)

    chunks = [
        chunk
        async for chunk in aiter_verified_model_stream(
            client,
            invocation,
            timeout_ms=1_000,
        )
    ]

    assert "".join(chunk.text for chunk in chunks) == "hello "
    assert [request.after_sequence for request in client.requests] == [0, 2]
    assert all(
        request.invocation.SerializeToString(deterministic=True) == original
        for request in client.requests
    )
    assert all(request.invocation.attempt_ref == ATTEMPT for request in client.requests)


class SyncClient:
    def __init__(self, frames: list[gateway_pb.StreamModelResponse]) -> None:
        self.frames = frames
        self.requests: list[gateway_pb.StreamModelRequest] = []

    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> Iterator[gateway_pb.StreamModelResponse]:
        del timeout_ms
        self.requests.append(request)
        return iter(self.frames)


def test_sync_consumer_uses_stream_rpc_and_finishes_only_on_terminal() -> None:
    client = SyncClient(_happy_frames())
    chunks = list(iter_verified_model_stream(client, _request(), timeout_ms=1_000))
    assert "".join(chunk.text for chunk in chunks) == "hello "
    assert [request.after_sequence for request in client.requests] == [0]


class DeniedSyncClient:
    def __init__(self) -> None:
        self.calls = 0

    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> Iterator[gateway_pb.StreamModelResponse]:
        del request, timeout_ms
        self.calls += 1
        raise ConnectError(Code.PERMISSION_DENIED, "denied")


def test_non_retryable_rpc_error_is_not_replayed() -> None:
    client = DeniedSyncClient()
    with pytest.raises(ModelStreamTransportError) as raised:
        list(iter_verified_model_stream(client, _request(), timeout_ms=1_000))
    assert raised.value.rpc_code == "PERMISSION_DENIED"
    assert client.calls == 1
