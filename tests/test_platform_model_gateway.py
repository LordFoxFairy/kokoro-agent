"""Contract tests for GA's production Platform Model Gateway adapter."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Iterator

import pytest
from deepagents.backends.state import StateBackend
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.stream import CustomTransformer

from kokoro.platform.model.v1 import model_gateway_pb2 as gateway_pb
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.model.platform_gateway import (
    ModelGatewayUnavailable,
    PlatformModelGatewayChatModel,
)
from kokoro_agent.model.streaming import (
    model_stream_frame_digest,
    model_stream_payload_bytes,
)
from kokoro_agent.state import RunScope

AUTHORIZATION_HANDLE = f"model-authorization:sha256:{'a' * 64}"
ZERO_DIGEST = "0" * 64


class UnusedAsyncClient:
    async def invoke_model(
        self,
        request: gateway_pb.InvokeModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> gateway_pb.InvokeModelResponse:
        del request, timeout_ms
        raise AssertionError("async client must not be used by sync tests")

    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[gateway_pb.StreamModelResponse]:
        del request, timeout_ms
        raise AssertionError("async client must not be used by sync tests")


class RecordingAsyncClient:
    def __init__(self) -> None:
        self.stream_requests: list[gateway_pb.StreamModelRequest] = []

    async def invoke_model(
        self,
        request: gateway_pb.InvokeModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> gateway_pb.InvokeModelResponse:
        del request, timeout_ms
        raise AssertionError("unary client must not be used by stream tests")

    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[gateway_pb.StreamModelResponse]:
        assert timeout_ms == 5_000
        self.stream_requests.append(request)

        async def responses() -> AsyncIterator[gateway_pb.StreamModelResponse]:
            for frame in stream_frames(request.invocation.attempt_ref):
                yield frame

        return responses()


class RecordingSyncClient:
    def __init__(self, *, mismatched_attempt: bool = False) -> None:
        self.mismatched_attempt = mismatched_attempt
        self.invoke_requests: list[gateway_pb.InvokeModelRequest] = []
        self.stream_requests: list[gateway_pb.StreamModelRequest] = []

    def invoke_model(
        self,
        request: gateway_pb.InvokeModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> gateway_pb.InvokeModelResponse:
        assert timeout_ms == 5_000
        self.invoke_requests.append(request)
        attempt_ref = "model-attempt:sha256:" + "b" * 64 if self.mismatched_attempt else request.attempt_ref
        return gateway_pb.InvokeModelResponse(
            invocation_ref="model-invocation:sha256:" + "c" * 64,
            attempt_ref=attempt_ref,
            completed=gateway_pb.ModelCompleted(
                response_id="response-1",
                content="hello",
                finish_reason="stop",
                usage=gateway_pb.ModelUsage(input_tokens=3, output_tokens=2),
            ),
        )

    def stream_model(
        self,
        request: gateway_pb.StreamModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> Iterator[gateway_pb.StreamModelResponse]:
        assert timeout_ms == 5_000
        self.stream_requests.append(request)
        return iter(stream_frames(request.invocation.attempt_ref))


def model(
    client: RecordingSyncClient,
    *,
    async_client: UnusedAsyncClient | RecordingAsyncClient | None = None,
) -> PlatformModelGatewayChatModel:
    return PlatformModelGatewayChatModel(
        model_name="chat-primary",
        authorization_handle=AUTHORIZATION_HANDLE,
        run_id="run-1",
        producer_generation=7,
        maximum_output_tokens=256,
        timeout_ms=5_000,
        async_client=async_client or UnusedAsyncClient(),
        sync_client=client,
    )


def invocation_config() -> RunnableConfig:
    return {"metadata": {"langgraph_checkpoint_ns": "graph:chat"}}


def test_unary_call_binds_admission_handle_and_stable_attempt_identity() -> None:
    client = RecordingSyncClient()
    result = model(client).invoke([HumanMessage(content="hi")], config=invocation_config())

    assert isinstance(result, AIMessage)
    assert result.text == "hello"
    assert result.usage_metadata == {"input_tokens": 3, "output_tokens": 2, "total_tokens": 5}
    request = client.invoke_requests[0]
    assert request.model_authorization_handle == AUTHORIZATION_HANDLE
    assert request.request.model == "chat-primary"
    assert request.request.max_output_tokens == 256
    assert request.producer_generation == 7
    assert request.logical_call_ref.startswith("model-call:sha256:")
    assert request.attempt_ref.startswith("model-attempt:sha256:")


def test_unary_call_rejects_a_response_for_another_attempt() -> None:
    with pytest.raises(ModelGatewayUnavailable, match="MODEL_GATEWAY_UNAVAILABLE") as raised:
        model(RecordingSyncClient(mismatched_attempt=True)).invoke(
            [HumanMessage(content="hi")],
            config=invocation_config(),
        )
    assert raised.value.rpc_code == "INVALID_RESPONSE"


def test_stream_call_uses_the_same_authorized_attempt_and_verified_terminal() -> None:
    client = RecordingSyncClient()
    chunks = list(model(client).stream([HumanMessage(content="hi")], config=invocation_config()))

    assert "".join(chunk.text for chunk in chunks) == "hello"
    request = client.stream_requests[0].invocation
    assert request.model_authorization_handle == AUTHORIZATION_HANDLE
    assert request.producer_generation == 7
    assert client.stream_requests[0].after_sequence == 0


async def test_async_stream_uses_checkpoint_identity_from_public_config() -> None:
    sync_client = RecordingSyncClient()
    async_client = RecordingAsyncClient()

    chunks = [
        chunk
        async for chunk in model(
            sync_client,
            async_client=async_client,
        ).astream([HumanMessage(content="hi")], config=invocation_config())
    ]

    assert "".join(chunk.text for chunk in chunks) == "hello"
    request = async_client.stream_requests[0].invocation
    assert request.logical_call_ref.startswith("model-call:sha256:")
    assert request.attempt_ref.startswith("model-attempt:sha256:")


@pytest.mark.filterwarnings("ignore:The v3 streaming protocol on Pregel is experimental")
async def test_real_deep_agent_text_block_system_prompt_reaches_gateway_once() -> None:
    async_client = RecordingAsyncClient()
    subject = build_agent(
        model=model(RecordingSyncClient(), async_client=async_client),
        tools=[],
        system_prompt="Fixture system prompt.",
        subagents=[],
        checkpointer=InMemorySaver(),
        permissions=[],
        interrupt_on={},
        backend=StateBackend(),
    )
    scope = RunScope(namespace="fixture", session_id="session-1", run_id="run-1", thread_id="thread-1")
    config: RunnableConfig = {
        "configurable": {"thread_id": scope.scoped_thread_id},
        "metadata": {"kokoro_run_id": scope.run_id},
    }

    events = await subject.astream_events(
        {
            "messages": [HumanMessage(content="hello")],
            "scope": scope.as_state(),
            "assembly_digest": "a" * 64,
        },
        version="v3",
        config=config,
        transformers=[CustomTransformer],
    )
    async with events:
        await asyncio.gather(
            _drain(events.messages),
            _drain(events.tool_calls),
            _drain(events.subagents),
            _drain(events.custom),
        )

    assert len(async_client.stream_requests) == 1
    request = async_client.stream_requests[0].invocation.request
    assert request.messages[0].role == gateway_pb.MODEL_MESSAGE_ROLE_SYSTEM
    assert request.messages[0].content.startswith("Fixture system prompt.")
    assert request.messages[-1].content == "hello"
    state = await subject.aget_state(config)
    state_messages: object = state.values.get("messages")
    assert isinstance(state_messages, list)
    assert isinstance(state_messages[-1], AIMessage)
    assert state_messages[-1].text == "hello"


def test_standard_text_blocks_are_normalized_to_equivalent_text() -> None:
    client = RecordingSyncClient()
    equivalent_client = RecordingSyncClient()

    result = model(client).invoke(
        [
            SystemMessage(
                content=[
                    {"type": "text", "text": "first"},
                    {"type": "text", "text": " second"},
                ]
            ),
            HumanMessage(content="hello"),
        ],
        config=invocation_config(),
    )
    model(equivalent_client).invoke(
        [SystemMessage(content="first second"), HumanMessage(content="hello")],
        config=invocation_config(),
    )

    assert client.invoke_requests[0].request.messages[0].content == "first second"
    assert client.invoke_requests[0] == equivalent_client.invoke_requests[0]
    assert result.usage_metadata == {"input_tokens": 3, "output_tokens": 2, "total_tokens": 5}


@pytest.mark.parametrize(
    "content",
    [
        [{"type": "image", "url": "https://invalid.example/image.png"}],
        [{"type": "tool_call", "id": "tool-1", "name": "probe", "args": {}}],
        [{"type": "unknown", "text": "not a standard text block"}],
        [{"type": "text", "text": 1}],
        [
            {"type": "text", "text": "safe"},
            {"type": "image", "url": "https://invalid.example/image.png"},
        ],
    ],
)
def test_non_text_content_blocks_remain_rejected(
    content: list[str | dict[str, object]],
) -> None:
    client = RecordingSyncClient()

    with pytest.raises(ValueError, match="^MODEL_GATEWAY_NON_TEXT_MESSAGE_UNSUPPORTED$"):
        model(client).invoke(
            [SystemMessage(content=content)],
            config=invocation_config(),
        )

    assert client.invoke_requests == []


async def _drain(values: AsyncIterable[object]) -> None:
    async for _value in values:
        pass


def stream_frames(attempt_ref: str) -> list[gateway_pb.StreamModelResponse]:
    payloads = [
        ("accepted", gateway_pb.ModelAccepted()),
        ("content_delta", gateway_pb.ModelContentDelta(content="hello")),
        (
            "completed",
            gateway_pb.ModelCompleted(
                response_id="response-stream-1",
                content="hello",
                finish_reason="stop",
                usage=gateway_pb.ModelUsage(input_tokens=2, output_tokens=1),
            ),
        ),
    ]
    frames: list[gateway_pb.StreamModelResponse] = []
    for sequence, (kind, payload) in enumerate(payloads, start=1):
        frame = gateway_pb.StreamModelResponse(
            invocation_ref="model-invocation:sha256:" + "c" * 64,
            attempt_ref=attempt_ref,
            sequence=sequence,
            previous_frame_digest=ZERO_DIGEST if not frames else frames[-1].frame_digest,
        )
        getattr(frame, kind).CopyFrom(payload)
        frame.frame_digest = model_stream_frame_digest(
            invocation_ref=frame.invocation_ref,
            attempt_ref=frame.attempt_ref,
            sequence=frame.sequence,
            previous_frame_digest=frame.previous_frame_digest,
            payload_kind=kind,
            payload_bytes=model_stream_payload_bytes(frame),
        )
        frames.append(frame)
    return frames
