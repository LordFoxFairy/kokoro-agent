"""Real locked-SDK retry behavior against an owned loopback HTTP peer."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

import anthropic
import openai
import pytest
from langchain_core.messages import AIMessageChunk, HumanMessage
from langchain_core.language_models import BaseChatModel
from pydantic import SecretStr

from kokoro_agent.policy import ModelConfig
from kokoro_agent.model.factory import ChatModelSettings, make_chat_model

ProviderKind: TypeAlias = Literal[
    "openai",
    "deepseek",
    "litellm",
    "anthropic-explicit",
    "anthropic-environment",
]
ProtocolKind: TypeAlias = Literal["openai", "anthropic"]
ResponseMode: TypeAlias = Literal["ok", "stream", "500", "429", "disconnect", "hang"]


@dataclass(frozen=True, slots=True)
class _ProviderCase:
    id: str
    kind: ProviderKind
    protocol: ProtocolKind
    model_name: str


_PROVIDER_CASES = (
    _ProviderCase("openai-plain", "openai", "openai", "gpt-4o-mini"),
    _ProviderCase("deepseek-reasoning", "deepseek", "openai", "deepseek-chat"),
    _ProviderCase("litellm-compatible", "litellm", "openai", "gateway-model"),
    _ProviderCase(
        "anthropic-explicit-key",
        "anthropic-explicit",
        "anthropic",
        "claude-3-5-haiku-latest",
    ),
    _ProviderCase(
        "anthropic-environment-key",
        "anthropic-environment",
        "anthropic",
        "claude-3-5-haiku-latest",
    ),
)

_FAILURE_CASES = (
    pytest.param(_PROVIDER_CASES[0], "500", id="openai-plain-500"),
    pytest.param(_PROVIDER_CASES[1], "429", id="deepseek-reasoning-429"),
    pytest.param(_PROVIDER_CASES[2], "disconnect", id="litellm-disconnect"),
    pytest.param(_PROVIDER_CASES[3], "500", id="anthropic-explicit-500"),
    pytest.param(_PROVIDER_CASES[4], "429", id="anthropic-environment-429"),
)


@dataclass(slots=True)
class _LoopbackPeer:
    protocol: ProtocolKind
    mode: ResponseMode
    request_count: int = 0
    first_request: asyncio.Event = field(default_factory=asyncio.Event)
    _server: asyncio.Server | None = None
    _writers: set[asyncio.StreamWriter] = field(
        default_factory=lambda: set[asyncio.StreamWriter]()
    )
    _release_hanging_requests: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def base_url(self) -> str:
        sockets = () if self._server is None else self._server.sockets
        assert sockets
        port = int(sockets[0].getsockname()[1])
        return f"http://127.0.0.1:{port}/v1"

    async def __aenter__(self) -> _LoopbackPeer:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        self._release_hanging_requests.set()
        for writer in tuple(self._writers):
            writer.close()
        for writer in tuple(self._writers):
            try:
                await writer.wait_closed()
            except (ConnectionError, RuntimeError):
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(writer)
        try:
            header_bytes = await reader.readuntil(b"\r\n\r\n")
            content_length = _content_length(header_bytes)
            if content_length:
                await reader.readexactly(content_length)
            self.request_count += 1
            self.first_request.set()

            if self.mode == "disconnect":
                return
            if self.mode == "hang":
                await self._release_hanging_requests.wait()
                return
            status, content_type, body, extra_headers = _response(
                self.protocol, self.mode
            )
            writer.write(
                (
                    f"HTTP/1.1 {status}\r\n"
                    f"Content-Type: {content_type}\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    "Connection: close\r\n"
                    f"{extra_headers}"
                    "\r\n"
                ).encode("ascii")
                + body
            )
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, RuntimeError):
                pass
            self._writers.discard(writer)


def _content_length(headers: bytes) -> int:
    for raw_line in headers.split(b"\r\n"):
        name, separator, value = raw_line.partition(b":")
        if separator and name.lower() == b"content-length":
            return int(value.strip())
    return 0


def _json_bytes(value: dict[str, object]) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _response(
    protocol: ProtocolKind, mode: ResponseMode
) -> tuple[str, str, bytes, str]:
    if mode == "500":
        return (
            "500 Internal Server Error",
            "application/json",
            _error_body(protocol, "api_error"),
            "",
        )
    if mode == "429":
        return (
            "429 Too Many Requests",
            "application/json",
            _error_body(protocol, "rate_limit_error"),
            "Retry-After: 0\r\n",
        )
    if mode == "stream":
        return "200 OK", "text/event-stream", _stream_body(protocol), ""
    assert mode == "ok"
    return "200 OK", "application/json", _success_body(protocol), ""


def _error_body(protocol: ProtocolKind, error_type: str) -> bytes:
    if protocol == "anthropic":
        return _json_bytes(
            {
                "type": "error",
                "error": {"type": error_type, "message": "sandbox failure"},
            }
        )
    return _json_bytes(
        {
            "error": {
                "message": "sandbox failure",
                "type": error_type,
                "param": None,
                "code": error_type,
            }
        }
    )


def _success_body(protocol: ProtocolKind) -> bytes:
    if protocol == "anthropic":
        return _json_bytes(
            {
                "id": "msg_sandbox",
                "type": "message",
                "role": "assistant",
                "model": "claude-sandbox",
                "content": [{"type": "text", "text": "ok"}],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
        )
    return _json_bytes(
        {
            "id": "chatcmpl-sandbox",
            "object": "chat.completion",
            "created": 1,
            "model": "sandbox-model",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }
    )


def _stream_body(protocol: ProtocolKind) -> bytes:
    if protocol == "anthropic":
        return (
            'event: message_start\ndata: {"type":"message_start","message":'
            '{"id":"msg_sandbox","type":"message","role":"assistant",'
            '"model":"claude-sandbox","content":[],"stop_reason":null,'
            '"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}\n\n'
            'event: content_block_start\ndata: {"type":"content_block_start",'
            '"index":0,"content_block":{"type":"text","text":""}}\n\n'
            'event: content_block_delta\ndata: {"type":"content_block_delta",'
            '"index":0,"delta":{"type":"text_delta","text":"ok"}}\n\n'
            'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n'
            'event: message_delta\ndata: {"type":"message_delta","delta":'
            '{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}\n\n'
            'event: message_stop\ndata: {"type":"message_stop"}\n\n'
        ).encode()

    return (
        'data: {"id":"chatcmpl-sandbox","object":"chat.completion.chunk",'
        '"created":1,"model":"sandbox-model","choices":[{"index":0,'
        '"delta":{"role":"assistant","content":"ok"},"finish_reason":null}]}\n\n'
        'data: {"id":"chatcmpl-sandbox","object":"chat.completion.chunk",'
        '"created":1,"model":"sandbox-model","choices":[{"index":0,'
        '"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,'
        '"completion_tokens":1,"total_tokens":2}}\n\ndata: [DONE]\n\n'
    ).encode()


def _model(
    case: _ProviderCase,
    base_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> BaseChatModel:
    if case.kind == "anthropic-environment":
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sandbox-anthropic-key")

    settings = ChatModelSettings(
        disable_streaming=False,
        openai_api_key=SecretStr("sandbox-openai-key"),
        openai_base_url=base_url,
        openai_reasoning=case.kind == "deepseek",
        anthropic_api_key=(
            SecretStr("sandbox-anthropic-key")
            if case.kind == "anthropic-explicit"
            else None
        ),
        anthropic_base_url=base_url,
        litellm_enabled=case.kind == "litellm",
        litellm_base_url=base_url,
        litellm_api_key=SecretStr("sandbox-litellm-key"),
    )
    provider: Literal["openai", "anthropic", "litellm"]
    if case.protocol == "anthropic":
        provider = "anthropic"
    elif case.kind == "litellm":
        provider = "litellm"
    else:
        provider = "openai"
    return make_chat_model(
        settings,
        ModelConfig(provider=provider, name=case.model_name),
    )


@asynccontextmanager
async def _peer(
    protocol: ProtocolKind, mode: ResponseMode
) -> AsyncGenerator[_LoopbackPeer, None]:
    async with _LoopbackPeer(protocol=protocol, mode=mode) as peer:
        yield peer


@pytest.mark.asyncio
@pytest.mark.parametrize(("case", "mode"), _FAILURE_CASES)
async def test_r196_native_sdk_failure_is_one_physical_request(
    case: _ProviderCase,
    mode: ResponseMode,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _peer(case.protocol, mode) as peer:
        model = _model(case, peer.base_url, monkeypatch)
        with pytest.raises((openai.APIError, anthropic.APIError)):
            await model.ainvoke([HumanMessage(content="hello")])

        assert peer.request_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.id)
async def test_r196_native_sdk_normal_response_is_not_over_retried(
    case: _ProviderCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _peer(case.protocol, "ok") as peer:
        model = _model(case, peer.base_url, monkeypatch)
        response = await model.ainvoke([HumanMessage(content="hello")])

        assert getattr(response, "content") == "ok"
        assert getattr(response, "usage_metadata") is not None
        assert peer.request_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    (_PROVIDER_CASES[0], _PROVIDER_CASES[3]),
    ids=lambda case: case.id,
)
async def test_r196_native_sdk_stream_response_is_not_over_retried(
    case: _ProviderCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _peer(case.protocol, "stream") as peer:
        model = _model(case, peer.base_url, monkeypatch)
        chunks: list[AIMessageChunk] = []
        async for chunk in model.astream([HumanMessage(content="hello")]):
            assert isinstance(chunk, AIMessageChunk)
            chunks.append(chunk)

        assert "".join(chunk.text for chunk in chunks) == "ok"
        assert any(chunk.usage_metadata is not None for chunk in chunks)
        assert peer.request_count == 1


@pytest.mark.asyncio
async def test_r196_native_sdk_explicit_cancel_cleans_up_one_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _PROVIDER_CASES[2]
    async with _peer(case.protocol, "hang") as peer:
        model = _model(case, peer.base_url, monkeypatch)
        invocation = asyncio.create_task(model.ainvoke([HumanMessage(content="hello")]))
        await asyncio.wait_for(peer.first_request.wait(), timeout=1.0)
        invocation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await invocation

        assert peer.request_count == 1


@pytest.mark.asyncio
async def test_r196_native_sdk_caller_timeout_cleans_up_one_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _PROVIDER_CASES[0]
    async with _peer(case.protocol, "hang") as peer:
        model = _model(case, peer.base_url, monkeypatch)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                model.ainvoke([HumanMessage(content="hello")]), timeout=0.5
            )

        assert peer.request_count == 1
