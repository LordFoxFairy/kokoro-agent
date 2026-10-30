"""Real locked-SDK retry behavior against an owned loopback HTTP peer."""

from __future__ import annotations

import asyncio
import gzip
import zlib
import zstandard
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, TypeAlias

import anthropic
import openai
import pytest
from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage
from langchain_core.language_models import BaseChatModel
from pydantic import SecretStr, TypeAdapter

from support.model_http_mechanism import ModelHttpMechanismClients

from kokoro_agent.model.usage_observation import (
    PhysicalUsageObservation,
    capture_model_usage,
)
from kokoro_agent.clients.system import SystemModelClient
from kokoro_agent.model.credentials import ModelCredential
from kokoro_agent.policy import ModelConfig
from kokoro_agent.model.factory import (
    ChatModelSettings,
    ModelHttpClients,
    make_chat_model,
    model_from_route,
)

ProviderKind: TypeAlias = Literal[
    "openai",
    "deepseek",
    "litellm",
    "anthropic-explicit",
    "anthropic-environment",
]
ProtocolKind: TypeAlias = Literal["openai", "anthropic"]
ResponseMode: TypeAlias = Literal[
    "ok", "stream", "500", "429", "disconnect", "hang", "system-route"
]


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


@dataclass(frozen=True, slots=True)
class _CapturedRequest:
    request_line: bytes
    body: bytes
    sdk_profile_headers: tuple[tuple[bytes, bytes], ...] = ()


@dataclass(slots=True)
class _LoopbackPeer:
    protocol: ProtocolKind
    mode: ResponseMode
    request_count: int = 0
    requests: list[_CapturedRequest] = field(
        default_factory=lambda: list[_CapturedRequest]()
    )
    model_clients: list[ModelHttpClients] = field(
        default_factory=lambda: list[ModelHttpClients]()
    )
    route_data: dict[str, object] | None = None
    response_body: bytes | None = None
    response_encoding: str | None = None
    held_tail_offset: int | None = None
    sent_response_bodies: list[bytes] = field(default_factory=lambda: list[bytes]())
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
        for clients in self.model_clients:
            await clients.aclose()
            assert clients.sync.is_closed
            assert clients.async_client.is_closed

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(writer)
        try:
            header_bytes = await reader.readuntil(b"\r\n\r\n")
            content_length = _content_length(header_bytes)
            request_body = (
                await reader.readexactly(content_length) if content_length else b""
            )
            request_line = header_bytes.split(b"\r\n", 1)[0]
            sdk_headers = tuple(
                (name.lower(), value.strip())
                for line in header_bytes.split(b"\r\n")
                for name, separator, value in (line.partition(b":"),)
                if separator
                and name.lower()
                in {
                    b"x-stainless-raw-response",
                    b"x-stainless-helper-method",
                }
            )
            self.requests.append(
                _CapturedRequest(request_line, request_body, sdk_headers)
            )
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
            if self.response_body is not None:
                assert self.mode in {"ok", "stream"}
                body = self.response_body
            if self.mode == "system-route" and self.route_data is not None:
                body = _json_bytes({"data": self.route_data})
            if self.mode == "system-route" and request_line != (
                b"POST /v1/system/model-catalog/resolve HTTP/1.1"
            ):
                status, content_type, body, extra_headers = (
                    "404 Not Found",
                    "application/json",
                    b"{}",
                    "",
                )
            if self.response_encoding is not None:
                extra_headers += f"Content-Encoding: {self.response_encoding}\r\n"
            queued_body = (
                body if self.held_tail_offset is None else body[: self.held_tail_offset]
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
                + queued_body
            )
            self.sent_response_bodies.append(queued_body)
            await writer.drain()
            if self.held_tail_offset is not None:
                await self._release_hanging_requests.wait()
                if writer.is_closing():
                    return
                writer.write(body[self.held_tail_offset :])
                self.sent_response_bodies.append(body[self.held_tail_offset :])
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
    if mode == "system-route":
        return (
            "200 OK",
            "application/json",
            _json_bytes({"data": _system_route_data()}),
            "",
        )
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


async def _model(
    case: _ProviderCase,
    peer: _LoopbackPeer,
    monkeypatch: pytest.MonkeyPatch,
    *,
    output_cap: int | None = None,
    disable_streaming: bool = False,
) -> BaseChatModel:
    base_url = peer.base_url
    if case.kind == "anthropic-environment":
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sandbox-anthropic-key")

    settings = ChatModelSettings(
        disable_streaming=disable_streaming,
        openai_api_key=SecretStr("sandbox-openai-key"),
        openai_base_url=base_url,
        openai_reasoning=case.kind == "deepseek",
        anthropic_api_key=(
            SecretStr("sandbox-anthropic-key")
            if case.kind == "anthropic-explicit"
            else None
        ),
        anthropic_base_url=base_url,
        model_allow_loopback_http=True,
    )
    provider: Literal["openai", "anthropic", "litellm"]
    if case.protocol == "anthropic":
        provider = "anthropic"
    elif case.kind == "litellm":
        provider = "litellm"
    else:
        provider = "openai"
    if provider == "litellm":
        data = _system_route_data(base_url)
        if output_cap is not None:
            data["max_output_tokens"] = output_cap
        async with _LoopbackPeer("openai", "system-route", route_data=data) as owner:
            async with SystemModelClient(
                owner.base_url.removesuffix("/v1"), SecretStr("sandbox-system-key")
            ) as resolver:
                route = await resolver.resolve(
                    tenant_id="tenant-r216",
                    feature_key="chat",
                    label="fast",
                    request_id="r216-sdk-route",
                )
            assert owner.request_count == 1
        clients = ModelHttpMechanismClients(
            settings,
            route,
            ModelCredential(
                provider_id=route.provider_id,
                secret_handle_ref=route.connection.secret_handle_ref,
                api_key=SecretStr("sandbox-litellm-key"),
            ),
        )
        peer.model_clients.append(clients)
        return make_chat_model(
            settings,
            model_from_route(route, None),
            model_clients=clients,
        )
    return make_chat_model(
        settings, ModelConfig(provider=provider, name=case.model_name)
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
        model = await _model(case, peer, monkeypatch)
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
        model = await _model(case, peer, monkeypatch)
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
        model = await _model(case, peer, monkeypatch)
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
        model = await _model(case, peer, monkeypatch)
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
        model = await _model(case, peer, monkeypatch)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                model.ainvoke([HumanMessage(content="hello")]), timeout=0.5
            )

        assert peer.request_count == 1


# R216 RED-A/B are independent: the decoder failure must not mask SDK wire loss.
def _system_route_data(
    base_url: str = "https://model.example.invalid/v1",
) -> dict[str, object]:
    return {
        "model_id": "11111111-1111-4111-8111-111111111111",
        "provider_id": "22222222-2222-4222-8222-222222222222",
        "revision_id": "33333333-3333-4333-8333-333333333333",
        "revision": 2,
        "digest": "a" * 64,
        "generation": "9007199254740993",
        "tenant_generation": "3",
        "provider_model_name": "provider-model",
        "request_model": "gateway-model",
        "feature_key": "chat",
        "label_key": "fast",
        "connection": {
            "protocol": "openai_chat_completions",
            "connection_type": "litellm",
            "base_url": base_url,
            "secret_handle_ref": "sandbox-litellm-key",
        },
        "context_window": 4096,
        "max_output_tokens": 128,
    }


@pytest.mark.asyncio
async def test_r216_system_client_accepts_and_binds_execution_limits() -> None:
    async with _peer("openai", "system-route") as peer:
        async with SystemModelClient(
            peer.base_url.removesuffix("/v1"),
            SecretStr("sandbox-system-key"),
        ) as resolver:
            try:
                route = await resolver.resolve(
                    tenant_id="tenant-r216",
                    feature_key="chat",
                    label="fast",
                    request_id="r216-system-bounds",
                )
            finally:
                assert peer.request_count == 1
                assert len(peer.requests) == 1
                observed = peer.requests[0]
                assert observed.request_line == (
                    b"POST /v1/system/model-catalog/resolve HTTP/1.1"
                )
                assert json.loads(observed.body) == {
                    "feature_key": "chat",
                    "label_key": "fast",
                }
        # Full decoded identity and limits must remain from one owner response.
        assert {
            **asdict(route),
            "connection": route.connection.model_dump(mode="json"),
        } == _system_route_data()


@pytest.mark.asyncio
async def test_r216_litellm_sdk_sends_owner_output_cap_on_wire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Same real owner response now supplies the factory and the wire oracle.
    case = _PROVIDER_CASES[2]
    async with _peer(case.protocol, "ok") as peer:
        model = await _model(case, peer, monkeypatch)
        response = await model.ainvoke([HumanMessage(content="hello")])
        assert getattr(response, "content") == "ok"
        assert peer.request_count == 1
        assert len(peer.requests) == 1
        observed = peer.requests[0]
        assert observed.request_line == b"POST /v1/chat/completions HTTP/1.1"
        body = TypeAdapter(dict[str, object]).validate_json(observed.body)
        assert isinstance(body, dict)
        assert body["model"] == "gateway-model"
        assert "max_tokens" not in body
        assert "max_output_tokens" not in body
        assert (
            body.get("max_completion_tokens")
            == peer.model_clients[0].route.max_output_tokens
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize("narrow", [False, True])
async def test_r216_real_sdk_default_and_narrow_cap_sync_async_stream(
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
    synchronous: bool,
    narrow: bool,
) -> None:
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        model = await _model(_PROVIDER_CASES[2], peer, monkeypatch, output_cap=73)
        clients = peer.model_clients[0]
        cap = clients.route.max_output_tokens
        expected = cap - 1 if narrow else cap
        runnable = model.bind(max_completion_tokens=expected) if narrow else model
        messages = [HumanMessage(content="hello")]
        if synchronous:

            def invoke() -> str:
                if streaming:
                    return "".join(chunk.text for chunk in runnable.stream(messages))
                return runnable.invoke(messages).text

            result = await asyncio.to_thread(invoke)
        elif streaming:
            result = "".join([chunk.text async for chunk in runnable.astream(messages)])
        else:
            result = (await runnable.ainvoke(messages)).text
        assert result == "ok"
        assert peer.request_count == 1
        request = peer.requests[0]
        assert request.request_line == b"POST /v1/chat/completions HTTP/1.1"
        payload = TypeAdapter(dict[str, object]).validate_json(request.body)
        assert payload["model"] == clients.route.request_model
        assert payload["max_completion_tokens"] == expected
        assert "max_tokens" not in payload
        assert "max_output_tokens" not in payload
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seam", ["invoke", "bind", "bind_tools", "extra_body", "native"]
)
@pytest.mark.parametrize(
    "invalid", ["upward", "null", "bool", "float", "string", "zero", "alias", "model"]
)
async def test_r216_real_sdk_override_rejected_before_peer(
    monkeypatch: pytest.MonkeyPatch,
    seam: str,
    invalid: str,
) -> None:
    async with _peer("openai", "ok") as peer:
        model = await _model(_PROVIDER_CASES[2], peer, monkeypatch, output_cap=57)
        clients = peer.model_clients[0]
        values: dict[str, object] = {
            "upward": clients.route.max_output_tokens + 1,
            "null": None,
            "bool": True,
            "float": 1.5,
            "string": "1",
            "zero": 0,
        }
        overrides: dict[str, Any]
        if invalid == "alias":
            overrides = {"extra_body": {"max_tokens": clients.route.max_output_tokens}}
        elif invalid == "model":
            overrides = {"model": "foreign-model"}
        else:
            overrides = {"max_completion_tokens": values[invalid]}
        if seam == "extra_body" and invalid != "alias":
            overrides = {"extra_body": overrides}
        with pytest.raises((ValueError, openai.APIConnectionError)):
            await _invoke_overridden(model, seam, overrides)
        assert peer.request_count == 0
        assert peer.requests == []
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize(
    "fault", ["endpoint", "model", "missing", "duplicate", "nonfinite", "alias"]
)
async def test_r216_public_client_final_wire_gate_rejects_invalid_bytes(
    monkeypatch: pytest.MonkeyPatch,
    synchronous: bool,
    fault: str,
) -> None:
    async with _peer("openai", "ok") as peer:
        await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        clients = peer.model_clients[0]
        payload: dict[str, object] = {
            "n": 1,
            "model": clients.route.request_model,
            "max_completion_tokens": clients.route.max_output_tokens,
        }
        endpoint = str(clients.endpoint)
        if fault == "endpoint":
            endpoint = peer.base_url + "/responses"
        elif fault == "model":
            payload["model"] = "foreign-model"
        elif fault == "missing":
            del payload["max_completion_tokens"]
        elif fault == "alias":
            payload["max_output_tokens"] = clients.route.max_output_tokens
        raw = _json_bytes(payload)
        if fault == "duplicate":
            raw = raw[:-1] + b',"max_completion_tokens":1}'
        elif fault == "nonfinite":
            raw = raw[:-1] + b',"temperature":NaN}'
        with pytest.raises(ValueError):
            if synchronous:
                await asyncio.to_thread(
                    clients.sync.post,
                    endpoint,
                    content=raw,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                )
            else:
                await clients.async_client.post(
                    endpoint,
                    content=raw,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                )
        assert peer.request_count == 0
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["missing", "model", "provider", "settings", "closed"]
)
async def test_r216_factory_rejects_absent_or_mismatched_owned_clients(
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    async with _peer("openai", "ok") as peer:
        await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        clients = peer.model_clients[0]
        config = model_from_route(clients.route, None)
        settings = clients.settings
        if fault == "model":
            config = config.model_copy(update={"name": "foreign-model"})
        elif fault == "provider":
            config = config.model_copy(update={"provider": "openai"})
        elif fault == "settings":
            settings = settings.model_copy(update={"model_allow_loopback_http": False})
        elif fault == "closed":
            await clients.aclose()
        with pytest.raises(ValueError):
            make_chat_model(
                settings, config, model_clients=None if fault == "missing" else clients
            )
        assert peer.request_count == 0


async def _invoke_overridden(
    model: BaseChatModel, seam: str, overrides: dict[str, Any]
) -> None:
    import langchain.agents
    from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
    from collections.abc import Awaitable, Callable

    messages = [HumanMessage(content="hello")]
    if seam in {"invoke", "extra_body"}:
        await model.ainvoke(messages, **overrides)
    elif seam == "bind":
        await model.bind(**overrides).ainvoke(messages)
    elif seam == "bind_tools":
        bind_tools: Any = getattr(model, "bind_tools")
        await bind_tools([], **overrides).ainvoke(messages)
    else:
        assert seam == "native"

        class Override(AgentMiddleware):
            async def awrap_model_call(
                self,
                request: ModelRequest,
                handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
            ) -> ModelResponse:
                return await handler(request.override(model_settings=overrides))

        # Isolated SDK-only graph, not a Run or a Billing authorization.
        # Public constructor stubs leave response/context generics unresolved.
        constructor: Any = getattr(langchain.agents, "create_agent")
        graph = constructor(model=model, middleware=[Override()])
        await graph.ainvoke({"messages": messages})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seam", ["invoke", "bind", "bind_tools", "extra_body", "native"]
)
async def test_r216_override_seam_has_real_sdk_positive_control(
    monkeypatch: pytest.MonkeyPatch,
    seam: str,
) -> None:
    async with _peer("openai", "ok") as peer:
        model = await _model(_PROVIDER_CASES[2], peer, monkeypatch, output_cap=61)
        route = peer.model_clients[0].route
        cap = route.max_output_tokens - 1
        overrides = {"max_completion_tokens": cap}
        await _invoke_overridden(
            model,
            seam,
            {"extra_body": overrides} if seam == "extra_body" else overrides,
        )
        assert peer.request_count == 1
        request = peer.requests[0]
        assert request.request_line == b"POST /v1/chat/completions HTTP/1.1"
        payload = TypeAdapter(dict[str, object]).validate_json(request.body)
        assert payload["max_completion_tokens"] == cap
        assert payload["model"] == route.request_model


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("synchronous", [False, True])
async def test_r216_sync_async_sdk_stream_and_invoke_upward_override_zero_requests(
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
    synchronous: bool,
) -> None:
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        model = await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        cap = peer.model_clients[0].route.max_output_tokens
        runnable = model.bind(extra_body={"max_completion_tokens": cap + 1})
        messages = [HumanMessage(content="hello")]
        with pytest.raises((ValueError, openai.APIConnectionError)):
            if synchronous:

                def invoke() -> None:
                    if streaming:
                        list(runnable.stream(messages))
                    else:
                        runnable.invoke(messages)

                await asyncio.to_thread(invoke)
            elif streaming:
                async for _chunk in runnable.astream(messages):
                    pass
            else:
                await runnable.ainvoke(messages)
        assert peer.request_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True], ids=["default", "explicit-one"])
async def test_r225_single_completion_wire_positive(
    monkeypatch: pytest.MonkeyPatch,
    explicit: bool,
) -> None:
    async with _peer("openai", "ok") as peer:
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        overrides: dict[str, Any] = {"n": 1} if explicit else {}
        response = await model.ainvoke([HumanMessage(content="hello")], **overrides)
        assert getattr(response, "content") == "ok"
        assert peer.request_count == 1
        assert len(peer.requests) == 1
        observed = peer.requests[0]
        assert observed.request_line == b"POST /v1/chat/completions HTTP/1.1"
        payload = TypeAdapter(dict[str, object]).validate_json(observed.body)
        assert payload["model"] == clients.route.request_model
        assert payload["max_completion_tokens"] == clients.route.max_output_tokens
        assert "max_tokens" not in payload
        assert "max_output_tokens" not in payload
        assert type(payload["n"]) is int
        assert payload["n"] == 1
        assert payload.get("stream") is not True
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seam", ["invoke", "bind", "bind_tools", "extra_body", "native"]
)
async def test_r225_multiple_completions_rejected_before_peer(
    monkeypatch: pytest.MonkeyPatch,
    seam: str,
) -> None:
    failure: Exception | None = None
    async with _peer("openai", "ok") as peer:
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        overrides: dict[str, Any] = {
            "n": 2,
            "max_completion_tokens": clients.route.max_output_tokens,
        }
        try:
            await _invoke_overridden(
                model,
                seam,
                {"extra_body": overrides} if seam == "extra_body" else overrides,
            )
        except Exception as error:
            # Observe the peer even if an SDK wrapper changes the exception type.
            failure = error
        captured = tuple(peer.requests)
        request_count = peer.request_count
    assert clients.sync.is_closed and clients.async_client.is_closed
    # Only synthetic request bodies are retained; never credentials or headers.
    wire = [(item.request_line, item.body) for item in captured]
    for observed in captured:
        assert observed.request_line == b"POST /v1/chat/completions HTTP/1.1"
        payload = TypeAdapter(dict[str, object]).validate_json(observed.body)
        assert payload["model"] == clients.route.request_model
        assert payload["max_completion_tokens"] == clients.route.max_output_tokens
        assert type(payload["n"]) is int and payload["n"] == 2
        assert payload.get("stream") is not True
    assert request_count == 0, {
        "seam": seam,
        "request_count": request_count,
        "route_output_cap": clients.route.max_output_tokens,
        "captured_wire": wire,
        "exception_type": None if failure is None else type(failure).__name__,
    }
    assert captured == ()
    assert isinstance(failure, (ValueError, openai.APIConnectionError))
    guard_failure = (
        failure.__cause__ if isinstance(failure, openai.APIConnectionError) else failure
    )
    assert isinstance(guard_failure, ValueError)
    assert str(guard_failure) == "model request execution bound is invalid"


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize("streaming", [False, True])
async def test_r225_single_completion_sync_async_stream_wire_positive(
    monkeypatch: pytest.MonkeyPatch,
    synchronous: bool,
    streaming: bool,
) -> None:
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        model = await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        clients = peer.model_clients[0]
        messages = [HumanMessage(content="hello")]
        if synchronous:

            def invoke() -> str:
                if streaming:
                    return "".join(chunk.text for chunk in model.stream(messages))
                return model.invoke(messages).text

            result = await asyncio.to_thread(invoke)
        elif streaming:
            result = "".join([chunk.text async for chunk in model.astream(messages)])
        else:
            result = (await model.ainvoke(messages)).text
        assert result == "ok"
        assert peer.request_count == 1
        assert len(peer.requests) == 1
        observed = peer.requests[0]
        assert observed.request_line == b"POST /v1/chat/completions HTTP/1.1"
        payload = TypeAdapter(dict[str, object]).validate_json(observed.body)
        assert payload["model"] == clients.route.request_model
        assert payload["max_completion_tokens"] == clients.route.max_output_tokens
        assert type(payload["n"]) is int and payload["n"] == 1
        assert (payload.get("stream") is True) is streaming
        assert dict(observed.sdk_profile_headers).get(b"x-stainless-raw-response") == (
            None if streaming else b"true"
        )
        assert "max_tokens" not in payload and "max_output_tokens" not in payload
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    "invalid", ["null", "bool", "float", "string", "zero", "negative", "multiple"]
)
async def test_r225_extra_body_completion_override_hits_final_guard(
    monkeypatch: pytest.MonkeyPatch,
    synchronous: bool,
    streaming: bool,
    invalid: str,
) -> None:
    values: dict[str, object] = {
        "null": None,
        "bool": True,
        "float": 1.0,
        "string": "1",
        "zero": 0,
        "negative": -1,
        "multiple": 2,
    }
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        model = await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        clients = peer.model_clients[0]
        # extra_body is merged after SDK parameter handling. Constructor n stays 1.
        runnable = model.bind(extra_body={"n": values[invalid]})
        messages = [HumanMessage(content="hello")]
        with pytest.raises((ValueError, openai.APIConnectionError)) as caught:
            if synchronous:

                def invoke() -> None:
                    if streaming:
                        list(runnable.stream(messages))
                    else:
                        runnable.invoke(messages)

                await asyncio.to_thread(invoke)
            elif streaming:
                async for _chunk in runnable.astream(messages):
                    pass
            else:
                await runnable.ainvoke(messages)
        assert peer.request_count == 0
        assert peer.requests == []
        error = caught.value
        guard = (
            error.__cause__ if isinstance(error, openai.APIConnectionError) else error
        )
        assert isinstance(guard, ValueError)
        assert str(guard) == "model request execution bound is invalid"
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize(
    "case",
    [
        "one",
        "missing",
        "null",
        "bool",
        "float",
        "string",
        "zero",
        "negative",
        "multiple",
    ],
)
async def test_r225_public_owned_client_requires_strict_single_completion(
    monkeypatch: pytest.MonkeyPatch,
    synchronous: bool,
    case: str,
) -> None:
    values: dict[str, object] = {
        "one": 1,
        "null": None,
        "bool": True,
        "float": 1.0,
        "string": "1",
        "zero": 0,
        "negative": -1,
        "multiple": 2,
    }
    async with _peer("openai", "ok") as peer:
        await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        clients = peer.model_clients[0]
        payload: dict[str, object] = {
            "model": clients.route.request_model,
            "max_completion_tokens": clients.route.max_output_tokens,
            "messages": [{"role": "user", "content": "hello"}],
        }
        if case != "missing":
            payload["n"] = values[case]
        raw = _json_bytes(payload)

        async def send() -> int:
            if synchronous:
                response = await asyncio.to_thread(
                    clients.sync.post,
                    clients.endpoint,
                    content=raw,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "Authorization": "Bearer "
                        + clients.credential.get_secret_value(),
                    },
                )
            else:
                response = await clients.async_client.post(
                    clients.endpoint,
                    content=raw,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "Authorization": "Bearer "
                        + clients.credential.get_secret_value(),
                    },
                )
            return response.status_code

        if case == "one":
            assert await send() == 200
            assert peer.request_count == 1
            assert len(peer.requests) == 1
            assert (
                peer.requests[0].request_line == b"POST /v1/chat/completions HTTP/1.1"
            )
            assert peer.requests[0].body == raw
        else:
            # No SDK parameter validation here: this is the public final-wire seam.
            with pytest.raises(
                ValueError, match="^model request execution bound is invalid$"
            ):
                await send()
            assert peer.request_count == 0
            assert peer.requests == []
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize("streaming", [False, True], ids=["json", "sse"])
@pytest.mark.parametrize(
    ("usage_present", "raw_usage", "expected_counts"),
    [
        pytest.param(False, None, None, id="usage-missing"),
        pytest.param(True, None, None, id="usage-null"),
        pytest.param(True, {"total_tokens": 0}, (0, 0, 0), id="counts-missing"),
        pytest.param(
            True,
            {"prompt_tokens": None, "completion_tokens": None, "total_tokens": None},
            (0, 0, 0),
            id="counts-null",
        ),
        pytest.param(
            True,
            {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            (0, 0, 0),
            id="explicit-zero",
        ),
        pytest.param(
            True,
            {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
            (2, 3, 5),
            id="positive",
        ),
        pytest.param(
            True,
            {"prompt_tokens": 2, "completion_tokens": 3},
            (2, 3, 5),
            id="total-missing",
        ),
        pytest.param(
            True,
            {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": None},
            (2, 3, 5),
            id="total-null",
        ),
    ],
)
async def test_r230_real_sdk_usage_presence_characterization(
    monkeypatch: pytest.MonkeyPatch,
    synchronous: bool,
    streaming: bool,
    usage_present: bool,
    raw_usage: dict[str, int | None] | None,
    expected_counts: tuple[int, int, int] | None,
) -> None:
    # The expected SDK normalization is a baseline, not a financial evidence oracle.
    document = TypeAdapter(dict[str, object]).validate_json(_success_body("openai"))
    document.pop("usage")
    if usage_present:
        document["usage"] = raw_usage
    if streaming:
        final: dict[str, object] = {
            "id": "chatcmpl-sandbox",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "sandbox-model",
            "choices": [],
        }
        if usage_present:
            final["usage"] = raw_usage
        # Keep the old content/stop events, but give this case its own final usage event.
        events = _stream_body("openai").split(b"\n\n")[:2]
        stop = TypeAdapter(dict[str, object]).validate_json(events[1][6:])
        stop.pop("usage")
        response_body = (
            events[0]
            + b"\n\ndata: "
            + _json_bytes(stop)
            + b"\n\ndata: "
            + _json_bytes(final)
            + b"\n\ndata: [DONE]\n\n"
        )
    else:
        response_body = _json_bytes(document)

    close_calls = {"sync": 0, "async": 0}
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        peer.response_body = response_body
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=not streaming
        )
        clients = peer.model_clients[0]
        real_sync_close = clients.sync.close
        real_async_close = clients.async_client.aclose

        def counted_sync_close() -> None:
            close_calls["sync"] += 1
            real_sync_close()

        async def counted_async_close() -> None:
            close_calls["async"] += 1
            await real_async_close()

        # Delegate real public close methods; never replace the HTTP transport or SDK.
        monkeypatch.setattr(clients.sync, "close", counted_sync_close)
        monkeypatch.setattr(clients.async_client, "aclose", counted_async_close)
        messages = [HumanMessage(content="hello")]
        responses: list[AIMessage]
        if synchronous:

            def invoke() -> list[AIMessage]:
                if streaming:
                    return [chunk for chunk in model.stream(messages)]
                return [model.invoke(messages)]

            responses = await asyncio.to_thread(invoke)
        elif streaming:
            responses = [chunk async for chunk in model.astream(messages)]
        else:
            responses = [await model.ainvoke(messages)]
        assert "".join(response.text for response in responses) == "ok"
        assert peer.request_count == 1
        assert peer.sent_response_bodies == [response_body]
        raw_body = peer.sent_response_bodies[0]
        raw_envelope = TypeAdapter(dict[str, object]).validate_json(
            raw_body.split(b"\n\n")[-3][6:] if streaming else raw_body
        )
        assert ("usage" in raw_envelope) is usage_present
        if usage_present:
            assert raw_envelope["usage"] == raw_usage
        usage_results = [
            response.usage_metadata
            for response in responses
            if response.usage_metadata is not None
        ]
        if expected_counts is None:
            assert usage_results == []
        else:
            assert len(usage_results) == 1
            observed = usage_results[0]
            assert (
                observed["input_tokens"],
                observed["output_tokens"],
                observed["total_tokens"],
            ) == expected_counts
        # Missing/null counts and totals above deliberately remain absent/null on wire.
        # The SDK zero/sum is therefore not proof that this peer reported those values.
        assert len(peer.requests) == 1
        request = peer.requests[0]
        assert request.request_line == b"POST /v1/chat/completions HTTP/1.1"
        payload = TypeAdapter(dict[str, object]).validate_json(request.body)
        assert payload["model"] == clients.route.request_model
        assert payload["max_completion_tokens"] == clients.route.max_output_tokens
        assert type(payload["n"]) is int and payload["n"] == 1
        assert (payload.get("stream") is True) is streaming
        assert "max_tokens" not in payload and "max_output_tokens" not in payload
        assert close_calls == {"sync": 0, "async": 0}
    assert clients.sync.is_closed and clients.async_client.is_closed
    assert close_calls == {"sync": 1, "async": 1}
    await clients.aclose()
    assert close_calls == {"sync": 1, "async": 1}


def _r231_body(usage: dict[str, object] | None, streaming: bool) -> bytes:
    document = TypeAdapter(dict[str, object]).validate_json(_success_body("openai"))
    document["usage"] = usage
    if not streaming:
        return _json_bytes(document)
    content = _stream_body("openai").split(b"\n\n")[0]
    final: dict[str, object] = {
        "id": "chatcmpl-sandbox",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "sandbox-model",
        "choices": [],
        "usage": usage,
    }
    return content + b"\n\ndata: " + _json_bytes(final) + b"\n\ndata: [DONE]\n\n"


def _r231_encoded(body: bytes, encoding: str) -> bytes:
    if encoding == "gzip":
        return gzip.compress(body)
    if encoding == "deflate":
        return zlib.compress(body)
    if encoding == "zstd":
        return zstandard.ZstdCompressor(write_checksum=True).compress(body)
    assert encoding == "identity"
    return body


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding", ["identity", "gzip", "deflate", "zstd"])
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize("shape", ["missing", "null", "zero", "positive", "no-total"])
async def test_r231_capture_retains_physical_usage_presence(
    monkeypatch: pytest.MonkeyPatch,
    encoding: str,
    streaming: bool,
    synchronous: bool,
    shape: str,
) -> None:
    shapes: dict[str, dict[str, object]] = {
        "missing": {"total_tokens": 0},
        "null": {
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
        },
        "zero": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        "positive": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
        "no-total": {"prompt_tokens": 2, "completion_tokens": 3},
    }
    usage = shapes[shape]
    close_calls = {"sync": 0, "async": 0}
    raw = _r231_encoded(_r231_body(usage, streaming), encoding)
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        peer.response_body = raw
        peer.response_encoding = encoding
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=not streaming
        )
        clients = peer.model_clients[0]
        real_sync_close = clients.sync.close
        real_async_close = clients.async_client.aclose

        def counted_sync_close() -> None:
            close_calls["sync"] += 1
            real_sync_close()

        async def counted_async_close() -> None:
            close_calls["async"] += 1
            await real_async_close()

        monkeypatch.setattr(clients.sync, "close", counted_sync_close)
        monkeypatch.setattr(clients.async_client, "aclose", counted_async_close)
        with capture_model_usage(clients.route) as capture:
            assert capture.snapshot().status == "pending"
            if synchronous:

                def invoke() -> str:
                    if streaming:
                        return "".join(chunk.text for chunk in model.stream("hello"))
                    return model.invoke("hello").text

                output = await asyncio.to_thread(invoke)
            elif streaming:
                output = "".join([chunk.text async for chunk in model.astream("hello")])
            else:
                output = (await model.ainvoke("hello")).text
            capture.mark_sdk_consumption_complete()
        result = capture.snapshot()
        assert output == "ok"
        assert result.status == "complete"
        assert result.compression_integrity == "validated"
        for raw_name in ("prompt_tokens", "completion_tokens", "total_tokens"):
            count = result.counts[raw_name]
            if raw_name not in usage:
                assert count.presence == "missing" and count.value is None
            elif usage[raw_name] is None:
                assert count.presence == "null" and count.value is None
            else:
                assert count.presence == "reported" and count.value == usage[raw_name]
        assert result.projection_digest is not None
        assert capture.snapshot() is result
        assert peer.sent_response_bodies == [raw]
        assert peer.request_count == 1
        wire = TypeAdapter(dict[str, object]).validate_json(peer.requests[0].body)
        assert wire["n"] == 1
        assert wire["max_completion_tokens"] == clients.route.max_output_tokens
        assert close_calls == {"sync": 0, "async": 0}
    assert clients.sync.is_closed and clients.async_client.is_closed
    assert close_calls == {"sync": 1, "async": 1}
    await clients.aclose()
    assert close_calls == {"sync": 1, "async": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing-final",
        "missing-done",
        "conflict",
        "duplicate",
        "invalid-bool",
        "invalid-relation",
        "output-budget",
        "event-budget",
    ],
)
async def test_r231_capture_unknown_does_not_replace_sdk_response(
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    usage: dict[str, object] = {
        "prompt_tokens": 2,
        "completion_tokens": 3,
        "total_tokens": 5,
    }
    streaming = fault in {
        "missing-final",
        "missing-done",
        "conflict",
        "duplicate",
        "event-budget",
    }
    if fault == "invalid-bool":
        usage["prompt_tokens"] = True
    elif fault == "invalid-relation":
        usage["total_tokens"] = 9
    raw = _r231_body(usage, streaming)
    if fault == "missing-final":
        raw = raw.split(b"\n\n")[0] + b"\n\ndata: [DONE]\n\n"
    elif fault == "missing-done":
        raw = raw.replace(b"data: [DONE]\n\n", b"")
    elif fault in {"conflict", "duplicate"}:
        final = raw.split(b"\n\n")[1]
        extra = (
            final
            if fault == "duplicate"
            else final.replace(b'"total_tokens":5', b'"total_tokens":6').replace(
                b'"completion_tokens":3', b'"completion_tokens":4'
            )
        )
        raw = raw.replace(b"data: [DONE]", extra + b"\n\ndata: [DONE]")
    elif fault == "event-budget":
        raw = b":" + b"x" * 65536 + b"\n\n" + raw
    elif fault == "output-budget":
        raw = b" " * (4 * 1024 * 1024) + raw
    encoding = "gzip" if fault == "output-budget" else "identity"
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        peer.response_body = _r231_encoded(raw, encoding)
        peer.response_encoding = encoding
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=not streaming
        )
        clients = peer.model_clients[0]
        with capture_model_usage(clients.route) as capture:
            if streaming:
                output = "".join([chunk.text async for chunk in model.astream("hello")])
            else:
                output = (await model.ainvoke("hello")).text
            capture.mark_sdk_consumption_complete()
        assert output == "ok"
        result = capture.snapshot()
        assert result.status == (
            "complete" if fault in {"duplicate", "missing-final"} else "unresolved"
        )
        if fault == "missing-final":
            from kokoro_agent.model.usage_producer import normalize_observed_usage

            normalized = normalize_observed_usage(result)
            assert (
                normalized.state == "unresolved"
                and normalized.reason == "missing_usage"
            )
            assert all(count.value is None for count in result.counts.values())
        if fault != "duplicate":
            assert result.projection_digest is None
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
async def test_r231_capture_cancel_and_ambiguous_requests_stay_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _peer("openai", "hang") as peer:
        model = await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        clients = peer.model_clients[0]
        capture = capture_model_usage(clients.route)
        with pytest.raises(asyncio.CancelledError):
            with capture:
                task = asyncio.create_task(model.ainvoke("hello"))
                await asyncio.wait_for(peer.first_request.wait(), timeout=1)
                task.cancel()
                await task
        assert capture.snapshot().status == "unresolved"
        assert capture.snapshot().projection_digest is None
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed
    async with _peer("openai", "ok") as peer:
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        with capture_model_usage(clients.route) as ambiguous:
            values = await asyncio.gather(model.ainvoke("a"), model.ainvoke("b"))
        assert [value.text for value in values] == ["ok", "ok"]
        assert ambiguous.snapshot().reason == "ambiguous_response"

        async def independent() -> str | None:
            with capture_model_usage(clients.route) as separate:
                assert (await model.ainvoke("c")).text == "ok"
                separate.mark_sdk_consumption_complete()
            assert separate.snapshot().status == "complete"
            return separate.snapshot().projection_digest

        digests = await asyncio.gather(independent(), independent())
        assert digests[0] is not None and digests[0] == digests[1]
        assert peer.request_count == 4
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding", ["gzip", "deflate", "zstd"])
async def test_r231_done_does_not_prefetch_unconsumed_compression_tail(
    monkeypatch: pytest.MonkeyPatch,
    encoding: str,
) -> None:
    raw = _r231_body(
        {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}, True
    )
    if encoding == "zstd":
        compressor = zstandard.ZstdCompressor(write_checksum=True).compressobj()
        prefix = compressor.compress(raw) + compressor.flush(
            zstandard.COMPRESSOBJ_FLUSH_BLOCK
        )
        tail = compressor.flush(zstandard.COMPRESSOBJ_FLUSH_FINISH)
    else:
        compressor = zlib.compressobj(wbits=31 if encoding == "gzip" else 15)
        prefix = compressor.compress(raw) + compressor.flush(zlib.Z_SYNC_FLUSH)
        tail = compressor.flush(zlib.Z_FINISH)
    assert tail
    async with _peer("openai", "stream") as peer:
        peer.response_body = prefix + tail
        peer.response_encoding = encoding
        peer.held_tail_offset = len(prefix)
        model = await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        clients = peer.model_clients[0]
        with capture_model_usage(clients.route) as capture:
            async with asyncio.timeout(2):
                output = "".join([chunk.text async for chunk in model.astream("hello")])
            capture.mark_sdk_consumption_complete()
        assert output == "ok"
        result = capture.snapshot()
        assert result.status == "complete"
        assert result.compression_integrity == "unverified_unconsumed_tail"
        assert result.counts["total_tokens"].value == 5
        assert peer.sent_response_bodies == [prefix]
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape", ["members", "too-many-frames", "input-budget", "depth", "unknown-encoding"]
)
async def test_r231_encoding_budget_and_projection_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    raw = _r231_body(
        {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}, False
    )
    encoding = "identity"
    if shape in {"members", "too-many-frames"}:
        encoder = zstandard.ZstdCompressor(write_checksum=True)
        encoding = "zstd"
        raw = encoder.compress(raw)
        raw += encoder.compress(b"") * (1 if shape == "members" else 4096)
    elif shape == "input-budget":
        raw = b" " * (4 * 1024 * 1024) + raw
    elif shape == "depth":
        document = TypeAdapter(dict[str, object]).validate_json(raw)
        document["irrelevant"] = [[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]
        raw = _json_bytes(document)
    else:
        encoding = "unsupported-observer-encoding"
    async with _peer("openai", "ok") as peer:
        peer.response_body = raw
        peer.response_encoding = encoding
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        with capture_model_usage(clients.route) as capture:
            assert (await model.ainvoke("hello")).text == "ok"
            capture.mark_sdk_consumption_complete()
        result = capture.snapshot()
        assert result.status == ("complete" if shape == "members" else "unresolved")
        if shape == "members":
            assert result.compression_integrity == "validated"
        else:
            assert result.projection_digest is None
            if shape == "input-budget":
                assert result.reason == "input_budget_exhausted"
            elif shape == "unknown-encoding":
                assert result.reason == "unsupported_encoding"
            elif shape == "depth":
                assert result.reason == "depth_budget_exhausted"
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
async def test_r231_projection_digest_excludes_content_and_keeps_result_immutable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import FrozenInstanceError

    results: list[PhysicalUsageObservation] = []
    async with _peer("openai", "ok") as peer:
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        for body_text in ("first private content", "second private content"):
            document = TypeAdapter(dict[str, object]).validate_json(
                _r231_body(
                    {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
                    False,
                )
            )
            document["choices"] = [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": body_text},
                    "finish_reason": "stop",
                }
            ]
            peer.response_body = _json_bytes(document)
            with capture_model_usage(clients.route) as capture:
                assert (await model.ainvoke("hello")).text == body_text
                capture.mark_sdk_consumption_complete()
            result = capture.snapshot()
            assert body_text not in repr(result)
            assert result.status == "complete"
            results.append(result)
            with pytest.raises(FrozenInstanceError):
                setattr(result, "projection_digest", "tampered")
        assert results[0].projection_digest == results[1].projection_digest
        assert peer.request_count == 2
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding", ["gzip", "deflate", "zstd"])
@pytest.mark.parametrize("fault", ["checksum", "truncated"])
async def test_r231_invalid_compression_preserves_sdk_failure_or_unknown(
    monkeypatch: pytest.MonkeyPatch, encoding: str, fault: str
) -> None:
    raw = _r231_encoded(
        _r231_body(
            {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}, False
        ),
        encoding,
    )
    raw = raw[:-4] if fault == "truncated" else raw[:-1] + bytes([raw[-1] ^ 1])
    async with _peer("openai", "ok") as peer:
        peer.response_body = raw
        peer.response_encoding = encoding
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        baseline_error: type[Exception] | None = None
        baseline_output: str | None = None
        try:
            baseline_output = (await model.ainvoke("baseline")).text
        except Exception as error:
            baseline_error = type(error)
        observed_error: type[Exception] | None = None
        observed_output: str | None = None
        capture = capture_model_usage(clients.route)
        try:
            with capture:
                observed_output = (await model.ainvoke("observed")).text
                capture.mark_sdk_consumption_complete()
        except Exception as error:
            observed_error = type(error)
        assert observed_error is baseline_error
        assert observed_output == baseline_output
        assert capture.snapshot().status == "unresolved"
        assert capture.snapshot().projection_digest is None
        assert peer.request_count == 2
        assert peer.sent_response_bodies == [raw, raw]
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("shape", "value"),
    [
        ("count", -1),
        ("count", 1.5),
        ("count", "2"),
        ("count", 1 << 63),
        ("details", []),
        ("cached", 3),
        ("reasoning", 4),
        ("count", float("inf")),
    ],
)
async def test_r231_invalid_raw_usage_is_not_repaired_by_sdk(
    monkeypatch: pytest.MonkeyPatch, shape: str, value: object
) -> None:
    usage: dict[str, object] = {
        "prompt_tokens": 2,
        "completion_tokens": 3,
        "total_tokens": 5,
    }
    if shape == "count":
        usage["prompt_tokens"] = value
    elif shape == "details":
        usage["prompt_tokens_details"] = value
    elif shape == "cached":
        usage["prompt_tokens_details"] = {"cached_tokens": value}
    else:
        usage["completion_tokens_details"] = {"reasoning_tokens": value}
    raw = _r231_body(usage, False)
    async with _peer("openai", "ok") as peer:
        peer.response_body = raw
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        capture = capture_model_usage(clients.route)
        # Some invalid fields are also rejected by the locked SDK; that exception must
        # propagate instead of turning the capture into a successful usage report.
        try:
            with capture:
                response = await model.ainvoke("hello")
                assert response.text == "ok"
                capture.mark_sdk_consumption_complete()
        except (TypeError, ValueError, OverflowError):
            pass
        assert capture.snapshot().status == "unresolved"
        assert capture.snapshot().projection_digest is None
        assert peer.sent_response_bodies == [raw]
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("usage_present", [False, True])
async def test_r231_json_usage_object_missing_and_null_remain_distinct(
    monkeypatch: pytest.MonkeyPatch, usage_present: bool
) -> None:
    document = TypeAdapter(dict[str, object]).validate_json(_r231_body(None, False))
    if not usage_present:
        del document["usage"]
    raw = _json_bytes(document)
    async with _peer("openai", "ok") as peer:
        peer.response_body = raw
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        with capture_model_usage(clients.route) as capture:
            response = await model.ainvoke("hello")
            assert response.text == "ok" and response.usage_metadata is None
            capture.mark_sdk_consumption_complete()
        result = capture.snapshot()
        assert result.status == "complete"
        assert result.usage_presence == ("null" if usage_present else "missing")
        assert all(
            count.presence == "missing" and count.value is None
            for count in result.counts.values()
        )
        assert peer.sent_response_bodies == [raw]
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
async def test_r231_zstd_window_limit_does_not_limit_sdk_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    params = zstandard.ZstdCompressionParameters(
        compression_level=1, window_log=23, write_content_size=0, write_checksum=1
    )
    compressor = zstandard.ZstdCompressor(compression_params=params).compressobj()
    raw = (
        compressor.compress(
            _r231_body(
                {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}, False
            )
        )
        + compressor.flush()
    )
    assert zstandard.get_frame_parameters(raw).window_size > 4 * 1024 * 1024
    async with _peer("openai", "ok") as peer:
        peer.response_body = raw
        peer.response_encoding = "zstd"
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        with capture_model_usage(clients.route) as capture:
            assert (await model.ainvoke("hello")).text == "ok"
            capture.mark_sdk_consumption_complete()
        assert capture.snapshot().status == "unresolved"
        assert capture.snapshot().projection_digest is None
        assert peer.request_count == 1
        assert peer.sent_response_bodies == [raw]
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "early_close", [False, True], ids=["invoke-no-marker", "stream-early-close"]
)
async def test_r232_capture_never_infers_sdk_consumption_from_raw_done(
    monkeypatch: pytest.MonkeyPatch, early_close: bool
) -> None:
    raw = _r231_body(
        {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}, early_close
    )
    async with _peer("openai", "stream" if early_close else "ok") as peer:
        # One transport write deliberately includes final usage and DONE. The caller
        # still consumes only the first SDK item; raw buffering is not exhaustion.
        peer.response_body = raw
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=not early_close
        )
        clients = peer.model_clients[0]
        with capture_model_usage(clients.route) as capture:
            if early_close:
                iterator = model.astream("hello")
                try:
                    first = await anext(iterator)
                    assert first.text == "ok"
                finally:
                    assert isinstance(iterator, AsyncGenerator)
                    await iterator.aclose()
            else:
                assert (await model.ainvoke("hello")).text == "ok"
            # No future API call: this is the currently observable missing seam.
        result = capture.snapshot()
        assert peer.sent_response_bodies == [raw]
        if early_close:
            assert b"data: [DONE]\n\n" in peer.sent_response_bodies[0]
            assert b'"choices":[],"usage"' in peer.sent_response_bodies[0]
        assert peer.request_count == 1
        wire = TypeAdapter(dict[str, object]).validate_json(peer.requests[0].body)
        assert wire["n"] == 1
        assert wire["max_completion_tokens"] == clients.route.max_output_tokens
        assert result.status == "unresolved", (
            "Raw response availability is not SDK caller consumption completion",
            result.status,
            result.reason,
            len(raw),
            peer.request_count,
        )
        assert result.reason == "sdk_consumption_incomplete"
        assert result.projection_digest is None
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize("streaming", [False, True])
async def test_r232_completion_owner_normal_repeat_and_closed_snapshot(
    monkeypatch: pytest.MonkeyPatch, synchronous: bool, streaming: bool
) -> None:
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        peer.response_body = _r231_body(
            {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}, streaming
        )
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=not streaming
        )
        clients = peer.model_clients[0]
        request_tasks: list[object] = []

        async def record_request_task(_: object) -> None:
            request_tasks.append(asyncio.current_task())

        clients.async_client.event_hooks["request"].append(record_request_task)
        owner = asyncio.current_task()
        with capture_model_usage(clients.route) as capture:
            if synchronous:

                def invoke() -> str:
                    if streaming:
                        return "".join(chunk.text for chunk in model.stream("hello"))
                    return model.invoke("hello").text

                output = await asyncio.to_thread(invoke)
            elif streaming:
                output = "".join([chunk.text async for chunk in model.astream("hello")])
            else:
                output = (await model.ainvoke("hello")).text
            assert output == "ok"
            capture.mark_sdk_consumption_complete()
            capture.mark_sdk_consumption_complete()
            assert capture.snapshot().status == "pending"
        result = capture.snapshot()
        assert result.status == "complete"
        if not synchronous and not streaming:
            # The locked public ainvoke path uses a gather child task for the wire.
            assert len(request_tasks) == 1 and request_tasks[0] is not owner
        with pytest.raises(ValueError, match="^invalid usage capture completion$"):
            capture.mark_sdk_consumption_complete()
        assert capture.snapshot() is result
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "misuse",
    ["child-task", "child-thread", "no-response", "wrong-scope", "second-request"],
)
async def test_r232_completion_misuse_cannot_be_repaired_by_owner(
    monkeypatch: pytest.MonkeyPatch, misuse: str
) -> None:
    async with _peer("openai", "ok") as peer:
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        with capture_model_usage(clients.route) as capture:
            if misuse != "no-response":
                assert (await model.ainvoke("hello")).text == "ok"
            if misuse == "second-request":
                assert (await model.ainvoke("second")).text == "ok"
            with pytest.raises(ValueError, match="^invalid usage capture completion$"):
                if misuse == "child-task":

                    async def child() -> None:
                        capture.mark_sdk_consumption_complete()

                    await asyncio.create_task(child())
                elif misuse == "child-thread":
                    await asyncio.to_thread(capture.mark_sdk_consumption_complete)
                elif misuse == "wrong-scope":
                    with capture_model_usage(clients.route):
                        capture.mark_sdk_consumption_complete()
                else:
                    capture.mark_sdk_consumption_complete()
            with pytest.raises(ValueError, match="^invalid usage capture completion$"):
                capture.mark_sdk_consumption_complete()
        result = capture.snapshot()
        assert result.status == "unresolved" and result.projection_digest is None
        assert result.reason == (
            "ambiguous_response" if misuse == "second-request" else "capture_misuse"
        )
        assert peer.request_count == (
            0 if misuse == "no-response" else 2 if misuse == "second-request" else 1
        )
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
async def test_r232_scope_exception_overrides_valid_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _peer("openai", "ok") as peer:
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=True
        )
        clients = peer.model_clients[0]
        capture = capture_model_usage(clients.route)
        sentinel = RuntimeError("caller failure")
        with pytest.raises(RuntimeError) as raised:
            with capture:
                assert (await model.ainvoke("hello")).text == "ok"
                capture.mark_sdk_consumption_complete()
                raise sentinel
        assert raised.value is sentinel
        assert capture.snapshot().status == "unresolved"
        assert capture.snapshot().reason == "scope_interrupted"
        assert capture.snapshot().projection_digest is None
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("mark_complete", [False, True])
async def test_r232_synchronous_scope_owner_requires_completion(
    monkeypatch: pytest.MonkeyPatch, streaming: bool, mark_complete: bool
) -> None:
    async with _peer("openai", "stream" if streaming else "ok") as peer:
        peer.response_body = _r231_body(
            {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}, streaming
        )
        model = await _model(
            _PROVIDER_CASES[2], peer, monkeypatch, disable_streaming=not streaming
        )
        clients = peer.model_clients[0]

        def invoke() -> PhysicalUsageObservation:
            with capture_model_usage(clients.route) as capture:
                output = (
                    "".join(chunk.text for chunk in model.stream("hello"))
                    if streaming
                    else model.invoke("hello").text
                )
                assert output == "ok"
                if mark_complete:
                    capture.mark_sdk_consumption_complete()
            return capture.snapshot()

        result = await asyncio.to_thread(invoke)
        assert result.status == ("complete" if mark_complete else "unresolved")
        assert result.reason == (
            None if mark_complete else "sdk_consumption_incomplete"
        )
        assert (result.projection_digest is not None) is mark_complete
        assert peer.request_count == 1
    assert clients.sync.is_closed and clients.async_client.is_closed


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("synchronous", [False, True])
async def test_r246_production_owned_clients_without_scope_never_send(
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
    synchronous: bool,
) -> None:
    from kokoro_agent.model.call_models import ModelCallAuthorizationUnavailable

    async with _peer("openai", "stream" if streaming else "ok") as peer:
        await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        mechanism = peer.model_clients[0]
        production = ModelHttpClients(
            mechanism.settings, mechanism.route, mechanism.model_credential
        )
        peer.model_clients.append(production)
        model = make_chat_model(
            production.settings,
            model_from_route(production.route, None),
            model_clients=production,
        )
        assert len(production.async_client.event_hooks["request"]) == 1
        assert len(production.sync.event_hooks["request"]) == 1
        with pytest.raises(openai.APIConnectionError) as caught:
            if synchronous:

                def invoke() -> None:
                    if streaming:
                        list(model.stream("r246-no-scope"))
                    else:
                        model.invoke("r246-no-scope")

                await asyncio.to_thread(invoke)
            elif streaming:
                async for _ in model.astream("r246-no-scope"):
                    pytest.fail("production request reached a response without scope")
            else:
                await model.ainvoke("r246-no-scope")
        assert isinstance(caught.value.__cause__, ModelCallAuthorizationUnavailable)
        assert peer.request_count == 0
        assert peer.requests == []


@pytest.mark.parametrize("streaming", [False, True])
async def test_r246_sdk_structured_helpers_keep_exact_header_profile(
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
) -> None:
    from pydantic import BaseModel

    class StructuredAnswer(BaseModel):
        value: str

    async with _peer("openai", "stream" if streaming else "ok") as peer:
        await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        clients = peer.model_clients[0]
        content = '{"value":"ok"}'
        if streaming:
            peer.response_body = _stream_body("openai").replace(
                b'"content":"ok"', b'"content":' + json.dumps(content).encode()
            )
        else:
            peer.response_body = _json_bytes(
                {
                    "id": "chatcmpl-r246",
                    "object": "chat.completion",
                    "created": 1,
                    "model": "sandbox-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": content},
                            "finish_reason": "stop",
                        }
                    ],
                }
            )
        sdk = openai.AsyncOpenAI(
            api_key=clients.credential.get_secret_value(),
            base_url=peer.base_url,
            http_client=clients.async_client,
            max_retries=0,
        )
        try:
            if streaming:
                async with sdk.chat.completions.stream(
                    model=clients.route.request_model,
                    n=1,
                    max_completion_tokens=clients.route.max_output_tokens,
                    messages=[{"role": "user", "content": "structured"}],
                    response_format=StructuredAnswer,
                ) as stream:
                    async for _ in stream:
                        pass
                    result = await stream.get_final_completion()
            else:
                result = await sdk.chat.completions.parse(
                    model=clients.route.request_model,
                    n=1,
                    max_completion_tokens=clients.route.max_output_tokens,
                    messages=[{"role": "user", "content": "structured"}],
                    response_format=StructuredAnswer,
                )
            parsed = result.choices[0].message.parsed
            assert parsed is not None and parsed.value == "ok"
            assert peer.request_count == 1
            body = TypeAdapter(dict[str, object]).validate_json(peer.requests[0].body)
            assert body["n"] == 1
            assert body["model"] == clients.route.request_model
            assert "response_format" in body
            headers = dict(peer.requests[0].sdk_profile_headers)
            assert headers[b"x-stainless-helper-method"] == (
                b"chat.completions.stream" if streaming else b"chat.completions.parse"
            )
            # Direct public helpers do not request the legacy raw-response wrapper.
            assert b"x-stainless-raw-response" not in headers
        finally:
            await sdk.close()


async def test_r246_production_rejects_unmaterialized_stream_without_consuming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.model.call_models import ModelCallInputInvalid

    consumed = False

    async def body() -> AsyncGenerator[bytes, None]:
        nonlocal consumed
        consumed = True
        yield b"{}"

    async with _peer("openai", "ok") as peer:
        await _model(_PROVIDER_CASES[2], peer, monkeypatch)
        mechanism = peer.model_clients[0]
        clients = ModelHttpClients(
            mechanism.settings, mechanism.route, mechanism.model_credential
        )
        peer.model_clients.append(clients)
        with pytest.raises(ModelCallInputInvalid, match="must be materialized"):
            await clients.async_client.post(clients.endpoint, content=body())
        assert not consumed
        assert peer.request_count == 0
