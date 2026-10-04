"""Real locked-SDK retry behavior against an owned loopback HTTP peer."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, TypeAlias

import anthropic
import openai
import pytest
from langchain_core.messages import AIMessageChunk, HumanMessage
from langchain_core.language_models import BaseChatModel
from pydantic import SecretStr, TypeAdapter

from kokoro_agent.clients.system import SystemModelClient
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
            self.requests.append(_CapturedRequest(request_line, request_body))
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
    if provider == "litellm":
        data = _system_route_data()
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
        clients = ModelHttpClients(settings, route)
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
def _system_route_data() -> dict[str, object]:
    return {
        "model_id": "11111111-1111-4111-8111-111111111111",
        "provider_id": "22222222-2222-4222-8222-222222222222",
        "revision_id": "33333333-3333-4333-8333-333333333333",
        "revision": 2,
        "digest": "a" * 64,
        "generation": "9007199254740993",
        "tenant_generation": "3",
        "provider_model_name": "provider-model",
        "gateway_model_name": "gateway-model",
        "feature_key": "chat",
        "label_key": "fast",
        "transport": "litellm",
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
        assert asdict(route) == _system_route_data()


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
        assert payload["model"] == clients.route.gateway_model_name
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
            "model": clients.route.gateway_model_name,
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
                await asyncio.to_thread(clients.sync.post, endpoint, content=raw)
            else:
                await clients.async_client.post(endpoint, content=raw)
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
            settings = settings.model_copy(
                update={"litellm_base_url": peer.base_url + "/other"}
            )
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
        assert payload["model"] == route.gateway_model_name


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
        assert payload["model"] == clients.route.gateway_model_name
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
        assert payload["model"] == clients.route.gateway_model_name
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
        assert payload["model"] == clients.route.gateway_model_name
        assert payload["max_completion_tokens"] == clients.route.max_output_tokens
        assert type(payload["n"]) is int and payload["n"] == 1
        assert (payload.get("stream") is True) is streaming
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
            "model": clients.route.gateway_model_name,
            "max_completion_tokens": clients.route.max_output_tokens,
            "messages": [{"role": "user", "content": "hello"}],
        }
        if case != "missing":
            payload["n"] = values[case]
        raw = _json_bytes(payload)

        async def send() -> int:
            if synchronous:
                response = await asyncio.to_thread(
                    clients.sync.post, clients.endpoint, content=raw
                )
            else:
                response = await clients.async_client.post(
                    clients.endpoint, content=raw
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
