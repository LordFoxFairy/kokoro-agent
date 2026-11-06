"""System-owned direct/optional LiteLLM snapshots and private worker credentials.

Local HTTP peers test protocol mechanics only, never a real model authorization.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Generator
from contextlib import contextmanager
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Literal
from pathlib import Path
from pydantic import SecretStr

import pytest
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from support.model_http_mechanism import ModelHttpMechanismClients

from kokoro_agent.config import AppConfig
from kokoro_agent.policy import ModelConfig
from kokoro_agent.clients.system import ModelConnection, ResolvedModel
from kokoro_agent.model.credentials import ModelCredential
from kokoro_agent.model.factory import ModelHttpClients, make_chat_model

GATEWAY_KEY = "gw-test-key-not-real"


@dataclass
class _Captured:
    """假端点记录到的一次请求，用于断言请求形状。"""

    path: str
    authorization: str | None
    model: str
    stream: bool
    max_completion_tokens: object


class _GatewayServer(ThreadingHTTPServer):
    """携带请求捕获缓冲的假 OpenAI 网关端点。"""

    captured: list[_Captured]

    def __init__(self, handler: type[BaseHTTPRequestHandler]) -> None:
        self.captured = []
        super().__init__(("127.0.0.1", 0), handler)


_NONSTREAM_BODY: dict[str, Any] = {
    "id": "chatcmpl-fake",
    "object": "chat.completion",
    "created": 0,
    "model": "gateway-alias",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "pong"},
            "finish_reason": "stop",
        }
    ],
    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
}

_STREAM_CHUNKS: list[dict[str, Any]] = [
    {
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {"content": "po"}, "finish_reason": None}],
    },
    {
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {"content": "ng"}, "finish_reason": None}],
    },
    {
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    },
]


class _FakeOpenAIHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - 覆盖基类签名
        # 静音测试期访问日志，保持输出干净。
        return

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler 约定方法名
        length_header = self.headers.get("Content-Length")
        length = int(length_header) if length_header else 0
        raw = self.rfile.read(length)
        parsed: Any = json.loads(raw) if raw else {}
        streaming = bool(parsed.get("stream"))
        assert isinstance(self.server, _GatewayServer)
        self.server.captured.append(
            _Captured(
                path=self.path,
                authorization=self.headers.get("Authorization"),
                model=str(parsed.get("model")),
                stream=streaming,
                max_completion_tokens=parsed.get("max_completion_tokens"),
            )
        )
        if streaming:
            self._send_sse()
        else:
            self._send_json(_NONSTREAM_BODY)

    def _send_json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_sse(self) -> None:
        # HTTP/1.0（默认）响应后关连接，客户端读到 EOF 即流终止。
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for chunk in _STREAM_CHUNKS:
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")


@pytest.fixture
def gateway() -> Any:
    server = _GatewayServer(_FakeOpenAIHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _base_url(server: _GatewayServer) -> str:
    host, port = server.server_address[0], server.server_address[1]
    return f"http://{host}:{port}/v1"


def _config() -> AppConfig:
    return AppConfig(model_allow_loopback_http=True)


def _credential(
    provider: str = "22222222-2222-4222-8222-222222222222",
    reference: str = "gateway-key",
) -> ModelCredential:
    return ModelCredential(
        provider_id=provider,
        secret_handle_ref=reference,
        api_key=SecretStr(GATEWAY_KEY),
    )


def _route(
    base_url: str, connection_type: Literal["direct", "litellm"] = "litellm"
) -> ResolvedModel:
    """One legal immutable owner route; never an implicit production default."""
    return ResolvedModel(
        model_id="11111111-1111-4111-8111-111111111111",
        provider_id="22222222-2222-4222-8222-222222222222",
        revision_id="33333333-3333-4333-8333-333333333333",
        revision=1,
        digest="a" * 64,
        generation="1",
        tenant_generation="1",
        provider_model_name="provider-model",
        request_model="gateway-alias",
        connection=ModelConnection(
            protocol="openai_chat_completions",
            connection_type=connection_type,
            base_url=base_url,
            secret_handle_ref="gateway-key",
        ),
        feature_key="chat",
        label_key="fast",
        context_window=8192,
        max_output_tokens=96,
    )


@contextmanager
def _owned_clients(
    config: AppConfig,
    base_url: str = "https://gateway.example.com/v1",
    *,
    mechanism: bool = False,
    connection_type: Literal["direct", "litellm"] = "litellm",
) -> Generator[ModelHttpClients]:
    clients = (ModelHttpMechanismClients if mechanism else ModelHttpClients)(
        config.model, _route(base_url, connection_type), _credential()
    )
    try:
        yield clients
    finally:
        # These tests use sync SDK calls; the owned async pool still must close.
        asyncio.run(clients.aclose())
        assert clients.sync.is_closed
        assert clients.async_client.is_closed


@pytest.mark.parametrize("connection_type", ["direct", "litellm"])
def test_model_protocol_construction_for_either_connection_type(
    connection_type: Literal["direct", "litellm"],
) -> None:
    # provider=litellm 复用现有 ModelConfig 形状；构造出指向网关的 OpenAI 兼容客户端。
    config = _config()
    with _owned_clients(config, connection_type=connection_type) as clients:
        model = make_chat_model(
            config.model,
            ModelConfig(provider="openai_chat_completions", name="gateway-alias"),
            model_clients=clients,
        )
        assert isinstance(model, ChatOpenAI)
        assert model.model_name == "gateway-alias"
        assert model.openai_api_base == "https://gateway.example.com/v1"
        # 网关 key 是否真被携带由真端点测试（test_litellm_invoke_hits_gateway_with_key）权威证明。


def test_litellm_thinking_maps_reasoning_effort() -> None:
    # OpenAI 兼容语义透传 reasoning_effort：thinking=True→high、False→minimal、None→回落 effort。
    config = _config()
    with _owned_clients(config) as clients:

        def effort_of(thinking: bool | None, effort: str | None) -> object:
            model = make_chat_model(
                config.model,
                ModelConfig(
                    provider="openai_chat_completions",
                    name="gateway-alias",
                    thinking=thinking,
                    effort=effort,
                ),
                model_clients=clients,
            )
            assert isinstance(model, ChatOpenAI)
            return model.reasoning_effort

        assert effort_of(True, None) == "high"
        assert effort_of(False, None) == "minimal"
        assert effort_of(None, "medium") == "medium"


@pytest.mark.parametrize(
    "name",
    ["KOKORO_LITELLM_ENABLED", "KOKORO_LITELLM_BASE_URL", "KOKORO_LITELLM_API_KEY"],
)
def test_retired_global_gateway_configuration_is_rejected(name: str) -> None:
    with pytest.raises(ValueError, match="retired global model gateway"):
        AppConfig.from_env({name: ""})


@pytest.mark.parametrize("mismatch", ["provider", "handle"])
def test_model_connection_requires_its_exact_provider_credential(
    gateway: _GatewayServer, mismatch: str
) -> None:
    config = AppConfig(
        model_allow_loopback_http=True,
        openai_api_key=SecretStr("unrelated-native-openai-key"),
    )
    credential = (
        _credential("different-provider")
        if mismatch == "provider"
        else _credential(reference="different-handle")
    )
    with pytest.raises(ValueError, match="model credentials unavailable"):
        ModelHttpClients(config.model, _route(_base_url(gateway)), credential)
    with pytest.raises(ValueError, match="routed protocol requires owned"):
        make_chat_model(
            config.model,
            ModelConfig(provider="openai_chat_completions", name="gateway-alias"),
        )
    assert gateway.captured == []


def test_http_loopback_model_connection_requires_explicit_deployment_permission(
    gateway: _GatewayServer,
) -> None:
    config = _config().model_copy(update={"model_allow_loopback_http": False})
    with pytest.raises(ValueError, match="explicit deployment permission"):
        ModelHttpClients(config.model, _route(_base_url(gateway)), _credential())
    assert gateway.captured == []


def test_litellm_invoke_hits_gateway_with_key(gateway: _GatewayServer) -> None:
    # 真 HTTP 假网关：证明请求打到网关地址、Authorization 带网关 key、body.model 为网关别名。
    config = _config()
    with _owned_clients(config, _base_url(gateway), mechanism=True) as clients:
        model = make_chat_model(
            config.model,
            ModelConfig(provider="openai_chat_completions", name="gateway-alias"),
            model_clients=clients,
        )
        reply = model.invoke([HumanMessage(content="ping")])
        assert reply.text == "pong"
        assert len(gateway.captured) == 1
        call = gateway.captured[0]
        assert call.path == "/v1/chat/completions"
        assert call.authorization == f"Bearer {GATEWAY_KEY}"
        assert call.model == "gateway-alias"
        assert call.stream is False

        assert type(call.max_completion_tokens) is int
        assert call.max_completion_tokens == clients.route.max_output_tokens


def test_litellm_streaming_over_gateway(gateway: _GatewayServer) -> None:
    # 流式可用：stream=True 走 SSE，分块拼回完整内容，请求同样落在网关且带网关 key。
    config = _config()
    with _owned_clients(config, _base_url(gateway), mechanism=True) as clients:
        model = make_chat_model(
            config.model,
            ModelConfig(provider="openai_chat_completions", name="gateway-alias"),
            model_clients=clients,
        )
        chunks = list(model.stream([HumanMessage(content="ping")]))
        assert "".join(chunk.text for chunk in chunks) == "pong"
        assert len(gateway.captured) == 1
        call = gateway.captured[0]
        assert call.stream is True
        assert call.authorization == f"Bearer {GATEWAY_KEY}"
        assert call.model == "gateway-alias"

        assert type(call.max_completion_tokens) is int
        assert call.max_completion_tokens == clients.route.max_output_tokens


@pytest.mark.parametrize("value", ["", "/missing/private.json"])
def test_retired_local_model_file_configuration_is_rejected(value: str) -> None:
    with pytest.raises(
        ValueError, match="retired local model credential file"
    ) as caught:
        AppConfig.from_env({"KOKORO_MODEL_CREDENTIALS_FILE": value})
    assert value == "" or value not in str(caught.value)


def test_retired_model_file_yaml_is_not_silently_ignored(tmp_path: Path) -> None:
    path = tmp_path / "agent.yaml"
    path.write_text("model:\n  credentials_file: /private/former-key.json\n")
    with pytest.raises(KeyError, match="unknown config key"):
        AppConfig.from_env({"KOKORO_AGENT_CONFIG": str(path)})
