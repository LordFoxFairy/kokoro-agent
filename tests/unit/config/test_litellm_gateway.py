"""litellm 网关档规格：OpenAI 兼容客户端指向网关，凭据只有网关地址 + 网关 key。

真端点验证：测试内起一个本地 HTTP 假 OpenAI 端点，断言请求真的打到网关地址、
Authorization 携带网关 key、body.model 为网关侧 model_name（别名），并验证流式可用。
网关 key 全程用明显假值。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from kokoro_agent.config import AppConfig
from kokoro_agent.policy import ModelConfig
from kokoro_agent.model.factory import make_chat_model

GATEWAY_KEY = "gw-test-key-not-real"


@dataclass
class _Captured:
    """假端点记录到的一次请求，用于断言请求形状。"""

    path: str
    authorization: str | None
    model: str
    stream: bool


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


def _config(base_url: str) -> AppConfig:
    return AppConfig.from_env(
        {
            "KOKORO_LITELLM_BASE_URL": base_url,
            "KOKORO_LITELLM_API_KEY": GATEWAY_KEY,
        }
    )


def test_litellm_construction_shape() -> None:
    # provider=litellm 复用现有 ModelConfig 形状；构造出指向网关的 OpenAI 兼容客户端。
    config = _config("https://gateway.example.com/v1")
    model = make_chat_model(
        config.model, ModelConfig(provider="litellm", name="gateway-alias")
    )
    assert isinstance(model, ChatOpenAI)
    assert model.model_name == "gateway-alias"
    assert model.openai_api_base == "https://gateway.example.com/v1"
    # 网关 key 是否真被携带由真端点测试（test_litellm_invoke_hits_gateway_with_key）权威证明。


def test_litellm_thinking_maps_reasoning_effort() -> None:
    # OpenAI 兼容语义透传 reasoning_effort：thinking=True→high、False→minimal、None→回落 effort。
    config = _config("https://gateway.example.com/v1")

    def effort_of(thinking: bool | None, effort: str | None) -> object:
        model = make_chat_model(
            config.model,
            ModelConfig(
                provider="litellm",
                name="gateway-alias",
                thinking=thinking,
                effort=effort,
            ),
        )
        assert isinstance(model, ChatOpenAI)
        return model.reasoning_effort

    assert effort_of(True, None) == "high"
    assert effort_of(False, None) == "minimal"
    assert effort_of(None, "medium") == "medium"


def test_litellm_requires_base_url_fail_loud() -> None:
    config = AppConfig.from_env({"KOKORO_LITELLM_API_KEY": GATEWAY_KEY})
    with pytest.raises(ValueError, match="KOKORO_LITELLM_BASE_URL"):
        make_chat_model(
            config.model, ModelConfig(provider="litellm", name="gateway-alias")
        )


def test_litellm_requires_api_key_fail_loud() -> None:
    config = AppConfig.from_env(
        {"KOKORO_LITELLM_BASE_URL": "https://gateway.example.com/v1"}
    )
    with pytest.raises(ValueError, match="KOKORO_LITELLM_API_KEY"):
        make_chat_model(
            config.model, ModelConfig(provider="litellm", name="gateway-alias")
        )


def test_litellm_invoke_hits_gateway_with_key(gateway: _GatewayServer) -> None:
    # 真 HTTP 假网关：证明请求打到网关地址、Authorization 带网关 key、body.model 为网关别名。
    config = _config(_base_url(gateway))
    model = make_chat_model(
        config.model, ModelConfig(provider="litellm", name="gateway-alias")
    )
    reply = model.invoke([HumanMessage(content="ping")])
    assert reply.text == "pong"
    assert len(gateway.captured) == 1
    call = gateway.captured[0]
    assert call.path == "/v1/chat/completions"
    assert call.authorization == f"Bearer {GATEWAY_KEY}"
    assert call.model == "gateway-alias"
    assert call.stream is False


def test_litellm_streaming_over_gateway(gateway: _GatewayServer) -> None:
    # 流式可用：stream=True 走 SSE，分块拼回完整内容，请求同样落在网关且带网关 key。
    config = _config(_base_url(gateway))
    model = make_chat_model(
        config.model, ModelConfig(provider="litellm", name="gateway-alias")
    )
    chunks = list(model.stream([HumanMessage(content="ping")]))
    assert "".join(chunk.text for chunk in chunks) == "pong"
    assert len(gateway.captured) == 1
    call = gateway.captured[0]
    assert call.stream is True
    assert call.authorization == f"Bearer {GATEWAY_KEY}"
    assert call.model == "gateway-alias"
