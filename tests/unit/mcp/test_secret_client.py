"""Capability 凭据解析客户端规格：出站信任头 + 请求体形状 / 响应信封解析 / 非2xx·网络·畸形→失败 /
空句柄不发请求 / env 装配开关。以 httpx.MockTransport 驱动，无真 Capability 服务。"""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest

import kokoro_agent.mcp.secret_client as secret_client
from kokoro_agent.mcp.secret_client import (
    CapabilitySecretResolver,
    CapabilitySecretSettings,
    SecretResolveError,
    capability_secret_resolver_from_env,
)

_HANDLE = "srt_" + "a1b2c3d4" * 4
_BASE = "http://capability.internal:4600"


def _resolver() -> CapabilitySecretResolver:
    return CapabilitySecretResolver(
        CapabilitySecretSettings.model_validate({"base_url": _BASE, "service_secret": "agent-shared-secret"})
    )


def _mock(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    """把 secret_client 内部 httpx.AsyncClient 的 transport 换成 MockTransport（保留其它 kwargs）。"""
    transport = httpx.MockTransport(handler)
    real = httpx.AsyncClient

    def fake(*args: object, **kwargs: object) -> httpx.AsyncClient:
        kwargs["transport"] = transport
        return real(*args, **kwargs)

    monkeypatch.setattr(secret_client.httpx, "AsyncClient", fake)


async def test_sends_trust_headers_body_and_parses_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["caller"] = request.headers.get("x-kokoro-service")
        seen["secret"] = request.headers.get("x-kokoro-internal-secret")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"data": {"secrets": {_HANDLE: "Bearer plain"}}, "requestId": "r1"})

    _mock(monkeypatch, handler)
    resolved = await _resolver().resolve("ns1", [_HANDLE])
    assert resolved == {_HANDLE: "Bearer plain"}
    assert seen["url"] == f"{_BASE}/capability/runtime/mcp/secrets/resolve"
    assert seen["caller"] == "agent"  # caller 身份
    assert seen["secret"] == "agent-shared-secret"  # per-caller 内部密钥
    assert seen["body"] == {"namespace": "ns1", "handles": [_HANDLE]}


async def test_non_2xx_raises_without_leaking_body(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # 404（跨 namespace/不存在，Capability 全有或全无）：错误体不得进异常文本。
        return httpx.Response(404, json={"error": {"code": "capability.secret_not_found", "message": "leak-me"}})

    _mock(monkeypatch, handler)
    with pytest.raises(SecretResolveError) as excinfo:
        await _resolver().resolve("ns1", [_HANDLE])
    assert "404" in str(excinfo.value)
    assert "leak-me" not in str(excinfo.value)


async def test_malformed_2xx_response_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"wrong": {}}})  # 缺 secrets

    _mock(monkeypatch, handler)
    with pytest.raises(SecretResolveError, match="malformed"):
        await _resolver().resolve("ns1", [_HANDLE])


async def test_network_error_raises_secret_resolve_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    _mock(monkeypatch, handler)
    with pytest.raises(SecretResolveError, match="request failed"):
        await _resolver().resolve("ns1", [_HANDLE])


async def test_empty_handles_makes_no_request(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - 不应被调用
        raise AssertionError("空句柄不应发起请求")

    _mock(monkeypatch, handler)
    assert await _resolver().resolve("ns1", []) == {}


def test_env_switch_requires_both_base_url_and_secret() -> None:
    assert capability_secret_resolver_from_env({}) is None
    assert capability_secret_resolver_from_env({"KOKORO_CAPABILITY_BASE_URL": _BASE}) is None
    assert capability_secret_resolver_from_env({"KOKORO_INTERNAL_SECRET_AGENT": "s"}) is None
    resolver = capability_secret_resolver_from_env(
        {"KOKORO_CAPABILITY_BASE_URL": _BASE, "KOKORO_INTERNAL_SECRET_AGENT": "s"}
    )
    assert isinstance(resolver, CapabilitySecretResolver)
