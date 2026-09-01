"""MCP 连接期 egress 防线规格：禁网段判定（与 Capability 同段表）/ 连接锁定解析 IP 防 rebinding /
禁 redirect / strict·off 两档 / build_connections 注入。fake resolver 驱动，无真外连。"""

from __future__ import annotations

import httpx
import pytest

from kokoro_agent.mcp.config import McpServerConfig
from kokoro_agent.mcp.egress import (
    EgressBlocked,
    GuardedTransport,
    build_mcp_client_factory,
    egress_mode_from_env,
    is_forbidden_ip,
)
from kokoro_agent.mcp.servers import build_connections

_FORBIDDEN = [
    "0.0.0.0",
    "10.0.0.5",
    "127.0.0.1",
    "169.254.169.254",  # 云元数据
    "172.16.0.1",
    "172.31.255.254",
    "192.168.1.1",
    "100.64.0.1",  # CGNAT
    "100.127.255.254",
    "224.0.0.1",  # 组播
    "240.0.0.1",  # 保留
    "255.255.255.255",  # 广播
    "::1",
    "::",
    "fc00::1",
    "fd12:3456::1",
    "fe80::1",
    "ff02::1",
    "::ffff:169.254.169.254",  # IPv4-mapped 元数据
    "::ffff:100.64.0.1",  # IPv4-mapped CGNAT
    "::ffff:127.0.0.1",
]

_ALLOWED = [
    "8.8.8.8",
    "1.1.1.1",
    "172.15.0.1",  # 172.16/12 边界外
    "172.32.0.1",
    "192.169.0.1",
    "100.63.255.254",  # 100.64/10 边界外
    "100.128.0.1",
    "223.255.255.255",  # 224/4 边界外
    "2606:4700:4700::1111",
    "::ffff:8.8.8.8",
]


@pytest.mark.parametrize("ip", _FORBIDDEN)
def test_forbidden_segments_rejected(ip: str) -> None:
    assert is_forbidden_ip(ip) is True


@pytest.mark.parametrize("ip", _ALLOWED)
def test_public_segments_allowed(ip: str) -> None:
    assert is_forbidden_ip(ip) is False


def test_unparseable_address_fails_closed() -> None:
    # 无法判定的地址形状：fail-closed 拒（与 Capability 同策）。
    assert is_forbidden_ip("not-an-ip") is True
    assert is_forbidden_ip("") is True


class _RecordingInner(httpx.AsyncBaseTransport):
    """替身内层 transport：记录到达连接层的最终 request（host/sni/port），不真连。"""

    def __init__(self) -> None:
        self.host: str | None = None
        self.sni: object = None
        self.port: int | None = None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.host = request.url.host
        self.sni = request.extensions.get("sni_hostname")
        self.port = request.url.port
        return httpx.Response(200, text="ok")

    async def aclose(self) -> None:
        pass


def _resolver(*addresses: str):
    async def resolve(host: str) -> list[str]:
        return list(addresses)

    return resolve


async def test_pins_connection_to_resolved_ip_and_keeps_sni() -> None:
    # 主机名解析后：连接 pin 到已校验 IP（URL host 改为 IP），SNI+证书校验仍用原主机名，端口不变。
    inner = _RecordingInner()
    transport = GuardedTransport(inner, _resolver("93.184.216.34"))
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get("https://mcp.example.com:8443/mcp")
    assert response.status_code == 200
    assert inner.host == "93.184.216.34"  # 连接目标是解析 IP（防 rebinding：同一次解析既校验又连）
    assert inner.sni == "mcp.example.com"  # TLS 主机名不被 pin 篡改
    assert inner.port == 8443


async def test_hostname_resolving_to_forbidden_is_blocked() -> None:
    # DNS 指向内网/元数据：解析全量校验，任一落禁段即拒（挡 rebinding 的内网跳板）。
    transport = GuardedTransport(_RecordingInner(), _resolver("93.184.216.34", "169.254.169.254"))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(EgressBlocked, match="禁止网段"):
            await client.get("https://rebind.example/")


async def test_literal_loopback_target_blocked() -> None:
    transport = GuardedTransport(_RecordingInner(), _resolver("8.8.8.8"))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(EgressBlocked, match="禁止网段"):
            await client.get("http://127.0.0.1:9/mcp")


async def test_unresolvable_host_blocked() -> None:
    transport = GuardedTransport(_RecordingInner(), _resolver())  # 空解析
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(EgressBlocked, match="无法解析"):
            await client.get("https://nowhere.example/")


async def test_factory_off_is_none_strict_disables_redirect() -> None:
    assert build_mcp_client_factory("off") is None
    assert build_mcp_client_factory("weird") is None  # 非 strict 一律不设防线工厂
    factory = build_mcp_client_factory("strict")
    assert factory is not None
    async with factory() as client:
        # strict 工厂关闭 redirect（禁开放重定向绕过防线）。
        assert client.follow_redirects is False


def test_egress_mode_from_env_defaults_strict() -> None:
    assert egress_mode_from_env({}) == "strict"
    assert egress_mode_from_env({"KOKORO_MCP_EGRESS_MODE": "off"}) == "off"
    assert egress_mode_from_env({"KOKORO_MCP_EGRESS_MODE": "OFF"}) == "off"
    assert egress_mode_from_env({"KOKORO_MCP_EGRESS_MODE": "bogus"}) == "strict"  # fail-safe


def _config(url: str) -> McpServerConfig:
    return McpServerConfig.model_validate({"url": url, "allowed_tools": ["t"]})


def test_build_connections_attaches_guard_in_strict_not_off() -> None:
    servers = {"a": _config("https://mcp.example/x")}
    strict = build_connections(servers, egress_mode="strict")
    off = build_connections(servers, egress_mode="off")
    assert "httpx_client_factory" in strict["a"]
    assert "httpx_client_factory" not in off["a"]
