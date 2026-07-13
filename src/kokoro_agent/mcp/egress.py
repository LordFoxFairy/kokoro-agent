"""MCP 连接期 egress 防线（连接构造处的动态防线，MCP-SECRET AGENT 半场）。

连接前对目标 host 全量解析 A/AAAA，任一落在禁网段即拒（与 kokoro-hub
`interfaces/http/mcp-url-guard.ts` 同段表，Python `ipaddress` 实现）；并把连接**锁定**到
这次已校验的解析 IP（httpx transport 层重写 URL host→IP + `sni_hostname` 保留原主机名做
TLS SNI 与证书校验），同时关闭 redirect。同一次解析既做校验又做连接，消除 DNS rebinding
的 TOCTOU 窗口——这是 tools/web_fetch.py「残余 TOCTOU 接受」之上的收紧。

模式 `KOKORO_MCP_EGRESS_MODE=strict|off`（缺省 strict）：off 供 gate/closure 的
127.0.0.1 fixture 放行环回地址；strict 下环回/私网/元数据面一律拒。
"""

from __future__ import annotations

import asyncio
import ipaddress
from collections.abc import Awaitable, Callable, Mapping

import httpx

from mcp.shared._httpx_utils import McpHttpClientFactory

from kokoro_agent import metrics

_STRICT = "strict"
_OFF = "off"
_EGRESS_MODE_ENV = "KOKORO_MCP_EGRESS_MODE"

# 与 mcp SDK create_mcp_http_client 缺省对齐（不 import 私有常量，避免上游改名连坐）。
_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_SSE_READ_TIMEOUT_S = 300.0

# 解析器：host → 解析出的 IP 串全集（getaddrinfo 全量 A/AAAA，含 /etc/hosts，贴合实际会连的 IP）。
AddressResolver = Callable[[str], Awaitable[list[str]]]

_IpAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


class EgressBlocked(Exception):
    """目标落在禁网段 / 无法解析：连接期 fail-closed。绝不含凭据，只述及 host/网段。"""

    def __init__(self, *args: object) -> None:
        # 每次构造即一次 egress 拒绝（全部实例都是 raise 现场）：单点计数，fail-open。
        super().__init__(*args)
        metrics.record_egress_blocked()


def _parse_ip(text: str) -> _IpAddress | None:
    # 去掉 scope id（fe80::1%eth0）后按字面 IP 解析；非 IP 文本返 None（=需 DNS 解析的主机名）。
    host = text.split("%", 1)[0]
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        return None


def _forbidden_ipv4(addr: ipaddress.IPv4Address) -> bool:
    # 禁网段（SSRF 防线）：本机/私网/共享(CGNAT)/链路本地(含 169.254.169.254 元数据)/组播/保留。
    a, b = addr.packed[0], addr.packed[1]
    if a == 0:  # 0.0.0.0/8 本网络/未指定
        return True
    if a == 10:  # 10/8 私网
        return True
    if a == 127:  # 127/8 loopback
        return True
    if a == 169 and b == 254:  # 169.254/16 链路本地（含云元数据 169.254.169.254）
        return True
    if a == 172 and 16 <= b <= 31:  # 172.16/12 私网
        return True
    if a == 192 and b == 168:  # 192.168/16 私网
        return True
    if a == 100 and 64 <= b <= 127:  # 100.64/10 CGNAT 共享（ipaddress 不覆盖，显式判）
        return True
    if a >= 224:  # 224/4 组播 + 240/4 保留（含 255.255.255.255 广播）
        return True
    return False


def _forbidden_ipv6(addr: ipaddress.IPv6Address) -> bool:
    # IPv4-mapped ::ffff:a.b.c.d → 按内嵌 IPv4 判（防 ::ffff:169.254.169.254 / ::ffff:100.64.x 绕过）。
    mapped = addr.ipv4_mapped
    if mapped is not None:
        return _forbidden_ipv4(mapped)
    if addr.is_unspecified or addr.is_loopback:  # :: / ::1
        return True
    b0, b1 = addr.packed[0], addr.packed[1]
    if (b0 & 0xFE) == 0xFC:  # fc00::/7 唯一本地地址(ULA)
        return True
    if b0 == 0xFE and (b1 & 0xC0) == 0x80:  # fe80::/10 链路本地
        return True
    if b0 == 0xFF:  # ff00::/8 组播
        return True
    return False


def is_forbidden_ip(text: str) -> bool:
    """字面 IP → 是否落在禁网段。无法判定的地址形状 fail-closed 拒（与 hub 同策）。"""
    addr = _parse_ip(text)
    if addr is None:
        return True
    if isinstance(addr, ipaddress.IPv4Address):
        return _forbidden_ipv4(addr)
    return _forbidden_ipv6(addr)


async def default_resolve(host: str) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, None)
    return [str(info[4][0]) for info in infos]


class GuardedTransport(httpx.AsyncBaseTransport):
    """校验并锁定解析 IP 的 httpx transport：字面 IP 直接判段；主机名解析全量校验后
    把连接 pin 到已校验 IP（改 URL host + sni_hostname），杜绝校验↔连接之间的 DNS 重绑。"""

    def __init__(self, inner: httpx.AsyncBaseTransport, resolver: AddressResolver) -> None:
        self._inner = inner
        self._resolve = resolver

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if _parse_ip(host) is not None:
            # 字面 IP（含 redirect 跳到 IP 的目标）：无 rebinding 面，直接判段。
            if is_forbidden_ip(host):
                raise EgressBlocked(f"目标地址 {host} 落在禁止网段")
            return await self._inner.handle_async_request(request)
        try:
            addresses = await self._resolve(host)
        except OSError as exc:
            raise EgressBlocked(f"目标主机 {host} 无法解析") from exc
        if not addresses:
            raise EgressBlocked(f"目标主机 {host} 无法解析")
        for address in addresses:
            if is_forbidden_ip(address):
                raise EgressBlocked(f"目标主机 {host} 解析到禁止网段")
        pinned = addresses[0]
        # 锁定到已校验 IP 连接：sni_hostname 保原主机名做 TLS SNI+证书校验；
        # Host 头在请求构造期已定为原主机名，改 URL host 不影响它。
        request.extensions = {**request.extensions, "sni_hostname": host}
        request.url = request.url.copy_with(host=pinned)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


def build_mcp_client_factory(
    mode: str, *, resolver: AddressResolver | None = None
) -> McpHttpClientFactory | None:
    """strict → 返回带 GuardedTransport + 禁 redirect 的 httpx client 工厂（供 adapter 注入）；
    off（或未知值以外的非 strict）→ 返回 None，adapter 用默认 client（无 egress 限制）。"""
    if mode != _STRICT:
        return None
    resolve = resolver if resolver is not None else default_resolve

    def factory(
        headers: dict[str, str] | None = None,
        timeout: httpx.Timeout | None = None,
        auth: httpx.Auth | None = None,
    ) -> httpx.AsyncClient:
        client_timeout = (
            timeout
            if timeout is not None
            else httpx.Timeout(_DEFAULT_TIMEOUT_S, read=_DEFAULT_SSE_READ_TIMEOUT_S)
        )
        return httpx.AsyncClient(
            transport=GuardedTransport(httpx.AsyncHTTPTransport(), resolve),
            timeout=client_timeout,
            headers=headers,
            auth=auth,
            follow_redirects=False,
        )

    return factory


def egress_mode_from_env(env: Mapping[str, str]) -> str:
    """缺省 strict；仅显式 off 关闭防线（未知值 fail-safe 归 strict）。"""
    return _OFF if env.get(_EGRESS_MODE_ENV, _STRICT).strip().lower() == _OFF else _STRICT


# 进程级 egress 模式（连接层策略）：启动期由 make_mcp_registry 从**注入 env** 配置一次
# （不读进程环境——env 单点纪律），build_connections 逐连接读取。缺省 strict：即便未配置
# 也是安全侧。连接是进程内单一策略，非 per-request，故 module-level 单点足够。
_egress_mode = _STRICT


def configure_egress_mode(mode: str) -> None:
    """由 worker 启动期（经注入 env）设定；非 off 一律归 strict（fail-safe）。"""
    global _egress_mode
    _egress_mode = _OFF if mode == _OFF else _STRICT


def current_egress_mode() -> str:
    return _egress_mode
