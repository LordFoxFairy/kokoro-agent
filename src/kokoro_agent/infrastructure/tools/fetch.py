"""内置工具：抓取 http/https 网页文本，带 SSRF 防护（拒绝内网/保留地址）。"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urljoin, urlparse

import httpx

FETCH_TIMEOUT_S = 10  # per-read 超时（两次字节读取之间）
FETCH_DEADLINE_S = 15  # 整体墙钟封顶，必须 > FETCH_TIMEOUT_S
FETCH_MAX_CHARS = 20_000
FETCH_MAX_BYTES = 64_000  # 字节硬上限，挡住单块解压尖峰（>20k 字符的 UTF-8 上界）
FETCH_MAX_REDIRECTS = 5

# RFC1918 私网 + IPv6 ULA。环回/链路本地/未指定走 ipaddress 标志位（更准）。
_PRIVATE_NETS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7")
)


def make_http_client() -> httpx.AsyncClient:
    # 手动逐跳跟随重定向，故每跳都能 host 复校验；identity 编码避免解压放大。
    return httpx.AsyncClient(
        timeout=httpx.Timeout(FETCH_TIMEOUT_S),
        follow_redirects=False,
        headers={"user-agent": "kokoro-agent/0.1", "accept-encoding": "identity"},
    )


def _resolve_ips(host: str) -> list[str]:
    return [str(info[4][0]) for info in socket.getaddrinfo(host, None)]


def _ip_is_internal(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    # 只拦真正危险目标（环回/链路本地含云 metadata/未指定/RFC1918），放过基准段免误伤 TUN 代理映射。
    if ip.is_loopback or ip.is_link_local or ip.is_unspecified or ip.is_multicast:
        return True
    return any(ip in net for net in _PRIVATE_NETS)


def _host_is_blocked(host: str) -> bool:
    # 按解析出的 IP（而非主机名）判内网，同时挡 DNS-rebinding；解析失败即拒（fail closed）。
    if not host:
        return True
    try:
        ips = _resolve_ips(host)
    except OSError:
        return True
    if not ips:
        return True
    for raw in ips:
        try:
            ip = ipaddress.ip_address(raw)
        except ValueError:
            return True
        if _ip_is_internal(ip):
            return True
    return False


async def _fetch_guarded(url: str) -> str:
    async with make_http_client() as client:
        for _ in range(FETCH_MAX_REDIRECTS + 1):
            if urlparse(url).scheme not in {"http", "https"}:
                return f"抓取失败：只支持 http/https，收到 {url!r}"
            host = urlparse(url).hostname or ""
            if await asyncio.to_thread(_host_is_blocked, host):
                return f"抓取失败：拒绝访问内网/保留地址 {host!r}"
            async with client.stream("GET", url) as response:
                if response.is_redirect:
                    url = urljoin(url, response.headers.get("location", ""))
                    continue
                response.raise_for_status()
                raw = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=FETCH_MAX_BYTES):
                    raw.extend(chunk)
                    if len(raw) >= FETCH_MAX_BYTES:
                        break
                text = raw.decode("utf-8", "replace")
            if len(text) > FETCH_MAX_CHARS:
                return f"{text[:FETCH_MAX_CHARS]}…（内容过长，已在 {FETCH_MAX_CHARS} 字符处截断）"
            return text
    return f"抓取失败：重定向超过 {FETCH_MAX_REDIRECTS} 跳"


async def fetch_url(url: str) -> str:
    try:
        async with asyncio.timeout(FETCH_DEADLINE_S):
            return await _fetch_guarded(url)
    except TimeoutError:
        # 墙钟超时以文本返回，避免阻塞整轮 run；TimeoutError 非 HTTPError，须先于其捕获。
        return f"抓取失败：超过 {FETCH_DEADLINE_S}s 墙钟超时"
    except httpx.HTTPError as error:
        # 工具错误以文本返回：模型可见并可改道，不中断 agent 循环。
        return f"抓取失败：{type(error).__name__}: {error}"
