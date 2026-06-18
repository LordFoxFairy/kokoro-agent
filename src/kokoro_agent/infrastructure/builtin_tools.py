"""Kokoro 内置域工具注册表：deepagents 之外、随 worker 出厂的真实工具。"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Iterable
from datetime import datetime
from urllib.parse import urljoin, urlparse

import httpx
from langchain_core.tools import StructuredTool

# deepagents 内置文件/规划/执行工具 + 本仓事件路由名（task/agent 由 stream_events.translate_stream_event 按名分发），撞名即事件族错乱。
RESERVED_TOOL_NAMES: frozenset[str] = frozenset(
    {
        "write_todos",
        "ls",
        "read_file",
        "write_file",
        "edit_file",
        "glob",
        "grep",
        "execute",
        "task",  # kokoro 路由名：子智能体
        "agent",  # kokoro 路由名：运行时自定义子智能体
    }
)

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


def assert_tool_names_allowed(names: Iterable[str]) -> None:
    seen: set[str] = set()
    for name in names:
        if name in RESERVED_TOOL_NAMES:
            msg = f"tool name {name!r} collides with a reserved deepagents/router name"
            raise ValueError(msg)
        if name in seen:
            msg = f"duplicate tool name {name!r}"
            raise ValueError(msg)
        seen.add(name)


def now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


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
    # 只拦真正危险的内部目标：环回 / 链路本地（含云 metadata 169.254.169.254）/ 未指定 / RFC1918。
    # 不拦 198.18.0.0/15 等基准段——TUN 代理把公网域名映射到该段，宽泛 is_private/is_reserved 会废掉抓取。
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
        # 慢速 drip：墙钟封顶以文本返回，不饿死整轮 run。TimeoutError 不是 HTTPError，须先于它捕获。
        return f"抓取失败：超过 {FETCH_DEADLINE_S}s 墙钟超时"
    except httpx.HTTPError as error:
        # 工具错误以文本返回：模型可见、可改道，agent 循环不死。
        return f"抓取失败：{type(error).__name__}: {error}"


BUILT_IN_TOOLS: list[StructuredTool] = [
    StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]  # langchain from_function classmethod is partially typed
        func=now,
        name="now",
        description="获取当前本地日期时间（ISO-8601，含时区）。涉及“今天/现在/几点”等时间问题时使用。",
    ),
    StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]  # langchain from_function classmethod is partially typed
        coroutine=fetch_url,
        name="fetch_url",
        description=f"抓取一个 http/https 网页并返回其文本内容（最长 {FETCH_MAX_CHARS} 字符，拒绝内网地址）。需要查看网页实际内容时使用。",
    ),
]

assert_tool_names_allowed(tool.name for tool in BUILT_IN_TOOLS)
