"""web_fetch 底层工具：公网页面抓取 + 正文提取，SSRF 防御（零 vendor 依赖）。"""

from __future__ import annotations

import asyncio
import ipaddress
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

WEB_FETCH_TOOL_NAME = "web_fetch"

FETCH_MAX_CHARS = 24_000
_FETCH_MAX_BYTES = 1_000_000
_FETCH_TIMEOUT_S = 15.0
_MAX_REDIRECTS = 5


class WebFetchArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    url: str = Field(min_length=1)


async def _assert_public_target(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"unsafe fetch target (scheme/host): {url!r}")
    infos = await asyncio.get_running_loop().getaddrinfo(parts.hostname, None)
    for info in infos:
        address = ipaddress.ip_address(info[4][0])
        if (
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_reserved
            or address.is_multicast
            or address.is_unspecified
        ):
            raise ValueError(f"unsafe fetch target (non-public address): {url!r}")


def _extract_text(content_type: str, body: str) -> str:
    if "html" in content_type:
        soup = BeautifulSoup(body, "html.parser")
        for node in soup(["script", "style", "noscript"]):
            node.decompose()
        return " ".join(soup.get_text(separator=" ").split())
    return body


def _clip(text: str) -> str:
    if len(text) <= FETCH_MAX_CHARS:
        return text
    return f"{text[:FETCH_MAX_CHARS]}…[truncated {len(text) - FETCH_MAX_CHARS} chars]"


def make_web_fetch_tool(*, allow_private: bool = False) -> StructuredTool:
    """allow_private 仅供本地开发/测试放行内网（如 fake-IP 代理环境）；生产默认拒。"""

    async def web_fetch(url: str) -> str:
        # 重定向手动跟随并逐跳复检；残余 TOCTOU（解析↔连接间 DNS 重绑）V1 接受。
        target = url
        async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT_S, follow_redirects=False) as client:
            for _ in range(_MAX_REDIRECTS + 1):
                if not allow_private:
                    await _assert_public_target(target)
                elif urlsplit(target).scheme not in ("http", "https"):
                    raise ValueError(f"unsafe fetch target (scheme): {target!r}")
                # 流式读取边读边封顶：非流式 .content 会先吞下完整 body（任意大 → OOM 面）。
                async with client.stream("GET", target) as response:
                    if response.is_redirect:
                        location = response.headers.get("location", "")
                        target = str(httpx.URL(target).join(location))
                        continue
                    response.raise_for_status()
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        chunks.append(chunk)
                        size += len(chunk)
                        if size >= _FETCH_MAX_BYTES:
                            break
                    body = b"".join(chunks)[:_FETCH_MAX_BYTES].decode(
                        response.charset_encoding or "utf-8", errors="replace"
                    )
                return _clip(_extract_text(response.headers.get("content-type", ""), body))
        raise ValueError(f"too many redirects fetching {url!r}")

    return StructuredTool(
        name=WEB_FETCH_TOOL_NAME,
        description="Fetch a public web page and return its readable text content.",
        args_schema=WebFetchArgs,
        coroutine=web_fetch,
    )
