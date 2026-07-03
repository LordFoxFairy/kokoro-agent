"""底层 web 工具：fetch（恒挂载，SSRF 防御）与 search（provider 注入，配置即挂载）。"""

from __future__ import annotations

import asyncio
import ipaddress
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

WEB_FETCH_TOOL_NAME = "web_fetch"
WEB_SEARCH_TOOL_NAME = "web_search"

FETCH_MAX_CHARS = 24_000
_FETCH_MAX_BYTES = 1_000_000
_FETCH_TIMEOUT_S = 15.0
_MAX_REDIRECTS = 5
_SEARCH_COUNT = 5


class WebFetchArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    url: str = Field(min_length=1)


class WebSearchArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    query: str = Field(min_length=1)


class SearchHit(BaseModel):
    model_config = ConfigDict(strict=False, extra="ignore")

    title: str = ""
    url: str
    snippet: str = ""


SearchFn = Callable[[str, int], Awaitable[list[SearchHit]]]


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
    """allow_private 仅供本地开发/测试放行内网；生产默认拒（worker 是服务端进程）。"""

    async def web_fetch(url: str) -> str:
        # 重定向手动跟随并逐跳复检；残余 TOCTOU（解析↔连接间 DNS 重绑）V1 接受。
        target = url
        async with httpx.AsyncClient(
            timeout=_FETCH_TIMEOUT_S, follow_redirects=False
        ) as client:
            for _ in range(_MAX_REDIRECTS + 1):
                if not allow_private:
                    await _assert_public_target(target)
                elif urlsplit(target).scheme not in ("http", "https"):
                    raise ValueError(f"unsafe fetch target (scheme): {target!r}")
                response = await client.get(target)
                if response.is_redirect:
                    location = response.headers.get("location", "")
                    target = str(httpx.URL(target).join(location))
                    continue
                response.raise_for_status()
                body = response.content[:_FETCH_MAX_BYTES].decode(
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


def make_web_search_tool(search: SearchFn) -> StructuredTool:
    async def web_search(query: str) -> str:
        hits = await search(query, _SEARCH_COUNT)
        if not hits:
            return "no results"
        return "\n".join(
            f"- {hit.title or hit.url} — {hit.url}\n  {hit.snippet}".rstrip() for hit in hits
        )

    return StructuredTool(
        name=WEB_SEARCH_TOOL_NAME,
        description="Search the web and return result titles, urls and snippets.",
        args_schema=WebSearchArgs,
        coroutine=web_search,
    )


# --- zhipu provider（open.bigmodel.cn；错误体 fail-loud 透传） ---

_ZHIPU_SEARCH_URL = "https://open.bigmodel.cn/api/paas/v4/web_search"
_RAW_RESULTS: TypeAdapter[list[dict[str, object]]] = TypeAdapter(list[dict[str, object]])


def parse_zhipu_response(raw: dict[str, object]) -> list[SearchHit]:
    results = _RAW_RESULTS.validate_python(raw.get("search_result") or [])
    hits: list[SearchHit] = []
    for item in results:
        url = str(item.get("link") or item.get("url") or "")
        if not url:
            continue
        hits.append(
            SearchHit(
                title=str(item.get("title") or ""),
                url=url,
                snippet=str(item.get("content") or ""),
            )
        )
    return hits


def make_zhipu_search(api_key: str) -> SearchFn:
    async def search(query: str, count: int) -> list[SearchHit]:
        async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT_S) as client:
            response = await client.post(
                _ZHIPU_SEARCH_URL,
                json={"search_query": query, "search_engine": "search_std", "count": count},
                headers={"authorization": f"Bearer {api_key}"},
            )
            if response.status_code != 200:
                raise ValueError(f"zhipu web_search failed: {response.status_code} {response.text[:200]}")
            return parse_zhipu_response(response.json())

    return search
