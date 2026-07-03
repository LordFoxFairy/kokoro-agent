"""web_search 底层工具：上半部=通用检索原语（协议注入）；下半部=provider 适配器注册表。"""

from __future__ import annotations

from typing import Final, Protocol

import httpx
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field, SecretStr, TypeAdapter

WEB_SEARCH_TOOL_NAME = "web_search"
_SEARCH_COUNT = 5


class SearchHit(BaseModel):
    model_config = ConfigDict(strict=False, extra="ignore")

    title: str = ""
    url: str
    snippet: str = ""


class SearchProvider(Protocol):
    async def search(self, query: str, count: int) -> list[SearchHit]: ...


class WebSearchArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    query: str = Field(min_length=1)


def make_web_search_tool(provider: SearchProvider) -> StructuredTool:
    async def web_search(query: str) -> str:
        hits = await provider.search(query, _SEARCH_COUNT)
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


# --- provider 适配器（tavily / searxng / zhipu）：工具原语只见 SearchProvider 协议 ---

_TIMEOUT_S = 15.0
_RAW_RESULTS: TypeAdapter[list[dict[str, object]]] = TypeAdapter(list[dict[str, object]])
_RAW_BODY: TypeAdapter[dict[str, object]] = TypeAdapter(dict[str, object])


class SearchProviderSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    provider: str
    api_key: SecretStr | None
    # searxng 实例地址（必填）；其他 provider 可留空用官方端点。
    base_url: str | None


def parse_hits(results: object, *, url_keys: tuple[str, ...] = ("url", "link")) -> list[SearchHit]:
    hits: list[SearchHit] = []
    for item in _RAW_RESULTS.validate_python(results or []):
        url = next((str(item[k]) for k in url_keys if item.get(k)), "")
        if not url:
            continue
        hits.append(
            SearchHit(
                title=str(item.get("title") or ""),
                url=url,
                snippet=str(item.get("content") or item.get("snippet") or ""),
            )
        )
    return hits


async def _request_json(request: httpx.Request) -> dict[str, object]:
    async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
        response = await client.send(request)
        if response.status_code != 200:
            raise ValueError(
                f"web search provider failed: {response.status_code} {response.text[:200]}"
            )
        raw: object = response.json()
        return _RAW_BODY.validate_python(raw)


class TavilySearch:
    def __init__(self, api_key: str, base_url: str | None) -> None:
        self._api_key = api_key
        self._url = base_url or "https://api.tavily.com/search"

    async def search(self, query: str, count: int) -> list[SearchHit]:
        body = await _request_json(
            httpx.Request(
                "POST",
                self._url,
                json={"query": query, "max_results": count},
                headers={"authorization": f"Bearer {self._api_key}"},
            )
        )
        return parse_hits(body.get("results"))


class SearxngSearch:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    async def search(self, query: str, count: int) -> list[SearchHit]:
        body = await _request_json(
            httpx.Request(
                "GET", f"{self._base_url}/search", params={"q": query, "format": "json"}
            )
        )
        return parse_hits(body.get("results"))[:count]


class ZhipuSearch:
    def __init__(self, api_key: str, base_url: str | None) -> None:
        self._api_key = api_key
        self._url = base_url or "https://open.bigmodel.cn/api/paas/v4/web_search"

    async def search(self, query: str, count: int) -> list[SearchHit]:
        body = await _request_json(
            httpx.Request(
                "POST",
                self._url,
                json={"search_query": query, "search_engine": "search_std", "count": count},
                headers={"authorization": f"Bearer {self._api_key}"},
            )
        )
        return parse_hits(body.get("search_result"))


SUPPORTED_SEARCH_PROVIDERS: Final[frozenset[str]] = frozenset({"tavily", "searxng", "zhipu"})


def make_search_provider(settings: SearchProviderSettings) -> SearchProvider:
    name = settings.provider
    if name not in SUPPORTED_SEARCH_PROVIDERS:
        raise ValueError(
            f"unsupported web search provider {name!r}: choose from {sorted(SUPPORTED_SEARCH_PROVIDERS)}"
        )
    if name == "searxng":
        if not settings.base_url:
            raise ValueError("searxng provider requires KOKORO_WEB_SEARCH_URL")
        return SearxngSearch(settings.base_url)
    if settings.api_key is None:
        raise ValueError(f"{name} provider requires KOKORO_WEB_SEARCH_API_KEY")
    key = settings.api_key.get_secret_value()
    if name == "tavily":
        return TavilySearch(key, settings.base_url)
    return ZhipuSearch(key, settings.base_url)
