"""web_search 规格：工具层零 vendor；provider 注册表矩阵与三家响应解析边界。"""

from __future__ import annotations

import pytest
from langchain_core.tools import StructuredTool
from pydantic import SecretStr

from kokoro_agent.tools.web_search import (
    SUPPORTED_SEARCH_PROVIDERS,
    SearchHit,
    SearchProvider,
    SearchProviderSettings,
    SearxngSearch,
    TavilySearch,
    ZhipuSearch,
    make_search_provider,
    make_web_search_tool,
    parse_hits,
)


def _coro(tool: StructuredTool):
    assert tool.coroutine is not None
    return tool.coroutine


class _FakeProvider:
    def __init__(self, hits: list[SearchHit]) -> None:
        self._hits = hits
        self.seen: list[tuple[str, int]] = []

    async def search(self, query: str, count: int) -> list[SearchHit]:
        self.seen.append((query, count))
        return self._hits


async def test_search_tool_formats_hits() -> None:
    provider = _FakeProvider([SearchHit(title="T1", url="https://a", snippet="S1")])
    out = await _coro(make_web_search_tool(provider))(query="python")
    assert "T1" in out and "https://a" in out and "S1" in out
    assert provider.seen == [("python", 5)]


async def test_search_tool_reports_empty() -> None:
    out = await _coro(make_web_search_tool(_FakeProvider([])))(query="python")
    assert "no results" in out


def test_tool_primitive_is_vendor_free() -> None:
    # 工具原语（tool 工厂/协议/命中模型）不得含 vendor 词汇：适配器同文件但不许倒灌。
    import inspect

    section = "".join(
        inspect.getsource(obj) for obj in (make_web_search_tool, SearchProvider, SearchHit)
    ).lower()
    for vendor in ("zhipu", "tavily", "searxng", "bigmodel"):
        assert vendor not in section


@pytest.mark.parametrize(
    ("raw", "expected_urls"),
    [
        ([], []),
        (None, []),
        ([{"title": "T", "url": "https://a", "content": "C"}], ["https://a"]),
        ([{"title": "T", "link": "https://b"}], ["https://b"]),  # link 别名（zhipu 形）
        ([{"title": "T", "url": "https://c", "snippet": "S"}], ["https://c"]),  # snippet 别名
        ([{"no_url": 1}], []),  # 无 url 脏条目剔除
    ],
)
def test_parse_hits_boundary_matrix(raw: object, expected_urls: list[str]) -> None:
    hits = parse_hits(raw)
    assert [hit.url for hit in hits] == expected_urls


def _settings(provider: str, *, key: str | None = "k", url: str | None = None) -> SearchProviderSettings:
    return SearchProviderSettings(
        provider=provider, api_key=None if key is None else SecretStr(key), base_url=url
    )


def test_provider_registry_matrix() -> None:
    assert isinstance(make_search_provider(_settings("tavily")), TavilySearch)
    assert isinstance(make_search_provider(_settings("zhipu")), ZhipuSearch)
    assert isinstance(
        make_search_provider(_settings("searxng", url="https://searx.local")), SearxngSearch
    )
    assert SUPPORTED_SEARCH_PROVIDERS == {"tavily", "searxng", "zhipu"}


@pytest.mark.parametrize(
    "settings",
    [
        _settings("bing"),  # 未知 provider
        _settings("tavily", key=None),  # 缺 key
        _settings("zhipu", key=None),
        _settings("searxng", url=None),  # searxng 缺实例地址
    ],
)
def test_provider_registry_fails_loud(settings: SearchProviderSettings) -> None:
    with pytest.raises(ValueError):
        make_search_provider(settings)
