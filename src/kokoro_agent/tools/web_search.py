"""web_search 底层工具：通用检索原语；provider 经协议注入，本模块零 vendor 代码。"""

from __future__ import annotations

from typing import Protocol

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

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
