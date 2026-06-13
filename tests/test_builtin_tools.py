from __future__ import annotations

from datetime import datetime

import httpx
import pytest
from _pytest.monkeypatch import MonkeyPatch

from kokoro_agent.infrastructure import builtin_tools
from kokoro_agent.infrastructure.builtin_tools import (
    BUILT_IN_TOOLS,
    FETCH_MAX_CHARS,
    RESERVED_TOOL_NAMES,
    assert_tool_names_allowed,
    fetch_url,
    now,
)


# --- registry guard -----------------------------------------------------------


def test_built_in_tools_expose_now_and_fetch_url() -> None:
    assert {tool.name for tool in BUILT_IN_TOOLS} == {"now", "fetch_url"}


@pytest.mark.parametrize("name", ["write_todos", "task", "agent", "read_file", "execute"])
def test_reserved_tool_name_is_rejected(name: str) -> None:
    # 撞 deepagents 内置/专用工具名会让事件族错乱（todo/subagent 路由按名字分发）。
    with pytest.raises(ValueError):
        assert_tool_names_allowed([name])


def test_duplicate_tool_name_is_rejected() -> None:
    with pytest.raises(ValueError):
        assert_tool_names_allowed(["now", "now"])


def test_registry_names_pass_their_own_guard() -> None:
    assert_tool_names_allowed([tool.name for tool in BUILT_IN_TOOLS])
    assert RESERVED_TOOL_NAMES.isdisjoint({tool.name for tool in BUILT_IN_TOOLS})


# --- now ------------------------------------------------------------------------


def test_now_returns_timezone_aware_iso() -> None:
    moment = datetime.fromisoformat(now())
    assert moment.tzinfo is not None


# --- fetch_url boundary matrix ---------------------------------------------------


def _client_with(handler: httpx.MockTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=handler)


@pytest.mark.parametrize(
    "url",
    ["file:///etc/passwd", "ftp://x/y", "javascript:alert(1)", "", "not-a-url"],
)
async def test_fetch_url_rejects_non_http_schemes(url: str) -> None:
    result = await fetch_url(url)
    assert "http" in result and "失败" in result or "只支持" in result


async def test_fetch_url_returns_body_text(monkeypatch: MonkeyPatch) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="hello kokoro")
    )
    monkeypatch.setattr(builtin_tools, "make_http_client", lambda: _client_with(transport))
    assert await fetch_url("https://example.com") == "hello kokoro"


async def test_fetch_url_truncates_huge_bodies(monkeypatch: MonkeyPatch) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="x" * (FETCH_MAX_CHARS * 3))
    )
    monkeypatch.setattr(builtin_tools, "make_http_client", lambda: _client_with(transport))
    result = await fetch_url("https://example.com")
    # 下载被截断且带标记，绝不把 60k 字符灌进事件流/模型上下文。
    assert len(result) <= FETCH_MAX_CHARS + 50
    assert "截断" in result


async def test_fetch_url_surfaces_http_errors_as_text(monkeypatch: MonkeyPatch) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(503))
    monkeypatch.setattr(builtin_tools, "make_http_client", lambda: _client_with(transport))
    result = await fetch_url("https://example.com")
    assert "失败" in result and "503" in result


async def test_fetch_url_surfaces_connect_errors_as_text(monkeypatch: MonkeyPatch) -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(
        builtin_tools, "make_http_client", lambda: _client_with(httpx.MockTransport(boom))
    )
    result = await fetch_url("https://example.com")
    # 工具错误以文本返回（模型可见、循环不死），绝不向上抛异常。
    assert "失败" in result
