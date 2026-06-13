"""Kokoro 内置域工具注册表：deepagents 之外、随 worker 出厂的真实工具。"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from urllib.parse import urlparse

import httpx
from langchain_core.tools import StructuredTool

# deepagents 内置 + 本仓专用路由名（todo/subagent 事件族按名字分发），撞名即错乱。
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
        "task",
        "agent",
    }
)

FETCH_TIMEOUT_S = 10
FETCH_MAX_CHARS = 20_000


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
    return httpx.AsyncClient(
        timeout=httpx.Timeout(FETCH_TIMEOUT_S),
        follow_redirects=True,
        headers={"user-agent": "kokoro-agent/0.1"},
    )


async def fetch_url(url: str) -> str:
    scheme = urlparse(url).scheme
    if scheme not in {"http", "https"}:
        return f"抓取失败：只支持 http/https，收到 {url!r}"

    try:
        async with make_http_client() as client, client.stream("GET", url) as response:
            response.raise_for_status()
            chunks: list[str] = []
            total = 0
            async for chunk in response.aiter_text():
                chunks.append(chunk)
                total += len(chunk)
                if total >= FETCH_MAX_CHARS:
                    break
            text = "".join(chunks)
    except httpx.HTTPError as error:
        # 工具错误以文本返回：模型可见、可改道，agent 循环不死。
        return f"抓取失败：{type(error).__name__}: {error}"

    if len(text) > FETCH_MAX_CHARS:
        return f"{text[:FETCH_MAX_CHARS]}…（内容过长，已在 {FETCH_MAX_CHARS} 字符处截断）"
    return text


def _fetch_url_sync(url: str) -> str:
    msg = "fetch_url requires async execution"
    raise RuntimeError(msg)


BUILT_IN_TOOLS: list[StructuredTool] = [
    StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]  # langchain from_function classmethod is partially typed
        func=now,
        name="now",
        description="获取当前本地日期时间（ISO-8601，含时区）。涉及“今天/现在/几点”等时间问题时使用。",
    ),
    StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]  # langchain from_function classmethod is partially typed
        func=_fetch_url_sync,
        coroutine=fetch_url,
        name="fetch_url",
        description=f"抓取一个 http/https 网页并返回其文本内容（最长 {FETCH_MAX_CHARS} 字符）。需要查看网页实际内容时使用。",
    ),
]

assert_tool_names_allowed(tool.name for tool in BUILT_IN_TOOLS)
