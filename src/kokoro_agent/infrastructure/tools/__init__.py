"""Kokoro 内置域工具注册表：deepagents 之外、随 worker 出厂的真实工具。"""

from __future__ import annotations

from collections.abc import Iterable

from langchain_core.tools import StructuredTool

from kokoro_agent.infrastructure.tools.clock import now
from kokoro_agent.infrastructure.tools.fetch import FETCH_MAX_CHARS, fetch_url

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

__all__ = ["BUILT_IN_TOOLS", "RESERVED_TOOL_NAMES", "assert_tool_names_allowed"]
