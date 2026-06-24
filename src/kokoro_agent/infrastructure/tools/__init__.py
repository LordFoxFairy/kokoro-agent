"""Kokoro 内置域工具注册表：deepagents 之外、随 worker 出厂的真实工具。"""

from __future__ import annotations

from collections.abc import Iterable

from langchain_core.tools import StructuredTool

from kokoro_agent.infrastructure.constants import (
    RUNTIME_SUBAGENT_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
    TODO_TOOL_NAME,
)
from kokoro_agent.infrastructure.tools.clock import NOW_TOOL
from kokoro_agent.infrastructure.tools.fetch import FETCH_URL_TOOL

# deepagents 内置文件/执行工具（其契约名，非本仓所有）。
_DEEPAGENTS_BUILTIN_TOOLS: frozenset[str] = frozenset(
    {"ls", "read_file", "write_file", "edit_file", "glob", "grep", "execute"}
)
# 保留名集合：工具名与之冲突会破坏 translator 的事件分发（deepagents 内置 + 本仓路由名 write_todos/task/agent）。
RESERVED_TOOL_NAMES: frozenset[str] = _DEEPAGENTS_BUILTIN_TOOLS | {
    TODO_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
    RUNTIME_SUBAGENT_TOOL_NAME,
}


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


BUILT_IN_TOOLS: list[StructuredTool] = [NOW_TOOL, FETCH_URL_TOOL]

assert_tool_names_allowed(tool.name for tool in BUILT_IN_TOOLS)

__all__ = ["BUILT_IN_TOOLS", "RESERVED_TOOL_NAMES", "assert_tool_names_allowed"]
