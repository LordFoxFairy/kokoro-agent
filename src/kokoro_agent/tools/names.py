"""工具名常量单点：deepagents 保留名、Kokoro 自有名与 MCP 命名规则。"""

from __future__ import annotations

from collections.abc import Iterable

ASK_USER_TOOL_NAME = "ask_user"
TODO_TOOL_NAME = "write_todos"  # deepagents 内置 TODO 工具
SUBAGENT_TOOL_NAME = "task"  # deepagents 子代理启动工具
EXECUTE_TOOL_NAME = "execute"  # deepagents 内置 shell 工具

# deepagents 内置文件/执行工具（其契约名，非本仓所有）。
DEEPAGENTS_BUILTIN_TOOLS: frozenset[str] = frozenset(
    {"ls", "read_file", "write_file", "edit_file", "glob", "grep", EXECUTE_TOOL_NAME}
)
# 保留名集合：与之冲突会破坏事件投影的通道分发。
RESERVED_TOOL_NAMES: frozenset[str] = DEEPAGENTS_BUILTIN_TOOLS | {
    TODO_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
}


def mcp_tool_name(server: str, tool: str) -> str:
    return f"mcp__{server}__{tool}"


def assert_tool_names_allowed(names: Iterable[str]) -> None:
    seen: set[str] = set()
    for name in names:
        if name in RESERVED_TOOL_NAMES:
            raise ValueError(f"tool name {name!r} collides with a reserved deepagents/router name")
        if name in seen:
            raise ValueError(f"duplicate tool name {name!r}")
        seen.add(name)
