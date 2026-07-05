"""工具集合治理：保留名/冲突断言 + runtime.tools 解析为可挂载工具，未知名 fail-loud。"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Final

from langchain_core.tools import StructuredTool

from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL
from kokoro_agent.tools.memory import SAVE_MEMORY_TOOL_NAME, SEARCH_MEMORY_TOOL_NAME
from kokoro_agent.tools.web_fetch import WEB_FETCH_TOOL_NAME
from kokoro_agent.tools.web_search import WEB_SEARCH_TOOL_NAME

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


def assert_tool_names_allowed(names: Iterable[str]) -> None:
    seen: set[str] = set()
    for name in names:
        if name in RESERVED_TOOL_NAMES:
            raise ValueError(f"tool name {name!r} collides with a reserved deepagents/router name")
        if name in seen:
            raise ValueError(f"duplicate tool name {name!r}")
        seen.add(name)

KOKORO_TOOLS: Final[dict[str, StructuredTool]] = {ASK_USER_TOOL.name: ASK_USER_TOOL}
# 恒挂载核心工具：ask_user（handbook 12 号）。记忆/联网工具是带参内置件
# （租户 scope / 进程配置），归 tools/toolbox.py 的 ProcessToolbox，不入常量表。
CORE_TOOLS: Final[tuple[StructuredTool, ...]] = (ASK_USER_TOOL,)
ASSEMBLY_TOOL_NAMES: Final[frozenset[str]] = frozenset(
    {SAVE_MEMORY_TOOL_NAME, SEARCH_MEMORY_TOOL_NAME, WEB_FETCH_TOOL_NAME, WEB_SEARCH_TOOL_NAME}
)

assert_tool_names_allowed(KOKORO_TOOLS)

KNOWN_TOOL_NAMES: frozenset[str] = (
    frozenset(KOKORO_TOOLS) | ASSEMBLY_TOOL_NAMES | DEEPAGENTS_BUILTIN_TOOLS
)


def resolve_tools(
    names: Sequence[str], *, core: Sequence[StructuredTool] = CORE_TOOLS
) -> list[StructuredTool]:
    """runtime.tools → 需显式挂载的 Kokoro 工具（deepagents 内置工具由框架自带）。"""
    unknown = sorted(set(names) - KNOWN_TOOL_NAMES)
    if unknown:
        raise ValueError(f"unknown tools in RuntimeConfig.tools: {unknown}")
    if len(set(names)) != len(names):
        raise ValueError("RuntimeConfig.tools contains duplicate names")
    # core=类型包的基础工具面政策（对话型含 ask_user；无 chat 面的 studio 类型传空）。
    tools = list(core)
    tools.extend(
        KOKORO_TOOLS[name]
        for name in names
        if name in KOKORO_TOOLS and KOKORO_TOOLS[name] not in tuple(core)
    )
    return tools
