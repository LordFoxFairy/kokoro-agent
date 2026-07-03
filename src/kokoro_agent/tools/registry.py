"""Kokoro 自有工具注册：runtime.tools 名单在此解析为可挂载工具，未知名 fail-loud。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from langchain_core.tools import StructuredTool

from kokoro_agent.tools.ask_user import ASK_USER_TOOL
from kokoro_agent.tools.names import (
    DEEPAGENTS_BUILTIN_TOOLS,
    assert_tool_names_allowed,
)

KOKORO_TOOLS: Final[dict[str, StructuredTool]] = {ASK_USER_TOOL.name: ASK_USER_TOOL}

assert_tool_names_allowed(KOKORO_TOOLS)

KNOWN_TOOL_NAMES: frozenset[str] = frozenset(KOKORO_TOOLS) | DEEPAGENTS_BUILTIN_TOOLS


def resolve_tools(names: Sequence[str]) -> list[StructuredTool]:
    """runtime.tools → 需显式挂载的 Kokoro 工具（deepagents 内置工具由框架自带）。"""
    unknown = sorted(set(names) - KNOWN_TOOL_NAMES)
    if unknown:
        raise ValueError(f"unknown tools in RuntimeConfig.tools: {unknown}")
    if len(set(names)) != len(names):
        raise ValueError("RuntimeConfig.tools contains duplicate names")
    # ask_user 是 Kokoro 默认工具（handbook 12 号）：恒挂载，不依赖名单。
    tools = [ASK_USER_TOOL]
    tools.extend(
        KOKORO_TOOLS[name]
        for name in names
        if name in KOKORO_TOOLS and KOKORO_TOOLS[name] is not ASK_USER_TOOL
    )
    return tools
