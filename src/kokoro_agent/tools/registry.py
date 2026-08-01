"""工具集合治理：保留名/冲突断言 + runtime.tools 解析为可挂载工具，未知名 fail-loud。"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Final

from langchain_core.tools import StructuredTool

from kokoro_agent.contract import RunRequest
from kokoro_agent.platform import MediaOperationPort
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL
from kokoro_agent.tools.media import CREATE_IMAGE_TOOL_NAME, make_create_image_tool
from kokoro_agent.tools.propose_plan import PROPOSE_PLAN_TOOL, PROPOSE_PLAN_TOOL_NAME
from kokoro_agent.tools.web_fetch import WEB_FETCH_TOOL_NAME
from kokoro_agent.tools.web_search import WEB_SEARCH_TOOL_NAME

TODO_TOOL_NAME = "write_todos"  # deepagents 内置 TODO 工具
SUBAGENT_TOOL_NAME = "task"  # deepagents 子代理启动工具
EXECUTE_TOOL_NAME = "execute"  # deepagents 内置 shell 工具

# ADR-013 M0 hard-cut. These names remain recognized only so stale Platform/Session catalogs receive
# a stable fail-closed error rather than a generic unknown-tool error. Their legacy implementations
# are deliberately not imported by any production module.
LEGACY_STORE_MEMORY_TOOL_NAMES: Final[frozenset[str]] = frozenset(
    {"save_memory", "search_memory"}
)

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

KOKORO_TOOLS: Final[dict[str, StructuredTool]] = {
    ASK_USER_TOOL.name: ASK_USER_TOOL,
    PROPOSE_PLAN_TOOL.name: PROPOSE_PLAN_TOOL,
}
# 恒挂载核心工具：ask_user + propose_plan。联网工具是进程配置件，归
# tools/toolbox.py 的 ProcessToolbox，不入常量表。
CORE_TOOLS: Final[tuple[StructuredTool, ...]] = (ASK_USER_TOOL, PROPOSE_PLAN_TOOL)
ASSEMBLY_TOOL_NAMES: Final[frozenset[str]] = frozenset(
    {
        WEB_FETCH_TOOL_NAME,
        WEB_SEARCH_TOOL_NAME,
        CREATE_IMAGE_TOOL_NAME,
    }
)

assert_tool_names_allowed(KOKORO_TOOLS)

KNOWN_TOOL_NAMES: frozenset[str] = (
    frozenset(KOKORO_TOOLS) | ASSEMBLY_TOOL_NAMES | DEEPAGENTS_BUILTIN_TOOLS
)

# R3 tool effect journal 豁免表（一处维护）：这些工具不落 GA local journal。
#   ① 纯读工具（幂等，重执行天然收敛）：ls/read_file/glob/grep + web_fetch/web_search。
#   ② Command 形态 / 框架自重放工具（journal 无法短路 Command 结果，其重放归 langgraph checkpoint）：
#      write_todos（纯状态覆盖，幂等）、task（子代理委派，重放安全由子图 checkpoint 兜底）。
# 不在此集 = 按副作用工具走 journal 守门（write_file/edit_file/execute/deliver
# 及全部 MCP 工具——「MCP 一律按非幂等处理」，绝不入表）。
# create_image 是唯一 owner-journaled effect：重复进入会先按稳定 command ref 向 Platform
# recovery，只有 owner 明确 NOT_FOUND 才提交一次，故不得被 GA 的 started 行提前短路。
JOURNAL_EXEMPT_TOOLS: frozenset[str] = frozenset(
    {
        "ls",
        "read_file",
        "glob",
        "grep",
        WEB_FETCH_TOOL_NAME,
        WEB_SEARCH_TOOL_NAME,
        TODO_TOOL_NAME,
        SUBAGENT_TOOL_NAME,
        PROPOSE_PLAN_TOOL_NAME,
        # switch_persona：Command 形态纯状态覆盖，重放归 checkpoint 的
        # active_persona LastValue，故不双写 effect journal。
        "switch_persona",
        # Platform Media owns the effect journal. Re-entry must reach its stable command-ref
        # recovery path; a local `started` row would otherwise block reconciliation forever.
        CREATE_IMAGE_TOOL_NAME,
    }
)


def media_tools_for_run(
    request: RunRequest, port: MediaOperationPort | None
) -> tuple[StructuredTool, ...]:
    """Mount Media tools only at the intersection of catalog and opaque authority."""
    allowed = CREATE_IMAGE_TOOL_NAME in request.runtime.tools
    grant = request.runtime.media
    if not allowed:
        return ()
    if grant is None:
        raise ValueError("MEDIA_RUNTIME_GRANT_REQUIRED")
    if port is None:
        raise ValueError("MEDIA_RUNTIME_TRANSPORT_REQUIRED")
    return (
        make_create_image_tool(
            port,
            media_access_handle=grant.media_access_handle,
            media_projection_reservation_handle=grant.media_projection_reservation_handle,
        ),
    )


def resolve_tools(
    names: Sequence[str], *, core: Sequence[StructuredTool] = CORE_TOOLS
) -> list[StructuredTool]:
    """runtime.tools → 需显式挂载的 Kokoro 工具（deepagents 内置工具由框架自带）。"""
    validate_requested_tools(names)
    # core=类型包的基础工具面政策（对话型含 ask_user；无 chat 面的 studio 类型传空）。
    tools = list(core)
    tools.extend(
        KOKORO_TOOLS[name]
        for name in names
        if name in KOKORO_TOOLS and KOKORO_TOOLS[name] not in tuple(core)
    )
    return tools


def validate_requested_tools(names: Sequence[str]) -> None:
    """Fail before external resolution/allocation when a production catalog is stale or invalid."""
    legacy = sorted(set(names) & LEGACY_STORE_MEMORY_TOOL_NAMES)
    if legacy:
        raise ValueError(
            "LEGACY_STORE_MEMORY_TOOLS_DISABLED: ADR-013 M0 removed "
            f"store-backed tools from production catalogs: {legacy}"
        )
    unknown = sorted(set(names) - KNOWN_TOOL_NAMES)
    if unknown:
        raise ValueError(f"unknown tools in RuntimeConfig.tools: {unknown}")
    if len(set(names)) != len(names):
        raise ValueError("RuntimeConfig.tools contains duplicate names")
