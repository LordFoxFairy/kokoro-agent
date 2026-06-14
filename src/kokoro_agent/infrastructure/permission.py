from __future__ import annotations

from collections.abc import Sequence

from deepagents import FilesystemPermission
from langchain_core.tools import StructuredTool

from kokoro_agent.domain.run_request import PermissionMode

# 「需要拦截确认」的敏感工具集（显式可配置）：默认含外部网络工具 fetch_url。
# 这是更常见的模型——默认 auto 全自动，但把个别工具设为需拦截确认。要拦更多工具，往这里加名字。
# （deepagents 内部工具如 execute/write_file 的门控见 HITL spec follow-up。）
REQUIRES_APPROVAL: frozenset[str] = frozenset({"fetch_url"})

# plan（只读规划）额外拦截的执行类工具：运行时子代理 "agent"（避免规划态派活）。
_PLAN_ONLY_BLOCKED: frozenset[str] = frozenset({"agent"})


def blocked_tools(mode: PermissionMode) -> frozenset[str]:
    """该权限模式下被拦的工具集：auto 不拦 / default 拦敏感集 / plan 只读再加严。"""
    if mode == "auto":
        return frozenset()
    if mode == "plan":
        return REQUIRES_APPROVAL | _PLAN_ONLY_BLOCKED
    return REQUIRES_APPROVAL


def tool_allowed(mode: PermissionMode, tool_name: str) -> bool:
    return tool_name not in blocked_tools(mode)


def fs_permissions(mode: PermissionMode) -> list[FilesystemPermission]:
    """deepagents 内部文件系统工具的门控（经 create_deep_agent(permissions=)）：
    plan 只读——拦写操作 write_file/edit_file，读类(ls/read_file/glob/grep)放行；
    auto/default 不限文件系统（default 只拦外部网络 fetch_url，见 blocked_tools）。"""
    if mode == "plan":
        return [FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")]
    return []


def gate_tools(
    tools: Sequence[StructuredTool], mode: PermissionMode
) -> list[StructuredTool]:
    """按权限模式包装工具：被拦的工具执行时返回拦截结果（模型据此调整），不真正执行。"""
    if mode == "auto":
        return list(tools)
    return [tool if tool_allowed(mode, tool.name) else _gate(tool, mode) for tool in tools]


def _gate(tool: StructuredTool, mode: PermissionMode) -> StructuredTool:
    message = f"工具 {tool.name} 被 {mode} 权限模式拦截：需要更高的信任档位才能执行。"

    def blocked_sync(*_args: object, **_kwargs: object) -> str:
        return message

    async def blocked_async(*_args: object, **_kwargs: object) -> str:
        return message

    return StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]  # langchain from_function classmethod is partially typed
        name=tool.name,
        description=tool.description,
        args_schema=tool.args_schema,
        func=blocked_sync,
        coroutine=blocked_async,
        infer_schema=False,
    )
