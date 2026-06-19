"""权限规则：按权限档位推导被拦工具集与文件系统门控。"""

from __future__ import annotations

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.infrastructure.agent_adapter import FilesystemPermission
from kokoro_agent.infrastructure.permission.policy import approval_policy


def blocked_tools(mode: PermissionMode) -> frozenset[str]:
    """该权限模式下被拦的工具集：auto 不拦 / default 拦敏感集 / plan 只读再加严。"""
    policy = approval_policy()
    match mode:
        case "auto":
            return frozenset()
        case "plan":
            return policy.requires_approval_tools | policy.plan_only_blocked_tools
        case _:
            return policy.requires_approval_tools


def tool_allowed(mode: PermissionMode, tool_name: str) -> bool:
    return tool_name not in blocked_tools(mode)


def fs_permissions(mode: PermissionMode) -> list[FilesystemPermission]:
    """deepagents 内部文件系统工具的门控（经 create_deep_agent(permissions=)）：
    plan 只读——拦写操作 write_file/edit_file，读类(ls/read_file/glob/grep)放行；
    auto/default 不限文件系统（default 只拦外部网络 fetch_url，见 blocked_tools）。"""
    if mode == "plan":
        return [FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")]
    return []
