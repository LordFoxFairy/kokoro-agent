"""权限规则：按权限档位推导需交互审批的工具集。"""

from __future__ import annotations

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.infrastructure.permission.policy import approval_policy


def blocked_tools(mode: PermissionMode) -> frozenset[str]:
    """该权限模式下需交互审批的工具集：auto 不拦 / default 拦敏感集。"""
    policy = approval_policy()
    match mode:
        case "auto":
            return frozenset()
        case "default":
            return policy.requires_approval_tools


def tool_allowed(mode: PermissionMode, tool_name: str) -> bool:
    return tool_name not in blocked_tools(mode)
