"""原生 HITL interrupt_on 配置构造：按权限模式生成工具级审批映射。"""

from langchain.agents.middleware import InterruptOnConfig
from langchain.agents.middleware.human_in_the_loop import DecisionType

from kokoro_agent.config import AppConfig
from kokoro_agent.run.request import PermissionMode
from kokoro_agent.sandbox.policy import build_filesystem_permissions
from kokoro_agent.tools.names import ASK_USER_TOOL_NAME

_APPROVAL_DECISIONS: list[DecisionType] = ["approve", "edit", "reject"]
_ASK_USER_DECISIONS: list[DecisionType] = ["respond"]


def build_interrupt_on(mode: PermissionMode) -> dict[str, InterruptOnConfig]:
    interrupt_on: dict[str, InterruptOnConfig] = {
        ASK_USER_TOOL_NAME: InterruptOnConfig(allowed_decisions=_ASK_USER_DECISIONS)
    }
    # auto 模式无需人工审批，但 ask_user 是语义暂停点，必须始终走 respond。
    if mode == "auto":
        return interrupt_on
    policy = AppConfig.from_env().approval
    interrupt_on.update(
        {
            tool: InterruptOnConfig(allowed_decisions=_APPROVAL_DECISIONS)
            for tool in policy.requires_approval_tools
        }
    )
    return interrupt_on


__all__ = ["build_filesystem_permissions", "build_interrupt_on"]
