"""原生 HITL interrupt_on 配置构造：按权限模式生成工具级审批映射。"""

from langchain.agents.middleware import InterruptOnConfig
from langchain.agents.middleware.human_in_the_loop import DecisionType

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.infrastructure.config import AppConfig

_APPROVAL_DECISIONS: list[DecisionType] = ["approve", "edit", "reject", "respond"]


def build_interrupt_on(mode: PermissionMode) -> dict[str, InterruptOnConfig]:
    # auto 模式无需人工审批，返回空映射让 HumanInTheLoopMiddleware 跳过所有工具。
    if mode == "auto":
        return {}
    policy = AppConfig.from_env().approval
    return {
        tool: InterruptOnConfig(allowed_decisions=_APPROVAL_DECISIONS)
        for tool in policy.requires_approval_tools
    }
