"""HITL interrupt_on 构造：审批工具集合每请求经 RuntimeConfig.permissions 注入。"""

from __future__ import annotations

from langchain.agents.middleware import InterruptOnConfig
from langchain.agents.middleware.human_in_the_loop import DecisionType

from kokoro_agent.tools.ask_user import ASK_USER_TOOL_NAME

# ask_user 是语义暂停点：只允许人工作答，不参与 approve/edit/reject。
_ASK_USER_DECISIONS: list[DecisionType] = ["respond"]
# 普通工具审批：放行 / 改参放行 / 拒绝；respond 不适用（非 ask_user 用 respond 是契约违例）。
_APPROVAL_DECISIONS: list[DecisionType] = ["approve", "edit", "reject"]


def build_interrupt_on(approval_tools: frozenset[str]) -> dict[str, InterruptOnConfig]:
    """ask_user 恒暂停（respond）；approval_tools 各挂 approve/edit/reject 门控。"""
    interrupt_on: dict[str, InterruptOnConfig] = {
        ASK_USER_TOOL_NAME: InterruptOnConfig(allowed_decisions=_ASK_USER_DECISIONS)
    }
    interrupt_on.update(
        {
            tool: InterruptOnConfig(allowed_decisions=_APPROVAL_DECISIONS)
            for tool in approval_tools
            if tool != ASK_USER_TOOL_NAME
        }
    )
    return interrupt_on
