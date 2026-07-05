"""HITL interrupt_on 构造：审批工具集合每请求经 RuntimeConfig.permissions 注入。"""

from __future__ import annotations

from langchain.agents.middleware import InterruptOnConfig
from langchain.agents.middleware.human_in_the_loop import DecisionType

from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME

# ask_user 是语义暂停点：只允许人工作答，不参与 approve/edit/reject。
_ASK_USER_DECISIONS: list[DecisionType] = ["respond"]
# 普通工具审批：放行 / 改参放行 / 拒绝；respond 不适用（非 ask_user 用 respond 是契约违例）。
_APPROVAL_DECISIONS: list[DecisionType] = ["approve", "edit", "reject"]


def build_interrupt_on(
    approval_tools: frozenset[str],
    *,
    subagent_create: str = "deny",
    pause_tools: frozenset[str] = frozenset({ASK_USER_TOOL_NAME}),
) -> dict[str, InterruptOnConfig]:
    """pause_tools=类型包的 respond 语义暂停点（对话型={ask_user}，studio 类型=∅）；
    approval_tools 挂 approve/edit/reject；subagent_create=ask 时委派工具同样进审批门控。"""
    interrupt_on: dict[str, InterruptOnConfig] = {
        tool: InterruptOnConfig(allowed_decisions=_ASK_USER_DECISIONS) for tool in pause_tools
    }
    interrupt_on.update(
        {
            tool: InterruptOnConfig(allowed_decisions=_APPROVAL_DECISIONS)
            for tool in approval_tools
            if tool not in pause_tools
        }
    )
    if subagent_create == "ask":
        interrupt_on[SUBAGENT_TOOL_NAME] = InterruptOnConfig(
            allowed_decisions=_APPROVAL_DECISIONS
        )
    return interrupt_on
