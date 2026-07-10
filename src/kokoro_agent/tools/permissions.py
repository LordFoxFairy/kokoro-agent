"""HITL interrupt_on 构造：审批工具集合每请求经 RuntimeConfig.permissions 注入。"""

from __future__ import annotations

from langchain.agents.middleware import InterruptOnConfig
from langchain.agents.middleware.human_in_the_loop import DecisionType

from kokoro_agent.hitl import APPROVAL_DECISIONS, QUESTION_DECISIONS
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME

# 三预设决策词汇的单一事实源在 hitl.presets（kind=question / kind=approval 形态）。
# ask_user 是 question 预设（语义暂停点，只人工作答）；普通工具审批是 approval 预设（放行/改参/拒绝）。
_ASK_USER_DECISIONS: list[DecisionType] = list(QUESTION_DECISIONS)
_APPROVAL_DECISIONS: list[DecisionType] = list(APPROVAL_DECISIONS)


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
