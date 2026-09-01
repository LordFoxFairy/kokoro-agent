"""HITL ``interrupt_on`` 构造：审批集合来自 worker-local Agent policy。"""

from __future__ import annotations

from langchain.agents.middleware import InterruptOnConfig
from langchain.agents.middleware.human_in_the_loop import DecisionType

from kokoro_agent.hitl import APPROVAL_DECISIONS, QUESTION_DECISIONS
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME

# langchain HITL 面（DecisionType）是 wire 词汇 AllowedDecision 的子集：不含 submit（那是 kind=input
# 分支的决策，不经 interrupt_on）。此处显式列出 DecisionType 子集与 wire 侧解耦，并断言与 hitl.presets
# 同值防漂移——presets 仍是 wire 决策词汇的单一事实源。
_ASK_USER_DECISIONS: list[DecisionType] = ["respond"]
_APPROVAL_DECISIONS: list[DecisionType] = ["approve", "edit", "reject"]
assert set(_ASK_USER_DECISIONS) == set(QUESTION_DECISIONS)
assert set(_APPROVAL_DECISIONS) == set(APPROVAL_DECISIONS)


def build_interrupt_on(
    approval_tools: frozenset[str],
    *,
    subagent_create: str = "deny",
    pause_tools: frozenset[str] = frozenset({ASK_USER_TOOL_NAME}),
) -> dict[str, bool | InterruptOnConfig]:
    """pause_tools=类型包的 respond 语义暂停点（对话型={ask_user}，studio 类型=∅）；
    approval_tools 挂 approve/edit/reject；subagent_create=ask 时委派工具同样进审批门控。"""
    interrupt_on: dict[str, bool | InterruptOnConfig] = {
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
