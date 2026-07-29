"""中间件链构建：守卫/审批在主链与子代理链上的组成规则，一处看全。"""

from __future__ import annotations

from dataclasses import dataclass

from langchain.agents.middleware import AgentMiddleware

from kokoro_agent.agents.deps import AssembleDeps
from kokoro_agent.contract import RunRequest
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.propose_plan import PROPOSE_PLAN_TOOL_NAME
from kokoro_agent.tools.middleware import (
    SteeringMiddleware,
    PlanProposalCallGuardMiddleware,
    TerminalGuardMiddleware,
    ToolEffectJournalMiddleware,
    TokenBudgetMiddleware,
    ToolPolicyMiddleware,
    ToolResultReviewMiddleware,
)


@dataclass(frozen=True, slots=True)
class GuardChains:
    """子代理链=守卫+审核+journal（缺一即 task 委派旁路政策）；主链另有 steering 与 policy——
    插话是用户↔主 agent 的对话（注入子代理即语义污染），policy 待工具面定型后由调用方注入。"""

    subagent: tuple[AgentMiddleware, ...]
    guards: tuple[AgentMiddleware, ...]
    steering: AgentMiddleware
    review: AgentMiddleware | None
    # R3 tool effect journal：恒挂在 tool-call 链最内层（最靠近副作用），主链与子代理链同下发。
    journal: AgentMiddleware

    def main(self, policy: ToolPolicyMiddleware) -> tuple[AgentMiddleware, ...]:
        # 主链顺序：守卫 → steering → policy → review → journal。policy 在 review 外层：
        # 先拦未授权调用，才轮到已授权调用的结果送审；journal 最内层守门真正的工具执行。
        chain: list[AgentMiddleware] = [*self.guards, self.steering, policy]
        if self.review is not None:
            chain.append(self.review)
        chain.append(self.journal)
        return tuple(chain)


def build_guard_chains(deps: AssembleDeps, request: RunRequest) -> GuardChains:
    """守卫两件套：终态闸恒挂 + 预算闸按政策；主 agent 与每个子代理同套下发
    （子代理 middleware 链独立，不下发即 task 委派旁路）。"""
    review_tools = frozenset(request.runtime.permissions.review_tools)
    if ASK_USER_TOOL_NAME in review_tools:
        # review_tools=结果须人工复核后才继续的工具集；ask_user 的结果本身就是人工答复，
        # 再送人工复核=人审人答死循环，装配期即拒绝。
        raise ValueError("ask_user cannot be a result-review tool")
    if PROPOSE_PLAN_TOOL_NAME in review_tools:
        # proposal 已在执行前经过专用 approve/reject；放进结果审核会在 accept 后二次暂停。
        raise ValueError("propose_plan cannot be a result-review tool")
    guards: list[AgentMiddleware] = [
        TerminalGuardMiddleware(store=deps.ledger, run_id=request.run_id),
        # 模型结果一出即检查完整 tool_calls，早于 tail HITL 与 ToolNode；混帧任何工具都不执行。
        PlanProposalCallGuardMiddleware(),
    ]
    if deps.run_token_budget > 0:
        guards.append(
            TokenBudgetMiddleware(
                budget=deps.run_token_budget, store=deps.ledger, run_id=request.run_id
            )
        )
    review = (
        ToolResultReviewMiddleware(review_tools, deps.ledger, request.run_id)
        if review_tools
        else None
    )
    journal = ToolEffectJournalMiddleware(store=deps.ledger, run_id=request.run_id)
    # 子代理链：守卫 →（审核）→ journal（最内层）。子代理内工具同经 backend 执行，同守门。
    subagent_guards: tuple[AgentMiddleware, ...] = tuple(guards)
    if review is not None:
        subagent_guards = (*subagent_guards, review)
    subagent_guards = (*subagent_guards, journal)
    return GuardChains(
        subagent=subagent_guards,
        guards=tuple(guards),
        steering=SteeringMiddleware(store=deps.ledger, run_id=request.run_id),
        review=review,
        journal=journal,
    )
