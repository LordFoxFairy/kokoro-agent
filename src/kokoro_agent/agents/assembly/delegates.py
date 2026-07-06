"""可委派子代理集合：内生 general-purpose + 部署 catalog + 本次 wire 预设，三路合流。

swarm 也在这条线上：V1 桥语义下 swarm_members ⊆ runtime.subagents（session resolve
已保证成员完整定义在 wire 里），所以成员经本模块挂为层级下属即已可达；
P2 langgraph-swarm 落地时在工厂加 swarm.py 一步，按 swarm_members 建 handoff 图。
"""

from __future__ import annotations

from dataclasses import dataclass

from deepagents.middleware.subagents import SubAgent
from langchain.agents.middleware import AgentMiddleware

from kokoro_agent.agents.deps import AssembleDeps
from kokoro_agent.agents.assembly.toolset import Toolset
from kokoro_agent.contract import RunRequest
from kokoro_agent.model.factory import make_chat_model
from kokoro_agent.subagents import catalog_subagents, general_purpose_subagent, wire_subagents


@dataclass(frozen=True, slots=True)
class Delegates:
    subagents: tuple[SubAgent, ...]
    # 委派执法（deny 档）声明集=真挂载 catalog + wire 预设；general-purpose 是内生件，
    # 同名覆盖只为挂满守卫，可达性政策不因此放宽（不进声明集）。
    declared: frozenset[str]


def build_delegates(
    request: RunRequest,
    toolset: Toolset,
    deps: AssembleDeps,
    chain: tuple[AgentMiddleware, ...],
) -> Delegates:
    catalog_defs, catalog_names = catalog_subagents(deps.catalog, toolset.by_name, chain)
    wired = wire_subagents(
        request,
        toolset.by_name,
        lambda spec: make_chat_model(deps.model, spec),
        chain,
        prompts=deps.prompts,
    )
    return Delegates(
        subagents=(general_purpose_subagent(chain), *catalog_defs, *wired),
        declared=catalog_names | frozenset(sub.name for sub in request.runtime.subagents),
    )
