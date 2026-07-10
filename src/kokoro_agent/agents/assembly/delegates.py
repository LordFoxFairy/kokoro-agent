"""可委派子代理集合：内生 general-purpose + 部署 catalog 两路合流。

wire 只传 subagent names（声明性白名单，定义住 agent 侧 catalog/资产库）；
P2 langgraph-swarm 落地时在工厂加 swarm.py 一步建对等 handoff 图。
"""

from __future__ import annotations

from dataclasses import dataclass

from deepagents.middleware.subagents import SubAgent
from langchain.agents.middleware import AgentMiddleware

from kokoro_agent.agents.deps import AssembleDeps
from kokoro_agent.agents.assembly.toolset import Toolset
from kokoro_agent.contract import RunRequest
from kokoro_agent.subagents import catalog_subagents, general_purpose_subagent


@dataclass(frozen=True, slots=True)
class Delegates:
    subagents: tuple[SubAgent, ...]
    # 委派执法（deny 档）声明集=真挂载 catalog + wire 点名；general-purpose 是内生件，
    # 同名覆盖只为挂满守卫，可达性政策不因此放宽（不进声明集）。
    declared: frozenset[str]


def build_delegates(
    request: RunRequest,
    toolset: Toolset,
    deps: AssembleDeps,
    chain: tuple[AgentMiddleware, ...],
) -> Delegates:
    catalog_defs, catalog_names = catalog_subagents(deps.catalog, toolset.by_name, chain)
    return Delegates(
        subagents=(general_purpose_subagent(chain), *catalog_defs),
        declared=catalog_names | frozenset(request.runtime.subagents),
    )
