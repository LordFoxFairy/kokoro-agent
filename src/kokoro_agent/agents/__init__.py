"""业务 agent 工厂层：一个类型一个 py 一个工厂（Factory Method），注册表分派。

web 传 entry → session 解析 agent_type 上 wire → FACTORIES 分派。新增 studio 类型 =
新 <type>.py 工厂 + 注册一行（+契约枚举一值），机制零改动；类型的工具面政策
（core_tools/pause_tools）是工厂类属性，看一个文件懂一个类型。

升级路径（P2 swarm/handoff）：langgraph-swarm 在本注册表之上组合各工厂产物成
handoff 图（general ⇄ studio 类型控制权转移），工厂形状不变。
"""

from __future__ import annotations

from kokoro_agent.agents.base import AgentFactory, AssembleDeps, AssembledAgent
from kokoro_agent.agents.general import GeneralAgentFactory
from kokoro_agent.contract import AgentType, RunRequest
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME

FACTORIES: dict[AgentType, AgentFactory] = {
    GeneralAgentFactory.name: GeneralAgentFactory(),
}


def _factory_for(request: RunRequest) -> AgentFactory:
    factory = FACTORIES.get(request.runtime.agent_type)
    if factory is None:
        raise NotImplementedError(
            f"agent_type {request.runtime.agent_type!r} has no registered factory"
        )
    return factory


async def assemble(deps: AssembleDeps, request: RunRequest) -> AssembledAgent:
    """唯一装配入口：按 wire 的 agent_type 分派工厂；未知类型 fail-loud。"""
    return await _factory_for(request).create(deps, request)


def approval_names(request: RunRequest) -> frozenset[str]:
    """pending 识别集 = wire 审批集 + 类型工厂 pause_tools（+ask 档委派工具）。"""
    names = frozenset(request.runtime.permissions.approval_tools) | _factory_for(request).pause_tools
    if request.runtime.permissions.subagent_create == "ask":
        names |= {SUBAGENT_TOOL_NAME}
    return names


__all__ = [
    "FACTORIES",
    "AgentFactory",
    "AssembleDeps",
    "AssembledAgent",
    "approval_names",
    "assemble",
]
