# pyright: reportUnknownArgumentType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
"""GA 唯一的 Agent 构造入口：Agent 定义进，DeepAgents native runnable 出。

构造顺序：
  backend    ① 创建本次运行的 DeepAgents backend
  skills     ② 将声明的 Skill 名称解析为 ``/.skills/`` 原生 backend 路由
  tools      ③ 合并 Agent、worker 内置工具及可选 MCP/Storage 工具
  middleware ④ 组装授权、审批和运行守卫
  subagents  ⑤ 注入 Agent 明确声明的 DeepAgents native subagents
  agent      ⑥ 直接调用上游 ``create_deep_agent``
Agent 定义只描述完整能力；请求和 worker 服务都不会进入 Agent/Feature 声明。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import logging

from deepagents import create_deep_agent
from langchain_core.tools import BaseTool
from deepagents.backends.composite import CompositeBackend
from deepagents.backends.protocol import BackendProtocol
from deepagents.backends.state import StateBackend

from kokoro_agent.agents.subagents import build_subagent_bundle
from kokoro_agent.tools.guards import build_guard_chains
from kokoro_agent.tools.toolset import build_toolset
from kokoro_agent.agents.definition import Agent
from kokoro_agent.worker.services import WorkerServices
from kokoro_agent.contract import RunRequest
from kokoro_agent.contract.storage import workspace_key
from kokoro_agent.policy import Backend, ModelConfig
from kokoro_agent.clients.skills import ResolvedSkill, SkillClient, SkillClientError
from kokoro_agent.execution.protocols import AgentRunnable, require_agent_runnable
from kokoro_agent.model.factory import make_chat_model, select_model_label
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend_for_run
from kokoro_agent.skills.backend import CapabilitySkillBackend, SKILLS_ROOT
from kokoro_agent.tools.middleware import ToolPolicyMiddleware
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.execution.scope import RunScope
from kokoro_agent.features.catalog import FEATURE_CATALOG, FeatureCatalog
from kokoro_agent.features.definition import Feature
from kokoro_agent.swarm import create_swarm
from langgraph_swarm import create_handoff_tool
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AgentHandle:
    """指向官方 runnable 的一次性句柄，不是第二个 Agent/runtime。"""

    runnable: AgentRunnable
    tool_descriptions: Mapping[str, str]

    def describe_tool(self, name: str) -> str | None:
        return self.tool_descriptions.get(name)


async def build_deep_agent(
    agent: Agent,
    services: WorkerServices,
    request: RunRequest,
    *,
    additional_tools: Sequence[BaseTool] = (),
    name: str | None = None,
) -> AgentHandle:
    scope = RunScope.of(request)
    policy = agent.permissions
    # 工作区=真实目录约定 {root}/{namespace:session_id}/：文件写下即可被 session files 端点直读。
    # docker/e2b 档带 run 级生命周期：resume 经 ledger 重连既往箱/容器。
    backend = await make_backend_for_run(
        agent.backend,
        services.sandbox,
        workspace=workspace_key(scope.namespace, scope.session_id),
        run_id=request.run_id,
        sandbox_store=services.ledger,
    )
    resolved_skills = await resolve_declared_skills(
        agent, services.skill_client, request
    )
    skill_backend = CapabilitySkillBackend(resolved_skills, services.skill_reader)
    native_backend = _with_native_skills(backend, skill_backend)
    toolset = await build_toolset(
        request,
        agent=agent,
        toolbox=services.toolbox,
        mcp_servers=services.mcp_servers,
        mcp_client=services.mcp_client,
        backend=native_backend,
        delivery=services.delivery,
    )
    if additional_tools:
        toolset = toolset.with_tools(additional_tools)
    chains = build_guard_chains(
        services.ledger,
        services.run_token_budget,
        request,
        policy,
    )
    subagent_bundle = build_subagent_bundle(
        toolset,
        services.subagent_catalog,
        chains.subagent,
        declared_subagents=agent.subagents,
    )
    main_chain = chains.main(
        ToolPolicyMiddleware(
            toolset.authorized,
            declared_subagents=subagent_bundle.declared,
            subagent_create=policy.subagent_create,
        )
    )
    # DeepAgents is the runtime.  This call must remain a direct call to the
    # upstream constructor; this module only translates GA's static Agent
    # declaration and worker-owned services into its documented arguments.
    # This is the only construction call in GA.  The returned object is the
    # upstream DeepAgents/LangGraph runnable; GA does not wrap its loop/state.
    candidate: object = create_deep_agent(
        model=make_chat_model(
            services.model,
            select_model_label(
                request.requested_model_label,
                agent.model or ModelConfig(provider="anthropic", name="claude"),
            ),
        ),
        tools=toolset.tools,
        system_prompt=agent.prompt,
        skills=[SKILLS_ROOT],
        subagents=subagent_bundle.subagents,
        checkpointer=services.checkpointer,
        permissions=build_filesystem_permissions(policy.filesystem),
        interrupt_on=build_interrupt_on(
            frozenset(policy.approval_tools),
            subagent_create=policy.subagent_create,
            pause_tools=agent.pause_tools,
        ),
        middleware=main_chain,
        backend=native_backend,
        # 长期记忆：后端随 checkpoint 对齐，工具侧按租户 namespace 前缀隔离。
        store=services.memory_store,
        name=name,
    )
    return AgentHandle(
        runnable=require_agent_runnable(candidate),
        tool_descriptions=toolset.descriptions,
    )


async def resolve_declared_skills(
    agent: Agent, skill_client: SkillClient, request: RunRequest
) -> tuple[ResolvedSkill, ...]:
    """Resolve declared Skill selectors at the GA boundary.

    Feature/Agent declarations carry names only; Capability resolves those names through the
    worker-owned client immediately before DeepAgents is constructed. The resulting resolved
    skills are
    transient assembly data and never become Agent or Session state.
    """
    if not agent.skills:
        return ()
    scope = RunScope.of(request)
    try:
        return await skill_client.resolve(
            agent.skills, request.execution_identity, scope.namespace
        )
    except SkillClientError:
        # Skills enhance an Agent; Capability unavailability must not remove the
        # Agent's base chat/tool loop. Do not include identity or client details in logs.
        LOGGER.warning(
            "declared skills unavailable for agent=%s; continuing without them",
            agent.key,
        )
        return ()


def _with_native_skills(
    backend: BackendProtocol | None, skill_backend: CapabilitySkillBackend
) -> CompositeBackend:
    """Route ``/.skills/`` into DeepAgents without copying Skill packages."""

    return CompositeBackend(
        default=backend or StateBackend(),
        routes={SKILLS_ROOT: skill_backend},
    )


class AgentFactory:
    """worker-local 构造器；共享服务存于实例，不以 ``deps`` 出现在运行 API。"""

    def __init__(
        self, services: WorkerServices, catalog: FeatureCatalog = FEATURE_CATALOG
    ) -> None:
        self._services = services
        self._catalog = catalog

    def feature(self, key: str) -> Feature:
        """Resolve a trusted product Feature from this worker's catalog."""
        return self._catalog.get(key)

    def backend_for(self, request: RunRequest) -> Backend:
        """Return the Feature-declared sandbox kind for terminal cleanup."""
        feature = self.feature(request.feature_key)
        return feature.agents[0].backend

    async def build(self, request: RunRequest) -> AgentHandle:
        """按受信 Feature key 构造；请求本身不携带 Agent/图配方。"""
        return await self._build_feature(self.feature(request.feature_key), request)

    async def _build_feature(
        self, feature: Feature, request: RunRequest
    ) -> AgentHandle:
        """构造一个已解析 Feature；多 peer 仅在声明 handoff 时进入官方 Swarm。"""
        if len(feature.agents) == 1:
            return await build_deep_agent(feature.agents[0], self._services, request)
        if not feature.handoffs:
            raise ValueError(
                f"feature {feature.key!r} has multiple agents but no handoffs"
            )
        built_agents: list[AgentHandle] = []
        for agent in feature.agents:
            targets = [
                target for source, target in feature.handoffs if source == agent.key
            ]
            handoffs = [create_handoff_tool(agent_name=target) for target in targets]
            built_agents.append(
                await build_deep_agent(
                    agent,
                    self._services,
                    request,
                    additional_tools=handoffs,
                    name=agent.key,
                )
            )
        native = create_swarm(
            [built.runnable for built in built_agents],
            entry_agent=feature.entry_agent,
            checkpointer=self._services.checkpointer,
            store=self._services.memory_store,
        )
        descriptions: dict[str, str] = {}
        for built in built_agents:
            descriptions.update(built.tool_descriptions)
        return AgentHandle(runnable=native, tool_descriptions=descriptions)

    def approval_names(self, request: RunRequest) -> frozenset[str]:
        feature = self.feature(request.feature_key)
        names: set[str] = set()
        for agent in feature.agents:
            names.update(agent.permissions.approval_tools)
            names.update(agent.pause_tools)
            if agent.permissions.subagent_create == "ask":
                names.add(SUBAGENT_TOOL_NAME)
        return frozenset(names)


__all__ = ["AgentFactory", "AgentHandle"]
