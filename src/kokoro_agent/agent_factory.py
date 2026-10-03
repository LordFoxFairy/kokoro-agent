"""GA 唯一的 Agent 构造入口：Agent 定义进，DeepAgents native runnable 出。

构造顺序：
  route      ⓪ 向 System 解析受信租户模型路由，失败时不创建外部资源
  backend    ① 创建本次运行的 DeepAgents backend
  skills     ② 将冻结的 Skill exact refs 解析为 ``/.skills/`` 原生 backend 路由
  tools      ③ 合并 Agent、worker 内置工具及可选 MCP/Storage 工具
  middleware ④ 组装授权、审批和运行守卫
  subagents  ⑤ 注入 Agent 明确声明的 DeepAgents native subagents
  agent      ⑥ 直接调用上游 ``create_deep_agent``
Agent 定义只描述完整能力；请求和 worker 服务都不会进入 Agent/Feature 声明。
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Awaitable, Callable
from dataclasses import dataclass
import logging
from time import monotonic
from uuid import uuid4
from typing import Any

import deepagents
from deepagents.backends.composite import CompositeBackend
from deepagents.backends.protocol import BackendProtocol
from deepagents.backends.state import StateBackend

from kokoro_agent.agents.subagents import build_subagent_bundle
from kokoro_agent.tools.guards import build_guard_chains
from kokoro_agent.tools.toolset import build_toolset, resolve_declared_mcp
from kokoro_agent.mcp.config import McpServerEntry
from kokoro_agent.agents.definition import Agent
from kokoro_agent.worker.dependencies import WorkerDependencies
from kokoro_agent.protocol import (
    RunRequest,
    SkillPhase,
    SkillProgressSink,
    SkillProgressCommitted,
)
from kokoro_agent.execution.events import ProgressPersistenceError
from kokoro_agent.sandbox.workspace import workspace_key
from kokoro_agent.policy import Backend
from kokoro_agent.clients.skills import ResolvedSkill, SkillClient, SkillClientError
from kokoro_agent.execution.protocols import AgentRunnable, require_agent_runnable
from kokoro_agent.model.factory import make_chat_model, model_from_route
from kokoro_agent.clients.system import ModelResolutionError
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend_for_run
from kokoro_agent.sandbox.archive import ArchivingWritesMixin
from kokoro_agent.sandbox.backend import backend_resource
from kokoro_agent.skills.backend import TypedSkillBackend, SKILLS_ROOT
from kokoro_agent.skills.middleware import RunSkillsMiddleware
from kokoro_agent.tools.middleware import ToolPolicyMiddleware
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.features.catalog import FEATURE_CATALOG, FeatureCatalog
from kokoro_agent.features.definition import Feature
from kokoro_agent.swarm import create_swarm
from kokoro_agent.execution.runtime_profile_plan import (
    PreparedPeerPlan,
    prepare_feature,
)
from kokoro_agent.agents.native_profile import validate_native_registry
from kokoro_agent.tools.registry import SUBAGENT_TOOL_NAME
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.domain.run.models import LeasedRun, StaticRecipeBinding

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AgentHandle:
    """指向官方 runnable 的一次性句柄，不是第二个 Agent/runtime。"""

    runnable: AgentRunnable
    tool_descriptions: Mapping[str, str]
    resources: tuple[ArchivingWritesMixin, ...] = ()

    def describe_tool(self, name: str) -> str | None:
        return self.tool_descriptions.get(name)

    async def aclose(self) -> None:
        """Close every factory-owned local resource; each resource is idempotent."""
        results = await asyncio.gather(
            *(resource.aclose_resources() for resource in self.resources),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise BaseExceptionGroup("agent resource cleanup failed", errors)


async def _settle_resource_cleanup(
    resources: tuple[ArchivingWritesMixin, ...],
) -> BaseException | None:
    """Finish cleanup despite repeated cancellation and return its own failure."""
    if not resources:
        return None

    async def close_all() -> None:
        results = await asyncio.gather(
            *(resource.aclose_resources() for resource in resources),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise BaseExceptionGroup("agent resource cleanup failed", errors)

    task = asyncio.create_task(close_all())
    while True:
        try:
            await asyncio.shield(task)
            return None
        except asyncio.CancelledError:
            if task.done():
                break
            continue
        except BaseException as error:
            return error
    if task.cancelled():
        return asyncio.CancelledError()
    return task.exception()


@dataclass(frozen=True, slots=True)
class _ResolvedCapabilities:
    skills: tuple[ResolvedSkill, ...]
    mcp: Mapping[str, McpServerEntry]
    skill_reader: SkillClient | None


async def _preflight(
    agent: Agent,
    dependencies: WorkerDependencies,
    request: RunRequest,
    lease: LeaseFence,
    progress: SkillProgressSink,
    before_load: Callable[[], Awaitable[None]],
) -> _ResolvedCapabilities:
    skills: tuple[ResolvedSkill, ...] = ()
    reader: SkillClient | None = None
    if request.selected_skill_source_refs:
        try:
            if dependencies.platform is None:
                raise SkillClientError("typed skill source reader unavailable")
            reader = dependencies.platform.skills_for_run(
                LeasedRun(request=request, lease=lease)
            )
            skills = await reader.resolve(request.selected_skill_source_refs)
        except ProgressPersistenceError:
            raise
        except Exception:
            await _confirm_progress(
                progress, SkillPhase(phase="failed", error_code="skill_resolve_failed")
            )
            raise
        for skill in skills:
            await before_load()
            try:
                await reader.load_package(skill)
            except ProgressPersistenceError:
                raise
            except Exception:
                await _confirm_progress(
                    progress, SkillPhase(phase="failed", error_code="skill_load_failed")
                )
                raise
    return _ResolvedCapabilities(skills=skills, mcp={}, skill_reader=reader)


async def _confirm_progress(progress: SkillProgressSink, phase: SkillPhase) -> None:
    confirmation = await progress(phase)
    try:
        SkillProgressCommitted.model_validate(confirmation)
    except ValueError as error:
        raise ProgressPersistenceError(
            "Skill progress has no durable confirmation"
        ) from error


async def build_deep_agent(
    agent: Agent,
    dependencies: WorkerDependencies,
    request: RunRequest,
    lease: LeaseFence,
    *,
    plan: PreparedPeerPlan,
    capabilities: _ResolvedCapabilities,
    name: str | None = None,
) -> AgentHandle:
    if plan.agent is not agent:
        raise ValueError("prepared peer does not match build declaration")
    validate_native_registry()
    started = monotonic()
    resolver = dependencies.model_resolver
    if resolver is None:
        raise ModelResolutionError("MODEL_RESOLVER_NOT_CONFIGURED")
    correlation_id = request.request_id or str(uuid4())
    route = await resolver.resolve(
        tenant_id=request.execution_identity.tenant_ref,
        feature_key=request.feature_key,
        label=request.requested_model_label,
        request_id=correlation_id,
    )
    model = model_from_route(route, agent.model)
    LOGGER.info(
        "model route resolved",
        extra={
            "service": "kokoro-agent",
            "operation": "model.resolve",
            "request_id": correlation_id,
            "trace_id": None,
            "run_id": request.run_id,
            "result": "success",
            "duration": monotonic() - started,
            "model_id": route.model_id,
            "revision_id": route.revision_id,
            "digest": route.digest,
            "generation": route.generation,
            "tenant_generation": route.tenant_generation,
        },
    )
    scope = RunScope.of(request)
    policy = agent.permissions
    # 工作区=真实目录约定 {root}/{namespace:session_id}/：文件写下即可被 session files 端点直读。
    # docker/e2b 档带 run 级生命周期：resume 经 run_repository 重连既往箱/容器。
    backend = await make_backend_for_run(
        agent.backend,
        dependencies.sandbox,
        workspace=workspace_key(scope.namespace, scope.session_id),
        run_id=request.run_id,
        lease=lease,
        sandbox_store=dependencies.run_repository,
    )
    resource = backend_resource(backend)
    resources = () if resource is None else (resource,)
    try:
        resolved_skills = capabilities.skills
        skill_backend = TypedSkillBackend(resolved_skills, capabilities.skill_reader)
        native_backend = _with_native_skills(backend, skill_backend)
        toolset = await build_toolset(
            request,
            agent=agent,
            plan=plan.tools,
            toolbox=dependencies.toolbox,
            mcp_servers=dependencies.mcp_servers,
            mcp_client=dependencies.mcp_client,
            backend=native_backend,
            delivery=dependencies.delivery,
            lease=lease,
            resolved_mcp=capabilities.mcp,
        )
        if plan.handoffs:
            toolset = toolset.with_tools(plan.handoffs)
        plan.verify_tools(toolset.tools)
        chains = build_guard_chains(
            dependencies.run_repository,
            dependencies.runtime_policy.run_token_budget,
            request,
            lease,
            policy,
        )
        subagent_bundle = build_subagent_bundle(
            toolset,
            plan.subagents,
            chains.subagent,
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
        # The upstream factory's ResponseT/ContextT generics are intentionally
        # unresolved in the installed stubs.  Keep that uncertainty at this one
        # official-constructor boundary; the returned value is validated below.
        native_constructor: Any = getattr(deepagents, "create_deep_agent")
        validate_native_registry()
        candidate: object = native_constructor(
            model=make_chat_model(dependencies.model, model),
            tools=toolset.tools,
            system_prompt=agent.prompt,
            skills=None,
            subagents=subagent_bundle.subagents,
            checkpointer=dependencies.checkpointer,
            permissions=build_filesystem_permissions(policy.filesystem),
            interrupt_on=build_interrupt_on(
                frozenset(policy.approval_tools),
                subagent_create=policy.subagent_create,
                pause_tools=agent.pause_tools,
            ),
            middleware=(
                *main_chain,
                RunSkillsMiddleware(backend=native_backend, sources=[SKILLS_ROOT]),
            ),
            backend=native_backend,
            # 长期记忆：后端随 checkpoint 对齐，工具侧按租户 namespace 前缀隔离。
            store=dependencies.memory_store,
            name=name,
        )
        validate_native_registry()
        return AgentHandle(
            runnable=require_agent_runnable(candidate),
            tool_descriptions=toolset.descriptions,
            resources=resources,
        )
    except BaseException as primary:
        cleanup_error = await _settle_resource_cleanup(resources)
        if cleanup_error is not None:
            primary.add_note(
                "resource cleanup also failed: " + type(cleanup_error).__name__
            )
            LOGGER.error(
                "agent construction resource cleanup failed run_id=%s",
                request.run_id,
                extra={"cleanup_error_type": type(cleanup_error).__name__},
            )
        raise


def _with_native_skills(
    backend: BackendProtocol | None, skill_backend: TypedSkillBackend
) -> CompositeBackend:
    """Route ``/.skills/`` into DeepAgents without copying Skill packages."""

    return CompositeBackend(
        default=backend or StateBackend(),
        routes={SKILLS_ROOT: skill_backend},
    )


class AgentFactory:
    """worker-local 构造器；运行依赖存于实例，不暴露给运行 API。"""

    def __init__(
        self,
        dependencies: WorkerDependencies,
        catalog: FeatureCatalog = FEATURE_CATALOG,
    ) -> None:
        self._dependencies = dependencies
        self._catalog = catalog

    def feature(self, key: str) -> Feature:
        """Resolve a trusted product Feature from this worker's catalog."""
        return self._catalog.get(key)

    def backend_for(self, request: RunRequest) -> Backend:
        """Return the Feature-declared sandbox kind for terminal cleanup."""
        feature = self.feature(request.feature_key)
        return feature.agents[0].backend

    async def build(
        self, request: RunRequest, lease: LeaseFence, progress: SkillProgressSink
    ) -> AgentHandle:
        """按受信 Feature key 构造；请求本身不携带 Agent/图配方。"""
        return await self._build_feature(
            self.feature(request.feature_key), request, lease, progress
        )

    async def _build_feature(
        self,
        feature: Feature,
        request: RunRequest,
        lease: LeaseFence,
        progress: SkillProgressSink,
    ) -> AgentHandle:
        """构造一个已解析 Feature；多 peer 仅在声明 handoff 时进入官方 Swarm。"""
        prepared = prepare_feature(
            feature,
            runtime_policy=self._dependencies.runtime_policy,
            manifest=self._dependencies.manifest,
            toolbox=self._dependencies.toolbox,
            subagent_catalog=self._dependencies.subagent_catalog,
            delivery_available=self._dependencies.delivery is not None,
        )
        await self._dependencies.run_repository.freeze_or_verify_static_recipe(
            request,
            lease,
            StaticRecipeBinding(
                canonical_bytes=prepared.recipe_bytes, fingerprint=prepared.fingerprint
            ),
        )
        loading = False

        async def before_load() -> None:
            nonlocal loading
            if not loading:
                await _confirm_progress(progress, SkillPhase(phase="loading"))
                loading = True

        if request.selected_skill_source_refs:
            await _confirm_progress(progress, SkillPhase(phase="resolving"))
        skills = {
            peer.agent.key: await _preflight(
                peer.agent, self._dependencies, request, lease, progress, before_load
            )
            for peer in prepared.peers
        }
        if request.selected_skill_source_refs:
            await _confirm_progress(progress, SkillPhase(phase="ready"))
        # MCP is not a Skill phase. All peer Skill preflights precede MCP assembly.
        capabilities: dict[str, _ResolvedCapabilities] = {}
        for peer in prepared.peers:
            resolved = skills[peer.agent.key]
            mcp = await resolve_declared_mcp(
                request,
                peer.agent,
                self._dependencies.mcp_client,
                self._dependencies.mcp_servers,
            )
            capabilities[peer.agent.key] = _ResolvedCapabilities(
                skills=resolved.skills, mcp=mcp, skill_reader=resolved.skill_reader
            )
        if len(prepared.peers) == 1:
            peer = prepared.peers[0]
            return await build_deep_agent(
                peer.agent,
                self._dependencies,
                request,
                lease,
                plan=peer,
                capabilities=capabilities[peer.agent.key],
            )
        built_agents: list[AgentHandle] = []
        try:
            for peer in prepared.peers:
                built_agents.append(
                    await build_deep_agent(
                        peer.agent,
                        self._dependencies,
                        request,
                        lease,
                        plan=peer,
                        name=peer.agent.key,
                        capabilities=capabilities[peer.agent.key],
                    )
                )
            native = create_swarm(
                [built.runnable for built in built_agents],
                entry_agent=feature.entry_agent,
                checkpointer=self._dependencies.checkpointer,
                store=self._dependencies.memory_store,
            )
        except BaseException as primary:
            cleanup_error = await _settle_resource_cleanup(
                tuple(
                    resource for built in built_agents for resource in built.resources
                )
            )
            if cleanup_error is not None:
                primary.add_note(
                    "partial feature resource cleanup also failed: "
                    + type(cleanup_error).__name__
                )
            raise
        descriptions: dict[str, str] = {}
        resources: list[ArchivingWritesMixin] = []
        for built in built_agents:
            descriptions.update(built.tool_descriptions)
            resources.extend(built.resources)
        return AgentHandle(
            runnable=native,
            tool_descriptions=descriptions,
            resources=tuple(resources),
        )

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
