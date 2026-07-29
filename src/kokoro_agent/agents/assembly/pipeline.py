"""共享装配管线（Template Method 主体）：政策进、装配图出，各类型工厂复用。

一步一模块，assemble_agent() 只是目录页：
  backend    ① 沙箱后端（先建：skill 资产按需供给依赖它）
  toolset    ② 工具面：注册表/底座/MCP/技能库四路合流 + 授权白名单
  guardrails ③ 中间件链：守卫/审批的主链与子代理链
  delegates  ④ 可委派子代理：内生 + catalog（wire 只传 names）
  prompt     ⑤ system prompt：agent（preset）名两级解析，恒定不随能力集变
类型专属只剩政策（core_tools/pause_tools/plan_tools/default_prompt），经 AgentPolicy 传入。
"""

from __future__ import annotations

from typing import Any

from langchain.agents.middleware import AgentMiddleware

from kokoro_agent.agents.assembly.delegates import build_delegates
from kokoro_agent.agents.assembly.guardrails import build_guard_chains
from kokoro_agent.agents.assembly.prompt import build_system_prompt
from kokoro_agent.agents.assembly.swarm import build_swarm_middleware, swarm_candidates
from kokoro_agent.agents.assembly.toolset import build_toolset
from kokoro_agent.agents.deps import AgentPolicy, AssembleDeps, AssembledAgent
from kokoro_agent.contract import RunRequest
from kokoro_agent.contract.storage import workspace_key
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.model.factory import make_chat_model
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend_for_run
from kokoro_agent.skills import SkillMaterializerMiddleware
from kokoro_agent.skills.supply import MaterializeBackend
from kokoro_agent.tools.middleware import ToolPolicyMiddleware
from kokoro_agent.tools.permissions import build_interrupt_on


async def assemble_agent(
    policy: AgentPolicy, deps: AssembleDeps, request: RunRequest
) -> AssembledAgent:
    runtime = request.runtime
    # 工作区=真实目录约定 {root}/{namespace:session_id}/：文件写下即可被 session files 端点直读。
    # docker/e2b 档带 run 级生命周期：resume 经 ledger 重连既往箱/容器。
    backend = await make_backend_for_run(
        runtime.backend,
        deps.sandbox,
        workspace=workspace_key(request.context.namespace, request.context.session_id),
        run_id=request.run_id,
        binding=deps.ledger,
    )
    toolset = await build_toolset(request, deps, core=policy.core_tools)
    chains = build_guard_chains(deps, request)
    delegates = build_delegates(request, toolset, deps, chains.subagent)
    main_chain = chains.main(
        ToolPolicyMiddleware(
            toolset.authorized,
            declared_subagents=delegates.declared,
            subagent_create=runtime.permissions.subagent_create,
        )
    )
    # 装配期人格前缀（=切轨定点）：单人格链路直用它作 system prompt；候选>1 时另挂人格中间件，
    # 按 graph state 的 active_agent 在此前缀上定点换轨（移交后人格），底座/技能清单原样保留。
    system_prompt = build_system_prompt(request, deps, default=policy.default_prompt)
    if len(swarm_candidates(deps.prompts)) > 1:
        main_chain = (
            *main_chain,
            build_swarm_middleware(request, deps.prompts, initial_prompt=system_prompt),
        )
    graph = build_agent(
        model=make_chat_model(deps.model, runtime.model),
        tools=toolset.tools,
        system_prompt=system_prompt,
        subagents=delegates.subagents,
        checkpointer=deps.checkpointer,
        permissions=build_filesystem_permissions(runtime.permissions.filesystem),
        interrupt_on=build_interrupt_on(
            frozenset(runtime.permissions.approval_tools),
            subagent_create=runtime.permissions.subagent_create,
            pause_tools=policy.pause_tools,
            plan_tools=policy.plan_tools,
        ),
        # 技能资产物化对账（before_agent，恒早于模型）：账本进 checkpoint,附件按 hash 增量落沙箱。
        # backend 缺省档（state）无沙箱面 → 无附件可物化 → 不挂对账中间件。
        middleware=_with_materializer(main_chain, request, deps, backend),
        backend=backend,
        # 长期记忆：后端随 checkpoint 对齐，工具侧按租户 namespace 前缀隔离。
        store=deps.memory_store,
    )
    return AssembledAgent(agent=graph, tool_descriptions=toolset.descriptions)


def _with_materializer(
    chain: tuple[AgentMiddleware, ...],
    request: RunRequest,
    deps: AssembleDeps,
    backend: MaterializeBackend | None,
) -> tuple[AgentMiddleware[Any, Any, Any], ...]:
    """无沙箱（state 档 backend=None）时不挂对账；有沙箱则追加 before_agent 物化中间件。"""
    if backend is None:
        return chain
    materializer = SkillMaterializerMiddleware(
        grants=request.runtime.skills,
        hub=deps.skill_hub,
        backend=backend,
    )
    return (*chain, materializer)
