"""共享装配管线（Template Method 主体）：政策进、装配图出，各类型工厂复用。

一步一模块，assemble_agent() 只是目录页：
  toolset    ① 工具面：四路来源合流 + 授权白名单
  guardrails ② 中间件链：守卫/审批的主链与子代理链
  delegates  ③ 可委派子代理：内生 + catalog + wire 三路
  prompt     ④ system prompt：agent prompt 三级解析 + skills 注入
类型专属只剩政策（core_tools/pause_tools/default_prompt），经 AgentPolicy 传入。
"""

from __future__ import annotations

from kokoro_agent.agents.assembly.delegates import build_delegates
from kokoro_agent.agents.assembly.guardrails import build_guard_chains
from kokoro_agent.agents.assembly.prompt import resolve_system_prompt
from kokoro_agent.agents.assembly.toolset import build_toolset
from kokoro_agent.agents.deps import AgentPolicy, AssembleDeps, AssembledAgent
from kokoro_agent.contract import RunRequest
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.model.factory import make_chat_model
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend_for_run
from kokoro_agent.skills import provision_skills
from kokoro_agent.tools.middleware import ToolPolicyMiddleware
from kokoro_agent.tools.permissions import build_interrupt_on


async def assemble_agent(
    policy: AgentPolicy, deps: AssembleDeps, request: RunRequest
) -> AssembledAgent:
    runtime = request.runtime
    toolset = await build_toolset(request, deps, core=policy.core_tools)
    chains = build_guard_chains(deps, request)
    delegates = build_delegates(request, toolset, deps, chains.subagent)
    # 工作区=真实目录约定 {root}/{namespace:session_id}/：文件写下即可被 session files 端点直读。
    # docker/e2b 档带 run 级生命周期：resume 经 ledger 重连既往箱/容器。
    backend = await make_backend_for_run(
        runtime.backend,
        deps.sandbox,
        workspace=f"{request.context.namespace}:{request.context.session_id}",
        run_id=request.run_id,
        binding=deps.ledger,
    )
    # Skills V2 供给：授权包物化进 backend（state 档转 initial_files 随首 invoke 注入）。
    provisioned = await provision_skills(runtime, deps.skills, backend)
    graph = build_agent(
        model=make_chat_model(deps.model, runtime.model),
        tools=toolset.tools,
        system_prompt=resolve_system_prompt(runtime, deps, default=policy.default_prompt),
        subagents=delegates.subagents,
        checkpointer=deps.checkpointer,
        permissions=build_filesystem_permissions(runtime.permissions.filesystem),
        interrupt_on=build_interrupt_on(
            frozenset(runtime.permissions.approval_tools),
            subagent_create=runtime.permissions.subagent_create,
            pause_tools=policy.pause_tools,
        ),
        middleware=chains.main(
            ToolPolicyMiddleware(
                toolset.authorized,
                declared_subagents=delegates.declared,
                subagent_create=runtime.permissions.subagent_create,
            )
        ),
        backend=backend,
        skills=provisioned.sources,
        # 长期记忆：后端随 checkpoint 对齐，工具侧按租户 namespace 前缀隔离。
        store=deps.memory_store,
    )
    return AssembledAgent(
        agent=graph,
        tool_descriptions=toolset.descriptions,
        initial_files=provisioned.initial_files,
    )
