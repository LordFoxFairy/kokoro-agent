"""通用配方=数据型成品共用的标准装配管线。

general 本身不是代码型成品——它没有专属编排逻辑，只是"标准管线 + prompts/general.md
缺省人格"的数据型成品之一；music 等数据型入口同走本管线（差异全在 wire 携带的 bundle）。
仅当某类型需要数据表达不了的专属编排时，才另立 <type>.py 配方并经契约分派键选择。
"""

from __future__ import annotations

from langchain.agents.middleware import AgentMiddleware
from langchain_core.tools import BaseTool

from kokoro_agent.contract import RunRequest
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.mcp.tools import load_mcp_tools
from kokoro_agent.orchestration.assemble import (
    AssembleDeps,
    AssembledAgent,
    catalog_subagents,
    general_purpose_subagent,
    wire_subagents,
)
from kokoro_agent.orchestration.context import compose_system_prompt
from kokoro_agent.prompts import GENERAL_PERSONA
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend
from kokoro_agent.skills.mounts import render_skills_prompt
from kokoro_agent.model.factory import make_chat_model
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.export_artifact import make_export_artifact_tool
from kokoro_agent.tools.memory import make_memory_tools
from kokoro_agent.tools.middleware import (
    SteeringMiddleware,
    TerminalGuardMiddleware,
    TokenBudgetMiddleware,
    ToolPolicyMiddleware,
    ToolResultReviewMiddleware,
)
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.tools.registry import RESERVED_TOOL_NAMES, resolve_tools


async def assemble_general(deps: AssembleDeps, request: RunRequest) -> AssembledAgent:
    """每请求标准装配：工具解析 → 守卫 → 子代理 → 上下文 → 图（数据型成品共用）。"""
    runtime = request.runtime
    tools: list[BaseTool] = list(resolve_tools(runtime.tools))
    # 记忆工具是通用原语，隔离政策（租户 scope）在此注入——工具体不含租户概念。
    tools.extend(make_memory_tools(request.context.namespace))
    # 产物导出恒挂载：归属（run_id）与共享库在装配期注入。
    tools.append(make_export_artifact_tool(deps.artifacts, request.run_id))
    tools.extend(deps.web_tools)
    tools.extend(await load_mcp_tools(runtime.mcp))
    # ToolPolicyMiddleware fail-closed 全集：本次工具名 + deepagents 保留工具（文件/执行/todo/task）。
    authorized = frozenset(tool.name for tool in tools) | RESERVED_TOOL_NAMES
    approval_tools = frozenset(runtime.permissions.approval_tools)
    review_tools = frozenset(runtime.permissions.review_tools)
    if ASK_USER_TOOL_NAME in review_tools:
        # ask_user 的"结果"就是人工答复本身，再审即循环悖论。
        raise ValueError("ask_user cannot be a result-review tool")
    # 执行守卫（终态闸恒挂 + 预算闸按政策）：主 agent 与每个子代理同套下发——
    # 子代理 middleware 链独立，不下发即 task 委派旁路。
    guards: list[AgentMiddleware] = [
        TerminalGuardMiddleware(store=deps.ledger, run_id=request.run_id)
    ]
    if deps.run_token_budget > 0:
        guards.append(
            TokenBudgetMiddleware(
                budget=deps.run_token_budget, store=deps.ledger, run_id=request.run_id
            )
        )
    review_middleware = (
        ToolResultReviewMiddleware(review_tools, deps.ledger, request.run_id)
        if review_tools
        else None
    )
    # 子代理链：守卫 + 审核一并下发（审核不下发=委派旁路审核政策）；
    # 主链顺序保持 policy 在 review 外层，故审核在主链单独追加。
    subagent_guards: list[AgentMiddleware] = (
        [*guards, review_middleware] if review_middleware is not None else guards
    )
    tool_index = {tool.name: tool for tool in tools}
    catalog_defs, catalog_names = catalog_subagents(deps.catalog, tool_index, subagent_guards)
    # 委派执法声明集 = 真挂载的 catalog 子代理 + 本次 wire 预设。
    declared_subagents = catalog_names | frozenset(sub.name for sub in runtime.subagents)
    middleware: list[AgentMiddleware] = [
        *guards,
        # steering 只挂主链：插话是用户↔主 agent 的对话，注入子代理即语义污染。
        SteeringMiddleware(store=deps.ledger, run_id=request.run_id),
        ToolPolicyMiddleware(
            authorized,
            declared_subagents=declared_subagents,
            subagent_create=runtime.permissions.subagent_create,
        ),
    ]
    if review_middleware is not None:
        middleware.append(review_middleware)
    graph = build_agent(
        model=make_chat_model(deps.model, runtime.model),
        tools=tools,
        # 上下文组合：具名入口人格（wire）或通用成品人格 + skills 全文；
        # 工具用法活在各工具 description（LangChain 经工具 schema 交给模型），不进 system prompt。
        system_prompt=compose_system_prompt(
            runtime.system_prompt or GENERAL_PERSONA,
            render_skills_prompt(runtime.skills),
        ),
        subagents=[
            # 同名覆盖内生 general-purpose：守卫齐挂，可达性政策不变（不进 declared 集）。
            general_purpose_subagent(subagent_guards),
            *catalog_defs,
            *wire_subagents(
                request,
                tool_index,
                lambda spec: make_chat_model(deps.model, spec),
                subagent_guards,
            ),
        ],
        checkpointer=deps.checkpointer,
        permissions=build_filesystem_permissions(runtime.permissions.filesystem),
        interrupt_on=build_interrupt_on(
            approval_tools, subagent_create=runtime.permissions.subagent_create
        ),
        middleware=middleware,
        backend=make_backend(runtime.backend, deps.sandbox),
        # 长期记忆：后端随 checkpoint 对齐，工具侧按租户 namespace 前缀隔离。
        store=deps.memory_store,
    )
    return AssembledAgent(
        agent=graph,
        # 审批卡数据源：真挂载工具的自述（deepagents 保留工具不在册，wire 发空串由 web 兜底文案）。
        tool_descriptions={tool.name: tool.description for tool in tools if tool.description},
    )
