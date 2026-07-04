"""编排主配方：RunRequest + RuntimeConfig → InvokableAgent。

系统最重要的组装点：工具解析 → 守卫构造 → 子代理装配 → 上下文组合 → 图构建。
政策全部在此注入（租户 scope/审批集/预算/后端），工具与执行层保持通用原语。
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from deepagents.middleware.subagents import SubAgent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore

from kokoro_agent.agents import GENERAL_AGENT
from kokoro_agent.contract import ModelConfig, RunRequest
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.mcp.tools import load_mcp_tools
from kokoro_agent.model.factory import ChatModelSettings, make_chat_model
from kokoro_agent.orchestration.context import compose_system_prompt
from kokoro_agent.sandbox import SandboxSettings, build_filesystem_permissions, make_backend
from kokoro_agent.skills.mounts import render_skills_prompt
from kokoro_agent.storage.run_state import RunStateStore
from kokoro_agent.subagents import SubagentCatalog
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.memory import make_memory_tools
from kokoro_agent.tools.middleware import (
    TerminalGuardMiddleware,
    TokenBudgetMiddleware,
    ToolPolicyMiddleware,
    ToolResultReviewMiddleware,
)
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.tools.registry import (
    RESERVED_TOOL_NAMES,
    SUBAGENT_TOOL_NAME,
    resolve_tools,
)
from kokoro_agent.tools.web_fetch import make_web_fetch_tool
from kokoro_agent.tools.web_search import (
    SearchProviderSettings,
    make_search_provider,
    make_web_search_tool,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AssembleDeps:
    """进程级共享件：worker 启动时构建一次，逐请求复用。
    只收领域设置，不收整个 AppConfig（config 单点消费法则）。"""

    model: ChatModelSettings
    sandbox: SandboxSettings
    run_token_budget: int
    catalog: SubagentCatalog
    web_tools: tuple[BaseTool, ...]
    checkpointer: BaseCheckpointSaver[str]
    run_state: RunStateStore
    memory_store: BaseStore


def build_web_tools(
    *, fetch_allow_private: bool, search: SearchProviderSettings | None
) -> list[BaseTool]:
    # fetch 恒挂载（SSRF 政策来自进程配置）；search 配置即挂载——无 provider 不挂空壳。
    tools: list[BaseTool] = [make_web_fetch_tool(allow_private=fetch_allow_private)]
    if search is None:
        return tools
    tools.append(make_web_search_tool(make_search_provider(search)))
    return tools


def approval_names(request: RunRequest) -> frozenset[str]:
    # ask_user 恒为语义暂停点，须与审批工具一同纳入 pending 识别集合；
    # 委派策略为 ask 时 task 同样是暂停点。
    names = frozenset(request.runtime.permissions.approval_tools) | {ASK_USER_TOOL_NAME}
    if request.runtime.permissions.subagent_create == "ask":
        names |= {SUBAGENT_TOOL_NAME}
    return names


def wire_subagents(
    request: RunRequest,
    tool_index: Mapping[str, BaseTool],
    make_model: Callable[[ModelConfig], BaseChatModel],
    guards: Sequence[AgentMiddleware] = (),
) -> list[SubAgent]:
    """wire 子代理 → deepagents 定义：tools 按名解析为已挂载实例（未知名 fail-loud，
    绝不静默丢弃），model 经工厂实例化；二者缺省即继承主 agent。"""
    out: list[SubAgent] = []
    for spec in request.runtime.subagents:
        sub: SubAgent = {
            "name": spec.name,
            "description": spec.description,
            "system_prompt": spec.system_prompt,
        }
        if spec.tools:
            missing = sorted(set(spec.tools) - set(tool_index))
            if missing:
                raise ValueError(f"subagent {spec.name!r} declares unmounted tools: {missing}")
            sub["tools"] = [tool_index[name] for name in spec.tools]
        if spec.model is not None:
            sub["model"] = make_model(spec.model)
        if guards:
            # 子代理 middleware 链独立于主 agent：预算/终态闸必须逐个下发，否则 task 委派即旁路。
            sub["middleware"] = list(guards)
        out.append(sub)
    return out


def catalog_subagents(
    catalog: SubagentCatalog,
    tool_index: Mapping[str, BaseTool],
    guards: Sequence[AgentMiddleware] = (),
) -> tuple[list[SubAgent], frozenset[str]]:
    """内建/配置子代理 → deepagents 定义：声明工具缺任一即整个不挂（不设空壳），
    返回 (定义, 实际可委派名集)——deny 声明集只含真挂载者。"""
    subs: list[SubAgent] = []
    mounted: set[str] = set()
    for spec in catalog.specs():
        missing = sorted(set(spec.tools) - set(tool_index))
        if missing:
            LOGGER.info(
                "built-in subagent %r not mounted (tools unavailable: %s)", spec.name, missing
            )
            continue
        sub: SubAgent = {
            "name": spec.name,
            "description": spec.description,
            "system_prompt": spec.system_prompt,
        }
        if spec.tools:
            sub["tools"] = [tool_index[name] for name in spec.tools]
        if guards:
            sub["middleware"] = list(guards)
        subs.append(sub)
        mounted.add(spec.name)
    return subs, frozenset(mounted)


async def assemble_agent(deps: AssembleDeps, request: RunRequest) -> InvokableAgent:
    """每请求主配方（原 worker/main.build() 收编于此）。"""
    runtime = request.runtime
    tools: list[BaseTool] = list(resolve_tools(runtime.tools))
    # 记忆工具是通用原语，隔离政策（租户 scope）在此注入——工具体不含租户概念。
    tools.extend(make_memory_tools(request.context.namespace))
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
        TerminalGuardMiddleware(store=deps.run_state, run_id=request.run_id)
    ]
    if deps.run_token_budget > 0:
        guards.append(
            TokenBudgetMiddleware(
                budget=deps.run_token_budget, store=deps.run_state, run_id=request.run_id
            )
        )
    tool_index = {tool.name: tool for tool in tools}
    catalog_defs, catalog_names = catalog_subagents(deps.catalog, tool_index, guards)
    # 委派执法声明集 = 真挂载的 catalog 子代理 + 本次 wire 预设。
    declared_subagents = catalog_names | frozenset(sub.name for sub in runtime.subagents)
    middleware: list[AgentMiddleware] = [
        *guards,
        ToolPolicyMiddleware(
            authorized,
            declared_subagents=declared_subagents,
            subagent_create=runtime.permissions.subagent_create,
        ),
    ]
    if review_tools:
        middleware.append(ToolResultReviewMiddleware(review_tools, deps.run_state, request.run_id))
    return build_agent(
        model=make_chat_model(deps.model, runtime.model),
        tools=tools,
        # 上下文组合：具名入口人格（wire）或通用成品人格 + 条件指引 + skills 全文。
        system_prompt=compose_system_prompt(
            runtime.system_prompt or GENERAL_AGENT.persona,
            frozenset(tool_index),
            render_skills_prompt(runtime.skills),
        ),
        subagents=[
            *catalog_defs,
            *wire_subagents(
                request,
                tool_index,
                lambda spec: make_chat_model(deps.model, spec),
                guards,
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
