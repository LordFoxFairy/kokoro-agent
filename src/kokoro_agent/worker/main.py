"""进程入口：os.environ 在此读一次 → AppConfig → 每请求从 RuntimeConfig 装配 → Supervisor.serve。"""

from __future__ import annotations

import asyncio
import logging
import os
import socket

from deepagents.middleware.subagents import SubAgent
from dotenv import load_dotenv
from langchain.agents.middleware import AgentMiddleware
from langchain_core.tools import BaseTool

from kokoro_agent.config import AppConfig
from kokoro_agent.contract import REQUESTS_STREAM, RunRequest
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.prompts import SYSTEM_PROMPT
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.mcp.tools import load_mcp_tools
from kokoro_agent.run.context import RunContext
from kokoro_agent.model.factory import make_chat_model
from kokoro_agent.observability import trace_config
from kokoro_agent.sandbox import build_filesystem_permissions, make_backend
from kokoro_agent.skills.mounts import render_skills_prompt
from kokoro_agent.storage.checkpoints import make_checkpointer
from kokoro_agent.storage.memory_store import make_memory_store
from kokoro_agent.storage.run_state import make_run_state_store
from kokoro_agent.streams.factory import make_stream
from kokoro_agent.subagents import build_catalog
from kokoro_agent.tools.memory import make_memory_tools
from kokoro_agent.tools.web_fetch import make_web_fetch_tool
from kokoro_agent.tools.web_search import (
    SearchProviderSettings,
    make_search_provider,
    make_web_search_tool,
)
from kokoro_agent.tools.middleware import ToolPolicyMiddleware, ToolResultReviewMiddleware
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME
from kokoro_agent.tools.registry import RESERVED_TOOL_NAMES, SUBAGENT_TOOL_NAME
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.tools.registry import resolve_tools
from kokoro_agent.worker.supervisor import RunSupervisor

LOGGER = logging.getLogger(__name__)


def _consumer_name() -> str:
    # consumer-group 内的成员身份：主机+pid 保多 pod/多进程不撞名。
    return f"{socket.gethostname()}-{os.getpid()}"


def _wire_subagents(request: RunRequest) -> list[SubAgent]:
    # wire 子代理转 deepagents 定义；V1 只透传身份/提示，tools/model 后续接。
    return [
        {
            "name": spec.name,
            "description": spec.description,
            "system_prompt": spec.system_prompt,
        }
        for spec in request.runtime.subagents
    ]


def _approval_names(request: RunRequest) -> frozenset[str]:
    # ask_user 恒为语义暂停点，须与审批工具一同纳入 pending 识别集合；
    # 委派策略为 ask 时 task 同样是暂停点。
    names = frozenset(request.runtime.permissions.approval_tools) | {ASK_USER_TOOL_NAME}
    if request.runtime.permissions.subagent_create == "ask":
        names |= {SUBAGENT_TOOL_NAME}
    return names


def build_web_tools(config: AppConfig) -> list[BaseTool]:
    # fetch 恒挂载（SSRF 政策来自进程配置）；search 配置即挂载——无 provider 不挂空壳。
    tools: list[BaseTool] = [
        make_web_fetch_tool(allow_private=config.web_tools.fetch_allow_private)
    ]
    if config.web_tools.search_provider is None:
        return tools
    provider = make_search_provider(
        SearchProviderSettings(
            provider=config.web_tools.search_provider,
            api_key=config.web_tools.search_api_key,
            base_url=config.web_tools.search_url,
        )
    )
    tools.append(make_web_search_tool(provider))
    return tools


async def _serve(config: AppConfig) -> None:
    bus = make_stream(config.stream)
    catalog = build_catalog(config.custom_subagents_json)
    web_tools = build_web_tools(config)
    # 进程级共享 checkpointer + run 状态存储：sqlite 落盘跨重启，多 pod 靠共享存储去重/租约/终态认领。
    async with (
        make_checkpointer(config.checkpoint) as saver,
        make_run_state_store(config.run_state) as store,
        make_memory_store(config.checkpoint) as memory_store,
    ):

        async def build(request: RunRequest) -> InvokableAgent:
            runtime = request.runtime
            tools: list[BaseTool] = list(resolve_tools(runtime.tools))
            # 记忆工具是通用原语，隔离政策（租户 scope）在此注入——工具体不含租户概念。
            tools.extend(make_memory_tools(request.context.namespace))
            tools.extend(web_tools)
            tools.extend(await load_mcp_tools(runtime.mcp))
            # ToolPolicyMiddleware fail-closed 全集：本次工具名 + deepagents 保留工具（文件/执行/todo/task）。
            authorized = frozenset(tool.name for tool in tools) | RESERVED_TOOL_NAMES
            approval_tools = frozenset(runtime.permissions.approval_tools)
            review_tools = frozenset(runtime.permissions.review_tools)
            if ASK_USER_TOOL_NAME in review_tools:
                # ask_user 的"结果"就是人工答复本身，再审即循环悖论。
                raise ValueError("ask_user cannot be a result-review tool")
            # 委派执法声明集 = 内建 catalog + 本次 wire 预设；策略源 = permissions.subagent_create。
            declared_subagents = frozenset(catalog.names()) | frozenset(
                sub.name for sub in runtime.subagents
            )
            middleware: list[AgentMiddleware] = [
                ToolPolicyMiddleware(
                    authorized,
                    declared_subagents=declared_subagents,
                    subagent_create=runtime.permissions.subagent_create,
                )
            ]
            if review_tools:
                middleware.append(ToolResultReviewMiddleware(review_tools, store, request.run_id))
            # skills 全文注入 system prompt（backend 无关；渐进披露待沙箱供给，见 mounts.py）。
            skills_prompt = render_skills_prompt(runtime.skills)
            base_prompt = runtime.system_prompt or SYSTEM_PROMPT
            return build_agent(
                model=make_chat_model(config.model, runtime.model),
                tools=tools,
                # 具名入口（专业 agent 作主 agent）：session 解析预设人格上 wire，缺省用内置。
                system_prompt=f"{base_prompt}\n\n{skills_prompt}" if skills_prompt else base_prompt,
                subagents=[*catalog.definitions(), *_wire_subagents(request)],
                checkpointer=saver,
                permissions=build_filesystem_permissions(runtime.permissions.filesystem),
                interrupt_on=build_interrupt_on(
                    approval_tools, subagent_create=runtime.permissions.subagent_create
                ),
                middleware=middleware,
                backend=make_backend(runtime.backend, config.sandbox),
                # runtime context：工具/middleware 依赖注入面（namespace/session/run 身份）。
                context_schema=RunContext,
                # 长期记忆：后端随 checkpoint 对齐，工具侧按 RunContext.namespace 前缀隔离。
                store=memory_store,
            )

        supervisor = RunSupervisor(
            agent_builder=build,
            store=store,
            approval_tool_names=_approval_names,
            trace_factory=lambda request: trace_config(config.observability, request),
            source_for=catalog.source_for,
            consumer=_consumer_name(),
            heartbeat_s=config.lease_heartbeat_s,
            recursion_limit=config.recursion_limit,
        )
        LOGGER.info("kokoro-agent worker consuming %s as %s", REQUESTS_STREAM, _consumer_name())
        await supervisor.serve(bus)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    asyncio.run(_serve(AppConfig.from_env(os.environ)))


if __name__ == "__main__":
    main()
