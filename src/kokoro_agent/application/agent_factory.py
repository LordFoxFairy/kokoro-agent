"""装配层：把模型/工具/子代理/权限组装成一个可流式的 agent。"""

from __future__ import annotations

from collections.abc import Sequence

from langchain_core.language_models import BaseChatModel
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.infrastructure.tools import BUILT_IN_TOOLS
from kokoro_agent.infrastructure.agent_builder import EventStreamingAgent, make_deep_agent
from kokoro_agent.infrastructure.permission import (
    fs_permissions,
    gate_tools,
    gate_tools_interactive,
)
from kokoro_agent.infrastructure.tools.runtime_subagent import build_runtime_custom_subagent_tool
from kokoro_agent.infrastructure.subagent import (
    RuntimeSubagentRegistry,
    materialize_runtime_subagents,
)
from kokoro_agent.infrastructure.transport import StreamProtocol

SYSTEM_PROMPT = (
    "你是 Kokoro，一个温和、克制的助手。遇到多步任务时，先用 write_todos 列出计划"
    "并随进展更新；需要时调用可用工具（如 now 查当前时间、fetch_url 抓网页），"
    "必要时用 task 委派子智能体。回答简洁、清晰。"
)


def build_base_tools(
    model: BaseChatModel,
    runtime_registry: RuntimeSubagentRegistry,
) -> tuple[StructuredTool, ...]:
    return (
        build_runtime_custom_subagent_tool(model, runtime_registry),
        *BUILT_IN_TOOLS,
    )


def gate_tools_for_run(
    tools: Sequence[StructuredTool],
    permission_mode: PermissionMode,
    run_id: str,
    control_port: StreamProtocol | None,
) -> list[StructuredTool]:
    return (
        gate_tools_interactive(tools, permission_mode, run_id, control_port)
        if control_port is not None
        else gate_tools(tools, permission_mode)
    )


def build_agent(
    model: BaseChatModel,
    permission_mode: PermissionMode,
    run_id: str,
    control_port: StreamProtocol | None,
    runtime_registry: RuntimeSubagentRegistry,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> EventStreamingAgent:
    base_tools = build_base_tools(model, runtime_registry)
    tools = gate_tools_for_run(base_tools, permission_mode, run_id, control_port)
    return make_deep_agent(
        model=model,
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
        subagents=materialize_runtime_subagents(model, runtime_registry=runtime_registry),
        checkpointer=checkpointer,
        permissions=fs_permissions(permission_mode),
    )
