"""基础设施装配：把 LangChain 模型、工具、子代理与 middleware 组装成 agent。"""

from __future__ import annotations

from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver

from kokoro_agent.config import AppConfig, RuntimeSettings
from kokoro_agent.execution.agent_graph import make_deep_agent
from kokoro_agent.execution.prompts import SYSTEM_PROMPT
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.run.request import PermissionMode
from kokoro_agent.sandbox.backend import backend_from_settings
from kokoro_agent.subagents import subagent_definitions
from kokoro_agent.tools.middleware import ToolPolicyMiddleware
from kokoro_agent.tools.permissions import build_filesystem_permissions, build_interrupt_on
from kokoro_agent.tools.registry import BUILT_IN_TOOLS


def build_agent(
    model: BaseChatModel,
    permission_mode: PermissionMode,
    checkpointer: BaseCheckpointSaver[str] | None = None,
    runtime: RuntimeSettings | None = None,
) -> InvokableAgent:
    settings = runtime if runtime is not None else AppConfig.from_env().runtime
    # default 档通过原生 interrupt_on 做工具级审批，auto 档空映射跳过。
    return make_deep_agent(
        model=model,
        tools=BUILT_IN_TOOLS,
        system_prompt=SYSTEM_PROMPT,
        subagents=subagent_definitions(),
        checkpointer=checkpointer,
        permissions=build_filesystem_permissions(permission_mode),
        interrupt_on=build_interrupt_on(permission_mode),
        middleware=(ToolPolicyMiddleware(),),
        skills=settings.skills,
        memory=settings.memory,
        backend=backend_from_settings(settings),
    )
