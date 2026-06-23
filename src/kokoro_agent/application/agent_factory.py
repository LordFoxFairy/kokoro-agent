"""装配层：把模型/工具/子代理/权限组装成一个可流式的 agent。"""

from __future__ import annotations

from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.application.protocols.agent import InvokableAgent
from kokoro_agent.infrastructure.tools import BUILT_IN_TOOLS
from kokoro_agent.infrastructure.agent_builder import make_deep_agent
from kokoro_agent.infrastructure.permission import gate_tools
from kokoro_agent.infrastructure.tools.runtime_subagent import build_runtime_custom_subagent_tool
from kokoro_agent.infrastructure.subagent import (
    RuntimeSubagentRegistry,
    materialize_runtime_subagents,
)
from kokoro_agent.application.prompts import SYSTEM_PROMPT


def build_agent(
    model: BaseChatModel,
    permission_mode: PermissionMode,
    run_id: str,
    runtime_registry: RuntimeSubagentRegistry,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> InvokableAgent:
    base_tools = (build_runtime_custom_subagent_tool(model, runtime_registry), *BUILT_IN_TOOLS)
    # 静态门控：default 档拦外部副作用工具；interrupt_on 接线留 R-approval。
    tools = gate_tools(base_tools, permission_mode)
    return make_deep_agent(
        model=model,
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
        subagents=materialize_runtime_subagents(model, runtime_registry=runtime_registry),
        checkpointer=checkpointer,
        # 当前不按档位限制文件系统：default 仅对外部敏感工具交互审批，不限制 FS;
        # FS 写工具属 deepagents 中间件、FilesystemPermission 仅 allow/deny 无法审批化。
        permissions=[],
    )
