"""装配层：把模型/工具/子代理/权限组装成一个可流式的 agent。"""

from __future__ import annotations

from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver

from kokoro_agent.domain.run_request import PermissionMode
from kokoro_agent.application.protocols.agent import InvokableAgent
from kokoro_agent.infrastructure.tools import BUILT_IN_TOOLS
from kokoro_agent.infrastructure.agent_builder import make_deep_agent
from kokoro_agent.infrastructure.permission import build_interrupt_on
from kokoro_agent.infrastructure.subagent import materialize_subagents
from kokoro_agent.domain.prompts import SYSTEM_PROMPT


def build_agent(
    model: BaseChatModel,
    permission_mode: PermissionMode,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> InvokableAgent:
    # default 档审批外部能力；auto 档仍让 ask_user_question 走 input_required。
    return make_deep_agent(
        model=model,
        tools=BUILT_IN_TOOLS,
        system_prompt=SYSTEM_PROMPT,
        subagents=materialize_subagents(model),
        checkpointer=checkpointer,
        # FilesystemPermission 仅 allow/deny 不支持审批暂停，故不限制 FS。
        permissions=[],
        interrupt_on=build_interrupt_on(permission_mode),
    )
