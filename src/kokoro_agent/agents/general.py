"""general 工厂：对话型通用 agent（对外缺省类型）。

纯政策声明——工具面 core_tools/pause_tools/plan_tools + 末级缺省 default_prompt；装配管线由
AgentFactory 模板方法（assemble_agent）共享。需要专属编排的新类型（studio 系）另立
一个 <type>.py 工厂类，同样只声明政策。
"""

from __future__ import annotations

from typing import ClassVar

from langchain_core.tools import StructuredTool

from kokoro_agent.agents.base import AgentFactory
from kokoro_agent.contract import AgentType
from kokoro_agent.prompts import GENERAL_PROMPT
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL, ASK_USER_TOOL_NAME
from kokoro_agent.tools.propose_plan import PROPOSE_PLAN_TOOL, PROPOSE_PLAN_TOOL_NAME


class GeneralAgentFactory(AgentFactory):
    name: ClassVar[AgentType] = "general"
    # 对话型工具面政策：人机问答 + 用户计划提案恒挂，各自使用专用决策集。
    core_tools: ClassVar[tuple[StructuredTool, ...]] = (ASK_USER_TOOL, PROPOSE_PLAN_TOOL)
    pause_tools: ClassVar[frozenset[str]] = frozenset({ASK_USER_TOOL_NAME})
    plan_tools: ClassVar[frozenset[str]] = frozenset({PROPOSE_PLAN_TOOL_NAME})
    default_prompt: ClassVar[str] = GENERAL_PROMPT
