"""General Agent：对话型通用能力（对外缺省能力）。

这里只声明该 Agent 的工具面和默认 prompt；共享构造由 ``AgentFactory`` 完成。
"""

from __future__ import annotations

from kokoro_agent.agents.definition import Agent
from kokoro_agent.prompts import GENERAL_PROMPT
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL, ASK_USER_TOOL_NAME

GENERAL_AGENT = Agent(
    key="general",
    prompt=GENERAL_PROMPT,
    tools=(ASK_USER_TOOL,),
    pause_tools=frozenset({ASK_USER_TOOL_NAME}),
)
