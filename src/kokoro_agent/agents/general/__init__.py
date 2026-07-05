"""general 业务包：对话型通用 agent（对外缺省类型）。

自包含：recipe.py 装配配方 + persona.md 人格资产；chat 面政策=ask_user 恒暂停。
"""

from __future__ import annotations

from kokoro_agent.agents.general.recipe import GENERAL_PERSONA, assemble_general
from kokoro_agent.agents.package import AgentTypePackage
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL_NAME

GENERAL_PACKAGE = AgentTypePackage(
    name="general",
    assemble=assemble_general,
    pause_tools=frozenset({ASK_USER_TOOL_NAME}),
)

__all__ = ["GENERAL_PACKAGE", "GENERAL_PERSONA"]
