"""可复用 Agent 定义。

这里仅导出静态能力声明；DeepAgents 的构造统一由
``kokoro_agent.agent_factory.AgentFactory`` 负责。
"""

from __future__ import annotations

from kokoro_agent.agents.definition import Agent
from kokoro_agent.agents.general import GENERAL_AGENT
from kokoro_agent.agents.music import MUSIC_AGENT

__all__ = ["Agent", "GENERAL_AGENT", "MUSIC_AGENT"]
