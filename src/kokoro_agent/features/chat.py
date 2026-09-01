"""Chat Feature：默认的通用对话入口。"""

from kokoro_agent.agents.general import GENERAL_AGENT
from kokoro_agent.features.definition import Feature

CHAT_FEATURE = Feature(key="chat", agents=(GENERAL_AGENT,), entry_agent="general")

__all__ = ["CHAT_FEATURE"]
