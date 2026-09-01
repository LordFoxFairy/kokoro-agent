"""Music Chat Feature：通用对话与音乐能力的 peer handoff 组合。"""

from kokoro_agent.agents.general import GENERAL_AGENT
from kokoro_agent.agents.music import MUSIC_AGENT
from kokoro_agent.features.definition import Feature

# 这是一个产品入口，不是新的 Agent 或角色。两个 Agent 仍保持各自完整、可独立运行；
# 只有该 Feature 需要同一会话内交接时，才声明 official Swarm 的 handoff 边。
MUSIC_CHAT_FEATURE = Feature(
    key="music_chat",
    agents=(GENERAL_AGENT, MUSIC_AGENT),
    entry_agent="general",
    handoffs=(("general", "music"), ("music", "general")),
)

__all__ = ["MUSIC_CHAT_FEATURE"]
