"""成品层：封装好的对外 agent 定义（每个成品一个子包，人格资源随包分发）。"""

from kokoro_agent.agents.general import GENERAL_ENTRY
from kokoro_agent.agents.entry import AgentEntry

__all__ = ["GENERAL_ENTRY", "AgentEntry"]
