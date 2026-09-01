"""Music Feature：直接对外的 Music Agent 产品能力。"""

from kokoro_agent.agents.music import MUSIC_AGENT
from kokoro_agent.features.definition import Feature

MUSIC_FEATURE = Feature(key="music", agents=(MUSIC_AGENT,), entry_agent="music")

__all__ = ["MUSIC_FEATURE"]
