"""共享装配积木：类型无关的工具面/中间件/子代理/prompt 四步与总管线。"""

from __future__ import annotations

from kokoro_agent.agents.assembly.delegates import Delegates, build_delegates
from kokoro_agent.agents.assembly.guardrails import GuardChains, build_guard_chains
from kokoro_agent.agents.assembly.pipeline import assemble_agent
from kokoro_agent.agents.assembly.prompt import PresetNotFoundError, resolve_system_prompt
from kokoro_agent.agents.assembly.swarm import (
    HANDOFF_TOOL_NAME,
    SwarmPersonaMiddleware,
    make_handoff_tool,
    swarm_candidates,
)
from kokoro_agent.agents.assembly.toolset import Toolset, build_toolset

__all__ = [
    "HANDOFF_TOOL_NAME",
    "Delegates",
    "GuardChains",
    "PresetNotFoundError",
    "SwarmPersonaMiddleware",
    "Toolset",
    "assemble_agent",
    "build_delegates",
    "build_guard_chains",
    "build_toolset",
    "make_handoff_tool",
    "resolve_system_prompt",
    "swarm_candidates",
]
