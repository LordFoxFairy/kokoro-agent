"""共享装配积木：类型无关的工具面/中间件/子代理/prompt 四步与总管线。"""

from __future__ import annotations

from kokoro_agent.agents.assembly.delegates import Delegates, build_delegates
from kokoro_agent.agents.assembly.guardrails import GuardChains, build_guard_chains
from kokoro_agent.agents.assembly.pipeline import assemble_agent
from kokoro_agent.agents.assembly.prompt import PresetNotFoundError, resolve_system_prompt
from kokoro_agent.agents.assembly.toolset import Toolset, build_toolset

__all__ = [
    "Delegates",
    "GuardChains",
    "PresetNotFoundError",
    "Toolset",
    "assemble_agent",
    "build_delegates",
    "build_guard_chains",
    "build_toolset",
    "resolve_system_prompt",
]
