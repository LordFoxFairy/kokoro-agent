"""类型包声明形状：配方 + 类型政策（工具面差异全在包声明，机制层零 if）。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from kokoro_agent.agents.parts import AssembleDeps, AssembledAgent
from kokoro_agent.contract import AgentType, RunRequest

Assembler = Callable[[AssembleDeps, RunRequest], Awaitable[AssembledAgent]]


@dataclass(frozen=True, slots=True)
class AgentTypePackage:
    name: AgentType
    assemble: Assembler
    # chat 面暂停工具：恒进 interrupt/pending 识别集（general={ask_user}；studio 类型=∅）。
    pause_tools: frozenset[str]
