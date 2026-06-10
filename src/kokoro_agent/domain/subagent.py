from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SubagentSource = Literal["built-in", "config-custom", "runtime-custom"]


@dataclass(frozen=True, slots=True)
class RegisteredSubagent:
    name: str
    description: str
    system_prompt: str
    source: SubagentSource
