"""通用 agent 成品：Kokoro 的缺省主 agent（session 入口表的内建 general 引用此身份）。"""

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files


@dataclass(frozen=True, slots=True)
class AgentProduct:
    """入口成品：可作主 agent 的封装定义（人格为身份核心；能力束由编排层按 wire 装配）。"""

    name: str
    description: str
    persona: str


def _load_persona(resource: str) -> str:
    return files("kokoro_agent.agents").joinpath(resource).read_text(encoding="utf-8").strip()


GENERAL_AGENT = AgentProduct(
    name="general",
    description="通用协调 agent：组装并调度本空间全部能力。",
    persona=_load_persona("general.md"),
)
