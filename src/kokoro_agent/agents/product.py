"""入口成品的形状：可作主 agent 的封装定义（人格为身份核心；能力束由编排层按 wire 装配）。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AgentProduct:
    name: str
    description: str
    persona: str
