"""产品 Feature 的静态 Agent 组装声明。"""

from __future__ import annotations

import re
from dataclasses import dataclass

from kokoro_agent.agents.definition import Agent

_KEY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


@dataclass(frozen=True, slots=True)
class Feature:
    """一个对外产品能力及其 Agent 组合；``entry_agent`` 是首个获得用户输入的 Agent。"""

    key: str
    agents: tuple[Agent, ...]
    entry_agent: str
    handoffs: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if _KEY.fullmatch(self.key) is None:
            raise ValueError(f"invalid feature key: {self.key!r}")
        if not self.agents:
            raise ValueError(f"feature {self.key!r} needs at least one agent")
        keys = tuple(agent.key for agent in self.agents)
        if len(set(keys)) != len(keys):
            raise ValueError(f"feature {self.key!r} has duplicate agents")
        if self.entry_agent not in keys:
            raise ValueError(f"feature {self.key!r} entry_agent is not one of its agents")
        backends = {agent.backend for agent in self.agents}
        if len(backends) > 1:
            raise ValueError(
                f"feature {self.key!r} must use one shared backend across its agents"
            )
        if len(self.agents) > 1 and not self.handoffs:
            raise ValueError(
                f"multi-agent feature {self.key!r} must declare official handoffs"
            )
        known = set(keys)
        for source, target in self.handoffs:
            if source not in known or target not in known:
                raise ValueError(f"feature {self.key!r} has an unknown handoff target")
            if source == target:
                raise ValueError(f"feature {self.key!r} has a self handoff")
        if len(set(self.handoffs)) != len(self.handoffs):
            raise ValueError(f"feature {self.key!r} has duplicate handoffs")
        if len(self.agents) == 1 and self.handoffs:
            raise ValueError(f"single-agent feature {self.key!r} cannot declare handoffs")
        if len(self.agents) > 1:
            reachable = {self.entry_agent}
            while True:
                expanded = reachable | {
                    target for source, target in self.handoffs if source in reachable
                }
                if expanded == reachable:
                    break
                reachable = expanded
            unreachable = known - reachable
            if unreachable:
                names = ", ".join(sorted(unreachable))
                raise ValueError(
                    f"feature {self.key!r} has unreachable agents from entry: {names}"
                )


__all__ = ["Feature"]
