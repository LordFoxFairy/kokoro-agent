from __future__ import annotations

from kokoro_agent.domain.registered_subagent import RegisteredSubagent
from kokoro_agent.infrastructure.subagent.catalog import (
    BUILT_IN_SUBAGENTS,
    normalize_definition,
    normalize_subagent_name,
)


class RuntimeSubagentRegistry:
    def __init__(self) -> None:
        self._subagents: dict[str, RegisteredSubagent] = {}

    def register(self, name: str, description: str, system_prompt: str) -> RegisteredSubagent:
        definition = normalize_definition(name, description, system_prompt)
        candidate = definition.name
        if candidate in {spec.name for spec in BUILT_IN_SUBAGENTS} or candidate in self._subagents:
            msg = f"duplicate or reserved subagent name: {candidate}"
            raise ValueError(msg)
        spec = RegisteredSubagent(
            name=candidate,
            description=definition.description,
            system_prompt=definition.system_prompt,
            source="runtime-custom",
        )
        self._subagents[candidate] = spec
        return spec

    def get(self, name: str) -> RegisteredSubagent | None:
        return self._subagents.get(normalize_subagent_name(name))

    def specs(self) -> list[RegisteredSubagent]:
        return list(self._subagents.values())
