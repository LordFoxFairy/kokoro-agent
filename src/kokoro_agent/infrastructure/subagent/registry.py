"""运行时子代理注册表：进程内登记模型临时创建的子代理。"""

from __future__ import annotations

from kokoro_agent.domain.registered_subagent import RegisteredSubagent
from kokoro_agent.infrastructure.subagent.catalog import (
    BUILT_IN_SUBAGENTS,
    SubagentCatalog,
    load_custom_subagents_from_env,
    normalize_definition,
    normalize_subagent_name,
)

_BUILT_IN_NAMES = frozenset(spec.name for spec in BUILT_IN_SUBAGENTS)


class RuntimeSubagentRegistry:
    def __init__(self) -> None:
        self._subagents: dict[str, RegisteredSubagent] = {}

    def register(self, name: str, description: str, system_prompt: str) -> RegisteredSubagent:
        definition = normalize_definition(name, description, system_prompt)
        candidate = definition.name
        if candidate in _BUILT_IN_NAMES or candidate in self._subagents:
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

    def register_or_get(self, name: str, description: str, system_prompt: str) -> RegisteredSubagent:
        """幂等入口：同名同定义复用既有 spec，定义冲突或撞保留名则抛错。"""
        definition = normalize_definition(name, description, system_prompt)
        candidate = definition.name
        if candidate in {spec.name for spec in load_custom_subagents_from_env()}:
            msg = f"duplicate or reserved subagent name: {candidate}"
            raise ValueError(msg)
        existing = self._subagents.get(candidate)
        if existing is None:
            return self.register(definition.name, definition.description, definition.system_prompt)
        if (
            existing.description != definition.description
            or existing.system_prompt != definition.system_prompt
        ):
            msg = f"conflicting runtime subagent definition: {candidate}"
            raise ValueError(msg)
        return existing

    def get(self, name: str) -> RegisteredSubagent | None:
        return self._subagents.get(normalize_subagent_name(name))

    def catalog(self, env: dict[str, str] | None = None) -> SubagentCatalog:
        """合并内建/配置/运行时为不可变目录，跨源唯一性在构建处一次校验。"""
        return SubagentCatalog(
            (
                *BUILT_IN_SUBAGENTS,
                *load_custom_subagents_from_env(env),
                *self._subagents.values(),
            )
        )

    def specs(self) -> list[RegisteredSubagent]:
        return list(self._subagents.values())
