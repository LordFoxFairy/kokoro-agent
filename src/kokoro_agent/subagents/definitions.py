"""子代理规格汇编：合并内建/配置声明来源，交给 DeepAgents 原生 SubAgent。"""

from __future__ import annotations

from deepagents.middleware.subagents import SubAgent

from kokoro_agent.subagents.types import RegisteredSubagent, SubagentSource
from kokoro_agent.subagents.catalog import (
    BUILT_IN_SUBAGENTS,
    SubagentCatalog,
    load_custom_subagents_from_env,
)


def _catalog(env: dict[str, str] | None) -> SubagentCatalog:
    return SubagentCatalog((*BUILT_IN_SUBAGENTS, *load_custom_subagents_from_env(env)))


def subagent_specs(
    env: dict[str, str] | None = None,
) -> list[RegisteredSubagent]:
    return list(_catalog(env).values())


def subagent_definitions(
    env: dict[str, str] | None = None,
) -> list[SubAgent]:
    return [
        {
            "name": spec.name,
            "description": spec.description,
            "system_prompt": spec.system_prompt,
        }
        for spec in _catalog(env).values()
    ]


def subagent_source_for(
    name: str,
    env: dict[str, str] | None = None,
) -> SubagentSource:
    return _catalog(env).source_for(name)
