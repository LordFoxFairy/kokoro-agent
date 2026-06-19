"""子代理规格汇编：合并内建/配置/运行时来源并物化为 deepagents SubAgent。"""

from __future__ import annotations

from deepagents.middleware.subagents import SubAgent
from langchain_core.language_models import BaseChatModel

from kokoro_agent.domain.registered_subagent import RegisteredSubagent, SubagentSource
from kokoro_agent.infrastructure.subagent.catalog import (
    BUILT_IN_SUBAGENTS,
    load_custom_subagents_from_env,
    normalize_subagent_name,
)
from kokoro_agent.infrastructure.subagent.registry import RuntimeSubagentRegistry


def _validate_unique_catalog(specs: list[RegisteredSubagent]) -> list[RegisteredSubagent]:
    seen: set[str] = set()
    for spec in specs:
        if spec.name in seen:
            msg = f"duplicate or reserved subagent name: {spec.name}"
            raise ValueError(msg)
        seen.add(spec.name)
    return specs


def runtime_subagent_specs(
    env: dict[str, str] | None = None,
    runtime_registry: RuntimeSubagentRegistry | None = None,
) -> list[RegisteredSubagent]:
    return _validate_unique_catalog(
        [
            *BUILT_IN_SUBAGENTS,
            *load_custom_subagents_from_env(env),
            *(runtime_registry.specs() if runtime_registry is not None else []),
        ]
    )


def materialize_runtime_subagents(
    model: BaseChatModel,
    env: dict[str, str] | None = None,
    runtime_registry: RuntimeSubagentRegistry | None = None,
) -> list[SubAgent]:
    runtime: list[SubAgent] = []
    for spec in runtime_subagent_specs(env, runtime_registry):
        runtime.append(
            {
                "name": spec.name,
                "description": spec.description,
                "system_prompt": spec.system_prompt,
                "model": model,
                "tools": [],
            }
        )
    return runtime


def subagent_source_for(
    name: str,
    env: dict[str, str] | None = None,
    runtime_registry: RuntimeSubagentRegistry | None = None,
) -> SubagentSource:
    normalized_name = normalize_subagent_name(name)
    for spec in runtime_subagent_specs(env, runtime_registry):
        if spec.name == normalized_name:
            return spec.source
    msg = f"unknown subagent name: {normalized_name}"
    raise ValueError(msg)
