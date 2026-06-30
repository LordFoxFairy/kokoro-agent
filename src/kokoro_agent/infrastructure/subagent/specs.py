"""子代理规格汇编：合并内建/配置来源并物化为 deepagents SubAgent。"""

from __future__ import annotations

from deepagents.middleware.subagents import CompiledSubAgent
from langchain_core.language_models import BaseChatModel

from kokoro_agent.domain.registered_subagent import RegisteredSubagent, SubagentSource
from kokoro_agent.infrastructure.agent_builder import make_subagent_runnable
from kokoro_agent.infrastructure.subagent.catalog import (
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


def materialize_subagents(
    model: BaseChatModel,
    env: dict[str, str] | None = None,
) -> list[CompiledSubAgent]:
    # 预编译子代理：deepagents 用 dict["name"] 作图归属，v3 ACL 取 trigger_call_id 结构化归属。
    return [
        {
            "name": spec.name,
            "description": spec.description,
            "runnable": make_subagent_runnable(
                model, system_prompt=spec.system_prompt, name=spec.name
            ),
        }
        for spec in _catalog(env).values()
    ]


def subagent_source_for(
    name: str,
    env: dict[str, str] | None = None,
) -> SubagentSource:
    return _catalog(env).source_for(name)
