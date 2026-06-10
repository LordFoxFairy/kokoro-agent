from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Final, cast

from langchain_core.language_models import BaseChatModel

from kokoro_agent.domain.subagent import RegisteredSubagent, SubagentSource

if TYPE_CHECKING:
    from deepagents.middleware.subagents import SubAgent

CUSTOM_SUBAGENTS_ENV = "KOKORO_CUSTOM_SUBAGENTS"


class RuntimeSubagentRegistry:
    def __init__(self) -> None:
        self._subagents: dict[str, RegisteredSubagent] = {}

    def register(self, name: str, description: str, system_prompt: str) -> RegisteredSubagent:
        candidate = name.strip()
        if not candidate or not description.strip() or not system_prompt.strip():
            msg = "runtime subagents require name, description, and system_prompt"
            raise ValueError(msg)
        if candidate in {spec.name for spec in BUILT_IN_SUBAGENTS} or candidate in self._subagents:
            msg = f"duplicate or reserved subagent name: {candidate}"
            raise ValueError(msg)
        spec = RegisteredSubagent(
            name=candidate,
            description=description.strip(),
            system_prompt=system_prompt.strip(),
            source="runtime-custom",
        )
        self._subagents[candidate] = spec
        return spec

    def get(self, name: str) -> RegisteredSubagent | None:
        return self._subagents.get(name)

    def specs(self) -> list[RegisteredSubagent]:
        return list(self._subagents.values())


BUILT_IN_SUBAGENTS: Final[tuple[RegisteredSubagent, ...]] = (
    RegisteredSubagent(
        name="researcher",
        description="查资料、做独立核查，并把结论反馈给主助手。",
        system_prompt=(
            "你是一个研究型子智能体。先独立核查，再用简洁中文返回结论；"
            "如果信息不足，就明确说不足，不要编造。"
        ),
        source="built-in",
    ),
)


def load_custom_subagents_from_env(env: dict[str, str] | None = None) -> list[RegisteredSubagent]:
    source = env if env is not None else dict(os.environ)
    raw = source.get(CUSTOM_SUBAGENTS_ENV)
    if not raw:
        return []

    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        msg = f"{CUSTOM_SUBAGENTS_ENV} must be a JSON array"
        raise ValueError(msg)

    items = cast("list[object]", parsed)
    built_in_names = {spec.name for spec in BUILT_IN_SUBAGENTS}
    custom: list[RegisteredSubagent] = []
    seen_names: set[str] = set()
    for item in items:
        if not isinstance(item, Mapping):
            msg = f"{CUSTOM_SUBAGENTS_ENV} items must be objects"
            raise ValueError(msg)
        payload = cast("Mapping[str, object]", item)
        name = str(payload.get("name") or "").strip()
        description = str(payload.get("description") or "").strip()
        system_prompt = str(payload.get("system_prompt") or "").strip()
        if not name or not description or not system_prompt:
            msg = f"{CUSTOM_SUBAGENTS_ENV} items require name, description, and system_prompt"
            raise ValueError(msg)
        if name in built_in_names or name in seen_names:
            msg = f"duplicate or reserved subagent name: {name}"
            raise ValueError(msg)
        seen_names.add(name)
        custom.append(
            RegisteredSubagent(
                name=name,
                description=description,
                system_prompt=system_prompt,
                source="config-custom",
            )
        )
    return custom


def runtime_subagent_specs(
    env: dict[str, str] | None = None,
    runtime_registry: RuntimeSubagentRegistry | None = None,
) -> list[RegisteredSubagent]:
    return [
        *BUILT_IN_SUBAGENTS,
        *load_custom_subagents_from_env(env),
        *(runtime_registry.specs() if runtime_registry is not None else []),
    ]


def materialize_runtime_subagents(
    model: BaseChatModel,
    env: dict[str, str] | None = None,
    runtime_registry: RuntimeSubagentRegistry | None = None,
) -> list["SubAgent"]:
    runtime: list["SubAgent"] = []
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
    for spec in runtime_subagent_specs(env, runtime_registry):
        if spec.name == name:
            return spec.source
    return "runtime-custom"
