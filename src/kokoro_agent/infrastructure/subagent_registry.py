from __future__ import annotations

import json
import os
from typing import Annotated, Final

from deepagents.middleware.subagents import SubAgent
from langchain_core.language_models import BaseChatModel
from pydantic import BaseModel, ConfigDict, StringConstraints, TypeAdapter

from kokoro_agent.domain.subagent import RegisteredSubagent, SubagentSource

CUSTOM_SUBAGENTS_ENV = "KOKORO_CUSTOM_SUBAGENTS"

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class _CustomSubagentPayload(BaseModel):
    # 外部不可信 env JSON：strict 拒非 str、extra=forbid 拒未知键（§5 strict 拒收）。
    model_config = ConfigDict(strict=True, extra="forbid")
    name: _NonEmpty
    description: _NonEmpty
    system_prompt: _NonEmpty


_CUSTOM_PAYLOADS = TypeAdapter(list[_CustomSubagentPayload])


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

    payloads = _CUSTOM_PAYLOADS.validate_python(json.loads(raw))
    built_in_names = {spec.name for spec in BUILT_IN_SUBAGENTS}
    custom: list[RegisteredSubagent] = []
    seen_names: set[str] = set()
    for payload in payloads:
        # 重名/保留是跨条目约束，pydantic 管不到，留手工 fail-loud。
        if payload.name in built_in_names or payload.name in seen_names:
            msg = f"duplicate or reserved subagent name: {payload.name}"
            raise ValueError(msg)
        seen_names.add(payload.name)
        custom.append(
            RegisteredSubagent(
                name=payload.name,
                description=payload.description,
                system_prompt=payload.system_prompt,
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
    for spec in runtime_subagent_specs(env, runtime_registry):
        if spec.name == name:
            return spec.source
    return "runtime-custom"
