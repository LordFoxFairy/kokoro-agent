"""子代理目录：内建子代理 + 从环境变量加载的配置自定义子代理。"""

from __future__ import annotations

import json
import os
from typing import Annotated, Final

from pydantic import BaseModel, ConfigDict, StringConstraints, TypeAdapter

from kokoro_agent.domain.registered_subagent import RegisteredSubagent

CUSTOM_SUBAGENTS_ENV = "KOKORO_CUSTOM_SUBAGENTS"

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class _SubagentDefinition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    name: _NonEmpty
    description: _NonEmpty
    system_prompt: _NonEmpty


_CUSTOM_PAYLOADS = TypeAdapter(list[_SubagentDefinition])


def normalize_subagent_name(name: str) -> str:
    normalized = name.strip()
    if not normalized:
        msg = "subagent name must not be blank"
        raise ValueError(msg)
    return normalized


def normalize_definition(name: str, description: str, system_prompt: str) -> _SubagentDefinition:
    return _SubagentDefinition(name=name, description=description, system_prompt=system_prompt)


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
