"""子代理目录：内建子代理 + 从环境变量加载的配置自定义子代理。"""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator, Mapping
from typing import Annotated, Final

from pydantic import BaseModel, ConfigDict, StringConstraints, TypeAdapter

from kokoro_agent.subagents.types import RegisteredSubagent, SubagentSource

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


class SubagentCatalog(Mapping[str, RegisteredSubagent]):
    """按 name 索引的不可变子代理目录：唯一性校验在构建处一次性收口。"""

    __slots__ = ("_by_name",)

    def __init__(self, specs: Iterable[RegisteredSubagent]) -> None:
        by_name: dict[str, RegisteredSubagent] = {}
        for spec in specs:
            if spec.name in by_name:
                msg = f"duplicate or reserved subagent name: {spec.name}"
                raise ValueError(msg)
            by_name[spec.name] = spec
        self._by_name = by_name

    def __getitem__(self, name: str) -> RegisteredSubagent:
        return self._by_name[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._by_name)

    def __len__(self) -> int:
        return len(self._by_name)

    def source_for(self, name: str) -> SubagentSource:
        spec = self._by_name.get(normalize_subagent_name(name))
        if spec is None:
            msg = f"unknown subagent name: {name.strip()}"
            raise ValueError(msg)
        return spec.source


def load_custom_subagents_from_env(env: Mapping[str, str] | None = None) -> list[RegisteredSubagent]:
    source = env if env is not None else os.environ
    raw = source.get(CUSTOM_SUBAGENTS_ENV)
    if not raw:
        return []

    payloads = _CUSTOM_PAYLOADS.validate_json(raw)
    custom = [
        RegisteredSubagent(
            name=payload.name,
            description=payload.description,
            system_prompt=payload.system_prompt,
            source="config-custom",
        )
        for payload in payloads
    ]
    # 借目录构建的唯一性校验拦截内部重名与内建保留名冲突，避免本处再造一份。
    SubagentCatalog((*BUILT_IN_SUBAGENTS, *custom))
    return custom
