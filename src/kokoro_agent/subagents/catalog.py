"""子代理目录：内建 + 配置自定义（JSON 经注入），source 标签解析单点。"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Annotated, Final

from deepagents.middleware.subagents import SubAgent
from pydantic import BaseModel, ConfigDict, StringConstraints, TypeAdapter

from kokoro_agent.contract import SubagentSource

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


@dataclass(frozen=True, slots=True)
class RegisteredSubagent:
    name: str
    description: str
    system_prompt: str
    source: SubagentSource


class _SubagentDefinition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: _NonEmpty
    description: _NonEmpty
    system_prompt: _NonEmpty


_CUSTOM_PAYLOADS: TypeAdapter[list[_SubagentDefinition]] = TypeAdapter(list[_SubagentDefinition])

# 内建目录只收"带真实工具挂载的真能力"；人格类预设归 namespace profile（wire 下发）。
# 现阶段无 web_search 等专属工具可挂 → 内建为空，不设能力与命名不符的空壳。
BUILT_IN_SUBAGENTS: Final[tuple[RegisteredSubagent, ...]] = ()


class SubagentCatalog:
    """按 name 索引的不可变子代理目录：唯一性校验在构建处一次性收口。"""

    __slots__ = ("_by_name",)

    def __init__(self, specs: Iterable[RegisteredSubagent]) -> None:
        by_name: dict[str, RegisteredSubagent] = {}
        for spec in specs:
            if spec.name in by_name:
                raise ValueError(f"duplicate or reserved subagent name: {spec.name}")
            by_name[spec.name] = spec
        self._by_name = by_name

    def names(self) -> frozenset[str]:
        # 委派执法的声明集来源：目录内即管理员声明，deny 策略下仍可被委派。
        return frozenset(self._by_name)

    def definitions(self) -> list[SubAgent]:
        return [
            {
                "name": spec.name,
                "description": spec.description,
                "system_prompt": spec.system_prompt,
            }
            for spec in self._by_name.values()
        ]

    def source_for(self, name: str) -> SubagentSource:
        spec = self._by_name.get(name.strip())
        # 目录之外的名字 = 运行期动态创建的子代理：契约一等来源，不是错误。
        return spec.source if spec is not None else "runtime-custom"


def build_catalog(custom_specs_json: str | None) -> SubagentCatalog:
    custom: list[RegisteredSubagent] = []
    if custom_specs_json:
        custom = [
            RegisteredSubagent(
                name=payload.name,
                description=payload.description,
                system_prompt=payload.system_prompt,
                source="config-custom",
            )
            for payload in _CUSTOM_PAYLOADS.validate_json(custom_specs_json)
        ]
    return SubagentCatalog((*BUILT_IN_SUBAGENTS, *custom))
