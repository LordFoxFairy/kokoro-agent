"""子代理目录：内建 + 配置自定义（JSON 经注入），source 标签解析单点。"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Annotated, Final

from deepagents.middleware.subagents import SubAgent
from pydantic import BaseModel, ConfigDict, StringConstraints, TypeAdapter

from kokoro_agent.contract import SubagentSource

from kokoro_agent.prompts import load_prompt

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


@dataclass(frozen=True, slots=True)
class RegisteredSubagent:
    name: str
    description: str
    system_prompt: str
    source: SubagentSource
    # 内建子代理的真实工具挂载（按名声明，装配点解析实例；缺任一即整个不挂——不设空壳）。
    tools: tuple[str, ...] = ()


class _SubagentDefinition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: _NonEmpty
    description: _NonEmpty
    system_prompt: _NonEmpty


_CUSTOM_PAYLOADS: TypeAdapter[list[_SubagentDefinition]] = TypeAdapter(list[_SubagentDefinition])

# 内建目录只收"带真实工具挂载的真能力"；prompt 类预设归 namespace profile（wire 下发）。
# 装配点按工具可用性过滤：声明的工具缺任一（如 web_search 未配 provider）则整个不挂。
BUILT_IN_SUBAGENTS: Final[tuple[RegisteredSubagent, ...]] = (
    RegisteredSubagent(
        name="web-researcher",
        description="联网检索并阅读网页，交叉核对后给出带来源链接的结论。",
        system_prompt=load_prompt("web-researcher"),
        source="built-in",
        tools=("web_search", "web_fetch"),
    ),
)


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

    def specs(self) -> tuple[RegisteredSubagent, ...]:
        return tuple(self._by_name.values())

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


def build_catalog(
    custom_specs_json: str | None, enabled_builtins: frozenset[str] = frozenset()
) -> SubagentCatalog:
    """enabled_builtins：内建按名显式启用（默认全关——用户裁定：实现但不默认挂）。"""
    unknown = enabled_builtins - {spec.name for spec in BUILT_IN_SUBAGENTS}
    if unknown:
        raise ValueError(f"unknown built-in subagents enabled: {sorted(unknown)}")
    builtins = tuple(spec for spec in BUILT_IN_SUBAGENTS if spec.name in enabled_builtins)
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
    return SubagentCatalog((*builtins, *custom))
