"""skills 资产库（Skills V2）：启动快照整包装载，供给期物化进 run 的 backend。

规范对齐 Anthropic agent skills（deepagents 原生 SkillsMiddleware 消费）：
每 skill 一目录，SKILL.md 必须带 YAML frontmatter（name 与目录同名、description
必填），可携任意辅助文本文件。渐进披露由原生中间件承担——prompt 只挂
name+description，agent 用到才读全文；本库不再向 system prompt 注入任何内容。
"""

from __future__ import annotations

from collections.abc import Mapping

import yaml
from pydantic import BaseModel, ConfigDict, TypeAdapter


class SkillAssetError(Exception):
    pass


class SkillFrontmatter(BaseModel):
    """SKILL.md 头部契约（规范必填面；其余键透传给原生中间件解析）。"""

    model_config = ConfigDict(strict=True, extra="allow")

    name: str
    description: str


_FRONTMATTER_ADAPTER: TypeAdapter[dict[str, object]] = TypeAdapter(dict[str, object])


def parse_frontmatter(name: str, skill_md: str) -> SkillFrontmatter:
    """装载期校验（fail-loud）：--- 包裹的 YAML 头、name 与目录同名、description 非空。"""
    if not skill_md.startswith("---"):
        raise SkillAssetError(f"skill {name!r}: SKILL.md missing YAML frontmatter (--- block)")
    parts = skill_md.split("---", 2)
    if len(parts) < 3:
        raise SkillAssetError(f"skill {name!r}: unterminated frontmatter block")
    raw = yaml.safe_load(parts[1])
    if not isinstance(raw, Mapping):
        raise SkillAssetError(f"skill {name!r}: frontmatter is not a mapping")
    meta = SkillFrontmatter.model_validate(_FRONTMATTER_ADAPTER.validate_python(raw))
    if meta.name != name:
        raise SkillAssetError(
            f"skill {name!r}: frontmatter name {meta.name!r} must match directory name"
        )
    if meta.description.strip() == "":
        raise SkillAssetError(f"skill {name!r}: frontmatter description must be non-empty")
    return meta


class SkillPackage(BaseModel):
    """一个 skill 的完整包：SKILL.md + 辅助文本文件（相对路径 → 全文）。"""

    model_config = ConfigDict(strict=True, frozen=True)

    name: str
    description: str
    files: Mapping[str, str]


class SkillLibrary:
    """不可变快照：空源=空库（用到未知名才报错）；内容进程期恒定（改资产=滚动重启）。"""

    def __init__(self, packages: Mapping[str, SkillPackage]) -> None:
        self._packages = dict(packages)

    def names(self) -> frozenset[str]:
        return frozenset(self._packages)

    def get(self, name: str) -> SkillPackage:
        package = self._packages.get(name)
        if package is None:
            raise SkillAssetError(
                f"skill {name!r} not in library (available: {sorted(self._packages)})"
            )
        return package


def build_packages(raw: Mapping[str, Mapping[str, str]]) -> dict[str, SkillPackage]:
    """源装载产物（name → {relpath: 全文}）→ 校验后的包快照；缺 SKILL.md/坏头 fail-loud。"""
    packages: dict[str, SkillPackage] = {}
    for name, files in raw.items():
        skill_md = files.get("SKILL.md")
        if skill_md is None:
            raise SkillAssetError(f"skill {name!r} has no SKILL.md")
        meta = parse_frontmatter(name, skill_md)
        packages[name] = SkillPackage(name=name, description=meta.description, files=dict(files))
    return packages
