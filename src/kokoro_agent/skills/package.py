"""Skill 包格式：本地 fixture 写入时校验，运行时由只读 backend 提供。

规范对齐 Anthropic agent skills（deepagents 原生 SkillsMiddleware 消费）：
每 skill 一目录，SKILL.md 必须带 YAML frontmatter（name 与目录同名、description
必填），可携任意辅助文本文件。渐进披露由原生中间件承担——prompt 只挂
name+description，agent 用到才读全文；本库不再向 system prompt 注入任何内容。
"""

from __future__ import annotations

from collections.abc import Mapping

import yaml
from pydantic import BaseModel, ConfigDict, TypeAdapter


class SkillPackageError(Exception):
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
        raise SkillPackageError(f"skill {name!r}: SKILL.md missing YAML frontmatter (--- block)")
    parts = skill_md.split("---", 2)
    if len(parts) < 3:
        raise SkillPackageError(f"skill {name!r}: unterminated frontmatter block")
    raw = yaml.safe_load(parts[1])
    if not isinstance(raw, Mapping):
        raise SkillPackageError(f"skill {name!r}: frontmatter is not a mapping")
    meta = SkillFrontmatter.model_validate(_FRONTMATTER_ADAPTER.validate_python(raw))
    if meta.name != name:
        raise SkillPackageError(
            f"skill {name!r}: frontmatter name {meta.name!r} must match directory name"
        )
    if meta.description.strip() == "":
        raise SkillPackageError(f"skill {name!r}: frontmatter description must be non-empty")
    return meta
