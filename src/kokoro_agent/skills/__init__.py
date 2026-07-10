"""skills 一等域：包契约（package）+ 资产供给布局（supply）。

消费面在 tools/skills.py（find_skill/read_skill，渐进披露）；
装配期全量物化（provision）已随"挂载=逻辑授权"退役。
"""

from __future__ import annotations

from kokoro_agent.skills.package import (
    SkillAssetError,
    SkillFrontmatter,
    SkillLibrary,
    SkillPackage,
    build_packages,
    parse_frontmatter,
)
from kokoro_agent.skills.supply import SKILLS_ROOT, UploadCapableBackend

__all__ = [
    "SKILLS_ROOT",
    "SkillAssetError",
    "SkillFrontmatter",
    "SkillLibrary",
    "SkillPackage",
    "UploadCapableBackend",
    "build_packages",
    "parse_frontmatter",
]
