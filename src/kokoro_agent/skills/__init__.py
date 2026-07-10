"""skills 一等域：包契约（package）+ hub（池的读写面）+ 资产供给布局（supply）。

消费面：装配清单（hub.resolve_cards）与 skill 工具（tools/skills.py，正文双路+附件按需物化）。
"""

from __future__ import annotations

from kokoro_agent.skills.hub import (
    OFFICIAL_SCOPE,
    SkillHub,
    SkillHubError,
    SkillHubSettings,
    content_hash_of,
    make_skill_hub,
    seed_official,
    validate_package,
)
from kokoro_agent.skills.package import (
    SkillAssetError,
    SkillFrontmatter,
    parse_frontmatter,
)
from kokoro_agent.skills.supply import SKILLS_ROOT, UploadCapableBackend

__all__ = [
    "OFFICIAL_SCOPE",
    "SKILLS_ROOT",
    "SkillAssetError",
    "SkillFrontmatter",
    "SkillHub",
    "SkillHubError",
    "SkillHubSettings",
    "UploadCapableBackend",
    "content_hash_of",
    "make_skill_hub",
    "parse_frontmatter",
    "seed_official",
    "validate_package",
]
