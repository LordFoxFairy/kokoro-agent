"""skills 一等域：包契约（package）+ hub（池的读写面）+ 资产供给布局（supply）。

池查询/管理面权威在 kokoro-hub（TS）；agent 只按会话快照卡 (scope,name,hash) 直读。
消费面：skill 工具（tools/skills.py，正文双路）与装配期物化（materialize.py，附件按需）。
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
from kokoro_agent.skills.materialize import (
    SkillMaterializerMiddleware,
    reconcile_skill_assets,
)
from kokoro_agent.skills.package import (
    SkillAssetError,
    SkillFrontmatter,
    parse_frontmatter,
)
from kokoro_agent.skills.supply import SKILLS_ROOT, MaterializeBackend

__all__ = [
    "OFFICIAL_SCOPE",
    "SKILLS_ROOT",
    "MaterializeBackend",
    "SkillAssetError",
    "SkillFrontmatter",
    "SkillHub",
    "SkillHubError",
    "SkillHubSettings",
    "SkillMaterializerMiddleware",
    "content_hash_of",
    "make_skill_hub",
    "parse_frontmatter",
    "reconcile_skill_assets",
    "seed_official",
    "validate_package",
]
