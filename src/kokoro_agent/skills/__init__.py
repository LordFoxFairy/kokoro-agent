"""skills 一等域：包契约 + run-scoped immutable Hub assembly + 资产供给布局。

池查询/管理面权威在 kokoro-hub；Agent 不读取 Hub persistence。
消费面：skill 工具（tools/skills.py，正文双路）与装配期物化（materialize.py，附件按需）。
"""

from __future__ import annotations

from kokoro_agent.skills.hub import (
    SkillHub,
    SkillHubError,
    content_hash_of,
    package_from_zip,
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
    "SKILLS_ROOT",
    "MaterializeBackend",
    "SkillAssetError",
    "SkillFrontmatter",
    "SkillHub",
    "SkillHubError",
    "SkillMaterializerMiddleware",
    "content_hash_of",
    "package_from_zip",
    "parse_frontmatter",
    "reconcile_skill_assets",
    "validate_package",
]
