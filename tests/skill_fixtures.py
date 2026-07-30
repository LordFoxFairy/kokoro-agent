from __future__ import annotations

from collections.abc import Mapping

from kokoro_agent.contract import SkillGrant
from kokoro_agent.skills.hub import SkillHub, content_hash_of

STYLE_MD = """---
name: style
description: 写作风格指南
---
先结论后论据。
"""
PDF_MD = """---
name: pdf
description: PDF 报告生成流程
---
处理数据并生成报告。
"""
STYLE_FILES = {"SKILL.md": STYLE_MD}
PDF_FILES = {"SKILL.md": PDF_MD, "make_report.py": "print('report')"}


def snapshot_grant(
    files: Mapping[str, str], name: str, scope: str = "official"
) -> SkillGrant:
    description_line = next(
        line.removeprefix("description: ")
        for line in files["SKILL.md"].splitlines()
        if line.startswith("description: ")
    )
    return SkillGrant(
        option_ref=f"skill:{scope}:{name}:{content_hash_of(files)[:12]}",
        name=name,
        content_hash=content_hash_of(files),
        description=description_line,
        scope=scope,
    )


def make_skill_hub(
    *entries: tuple[str, str, Mapping[str, str]],
) -> SkillHub:
    return SkillHub(
        {
            (scope, name, content_hash_of(files)): files
            for scope, name, files in entries
        }
    )
