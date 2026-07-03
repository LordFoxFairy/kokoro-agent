"""skill 挂载：sha256 lock fail-closed 校验 + 全文渲染进 system prompt。

state/e2b 等虚拟 backend 下 deepagents 渐进披露（模型 read_file 宿主路径）读不到
SKILL.md（实证），V1 采用 backend 无关的全文注入；沙箱供给落地后可回归原生机制。
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path

from kokoro_agent.contract import SkillMount

SKILL_MAX_CHARS = 32_000


class SkillMountError(Exception):
    pass


def render_skills_prompt(mounts: Sequence[SkillMount]) -> str | None:
    """校验（存在性/lock/大小）并渲染 skills 段；空挂载返回 None（不占 prompt）。"""
    sections: list[str] = []
    seen: set[str] = set()
    for mount in mounts:
        if mount.path in seen:
            continue
        seen.add(mount.path)
        skill_md = Path(mount.path) / "SKILL.md"
        if not skill_md.is_file():
            raise SkillMountError(f"skill {mount.name!r}: missing SKILL.md at {skill_md}")
        raw = skill_md.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if digest != mount.lock:
            raise SkillMountError(
                f"skill {mount.name!r}: lock mismatch (expected {mount.lock}, got {digest})"
            )
        content = raw.decode("utf-8")
        if len(content) > SKILL_MAX_CHARS:
            raise SkillMountError(
                f"skill {mount.name!r}: SKILL.md exceeds {SKILL_MAX_CHARS} chars ({len(content)})"
            )
        sections.append(f"### Skill: {mount.name}\n\n{content.strip()}")
    if not sections:
        return None
    return "## Skills\n\n遵循以下技能的指引完成对应任务。\n\n" + "\n\n".join(sections)
