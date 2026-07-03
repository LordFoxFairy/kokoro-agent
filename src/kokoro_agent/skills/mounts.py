"""skill 挂载解析：SKILL.md 存在性与 sha256 lock 的 fail-closed 校验。"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path

from kokoro_agent.contract import SkillMount


class SkillMountError(Exception):
    pass


def resolve_skill_mounts(mounts: Sequence[SkillMount]) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for mount in mounts:
        skill_md = Path(mount.path) / "SKILL.md"
        if not skill_md.is_file():
            raise SkillMountError(f"skill {mount.name!r}: missing SKILL.md at {skill_md}")
        digest = hashlib.sha256(skill_md.read_bytes()).hexdigest()
        if digest != mount.lock:
            raise SkillMountError(
                f"skill {mount.name!r}: lock mismatch (expected {mount.lock}, got {digest})"
            )
        if mount.path not in seen:
            seen.add(mount.path)
            resolved.append(mount.path)
    return resolved
