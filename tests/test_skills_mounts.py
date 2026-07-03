"""skill 挂载规格：SKILL.md 存在性 + sha256 lock fail-closed。"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from kokoro_agent.contract import SkillMount
from kokoro_agent.skills.mounts import SkillMountError, resolve_skill_mounts


def _make_skill(root: Path, name: str, body: bytes) -> tuple[str, str]:
    directory = root / name
    directory.mkdir()
    (directory / "SKILL.md").write_bytes(body)
    return str(directory), hashlib.sha256(body).hexdigest()


def test_empty_returns_empty() -> None:
    assert resolve_skill_mounts([]) == []


def test_multiple_valid_mounts(tmp_path: Path) -> None:
    path_a, lock_a = _make_skill(tmp_path, "a", b"alpha")
    path_b, lock_b = _make_skill(tmp_path, "b", b"beta")
    mounts = [
        SkillMount(name="a", path=path_a, lock=lock_a),
        SkillMount(name="b", path=path_b, lock=lock_b),
    ]
    assert resolve_skill_mounts(mounts) == [path_a, path_b]


def test_dedupe_preserves_order(tmp_path: Path) -> None:
    path_a, lock_a = _make_skill(tmp_path, "a", b"alpha")
    mounts = [
        SkillMount(name="a", path=path_a, lock=lock_a),
        SkillMount(name="a2", path=path_a, lock=lock_a),
    ]
    assert resolve_skill_mounts(mounts) == [path_a]


def test_lock_mismatch_raises(tmp_path: Path) -> None:
    path_a, _ = _make_skill(tmp_path, "a", b"alpha")
    mounts = [SkillMount(name="a", path=path_a, lock="0" * 64)]
    with pytest.raises(SkillMountError):
        resolve_skill_mounts(mounts)


def test_missing_skill_md_raises(tmp_path: Path) -> None:
    directory = tmp_path / "empty"
    directory.mkdir()
    mounts = [SkillMount(name="empty", path=str(directory), lock="0" * 64)]
    with pytest.raises(SkillMountError):
        resolve_skill_mounts(mounts)
