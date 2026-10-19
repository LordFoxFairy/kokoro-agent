"""Skill-specific manifest policy over the shared bounded archive reader."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from types import MappingProxyType
from typing import NoReturn
from kokoro_agent.infrastructure.package_archive import (
    PackageArchiveError,
    read_package_archive,
    PackageMember,
)


class SkillPackageError(ValueError):
    """Stable Skill package failure; never includes package content."""


def _fail(reason: str) -> NoReturn:
    raise SkillPackageError(reason)


def validate_package(
    payload: bytes, *, manifest_identity: str | None
) -> Mapping[str, bytes]:
    failure: str | None = None
    members: Mapping[str, PackageMember] | None = None
    try:
        members = read_package_archive(payload)
    except PackageArchiveError as error:
        failure = str(error)
    if members is None:
        _fail(failure or "package_zip_invalid")
    files = {
        name: member.content for name, member in members.items() if not member.directory
    }
    if "SKILL.md" not in files:
        _fail("package_manifest_invalid")
    raw_markdown = files["SKILL.md"]
    if len(raw_markdown) > 1048576:
        _fail("package_manifest_invalid")
    try:
        raw_markdown.decode("utf-8")
    except UnicodeError:
        _fail("package_manifest_invalid")
    identity = "skill-md-v1:sha256:" + hashlib.sha256(raw_markdown).hexdigest()
    if manifest_identity is not None and manifest_identity != identity:
        _fail("package_identity_mismatch")
    return MappingProxyType(files)
