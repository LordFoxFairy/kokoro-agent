"""Run-scoped immutable Skill packages resolved by the Hub RPC boundary.

Hub owns catalog metadata, revisions, and package storage. Agent receives an exact frozen
assembly, validates every streamed archive defensively, and exposes only immutable in-memory
packages to tools and materialization middleware. This module deliberately has no Hub database
or object-store read path.

`PackageStore` remains here temporarily as the narrow content-addressed store used by the
independent delivery tool; it is not a Skill/Hub authority.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import stat
from collections.abc import Mapping
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Protocol
from zipfile import BadZipFile, ZipFile

import boto3
from botocore.config import Config as BotoConfig
from pydantic import BaseModel, ConfigDict, SecretStr

from kokoro_agent.sandbox.archive import LocalWorkspace, S3Workspace, StoreLocation
from kokoro_agent.skills.package import SkillAssetError, parse_frontmatter

MAX_FILES = 100
MAX_PACKAGE_BYTES = 30 * 1024 * 1024
MAX_DESCRIPTION_LEN = 500
MAX_PATH_BYTES = 512
MAX_PATH_DEPTH = 16
MAX_COMPRESSION_RATIO = 100
_SKILL_NAME = re.compile(r"^[a-z][a-z0-9-]{1,63}$")
_RESERVED = frozenset({"official", "skill", "skills", "general", "system"})


class SkillHubError(Exception):
    """Stable Skill assembly failure surface; never carries artifact bytes."""


def content_hash_of(files: Mapping[str, str]) -> str:
    canonical = json.dumps(dict(sorted(files.items())), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_package(name: str, files: Mapping[str, str]) -> tuple[str, int]:
    """Validate the semantic Skill package contract after strict archive extraction."""
    if _SKILL_NAME.fullmatch(name) is None:
        raise SkillHubError(f"skill name {name!r} invalid")
    if name in _RESERVED:
        raise SkillHubError(f"skill name {name!r} is reserved")
    if len(files) < 1 or len(files) > MAX_FILES:
        raise SkillHubError(f"skill {name!r} file count invalid")
    size = sum(len(value.encode("utf-8")) for value in files.values())
    if size > MAX_PACKAGE_BYTES:
        raise SkillHubError(f"skill {name!r} package too large")
    try:
        meta = parse_frontmatter(name, files.get("SKILL.md", ""))
    except SkillAssetError as error:
        raise SkillHubError(f"skill {name!r} manifest invalid") from error
    if len(meta.description) > MAX_DESCRIPTION_LEN:
        raise SkillHubError(f"skill {name!r} description too long")
    if any(mark in value for value in (name, meta.description) for mark in ("<", ">")):
        raise SkillHubError(f"skill {name!r} metadata invalid")
    return meta.description, size


def package_from_zip(
    *,
    name: str,
    expected_description: str,
    expected_content_hash: str,
    data: bytes,
) -> dict[str, str]:
    """Decode one archive with traversal/link/bomb/collision defenses and exact content locks."""
    files: dict[str, str] = {}
    folded: set[str] = set()
    declared_total = 0
    try:
        with ZipFile(BytesIO(data), "r") as archive:
            entries = [entry for entry in archive.infolist() if not entry.is_dir()]
            if len(entries) < 1 or len(entries) > MAX_FILES:
                raise SkillHubError("skill artifact file count invalid")
            for entry in entries:
                path = _safe_archive_path(entry.filename)
                folded_path = path.casefold()
                if folded_path in folded:
                    raise SkillHubError("skill artifact contains duplicate paths")
                folded.add(folded_path)
                if entry.flag_bits & 0x1:
                    raise SkillHubError("skill artifact contains encrypted entries")
                file_type = stat.S_IFMT(entry.external_attr >> 16)
                if file_type not in (0, stat.S_IFREG):
                    raise SkillHubError("skill artifact contains non-regular entries")
                if entry.file_size < 0 or entry.compress_size < 0:
                    raise SkillHubError("skill artifact contains invalid sizes")
                if entry.file_size > 0 and (
                    entry.compress_size == 0
                    or entry.file_size > max(entry.compress_size, 1) * MAX_COMPRESSION_RATIO
                ):
                    raise SkillHubError("skill artifact compression ratio invalid")
                declared_total += entry.file_size
                if declared_total > MAX_PACKAGE_BYTES:
                    raise SkillHubError("skill artifact expands past package limit")
                raw = archive.read(entry, pwd=None)
                if len(raw) != entry.file_size:
                    raise SkillHubError("skill artifact entry length changed")
                files[path] = raw.decode("utf-8", errors="strict")
    except (BadZipFile, UnicodeDecodeError, RuntimeError, ValueError, OSError) as error:
        raise SkillHubError("skill artifact archive invalid") from error
    description, _ = validate_package(name, files)
    if description != expected_description or content_hash_of(files) != expected_content_hash:
        raise SkillHubError("skill artifact content lock mismatch")
    return files


def _safe_archive_path(value: str) -> str:
    if (
        not value
        or "\\" in value
        or "\x00" in value
        or value.startswith("/")
        or len(value.encode("utf-8")) > MAX_PATH_BYTES
    ):
        raise SkillHubError("skill artifact path invalid")
    path = PurePosixPath(value)
    parts = path.parts
    if (
        len(parts) < 1
        or len(parts) > MAX_PATH_DEPTH
        or any(part in ("", ".", "..") for part in parts)
        or ":" in parts[0]
    ):
        raise SkillHubError("skill artifact path invalid")
    return path.as_posix()


class SkillHub:
    """Immutable exact package set for one resolved execution assembly."""

    def __init__(self, packages: Mapping[tuple[str, str, str], Mapping[str, str]]) -> None:
        self._packages = {
            key: dict(files)
            for key, files in packages.items()
        }

    async def read_body(self, scope: str, name: str, content_hash: str | None = None) -> str:
        if content_hash is None:
            raise SkillHubError("skill content hash required")
        files = self._package(scope, name, content_hash)
        return files["SKILL.md"]

    async def load_package(self, scope: str, name: str, content_hash: str) -> dict[str, str]:
        return dict(self._package(scope, name, content_hash))

    async def load_package_if_assets(
        self, scope: str, name: str, content_hash: str
    ) -> dict[str, str] | None:
        files = self._package(scope, name, content_hash)
        return None if sorted(files) == ["SKILL.md"] else dict(files)

    def _package(self, scope: str, name: str, content_hash: str) -> Mapping[str, str]:
        files = self._packages.get((scope, name, content_hash))
        if files is None:
            raise SkillHubError(f"skill {scope}/{name} unavailable in resolved assembly")
        return files


class PackageStore(Protocol):
    async def put(self, ref: str, data: bytes) -> None: ...

    async def get(self, ref: str) -> bytes: ...


class LocalPackageStore:
    def __init__(self, root: str) -> None:
        self._root = Path(root)

    def _path(self, ref: str) -> Path:
        if ref.startswith("/") or "\\" in ref or any(part == ".." for part in ref.split("/")):
            raise SkillHubError("package ref unsafe")
        return self._root / ref

    async def put(self, ref: str, data: bytes) -> None:
        path = self._path(ref)
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    async def get(self, ref: str) -> bytes:
        try:
            return self._path(ref).read_bytes()
        except OSError as error:
            raise SkillHubError("package unavailable") from error


class S3Credentials(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    access_key: SecretStr
    secret_key: SecretStr


class S3PackageStore:
    def __init__(self, location: S3Workspace, credentials: S3Credentials) -> None:
        self._bucket = location.bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=location.endpoint,
            region_name=location.region,
            aws_access_key_id=credentials.access_key.get_secret_value(),
            aws_secret_access_key=credentials.secret_key.get_secret_value(),
            config=BotoConfig(
                s3={"addressing_style": "path" if location.force_path_style else "auto"}
            ),
        )

    async def put(self, ref: str, data: bytes) -> None:
        def put() -> None:
            try:
                self._client.head_object(Bucket=self._bucket, Key=ref)
                return
            except Exception:
                self._client.put_object(Bucket=self._bucket, Key=ref, Body=data)

        await asyncio.to_thread(put)

    async def get(self, ref: str) -> bytes:
        try:
            return await asyncio.to_thread(
                lambda: self._client.get_object(Bucket=self._bucket, Key=ref)["Body"].read()
            )
        except Exception as error:
            raise SkillHubError("package unavailable") from error


def make_package_store(location: StoreLocation, credentials: S3Credentials | None) -> PackageStore:
    if isinstance(location, LocalWorkspace):
        return LocalPackageStore(location.root)
    if credentials is None:
        raise SkillHubError("delivery s3 store requires credentials")
    return S3PackageStore(location, credentials)
