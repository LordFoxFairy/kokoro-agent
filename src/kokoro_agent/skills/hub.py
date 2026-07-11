"""Skills Hub（池的读写面）：Mongo 元数据+正文快照 × S3/local 包体权威源（内容寻址 zip）。

- 写入即生效（下一会话可见）；(scope,name) 唯一；文档级 revision CAS 防并发写半截。
- 内容锁真源 = 包体存储（zip 按 content_hash 寻址，不可变）：官方升级不影响
  已快照旧 hash 的进行中会话——正文当前版走 Mongo 快读，旧版走 zip 取回（双路）。
- 校验清单在 upsert 强制：坏包 fail-loud 不入库（含尖括号注入/路径穿越等安全项）。
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
import time
import zipfile
from collections.abc import Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Protocol

import boto3
from botocore.config import Config as BotoConfig
from mypy_boto3_s3 import S3Client
from pymongo import AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pydantic import BaseModel, ConfigDict, SecretStr, TypeAdapter

from kokoro_agent.contract.storage import (
    SKILL_STATE_COLLECTION,
    SKILLS_COLLECTION,
    SkillDoc,
    SkillFileEntry,
    skills_doc_adapter,
)
from kokoro_agent.sandbox.archive import LocalWorkspace, S3Workspace, StoreLocation
from kokoro_agent.skills.package import parse_frontmatter

OFFICIAL_SCOPE = "official"

_MANIFEST_ADAPTER: TypeAdapter[list[SkillFileEntry]] = TypeAdapter(list[SkillFileEntry])


class SkillHubError(Exception):
    pass


# --------------------------------------------------------------------------- #
# 校验清单（upsert 强制；每条都是安全/卫生边界，fail-loud）
# --------------------------------------------------------------------------- #

_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{1,63}$")
_RESERVED_NAMES = frozenset({"official", "skill", "skills", "general", "system"})
MAX_FILES = 100
MAX_PACKAGE_BYTES = 30 * 1024 * 1024
MAX_DESCRIPTION_LEN = 500


def validate_package(name: str, files: Mapping[str, str]) -> tuple[str, int]:
    """返回 (description, package_size)；任何违规抛 SkillHubError。"""
    if not _NAME_RE.fullmatch(name):
        raise SkillHubError(f"skill name {name!r} invalid (lowercase, digits, hyphen, 2-64 chars)")
    if name in _RESERVED_NAMES:
        raise SkillHubError(f"skill name {name!r} is reserved")
    if len(files) > MAX_FILES:
        raise SkillHubError(f"skill {name!r} has {len(files)} files (max {MAX_FILES})")
    size = 0
    for rel in files:
        path = Path(rel)
        if path.is_absolute() or ".." in path.parts:
            raise SkillHubError(f"skill {name!r} has unsafe path {rel!r}")  # 路径穿越
        size += len(files[rel].encode("utf-8"))
    if size > MAX_PACKAGE_BYTES:
        raise SkillHubError(f"skill {name!r} package too large ({size} bytes)")
    meta = parse_frontmatter(name, files.get("SKILL.md", ""))  # 缺/坏 SKILL.md fail-loud
    if len(meta.description) > MAX_DESCRIPTION_LEN:
        raise SkillHubError(f"skill {name!r} description too long")
    for field_name, value in (("name", name), ("description", meta.description)):
        if "<" in value or ">" in value:
            raise SkillHubError(f"skill {name!r} {field_name} contains angle brackets")  # 注入防线
    return meta.description, size


# --------------------------------------------------------------------------- #
# 包体存储（内容寻址 zip；local / s3 双实现,同一 Protocol）
# --------------------------------------------------------------------------- #


def package_ref(scope: str, name: str, content_hash: str) -> str:
    return f"skills/{scope}/{name}/{content_hash}.zip"


def _zip_files(files: Mapping[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for rel in sorted(files):
            archive.writestr(rel, files[rel])
    return buffer.getvalue()


def _unzip_files(data: bytes) -> dict[str, str]:
    out: dict[str, str] = {}
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            out[info.filename] = archive.read(info).decode("utf-8")
    return out


class PackageStore(Protocol):
    """内容寻址存取：同 ref 幂等（已存在跳过），永不覆盖异内容。"""

    async def put(self, ref: str, data: bytes) -> None: ...

    async def get(self, ref: str) -> bytes: ...


class LocalPackageStore:
    def __init__(self, root: str) -> None:
        self._root = Path(root)

    async def put(self, ref: str, data: bytes) -> None:
        target = self._root / ref
        if target.exists():  # 内容寻址：同 ref 即同内容，幂等跳过。
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(target.write_bytes, data)

    async def get(self, ref: str) -> bytes:
        target = self._root / ref
        if not target.exists():
            raise SkillHubError(f"package {ref!r} not found in local store")
        return await asyncio.to_thread(target.read_bytes)


class S3Credentials(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    access_key: SecretStr
    secret_key: SecretStr


class S3PackageStore:
    """boto3 同步客户端（线程安全），异步经 to_thread；minio/AWS/R2 仅 endpoint 之差。"""

    def __init__(self, config: S3Workspace, credentials: S3Credentials) -> None:
        self._bucket = config.bucket
        self._client: S3Client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            region_name=config.region,
            aws_access_key_id=credentials.access_key.get_secret_value(),
            aws_secret_access_key=credentials.secret_key.get_secret_value(),
            config=BotoConfig(s3={"addressing_style": "path" if config.force_path_style else "auto"}),
        )

    async def put(self, ref: str, data: bytes) -> None:
        def _put() -> None:
            try:
                self._client.head_object(Bucket=self._bucket, Key=ref)
                return  # 内容寻址：已存在=同内容，幂等跳过。
            except Exception:
                pass
            self._client.put_object(Bucket=self._bucket, Key=ref, Body=data)

        await asyncio.to_thread(_put)

    async def get(self, ref: str) -> bytes:
        def _get() -> bytes:
            response = self._client.get_object(Bucket=self._bucket, Key=ref)
            return response["Body"].read()

        try:
            return await asyncio.to_thread(_get)
        except Exception as exc:
            raise SkillHubError(f"package {ref!r} not found in s3 store: {exc}") from exc


def make_package_store(location: StoreLocation, credentials: S3Credentials | None) -> PackageStore:
    if isinstance(location, LocalWorkspace):
        return LocalPackageStore(location.root)
    if credentials is None:
        raise SkillHubError("hub s3 store requires credentials (env-only, ADR-010)")
    return S3PackageStore(location, credentials)


# --------------------------------------------------------------------------- #
# Hub（Mongo 元数据 + 包体存储 组合）
# --------------------------------------------------------------------------- #


def content_hash_of(files: Mapping[str, str]) -> str:
    canonical = json.dumps(dict(sorted(files.items())), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _now_ms() -> int:
    return int(time.time() * 1000)


class SkillHub:
    def __init__(
        self,
        skills: AsyncCollection[dict[str, object]],
        state: AsyncCollection[dict[str, object]],
        packages: PackageStore,
    ) -> None:
        self._skills = skills
        self._state = state
        self._packages = packages
        self._indexed = False

    async def _ensure_indexes(self) -> None:
        if self._indexed:
            return
        await self._skills.create_index([("scope", 1), ("name", 1)], unique=True)
        await self._state.create_index([("namespace", 1), ("name", 1)], unique=True)
        self._indexed = True

    # --- 写面 ---

    async def upsert(
        self, scope: str, name: str, files: Mapping[str, str], *, source: str = "deploy"
    ) -> SkillDoc:
        await self._ensure_indexes()
        description, package_size = validate_package(name, files)
        digest = content_hash_of(files)
        current = await self._skills.find_one({"scope": scope, "name": name})
        if current is not None and current.get("content_hash") == digest and current.get("deleted_at") is None:
            return skills_doc_adapter.validate_python(_strip_id(current))  # 幂等：hash 未变不写。
        ref = package_ref(scope, name, digest)
        await self._packages.put(ref, _zip_files(files))  # 包体先落（内容寻址，失败则元数据不动）。
        manifest = [
            SkillFileEntry(path=rel, size=len(files[rel].encode("utf-8"))) for rel in sorted(files)
        ]
        revision_raw = current.get("revision") if current is not None else None
        base_revision = revision_raw if isinstance(revision_raw, int) else 0
        update = {
            "description": description,
            "skill_md": files["SKILL.md"],
            "files_manifest": [entry.model_dump() for entry in manifest],
            "file_count": len(files),
            "package_size": package_size,
            "content_hash": digest,
            "package_ref": ref,
            "source": source,
            "revision": base_revision + 1,
            "updated_at": _now_ms(),
            "deleted_at": None,
        }
        # CAS：revision 匹配才写；竞争者先写则本次失败（fail-loud，调用方可重试）。
        query = (
            {"scope": scope, "name": name, "revision": base_revision}
            if current is not None
            else {"scope": scope, "name": name}
        )
        written = await self._skills.find_one_and_update(
            query,
            {
                "$set": update,
                "$setOnInsert": {
                    "scope": scope,
                    "name": name,
                    "official_enabled": True,
                    "official_required": False,
                },
            },
            upsert=current is None,
            return_document=ReturnDocument.AFTER,
        )
        if written is None:
            raise SkillHubError(f"concurrent write conflict on skill {scope}/{name}")
        return skills_doc_adapter.validate_python(_strip_id(written))

    async def mark_deleted(self, scope: str, name: str) -> None:
        await self._ensure_indexes()
        await self._skills.update_one(
            {"scope": scope, "name": name}, {"$set": {"deleted_at": _now_ms()}}
        )

    async def set_official_flags(
        self, name: str, *, enabled: bool | None = None, required: bool | None = None
    ) -> None:
        """官方状态位（管理面）：enabled=全局上架开关；required=恒注入且拒绝用户关闭。"""
        await self._ensure_indexes()
        update: dict[str, object] = {}
        if enabled is not None:
            update["official_enabled"] = enabled
        if required is not None:
            update["official_required"] = required
        if update:
            await self._skills.update_one(
                {"scope": OFFICIAL_SCOPE, "name": name}, {"$set": update}
            )

    async def set_enabled(self, namespace: str, name: str, *, enabled: bool) -> None:
        """per-user 启停偏好（独立表，不改共享本体）；required 技能拒绝关闭。"""
        await self._ensure_indexes()
        doc = await self._skills.find_one(
            {"scope": OFFICIAL_SCOPE, "name": name, "deleted_at": None}
        )
        if doc is not None and doc.get("official_required") and not enabled:
            raise SkillHubError(f"skill {name!r} is required and cannot be disabled")
        await self._state.update_one(
            {"namespace": namespace, "name": name},
            {"$set": {"enabled": enabled, "updated_at": _now_ms()}},
            upsert=True,
        )

    # --- 读面（池查询/管理面权威在 kokoro-hub；agent 只按会话快照卡 (scope,name,hash) 直读）---

    async def read_body(self, scope: str, name: str, content_hash: str | None = None) -> str:
        """SKILL.md 正文双路：hash 缺省/等于当前版 → Mongo 快读；旧版 → 包体 zip 取回。
        scope 由会话快照卡定死（同名跨 scope 不再猜测），取包按此 scope 拼 ref。"""
        doc = await self._find_one(scope, name)
        if doc is None:
            raise SkillHubError(f"skill {scope}/{name} not found")
        if content_hash is None or content_hash == doc.get("content_hash"):
            return str(doc["skill_md"])
        files = await self.load_package(scope, name, content_hash)
        return files["SKILL.md"]

    async def load_package(self, scope: str, name: str, content_hash: str) -> dict[str, str]:
        """整包取回（物化/旧版正文）：内容寻址，官方升级后旧 hash 永远取得到。"""
        data = await self._packages.get(package_ref(scope, name, content_hash))
        return _unzip_files(data)

    async def load_package_if_assets(
        self, scope: str, name: str, content_hash: str
    ) -> dict[str, str] | None:
        """有附件（非 SKILL.md 文件）才取整包；纯文档包返回 None（不白走包体存储）。
        scope 由会话快照卡定死，取包按此 scope 拼 ref（同名跨 scope 归属不错位）。"""
        doc = await self._find_one(scope, name)
        if doc is None:
            raise SkillHubError(f"skill {scope}/{name} not found")
        if content_hash == doc.get("content_hash"):
            entries = _MANIFEST_ADAPTER.validate_python(doc.get("files_manifest") or [])
            if sorted(entry.path for entry in entries) == ["SKILL.md"]:
                return None
            return await self.load_package(scope, name, content_hash)
        # 旧版（会话内容锁场景）：manifest 只反映当前版，直接取包判断。
        files = await self.load_package(scope, name, content_hash)
        return None if sorted(files) == ["SKILL.md"] else files

    async def _find_one(self, scope: str, name: str) -> dict[str, object] | None:
        """(scope,name) 直取活文档（快照卡定死归属的读路径）。"""
        await self._ensure_indexes()
        return await self._skills.find_one({"scope": scope, "name": name, "deleted_at": None})


def _strip_id(doc: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in doc.items() if key != "_id"}


# --------------------------------------------------------------------------- #
# 装配与生命周期
# --------------------------------------------------------------------------- #


class SkillHubSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    mongo_url: str
    mongo_db: str
    packages: LocalWorkspace | S3Workspace
    s3_access_key: SecretStr | None = None
    s3_secret_key: SecretStr | None = None


@asynccontextmanager
async def make_skill_hub(settings: SkillHubSettings) -> AsyncGenerator[SkillHub, None]:
    credentials = (
        S3Credentials(access_key=settings.s3_access_key, secret_key=settings.s3_secret_key)
        if settings.s3_access_key is not None and settings.s3_secret_key is not None
        else None
    )
    packages = make_package_store(settings.packages, credentials)
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(settings.mongo_url)
    try:
        yield SkillHub(
            client[settings.mongo_db][SKILLS_COLLECTION],
            client[settings.mongo_db][SKILL_STATE_COLLECTION],
            packages,
        )
    finally:
        await client.close()


async def seed_official(hub: SkillHub, packages: Mapping[str, Mapping[str, str]]) -> None:
    """部署目录 → hub：目录只是 seed 输入，真源是库+包体（幂等，hash 未变不写）。"""
    for name, files in packages.items():
        await hub.upsert(OFFICIAL_SCOPE, name, files, source="deploy")
