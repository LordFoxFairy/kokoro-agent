"""Typed mTLS client for exact Hub execution assemblies and bounded Skill streams."""

from __future__ import annotations

import hashlib
import json
import errno
import os
import re
import shutil
import stat
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

import pyqwest
from connectrpc.errors import ConnectError
from pydantic import BaseModel, ConfigDict, Field

from kokoro.platform.capability.v1 import capability_catalog_pb2 as capability_pb
from kokoro.platform.capability.v1.capability_catalog_connect import HubRuntimeServiceClient
from kokoro_agent.contract import McpGrant, SkillGrant
from kokoro_agent.mcp.config import McpServerConfig
from kokoro_agent.skills.hub import (
    MAX_PACKAGE_BYTES,
    SkillHub,
    SkillHubError,
    content_hash_of,
    package_from_zip,
    validate_package,
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_CATALOG_REF = re.compile(r"^agent-catalog:sha256:[0-9a-f]{64}$")
_HOSTNAME = re.compile(r"^[A-Za-z0-9.-]{1,253}$")
_MAX_SKILLS = 64
_MAX_MCP = 64
_MAX_ARTIFACT_BYTES = 32 * 1024 * 1024
_MAX_ASSEMBLY_ARTIFACT_BYTES = 64 * 1024 * 1024
_MAX_ASSEMBLY_UNPACKED_BYTES = 128 * 1024 * 1024
_MAX_METADATA_BYTES = 4 * 1024 * 1024
_MAX_AUTHORIZATION_BYTES = 8 * 1024


class ExecutionAssemblyError(Exception):
    """Stable boundary error that never includes secret or remote response material."""

    def __init__(self, code: str = "HUB_EXECUTION_ASSEMBLY_FAILED") -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class ExecutionAssembly:
    agent_catalog_ref: str
    assembly_digest: str
    skills: SkillHub
    mcp_servers: Mapping[str, McpServerConfig]


class ExecutionAssemblyResolver(Protocol):
    async def resolve(
        self,
        namespace: str,
        agent_catalog_ref: str,
        skills: Sequence[SkillGrant],
        mcp_servers: Sequence[McpGrant],
    ) -> ExecutionAssembly: ...


class AsyncHubRuntimeClient(Protocol):
    async def resolve_execution_assembly(
        self,
        request: capability_pb.ResolveExecutionAssemblyRequest,
        *,
        timeout_ms: int | None = None,
    ) -> capability_pb.ResolveExecutionAssemblyResponse: ...

    def fetch_skill_artifact(
        self,
        request: capability_pb.FetchSkillArtifactRequest,
        *,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[capability_pb.FetchSkillArtifactResponse]: ...


class HubRuntimeSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    rpc_url: str
    server_name: str
    ca_file: str
    cert_file: str
    key_file: str
    artifact_cache_dir: str
    timeout_ms: int = Field(default=30_000, ge=100, le=30_000)


@dataclass(frozen=True, slots=True)
class _SkillManifest:
    grant: SkillGrant
    artifact_ref: str
    artifact_size: int
    artifact_sha256: str


class HubExecutionAssemblyClient:
    """Process-scoped transport; each run resolves one immutable assembly."""

    def __init__(
        self,
        settings: HubRuntimeSettings,
        *,
        client: AsyncHubRuntimeClient | None = None,
    ) -> None:
        self._timeout_ms = settings.timeout_ms
        self._cache = _ArtifactCache(Path(settings.artifact_cache_dir))
        address = _hub_address(settings.rpc_url, settings.server_name)
        if client is None:
            ca = _tls_file(settings.ca_file, "ca")
            cert = _tls_file(settings.cert_file, "cert")
            key = _tls_file(settings.key_file, "key", private=True)
            if b"BEGIN CERTIFICATE" not in ca or b"BEGIN CERTIFICATE" not in cert:
                raise ValueError("HUB_RUNTIME_TLS_CERTIFICATE_INVALID")
            if b"PRIVATE KEY" not in key:
                raise ValueError("HUB_RUNTIME_TLS_PRIVATE_KEY_INVALID")
            transport = pyqwest.HTTPTransport(
                tls_ca_cert=ca,
                tls_include_system_certs=False,
                tls_key=key,
                tls_cert=cert,
                http_version=pyqwest.HTTPVersion.HTTP2,
                enable_cookie_store=False,
            )
            client = HubRuntimeServiceClient(
                address,
                accept_compression=(),
                send_compression=None,
                timeout_ms=settings.timeout_ms,
                read_max_bytes=_MAX_METADATA_BYTES,
                http_client=pyqwest.Client(transport),
            )
        self._client = client

    async def resolve(
        self,
        namespace: str,
        agent_catalog_ref: str,
        skills: Sequence[SkillGrant],
        mcp_servers: Sequence[McpGrant],
    ) -> ExecutionAssembly:
        _validate_request(namespace, agent_catalog_ref, skills, mcp_servers)
        try:
            response = await self._client.resolve_execution_assembly(
                capability_pb.ResolveExecutionAssemblyRequest(
                    namespace=namespace,
                    agent_catalog_ref=agent_catalog_ref,
                    skill_grants=[_skill_selection(grant) for grant in skills],
                    mcp_grants=[_mcp_selection(grant) for grant in mcp_servers],
                ),
                timeout_ms=self._timeout_ms,
            )
        except ConnectError:
            raise ExecutionAssemblyError() from None
        except Exception:
            raise ExecutionAssemblyError() from None

        manifests = _validate_skill_response(agent_catalog_ref, skills, response)
        definitions, digest_mcp = _validate_mcp_response(mcp_servers, response)
        expected_digest = _assembly_digest(
            namespace=namespace,
            agent_catalog_ref=agent_catalog_ref,
            skills=manifests,
            mcp_servers=digest_mcp,
        )
        if response.assembly_digest != expected_digest:
            raise ExecutionAssemblyError("HUB_EXECUTION_ASSEMBLY_DIGEST_INVALID")

        packages: dict[tuple[str, str, str], Mapping[str, str]] = {}
        unpacked_total = 0
        for manifest in manifests:
            files = await self._cache.load_or_fetch(
                manifest,
                lambda manifest=manifest: self._fetch_artifact(
                    namespace, agent_catalog_ref, manifest
                ),
            )
            unpacked_total += sum(len(value.encode("utf-8")) for value in files.values())
            if unpacked_total > _MAX_ASSEMBLY_UNPACKED_BYTES:
                raise ExecutionAssemblyError("HUB_EXECUTION_ASSEMBLY_UNPACKED_BUDGET_EXCEEDED")
            key = (manifest.grant.scope, manifest.grant.name, manifest.grant.content_hash)
            packages[key] = files
        return ExecutionAssembly(
            agent_catalog_ref=agent_catalog_ref,
            assembly_digest=response.assembly_digest,
            skills=SkillHub(packages),
            mcp_servers=MappingProxyType(definitions),
        )

    async def _fetch_artifact(
        self, namespace: str, agent_catalog_ref: str, manifest: _SkillManifest
    ) -> bytes:
        request = capability_pb.FetchSkillArtifactRequest(
            namespace=namespace,
            agent_catalog_ref=agent_catalog_ref,
            grant=_skill_selection(manifest.grant),
            artifact_ref=manifest.artifact_ref,
            expected_size=manifest.artifact_size,
            expected_sha256=manifest.artifact_sha256,
        )
        payload = bytearray()
        try:
            async for chunk in self._client.fetch_skill_artifact(
                request, timeout_ms=self._timeout_ms
            ):
                if (
                    chunk.artifact_ref != manifest.artifact_ref
                    or chunk.offset != len(payload)
                    or not chunk.data
                    or len(payload) + len(chunk.data) > manifest.artifact_size
                    or len(payload) + len(chunk.data) > _MAX_ARTIFACT_BYTES
                ):
                    raise ExecutionAssemblyError("HUB_SKILL_ARTIFACT_STREAM_INVALID")
                payload.extend(chunk.data)
        except ExecutionAssemblyError:
            raise
        except ConnectError:
            raise ExecutionAssemblyError("HUB_SKILL_ARTIFACT_FETCH_FAILED") from None
        except Exception:
            raise ExecutionAssemblyError("HUB_SKILL_ARTIFACT_FETCH_FAILED") from None
        data = bytes(payload)
        if (
            len(data) != manifest.artifact_size
            or hashlib.sha256(data).hexdigest() != manifest.artifact_sha256
        ):
            raise ExecutionAssemblyError("HUB_SKILL_ARTIFACT_STREAM_INVALID")
        return data


class _ArtifactCache:
    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("HUB_ARTIFACT_CACHE_PATH_INVALID")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if root.is_symlink() or not root.is_dir():
            raise ValueError("HUB_ARTIFACT_CACHE_PATH_INVALID")
        self._root = root

    async def load_or_fetch(
        self,
        manifest: _SkillManifest,
        fetch: Callable[[], Awaitable[bytes]],
    ) -> Mapping[str, str]:
        target = self._root / manifest.artifact_sha256
        if target.exists():
            return self._read(target, manifest)
        data = await fetch()
        try:
            files = package_from_zip(
                name=manifest.grant.name,
                expected_description=manifest.grant.description,
                expected_content_hash=manifest.grant.content_hash,
                data=data,
            )
        except SkillHubError as error:
            raise ExecutionAssemblyError("HUB_SKILL_ARTIFACT_INVALID") from error
        temporary: Path | None = None
        try:
            temporary = Path(tempfile.mkdtemp(prefix=".partial-", dir=self._root))
            for relative, value in files.items():
                output = temporary.joinpath(*relative.split("/"))
                output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                with output.open("x", encoding="utf-8", newline="") as stream:
                    stream.write(value)
            try:
                os.rename(temporary, target)
            except OSError as error:
                # Another worker may publish the same content-addressed directory first.
                if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
        except OSError as error:
            raise ExecutionAssemblyError("HUB_ARTIFACT_CACHE_UNAVAILABLE") from error
        finally:
            if temporary is not None and temporary.exists():
                try:
                    shutil.rmtree(temporary)
                except OSError:
                    pass
        return self._read(target, manifest)

    def _read(self, target: Path, manifest: _SkillManifest) -> Mapping[str, str]:
        if target.is_symlink() or not target.is_dir():
            raise ExecutionAssemblyError("HUB_ARTIFACT_CACHE_INVALID")
        files: dict[str, str] = {}
        total_bytes = 0
        for item in target.rglob("*"):
            if item.is_symlink():
                raise ExecutionAssemblyError("HUB_ARTIFACT_CACHE_INVALID")
            if not item.is_file():
                continue
            relative = item.relative_to(target).as_posix()
            if len(files) >= 100:
                raise ExecutionAssemblyError("HUB_ARTIFACT_CACHE_INVALID")
            try:
                before = item.lstat()
                if not stat.S_ISREG(before.st_mode) or before.st_size < 0:
                    raise OSError("not a regular cache file")
                total_bytes += before.st_size
                if total_bytes > MAX_PACKAGE_BYTES:
                    raise OSError("cache package exceeds byte budget")
                flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(item, flags)
                try:
                    opened = os.fstat(descriptor)
                    if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                        raise OSError("cache file changed")
                    raw = os.read(descriptor, before.st_size + 1)
                finally:
                    os.close(descriptor)
                if len(raw) != before.st_size:
                    raise OSError("cache file changed")
                files[relative] = raw.decode("utf-8", errors="strict")
            except (OSError, UnicodeError) as error:
                raise ExecutionAssemblyError("HUB_ARTIFACT_CACHE_INVALID") from error
        try:
            description, _ = validate_package(manifest.grant.name, files)
            if (
                description != manifest.grant.description
                or content_hash_of(files) != manifest.grant.content_hash
            ):
                raise SkillHubError("cache content mismatch")
        except SkillHubError as error:
            raise ExecutionAssemblyError("HUB_ARTIFACT_CACHE_INVALID") from error
        return MappingProxyType(files)


def _validate_request(
    namespace: str,
    agent_catalog_ref: str,
    skills: Sequence[SkillGrant],
    mcp_servers: Sequence[McpGrant],
) -> None:
    if (
        not _reference(namespace, 256)
        or _CATALOG_REF.fullmatch(agent_catalog_ref) is None
        or len(skills) > _MAX_SKILLS
        or len(mcp_servers) > _MAX_MCP
        or _duplicates(skills)
        or _duplicates(mcp_servers)
        or any(
            not _reference(grant.option_ref, 256)
            or not _reference(grant.scope, 256)
            or not _reference(grant.name, 256)
            or grant.scope not in ("official", namespace)
            or _DIGEST.fullmatch(grant.content_hash) is None
            or not 1 <= _utf8_length(grant.description) <= 2_048
            for grant in skills
        )
        or any(
            not _reference(grant.option_ref, 256)
            or not _reference(grant.scope, 256)
            or not _reference(grant.name, 256)
            or grant.scope not in ("official", namespace)
            or grant.revision < 1
            or _DIGEST.fullmatch(grant.config_hash) is None
            for grant in mcp_servers
        )
    ):
        raise ExecutionAssemblyError("HUB_EXECUTION_ASSEMBLY_REQUEST_INVALID")


def _duplicates(values: Sequence[SkillGrant] | Sequence[McpGrant]) -> bool:
    return (
        len({value.option_ref for value in values}) != len(values)
        or len({value.name for value in values}) != len(values)
        or len({(value.scope, value.name) for value in values}) != len(values)
    )


def _validate_skill_response(
    agent_catalog_ref: str,
    grants: Sequence[SkillGrant],
    response: capability_pb.ResolveExecutionAssemblyResponse,
) -> tuple[_SkillManifest, ...]:
    if response.agent_catalog_ref != agent_catalog_ref or len(response.skills) != len(grants):
        raise ExecutionAssemblyError("HUB_EXECUTION_ASSEMBLY_RESPONSE_INVALID")
    manifests: list[_SkillManifest] = []
    compressed_total = 0
    for grant, item in zip(grants, response.skills, strict=True):
        if (
            (item.option_ref, item.scope, item.name, item.content_hash, item.description)
            != (grant.option_ref, grant.scope, grant.name, grant.content_hash, grant.description)
            or not _reference(item.artifact_ref, 1024)
            or item.artifact_size < 1
            or item.artifact_size > _MAX_ARTIFACT_BYTES
            or _DIGEST.fullmatch(item.artifact_sha256) is None
        ):
            raise ExecutionAssemblyError("HUB_EXECUTION_ASSEMBLY_RESPONSE_INVALID")
        compressed_total += item.artifact_size
        if compressed_total > _MAX_ASSEMBLY_ARTIFACT_BYTES:
            raise ExecutionAssemblyError("HUB_EXECUTION_ASSEMBLY_ARTIFACT_BUDGET_EXCEEDED")
        manifests.append(
            _SkillManifest(
                grant=grant,
                artifact_ref=item.artifact_ref,
                artifact_size=item.artifact_size,
                artifact_sha256=item.artifact_sha256,
            )
        )
    return tuple(manifests)


def _validate_mcp_response(
    grants: Sequence[McpGrant],
    response: capability_pb.ResolveExecutionAssemblyResponse,
) -> tuple[dict[str, McpServerConfig], tuple[dict[str, object], ...]]:
    if len(response.mcp_servers) != len(grants):
        raise ExecutionAssemblyError("HUB_EXECUTION_ASSEMBLY_RESPONSE_INVALID")
    definitions: dict[str, McpServerConfig] = {}
    digest_items: list[dict[str, object]] = []
    for grant, item in zip(grants, response.mcp_servers, strict=True):
        has_authorization = item.HasField("authorization_value")
        if (
            (item.option_ref, item.scope, item.name, item.revision, item.config_hash)
            != (grant.option_ref, grant.scope, grant.name, grant.revision, grant.config_hash)
            or item.transport not in ("http", "streamable_http")
            or not _secure_url(item.url)
            or len(item.allowed_tools) > 256
            or len(set(item.allowed_tools)) != len(item.allowed_tools)
            or any(not _reference(tool, 256) for tool in item.allowed_tools)
            or (
                has_authorization
                and (
                    not 1
                    <= _utf8_length(item.authorization_value)
                    <= _MAX_AUTHORIZATION_BYTES
                )
            )
        ):
            raise ExecutionAssemblyError("HUB_EXECUTION_ASSEMBLY_RESPONSE_INVALID")
        definitions[grant.name] = McpServerConfig(
            transport=item.transport,
            url=item.url,
            allowed_tools=list(item.allowed_tools),
            headers=(
                {"authorization": item.authorization_value} if has_authorization else None
            ),
        )
        digest_items.append(
            {
                "optionRef": item.option_ref,
                "scope": item.scope,
                "name": item.name,
                "revision": item.revision,
                "configHash": item.config_hash,
                "transport": item.transport,
                "url": item.url,
                "allowedTools": list(item.allowed_tools),
                "hasAuthorization": has_authorization,
            }
        )
    return definitions, tuple(digest_items)


def _assembly_digest(
    *,
    namespace: str,
    agent_catalog_ref: str,
    skills: Sequence[_SkillManifest],
    mcp_servers: Sequence[Mapping[str, object]],
) -> str:
    canonical = {
        "schemaVersion": 1,
        "namespace": namespace,
        "agentCatalogRef": agent_catalog_ref,
        "skills": [
            {
                "optionRef": item.grant.option_ref,
                "scope": item.grant.scope,
                "name": item.grant.name,
                "contentHash": item.grant.content_hash,
                "description": item.grant.description,
                "artifactRef": item.artifact_ref,
                "artifactSize": item.artifact_size,
                "artifactSha256": item.artifact_sha256,
            }
            for item in skills
        ],
        "mcpServers": list(mcp_servers),
    }
    encoded = json.dumps(canonical, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _skill_selection(grant: SkillGrant) -> capability_pb.SkillGrantSelection:
    return capability_pb.SkillGrantSelection(
        option_ref=grant.option_ref,
        scope=grant.scope,
        name=grant.name,
        content_hash=grant.content_hash,
        description=grant.description,
    )


def _mcp_selection(grant: McpGrant) -> capability_pb.McpGrantSelection:
    return capability_pb.McpGrantSelection(
        option_ref=grant.option_ref,
        scope=grant.scope,
        name=grant.name,
        revision=grant.revision,
        config_hash=grant.config_hash,
    )


def _secure_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme == "https"
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and 0 < _utf8_length(value) <= 4096
        )
    except ValueError:
        return False


def _hub_address(value: str, server_name: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
        or _HOSTNAME.fullmatch(server_name) is None
        or parsed.hostname.casefold() != server_name.casefold()
    ):
        raise ValueError("HUB_RUNTIME_RPC_IDENTITY_INVALID")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _tls_file(value: str, kind: str, *, private: bool = False) -> bytes:
    path = Path(value)
    try:
        before = path.lstat()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise OSError("changed")
            material = os.read(descriptor, 256 * 1024 + 1)
        finally:
            os.close(descriptor)
    except OSError as error:
        raise ValueError(f"HUB_RUNTIME_TLS_{kind.upper()}_INVALID") from error
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not stat_is_regular(before.st_mode)
        or before.st_size < 1
        or before.st_size > 256 * 1024
        or len(material) != before.st_size
        or (private and before.st_mode & 0o077 != 0)
    ):
        raise ValueError(f"HUB_RUNTIME_TLS_{kind.upper()}_INVALID")
    return material


def stat_is_regular(mode: int) -> bool:
    return stat.S_ISREG(mode)


def _reference(value: str, maximum: int) -> bool:
    return 1 <= len(value) <= maximum and value.strip() == value and _utf8_length(value) > 0


def _utf8_length(value: str) -> int:
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return -1
