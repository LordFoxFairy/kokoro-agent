"""Exact Hub assembly consumer: binding, digest, bounded stream, cache, and archive safety."""

from __future__ import annotations

import hashlib
import io
import json
from collections.abc import AsyncIterator
from pathlib import Path
from zipfile import ZipFile

import pytest

from kokoro.platform.capability.v1 import capability_catalog_pb2 as capability_pb
from kokoro_agent.contract import McpGrant, SkillGrant
from kokoro_agent.hub import (
    ExecutionAssemblyError,
    HubExecutionAssemblyClient,
    HubRuntimeSettings,
)
from kokoro_agent.skills.hub import SkillHubError, content_hash_of, package_from_zip
from skill_fixtures import PDF_FILES

NAMESPACE = "namespace-a"
CATALOG_REF = f"agent-catalog:sha256:{'a' * 64}"
MCP_HASH = "b" * 64
AUTHORIZATION = "Bearer example-secret"


class FakeHubRuntimeClient:
    def __init__(
        self,
        response: capability_pb.ResolveExecutionAssemblyResponse,
        artifact: bytes,
        *,
        bad_offset: bool = False,
    ) -> None:
        self.response = response
        self.artifact = artifact
        self.bad_offset = bad_offset
        self.resolve_calls: list[capability_pb.ResolveExecutionAssemblyRequest] = []
        self.fetch_calls: list[capability_pb.FetchSkillArtifactRequest] = []

    async def resolve_execution_assembly(
        self,
        request: capability_pb.ResolveExecutionAssemblyRequest,
        *,
        timeout_ms: int | None = None,
    ) -> capability_pb.ResolveExecutionAssemblyResponse:
        del timeout_ms
        self.resolve_calls.append(request)
        return self.response

    def fetch_skill_artifact(
        self,
        request: capability_pb.FetchSkillArtifactRequest,
        *,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[capability_pb.FetchSkillArtifactResponse]:
        del timeout_ms
        self.fetch_calls.append(request)

        async def chunks() -> AsyncIterator[capability_pb.FetchSkillArtifactResponse]:
            midpoint = max(1, len(self.artifact) // 2)
            yield capability_pb.FetchSkillArtifactResponse(
                artifact_ref=request.artifact_ref,
                offset=1 if self.bad_offset else 0,
                data=self.artifact[:midpoint],
            )
            yield capability_pb.FetchSkillArtifactResponse(
                artifact_ref=request.artifact_ref,
                offset=midpoint,
                data=self.artifact[midpoint:],
            )

        return chunks()


def _skill() -> SkillGrant:
    return SkillGrant(
        option_ref="skill:pdf",
        scope=NAMESPACE,
        name="pdf",
        content_hash=content_hash_of(PDF_FILES),
        description="PDF 报告生成流程",
    )


def _mcp() -> McpGrant:
    return McpGrant(
        option_ref="mcp:docs",
        scope="official",
        name="docs",
        revision=7,
        config_hash=MCP_HASH,
    )


def _zip(files: dict[str, str]) -> bytes:
    output = io.BytesIO()
    with ZipFile(output, "w") as archive:
        for path, value in files.items():
            archive.writestr(path, value)
    return output.getvalue()


def _response(artifact: bytes) -> capability_pb.ResolveExecutionAssemblyResponse:
    skill = _skill()
    mcp = _mcp()
    artifact_ref = f"skills/{skill.scope}/{skill.name}/{skill.content_hash}.zip"
    artifact_sha = hashlib.sha256(artifact).hexdigest()
    response = capability_pb.ResolveExecutionAssemblyResponse(
        agent_catalog_ref=CATALOG_REF,
        skills=[
            capability_pb.SkillArtifactManifest(
                option_ref=skill.option_ref,
                scope=skill.scope,
                name=skill.name,
                content_hash=skill.content_hash,
                description=skill.description,
                artifact_ref=artifact_ref,
                artifact_size=len(artifact),
                artifact_sha256=artifact_sha,
            )
        ],
        mcp_servers=[
            capability_pb.McpAssemblyConfig(
                option_ref=mcp.option_ref,
                scope=mcp.scope,
                name=mcp.name,
                revision=mcp.revision,
                config_hash=mcp.config_hash,
                transport="streamable_http",
                url="https://mcp.example.com/rpc",
                allowed_tools=["search", "read"],
                authorization_value=AUTHORIZATION,
            )
        ],
    )
    response.assembly_digest = _digest(response)
    return response


def _digest(response: capability_pb.ResolveExecutionAssemblyResponse) -> str:
    canonical = {
        "schemaVersion": 1,
        "namespace": NAMESPACE,
        "agentCatalogRef": response.agent_catalog_ref,
        "skills": [
            {
                "optionRef": item.option_ref,
                "scope": item.scope,
                "name": item.name,
                "contentHash": item.content_hash,
                "description": item.description,
                "artifactRef": item.artifact_ref,
                "artifactSize": item.artifact_size,
                "artifactSha256": item.artifact_sha256,
            }
            for item in response.skills
        ],
        "mcpServers": [
            {
                "optionRef": item.option_ref,
                "scope": item.scope,
                "name": item.name,
                "revision": item.revision,
                "configHash": item.config_hash,
                "transport": item.transport,
                "url": item.url,
                "allowedTools": list(item.allowed_tools),
                "hasAuthorization": item.HasField("authorization_value"),
            }
            for item in response.mcp_servers
        ],
    }
    payload = json.dumps(canonical, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def _client(tmp_path: Path, fake: FakeHubRuntimeClient) -> HubExecutionAssemblyClient:
    return HubExecutionAssemblyClient(
        HubRuntimeSettings(
            rpc_url="https://hub.internal:4252",
            server_name="hub.internal",
            ca_file="/not-read/ca.pem",
            cert_file="/not-read/agent.pem",
            key_file="/not-read/agent-key.pem",
            artifact_cache_dir=str(tmp_path / "cache"),
            timeout_ms=731,
        ),
        client=fake,
    )


async def test_resolves_exact_assembly_and_streams_artifact_once(tmp_path: Path) -> None:
    artifact = _zip(PDF_FILES)
    fake = FakeHubRuntimeClient(_response(artifact), artifact)
    client = _client(tmp_path, fake)

    assembly = await client.resolve(NAMESPACE, CATALOG_REF, [_skill()], [_mcp()])

    assert await assembly.skills.read_body(NAMESPACE, "pdf", _skill().content_hash) == PDF_FILES["SKILL.md"]
    assert assembly.mcp_servers["docs"].headers == {"authorization": AUTHORIZATION}
    assert len(fake.resolve_calls) == 1
    assert len(fake.fetch_calls) == 1
    request = fake.resolve_calls[0]
    assert request.agent_catalog_ref == CATALOG_REF
    assert request.skill_grants[0].option_ref == _skill().option_ref
    assert request.mcp_grants[0].option_ref == _mcp().option_ref

    await client.resolve(NAMESPACE, CATALOG_REF, [_skill()], [_mcp()])
    assert len(fake.fetch_calls) == 1  # validated content-addressed cache hit


async def test_rejects_digest_mismatch_before_fetch(tmp_path: Path) -> None:
    artifact = _zip(PDF_FILES)
    response = _response(artifact)
    response.assembly_digest = "0" * 64
    fake = FakeHubRuntimeClient(response, artifact)
    with pytest.raises(ExecutionAssemblyError, match="DIGEST_INVALID"):
        await _client(tmp_path, fake).resolve(NAMESPACE, CATALOG_REF, [_skill()], [_mcp()])
    assert fake.fetch_calls == []


async def test_rejects_non_contiguous_stream(tmp_path: Path) -> None:
    artifact = _zip(PDF_FILES)
    fake = FakeHubRuntimeClient(_response(artifact), artifact, bad_offset=True)
    with pytest.raises(ExecutionAssemblyError, match="STREAM_INVALID"):
        await _client(tmp_path, fake).resolve(NAMESPACE, CATALOG_REF, [_skill()], [_mcp()])


async def test_rejects_duplicate_runtime_names_before_rpc(tmp_path: Path) -> None:
    artifact = _zip(PDF_FILES)
    fake = FakeHubRuntimeClient(_response(artifact), artifact)
    duplicate = _skill().model_copy(
        update={"option_ref": "skill:pdf-custom", "scope": "custom"}
    )
    with pytest.raises(ExecutionAssemblyError, match="REQUEST_INVALID"):
        await _client(tmp_path, fake).resolve(
            NAMESPACE, CATALOG_REF, [_skill(), duplicate], [_mcp()]
        )
    assert fake.resolve_calls == []


@pytest.mark.parametrize("path", ["../escape", "/absolute", "folder\\evil", "a/../../evil"])
def test_archive_rejects_unsafe_paths(path: str) -> None:
    files = dict(PDF_FILES)
    files[path] = "bad"
    with pytest.raises(SkillHubError, match="path invalid"):
        package_from_zip(
            name="pdf",
            expected_description="PDF 报告生成流程",
            expected_content_hash=content_hash_of(files),
            data=_zip(files),
        )


def test_rpc_identity_has_no_http_or_path_fallback(tmp_path: Path) -> None:
    settings = HubRuntimeSettings(
        rpc_url="http://hub.internal:4252/path",
        server_name="hub.internal",
        ca_file="/not-read/ca.pem",
        cert_file="/not-read/agent.pem",
        key_file="/not-read/agent-key.pem",
        artifact_cache_dir=str(tmp_path / "cache"),
    )
    with pytest.raises(ValueError, match="RPC_IDENTITY_INVALID"):
        HubExecutionAssemblyClient(
            settings,
            client=FakeHubRuntimeClient(
                capability_pb.ResolveExecutionAssemblyResponse(), b"x"
            ),
        )
