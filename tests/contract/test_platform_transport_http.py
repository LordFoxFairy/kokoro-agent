"""Owned loopback OAuth/Connect fixture, not a real IAM or Platform owner service."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Generator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time
from typing import Any
from urllib.parse import parse_qs

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import jwt
import pytest
import rfc8785

from kokoro_agent.clients.platform_transport import PlatformCallError
from kokoro_agent.config import AppConfig
from kokoro_agent.domain.run.models import LeaseFence, LeasedRun
from kokoro_agent.execution.execution_proof_supplier import (
    CurrentLeaseObservation,
    ExecutionProofUnavailableError,
)
from kokoro_agent.execution.platform_request_binding import project_request_binding
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.worker.platform import worker_platform_runtime


class Loopback:
    def __init__(self) -> None:
        self.private = Ed25519PrivateKey.generate()
        self.tokens = 0
        self.expected_generation = 7
        self.requests: list[pb.GetMcpConnectorRequest] = []
        self.claims: list[dict[str, Any]] = []
        self.errors: list[BaseException] = []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_POST(self) -> None:
                try:
                    body = self.rfile.read(int(self.headers["content-length"]))
                    if self.path == "/iam/oauth2/token":
                        fixture.tokens += 1
                        assert (
                            self.headers["authorization"]
                            == "Basic " + base64.b64encode(b"agent:secret").decode()
                        )
                        assert parse_qs(body.decode()) == {
                            "grant_type": ["client_credentials"],
                            "resource": [
                                "https://kokoro.dev/resources/platform-internal"
                            ],
                            "scope": ["platform:execution.invoke"],
                        }
                        self.respond(
                            200,
                            "application/json",
                            json.dumps(
                                {
                                    "access_token": "TOKEN",
                                    "token_type": "Bearer",
                                    "expires_in": 300,
                                }
                            ).encode(),
                        )
                        return
                    assert (
                        self.path
                        == "/kokoro.platform.v1.McpConnectorService/GetMcpConnector"
                    )
                    assert self.headers["authorization"] == "Bearer TOKEN"
                    assert self.headers["content-type"] == "application/proto"
                    request = pb.GetMcpConnectorRequest.from_binary(body)
                    fixture.requests.append(request)
                    claims: dict[str, Any] = jwt.decode(
                        request.execution_proof,
                        fixture.private.public_key(),
                        algorithms=["EdDSA"],
                        audience="https://kokoro.dev/resources/iam-execution-authorization",
                        issuer="urn:test:agent",
                    )
                    fixture.claims.append(claims)
                    assert claims["tenant_ref"] == "tenant"
                    assert claims["lease_generation"] == fixture.expected_generation
                    assert claims["operation"] == "mcp.get_connector"
                    assert (
                        claims["request_binding_sha256"]
                        == project_request_binding(
                            tenant_ref="tenant", request=request
                        ).sha256
                    )
                    if request.request_id == "slow":
                        time.sleep(0.2)
                    if request.request_id == "redirect":
                        self.send_response(302)
                        self.send_header("location", "/unexpected")
                        self.send_header("content-length", "0")
                        self.end_headers()
                        return
                    if request.request_id == "large":
                        self.respond(200, "application/proto", b"x" * 1_048_577)
                        return
                    self.respond(
                        200,
                        "application/proto",
                        pb.GetMcpConnectorResponse().to_binary(),
                    )
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except BaseException as error:
                    fixture.errors.append(error)
                    self.respond(500, "application/json", b'{"code":"internal"}')

            def respond(self, status: int, content_type: str, body: bytes) -> None:
                self.send_response(status)
                self.send_header("content-type", content_type)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def config(self, tmp_path: Path) -> AppConfig:
        key = tmp_path / "private.pem"
        key.write_bytes(
            self.private.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        key.chmod(0o600)
        x = (
            base64.urlsafe_b64encode(
                self.private.public_key().public_bytes(
                    serialization.Encoding.Raw, serialization.PublicFormat.Raw
                )
            )
            .rstrip(b"=")
            .decode()
        )
        thumbprint = (
            base64.urlsafe_b64encode(
                hashlib.sha256(
                    rfc8785.dumps({"kty": "OKP", "crv": "Ed25519", "x": x})
                ).digest()
            )
            .rstrip(b"=")
            .decode()
        )
        credentials = tmp_path / "credentials.json"
        credentials.write_text(
            json.dumps(
                [
                    dict(
                        tenant_id="tenant",
                        generation=1,
                        credential_ref_version="v1",
                        client_id="agent",
                        client_secret="secret",
                        resource="https://kokoro.dev/resources/platform-internal",
                        scope="platform:execution.invoke",
                    )
                ]
            )
        )
        credentials.chmod(0o600)
        return AppConfig.from_env(
            {
                "KOKORO_PLATFORM_BASE_URL": self.url,
                "KOKORO_AGENT_IAM_BASE_URL": self.url,
                "KOKORO_AGENT_PLATFORM_CLIENT_CREDENTIALS_FILE": str(credentials),
                "KOKORO_AGENT_EXECUTION_PROOF_ISSUER": "urn:test:agent",
                "KOKORO_AGENT_EXECUTION_PROOF_PRIVATE_KEY_FILE": str(key),
                "KOKORO_AGENT_EXECUTION_PROOF_WORKER_ACTIVE_KID": "test",
                "KOKORO_AGENT_EXECUTION_PROOF_WORKER_ACTIVE_JWK_THUMBPRINT_SHA256": thumbprint,
            }
        )


@pytest.fixture
def loopback() -> Generator[Loopback, None, None]:
    fixture = Loopback()
    try:
        yield fixture
        assert fixture.errors == []
    finally:
        fixture.server.shutdown()
        fixture.server.server_close()
        fixture.thread.join(timeout=2)


def leased_run() -> LeasedRun:
    return LeasedRun(
        request=RunRequest(
            kind="run.request",
            run_id="run",
            session_id="session",
            feature_key="chat",
            selected_skill_source_refs=(),
            execution_identity=ExecutionIdentity(
                tenant_ref="tenant",
                actor=IdentityRef(kind="user", opaque_ref="actor"),
                subject=IdentityRef(kind="user", opaque_ref="subject"),
                identity_assertion_ref="assertion",
            ),
            input=RunInput(message_id="message", content="hello"),
        ),
        lease=LeaseFence(owner="worker", generation=7),
    )


class LeaseReader:
    active = True

    async def observe_current_lease(
        self, *, run_id: str, fence: LeaseFence
    ) -> CurrentLeaseObservation | None:
        assert run_id == "run" and fence.generation == 7
        now = datetime.now(UTC)
        return (
            CurrentLeaseObservation(
                database_now=now, lease_expires_at=now + timedelta(seconds=45)
            )
            if self.active
            else None
        )


async def test_worker_composition_real_http_proof_retry_and_close(
    loopback: Loopback, tmp_path: Path
) -> None:
    async with worker_platform_runtime(loopback.config(tmp_path)) as runtime:
        assert runtime is not None
        reader = LeaseReader()
        sender = replace(runtime, lease_reader=reader).for_run(leased_run())
        request = pb.GetMcpConnectorRequest(
            request_id="logical", connector_id=pb.McpConnectorId(value="connector")
        )
        await sender.send(request)
        await sender.send(request)
        assert loopback.tokens == 1
        assert len(loopback.requests) == 2
        assert loopback.claims[0]["jti"] != loopback.claims[1]["jti"]
        assert request.execution_proof == ""
        reader.active = False
        with pytest.raises(ExecutionProofUnavailableError):
            await sender.send(request)
        assert len(loopback.requests) == 2
    with pytest.raises(Exception, match="CLOSED"):
        await sender.send(request)


@pytest.mark.parametrize("request_id", ["redirect", "large", "slow"])
async def test_real_connect_transport_boundaries(
    loopback: Loopback, tmp_path: Path, request_id: str
) -> None:
    async with worker_platform_runtime(loopback.config(tmp_path)) as runtime:
        assert runtime is not None
        sender = replace(runtime, lease_reader=LeaseReader()).for_run(leased_run())
        with pytest.raises(PlatformCallError):
            await sender.send(
                pb.GetMcpConnectorRequest(
                    request_id=request_id,
                    connector_id=pb.McpConnectorId(value="connector"),
                ),
                timeout_s=0.05 if request_id == "slow" else 2,
            )
        assert len(loopback.requests) == 1


async def test_real_connect_caller_cancel(loopback: Loopback, tmp_path: Path) -> None:
    async with worker_platform_runtime(loopback.config(tmp_path)) as runtime:
        assert runtime is not None
        sender = replace(runtime, lease_reader=LeaseReader()).for_run(leased_run())
        task = asyncio.create_task(
            sender.send(
                pb.GetMcpConnectorRequest(
                    request_id="slow", connector_id=pb.McpConnectorId(value="connector")
                )
            )
        )
        for _ in range(100):
            if loopback.requests:
                break
            await asyncio.sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.integration
async def test_real_postgres_lease_with_worker_owned_http(
    loopback: Loopback, tmp_path: Path
) -> None:
    """Own a fresh database; no shared app data, IAM service, or Platform service."""
    import os
    from uuid import uuid4
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from kokoro_agent.infrastructure.postgres import connect_pg
    from kokoro_agent.infrastructure.schema import apply_agent_schema
    from kokoro_agent.infrastructure.postgres_run_repository import (
        RunRepositorySettings,
        make_run_repository,
    )

    admin_url = os.environ["KOKORO_LOCAL_POSTGRES_ADMIN_URL"]
    database_name = "agent_transport_" + uuid4().hex
    database_url = make_conninfo(admin_url, dbname=database_name)
    admin = await psycopg.AsyncConnection.connect(admin_url, autocommit=True)
    created = False
    try:
        await admin.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
        )
        created = True
        async with connect_pg(database_url) as connection:
            await apply_agent_schema(connection, "kokoro_agent", require_blank=True)
        config = loopback.config(tmp_path).model_copy(
            update={"database_url": database_url}
        )
        async with make_run_repository(
            RunRepositorySettings(
                database_url=database_url,
                schema_name="kokoro_agent",
                lease_ttl_ms=60_000,
            )
        ) as repository:
            request = leased_run().request
            fence = await repository.try_claim(request, "worker")
            assert fence is not None
            loopback.expected_generation = fence.generation
            async with worker_platform_runtime(config) as runtime:
                assert runtime is not None
                sender = runtime.for_run(LeasedRun(request=request, lease=fence))
                operation = pb.GetMcpConnectorRequest(
                    request_id="pg-logical",
                    connector_id=pb.McpConnectorId(value="connector"),
                )
                await sender.send(operation)
                await sender.send(operation)
                assert loopback.tokens == 1 and len(loopback.requests) == 2
                assert loopback.claims[0]["jti"] != loopback.claims[1]["jti"]
                assert await repository.pause(request.run_id, fence)
                with pytest.raises(ExecutionProofUnavailableError):
                    await sender.send(operation)
                assert len(loopback.requests) == 2
    finally:
        if created:
            await admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(database_name)
                )
            )
        await admin.close()


async def test_typed_reader_uses_current_run_sender_fresh_proof_and_lease(
    loopback: Loopback, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real signer/worker/OAuth; Platform and object responses are named fixtures."""
    import httpx
    from protobuf.wkt import Timestamp
    from kokoro_agent.clients.platform_transport import (
        PlatformRequest,
        PlatformResponse,
    )
    from kokoro_agent.clients.skill_package_transport import SkillPackageTransport

    vector = json.loads(
        (
            Path(__file__).parents[2]
            / "contract/platform/v1/execution-operations/v4/vectors/zip-v1.json"
        ).read_bytes()
    )["vectors"][0]
    claims: list[dict[str, Any]] = []
    gets = 0

    async def signed_send(
        request: PlatformRequest, token: str, timeout_ms: int
    ) -> PlatformResponse:
        assert token == "TOKEN" and timeout_ms > 0
        proof: dict[str, Any] = jwt.decode(
            request.execution_proof,
            loopback.private.public_key(),
            algorithms=["EdDSA"],
            audience="https://kokoro.dev/resources/iam-execution-authorization",
            issuer="urn:test:agent",
        )
        assert proof["lease_generation"] == 7
        assert (
            proof["request_binding_sha256"]
            == project_request_binding(tenant_ref="tenant", request=request).sha256
        )
        claims.append(proof)
        if isinstance(request, pb.ResolveVisibleSkillRequest):
            return pb.ResolveVisibleSkillResponse(
                source=pb.SkillSource(
                    source_ref=pb.SkillSourceRef(value="skill:skill-1"),
                    skill_id=pb.SkillId(value="skill-1"),
                    series_id=pb.SkillSeriesId(value="series"),
                    revision=1,
                    scope_kind=pb.SkillScopeKind.PERSONAL,
                    package_asset_ref="asset",
                    content_digest=vector["zipSha256"],
                    manifest_identity=vector["manifestIdentity"],
                )
            )
        assert isinstance(request, pb.GetApprovedSkillPackageReferenceRequest)
        return pb.GetApprovedSkillPackageReferenceResponse(
            asset_ref="asset",
            content_digest=vector["zipSha256"],
            manifest_identity=vector["manifestIdentity"],
            transfer_reference=pb.PackageTransferReference(
                method="GET",
                url="https://objects.test/pkg",
                expires_at=Timestamp.from_datetime(
                    datetime.now(UTC) + timedelta(seconds=30)
                ),
            ),
        )

    async def get(request: httpx.Request) -> httpx.Response:
        nonlocal gets
        gets += 1
        assert "authorization" not in request.headers
        return httpx.Response(
            200, stream=httpx.ByteStream(base64.b64decode(vector["zipBase64"]))
        )

    async with (
        worker_platform_runtime(loopback.config(tmp_path)) as runtime,
        SkillPackageTransport(
            "https://objects.test", transport=httpx.MockTransport(get)
        ) as packages,
    ):
        assert runtime is not None
        reader = LeaseReader()
        monkeypatch.setattr(runtime.transport, "send", signed_send)
        current = replace(runtime, lease_reader=reader, packages=packages)
        client = current.skills_for_run(leased_run())
        skills = await client.resolve(("skill:skill-1",))
        assert (await client.load_package(skills[0]))["SKILL.md"] == b"# Skill"
        await client.load_package(skills[0])
        assert gets == 2 and len(claims) == 3
        assert len({c["jti"] for c in claims}) == 3
        assert [c["operation"] for c in claims] == [
            "skill.resolve_visible_skill",
            "skill.get_approved_package_reference",
            "skill.get_approved_package_reference",
        ]
        reader.active = False
        with pytest.raises(ExecutionProofUnavailableError):
            await client.load_package(skills[0])
        assert gets == 2 and len(claims) == 3
