"""Bounded, dependency-aware process readiness checks."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol
from urllib.parse import urlsplit, urlunsplit
import uuid

from connectrpc.code import Code
from connectrpc.errors import ConnectError
import pyqwest
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from redis.asyncio import Redis, from_url

from kokoro.agent.execution.v1 import agent_execution_evidence_pb2 as evidence_wire
from kokoro.agent.execution.v1.agent_execution_evidence_connect import (
    AgentExecutionEvidenceServiceClient,
)
from kokoro.agent.presentation.v1 import presentation_pb2 as presentation_wire
from kokoro.agent.presentation.v1.presentation_connect import PresentationServiceClient
from kokoro.platform.capability.v1 import capability_catalog_pb2 as hub_wire
from kokoro.platform.capability.v1.capability_catalog_connect import HubRuntimeServiceClient
from kokoro.platform.model.v1 import model_gateway_pb2 as model_wire
from kokoro.platform.model.v1.model_gateway_connect import ModelGatewayServiceClient
from kokoro_agent.presentation.delivery import AGENT_PRESENTATION_CONTRACT_REVISION
from kokoro_agent.security import read_secure_tls_material


@dataclass(frozen=True, slots=True)
class DependencyProbe:
    name: str
    check: Callable[[], Awaitable[None]]


class ReadinessResult(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    ready: bool
    failed_dependencies: tuple[str, ...]


class MongoReadinessSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    url: str
    database: str
    timeout_ms: int


class RedisReadinessSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    url: str
    timeout_ms: int


class MtlsRpcReadinessSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    url: str
    ca_file: str
    cert_file: str
    key_file: str
    timeout_ms: int = Field(ge=100, le=30_000)

    @model_validator(mode="after")
    def validate_url(self) -> "MtlsRpcReadinessSettings":
        parsed = urlsplit(self.url)
        if (
            parsed.scheme != "https"
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
        ):
            raise ValueError("READINESS_MTLS_RPC_URL_INVALID")
        return self


class ProcessReadinessSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    role: Literal["worker", "evidence", "presentation"]
    timeout_ms: int = Field(ge=100, le=30_000)
    mongo: MongoReadinessSettings
    redis: RedisReadinessSettings | None = None
    hub: MtlsRpcReadinessSettings | None = None
    model_gateway: MtlsRpcReadinessSettings | None = None
    listener: MtlsRpcReadinessSettings | None = None

    @model_validator(mode="after")
    def validate_role_dependencies(self) -> "ProcessReadinessSettings":
        worker = (self.redis, self.hub, self.model_gateway, self.listener)
        if self.role == "worker" and (
            any(value is None for value in worker[:3]) or self.listener is not None
        ):
            raise ValueError("WORKER_READINESS_DEPENDENCIES_INVALID")
        if self.role != "worker" and (
            any(value is not None for value in worker[:3]) or self.listener is None
        ):
            raise ValueError("PROVIDER_READINESS_DEPENDENCIES_INVALID")
        return self


class HubReadinessClient(Protocol):
    async def resolve_execution_assembly(
        self,
        request: hub_wire.ResolveExecutionAssemblyRequest,
        *,
        timeout_ms: int | None = None,
    ) -> hub_wire.ResolveExecutionAssemblyResponse: ...


class ModelGatewayReadinessClient(Protocol):
    async def invoke_model(
        self,
        request: model_wire.InvokeModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> model_wire.InvokeModelResponse: ...


class EvidenceReadinessClient(Protocol):
    async def get_run_durable_checkpoint(
        self,
        request: evidence_wire.GetRunDurableCheckpointRequest,
        *,
        timeout_ms: int | None = None,
    ) -> evidence_wire.GetRunDurableCheckpointResponse: ...


class PresentationReadinessClient(Protocol):
    async def check_active(
        self,
        request: presentation_wire.CheckActiveRequest,
        *,
        timeout_ms: int | None = None,
    ) -> presentation_wire.CheckActiveResponse: ...


async def check_dependencies(
    probes: Sequence[DependencyProbe], *, timeout_s: float
) -> ReadinessResult:
    """Run every dependency check concurrently without returning exception material."""

    async def bounded(probe: DependencyProbe) -> str | None:
        try:
            async with asyncio.timeout(timeout_s):
                await probe.check()
        except Exception:  # noqa: BLE001 - readiness deliberately closes error details
            return probe.name
        return None

    failed = await asyncio.gather(*(bounded(probe) for probe in probes))
    names = tuple(sorted(name for name in failed if name is not None))
    return ReadinessResult(ready=not names, failed_dependencies=names)


def process_dependency_probes(
    settings: ProcessReadinessSettings,
) -> tuple[DependencyProbe, ...]:
    mongo = DependencyProbe(
        name="mongodb-replica-set",
        check=lambda: check_mongodb_replica_set(settings.mongo),
    )
    if settings.role == "worker":
        redis = settings.redis
        hub = settings.hub
        model_gateway = settings.model_gateway
        if redis is None or hub is None or model_gateway is None:
            raise AssertionError("validated worker readiness dependencies missing")
        return (
            mongo,
            DependencyProbe(
                name="redis-streams",
                check=lambda: check_redis_streams(redis),
            ),
            DependencyProbe(
                name="hub-runtime-mtls-rpc",
                check=lambda: check_hub_runtime_rpc(hub),
            ),
            DependencyProbe(
                name="model-gateway-mtls-rpc",
                check=lambda: check_model_gateway_rpc(model_gateway),
            ),
        )
    listener_settings = settings.listener
    if listener_settings is None:
        raise AssertionError("validated provider readiness listener missing")
    if settings.role == "evidence":
        listener = DependencyProbe(
            name="evidence-connect-mtls-listener",
            check=lambda: check_evidence_listener(listener_settings),
        )
    else:
        listener = DependencyProbe(
            name="presentation-connect-mtls-listener",
            check=lambda: check_presentation_listener(listener_settings),
        )
    return (mongo, listener)


async def check_process_readiness(
    settings: ProcessReadinessSettings,
) -> ReadinessResult:
    return await check_dependencies(
        process_dependency_probes(settings), timeout_s=settings.timeout_ms / 1000
    )


async def check_mongodb_replica_set(settings: MongoReadinessSettings) -> None:
    """Prove writable replica-set transactions rather than accepting a TCP/ping response."""

    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(
        settings.url,
        appname="kokoro-agent-readiness",
        connectTimeoutMS=settings.timeout_ms,
        serverSelectionTimeoutMS=settings.timeout_ms,
        socketTimeoutMS=settings.timeout_ms,
    )
    marker = uuid.uuid4().hex
    try:
        async with asyncio.timeout(settings.timeout_ms / 1000):
            hello = await client.admin.command("hello")
            if (
                not isinstance(hello.get("setName"), str)
                or hello.get("isWritablePrimary") is not True
                or hello.get("logicalSessionTimeoutMinutes") is None
            ):
                raise RuntimeError("MONGODB_REPLICA_SET_NOT_WRITABLE")
            # Reuse the declared ledger collection. A dedicated probe collection would be
            # permanent Mongo schema surface even after deleting its only marker.
            collection = client[settings.database]["ledger"]
            async with client.start_session() as session:

                async def transact(active: AsyncClientSession) -> None:
                    await collection.insert_one(
                        {"_id": marker, "probe": "transaction-read-write"},
                        session=active,
                    )
                    row = await collection.find_one({"_id": marker}, session=active)
                    if row is None or row.get("probe") != "transaction-read-write":
                        raise RuntimeError("MONGODB_TRANSACTION_READ_FAILED")
                    deleted = await collection.delete_one({"_id": marker}, session=active)
                    if deleted.deleted_count != 1:
                        raise RuntimeError("MONGODB_TRANSACTION_WRITE_FAILED")

                await session.with_transaction(
                    transact,
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern("majority"),
                    read_preference=ReadPreference.PRIMARY,
                )
    finally:
        await client.close()


async def check_redis_streams(settings: RedisReadinessSettings) -> None:
    """Exercise the exact stream/group/claim/ack primitives used by the worker."""

    timeout_s = settings.timeout_ms / 1000
    client: Redis = from_url(
        settings.url,
        protocol=2,
        decode_responses=True,
        socket_connect_timeout=timeout_s,
        socket_timeout=timeout_s,
    )
    suffix = uuid.uuid4().hex
    stream = f"readiness:kokoro-agent:{suffix}"
    group = f"readiness-{suffix}"
    try:
        async with asyncio.timeout(timeout_s):
            await client.xgroup_create(stream, group, id="0", mkstream=True)
            entry_id = await client.xadd(stream, {"probe": "stream-consumer"})
            received = await client.xreadgroup(
                group, "probe-owner", {stream: ">"}, count=1, block=settings.timeout_ms
            )
            if not received:
                raise RuntimeError("REDIS_STREAM_CONSUMER_READ_FAILED")
            claimed = await client.xautoclaim(
                stream,
                group,
                "probe-reclaimer",
                min_idle_time=0,
                start_id="0-0",
                count=1,
            )
            if len(claimed) < 2 or not claimed[1]:
                raise RuntimeError("REDIS_STREAM_AUTOCLAIM_FAILED")
            acknowledged = await client.xack(stream, group, entry_id)
            if acknowledged != 1:
                raise RuntimeError("REDIS_STREAM_ACK_FAILED")
            pending = await client.xpending(stream, group)
            if pending.get("pending") != 0:
                raise RuntimeError("REDIS_STREAM_PENDING_NOT_EMPTY")
    finally:
        await client.delete(stream)
        await client.aclose()


def expected_authenticated_rpc_result(
    error: ConnectError, *, code: Code, message: str
) -> None:
    """Accept only the closed, side-effect-free application errors of invalid probes."""

    if error.code is not code or error.message != message:
        raise RuntimeError("DEPENDENCY_RPC_NOT_READY") from None


async def check_hub_runtime_rpc(
    settings: MtlsRpcReadinessSettings,
    *,
    client: HubReadinessClient | None = None,
) -> None:
    """Call the exact Hub read RPC with a request that must fail before any lookup/write."""

    if client is None:
        client = HubRuntimeServiceClient(
            _rpc_address(settings.url),
            accept_compression=(),
            send_compression=None,
            timeout_ms=settings.timeout_ms,
            read_max_bytes=64 * 1024,
            http_client=_mtls_http_client(settings),
        )
    try:
        await client.resolve_execution_assembly(
            hub_wire.ResolveExecutionAssemblyRequest(),
            timeout_ms=settings.timeout_ms,
        )
    except ConnectError as error:
        expected_authenticated_rpc_result(
            error,
            code=Code.INVALID_ARGUMENT,
            message="execution assembly request invalid",
        )
        return
    raise RuntimeError("DEPENDENCY_RPC_PROBE_UNEXPECTED_SUCCESS")


async def check_model_gateway_rpc(
    settings: MtlsRpcReadinessSettings,
    *,
    client: ModelGatewayReadinessClient | None = None,
) -> None:
    """Reach InvokeModel with an empty body that is rejected before authorization/provider work."""

    if client is None:
        client = ModelGatewayServiceClient(
            _rpc_address(settings.url),
            accept_compression=(),
            send_compression=None,
            timeout_ms=settings.timeout_ms,
            read_max_bytes=64 * 1024,
            http_client=_mtls_http_client(settings),
        )
    try:
        await client.invoke_model(
            model_wire.InvokeModelRequest(),
            timeout_ms=settings.timeout_ms,
        )
    except ConnectError as error:
        expected_authenticated_rpc_result(
            error,
            code=Code.INVALID_ARGUMENT,
            message="model request invalid",
        )
        return
    raise RuntimeError("DEPENDENCY_RPC_PROBE_UNEXPECTED_SUCCESS")


async def check_evidence_listener(
    settings: MtlsRpcReadinessSettings,
    *,
    client: EvidenceReadinessClient | None = None,
) -> None:
    """Authenticate to the real Evidence listener and perform one indexed missing-run read."""

    if client is None:
        client = AgentExecutionEvidenceServiceClient(
            _rpc_address(settings.url),
            accept_compression=(),
            send_compression=None,
            timeout_ms=settings.timeout_ms,
            read_max_bytes=64 * 1024,
            http_client=_mtls_http_client(settings),
        )
    request = evidence_wire.GetRunDurableCheckpointRequest(
        run_id=f"readiness-probe-{uuid.uuid4().hex}"
    )
    response = await client.get_run_durable_checkpoint(
        request, timeout_ms=settings.timeout_ms
    )
    if not response.HasField("not_found") or response.HasField("evidence"):
        raise RuntimeError("EVIDENCE_READINESS_RESPONSE_INVALID")


async def check_presentation_listener(
    settings: MtlsRpcReadinessSettings,
    *,
    client: PresentationReadinessClient | None = None,
) -> None:
    """Authenticate to Presentation CheckActive, whose store check is dependency-aware."""

    if client is None:
        client = PresentationServiceClient(
            _rpc_address(settings.url),
            accept_compression=(),
            send_compression=None,
            timeout_ms=settings.timeout_ms,
            read_max_bytes=64 * 1024,
            http_client=_mtls_http_client(settings),
        )
    response = await client.check_active(
        presentation_wire.CheckActiveRequest(), timeout_ms=settings.timeout_ms
    )
    if response.contract_revision != AGENT_PRESENTATION_CONTRACT_REVISION:
        raise RuntimeError("PRESENTATION_READINESS_RESPONSE_INVALID")


def _rpc_address(value: str) -> str:
    parsed = urlsplit(value)
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _mtls_http_client(settings: MtlsRpcReadinessSettings) -> pyqwest.Client:
    return pyqwest.Client(
        pyqwest.HTTPTransport(
            tls_ca_cert=read_secure_tls_material(
                settings.ca_file, error_code="READINESS_MTLS_CA_INVALID"
            ),
            tls_include_system_certs=False,
            tls_key=read_secure_tls_material(
                settings.key_file,
                error_code="READINESS_MTLS_KEY_INVALID",
                private=True,
            ),
            tls_cert=read_secure_tls_material(
                settings.cert_file, error_code="READINESS_MTLS_CERT_INVALID"
            ),
            http_version=pyqwest.HTTPVersion.HTTP2,
            enable_cookie_store=False,
        )
    )


__all__ = [
    "DependencyProbe",
    "MongoReadinessSettings",
    "MtlsRpcReadinessSettings",
    "ProcessReadinessSettings",
    "ReadinessResult",
    "RedisReadinessSettings",
    "check_dependencies",
    "check_evidence_listener",
    "check_hub_runtime_rpc",
    "check_model_gateway_rpc",
    "check_mongodb_replica_set",
    "check_presentation_listener",
    "check_process_readiness",
    "check_redis_streams",
    "expected_authenticated_rpc_result",
    "process_dependency_probes",
]
