from __future__ import annotations

import asyncio
from collections.abc import Mapping
import os
import uuid

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from pymongo import AsyncMongoClient

from kokoro.agent.execution.v1 import agent_execution_evidence_pb2 as evidence_wire
from kokoro.agent.presentation.v1 import presentation_pb2 as presentation_wire
from kokoro.platform.capability.v1 import capability_catalog_pb2 as hub_wire
from kokoro.platform.model.v1 import model_gateway_pb2 as model_wire

from kokoro_agent.readiness import (
    DependencyProbe,
    MongoReadinessSettings,
    MtlsRpcReadinessSettings,
    ProcessReadinessSettings,
    RedisReadinessSettings,
    check_dependencies,
    check_evidence_listener,
    check_hub_runtime_rpc,
    check_model_gateway_rpc,
    check_mongodb_replica_set,
    check_presentation_listener,
    check_redis_streams,
    expected_authenticated_rpc_result,
    process_dependency_probes,
)
from kokoro_agent.config import AppConfig
from kokoro_agent.readiness import ReadinessResult


MONGO_URL = os.environ.get(
    "KOKORO_MONGO_URL",
    "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true",
)
REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")


async def test_dependency_checks_start_concurrently_and_recover() -> None:
    started: set[str] = set()
    release = asyncio.Event()

    async def wait_for_peer(name: str) -> None:
        started.add(name)
        if len(started) == 2:
            release.set()
        await release.wait()

    probes = (
        DependencyProbe(name="mongodb-replica-set", check=lambda: wait_for_peer("mongo")),
        DependencyProbe(name="redis-streams", check=lambda: wait_for_peer("redis")),
    )

    failed = await check_dependencies(probes, timeout_s=0.1)
    recovered = await check_dependencies(probes, timeout_s=0.1)

    assert failed.ready is True
    assert recovered.ready is True
    assert failed.failed_dependencies == ()


async def test_dependency_failure_is_bounded_and_reports_names_only() -> None:
    secret = "redis://user:do-not-log@example.invalid/0"

    async def leak_secret() -> None:
        raise RuntimeError(secret)

    async def hang() -> None:
        await asyncio.Event().wait()

    result = await check_dependencies(
        (
            DependencyProbe(name="redis-streams", check=leak_secret),
            DependencyProbe(name="hub-runtime-mtls-rpc", check=hang),
        ),
        timeout_s=0.01,
    )

    assert result.ready is False
    assert result.failed_dependencies == (
        "hub-runtime-mtls-rpc",
        "redis-streams",
    )
    assert secret not in result.model_dump_json()


async def test_external_cancellation_is_never_reported_as_dependency_failure() -> None:
    entered = asyncio.Event()

    async def hang() -> None:
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        check_dependencies(
            (DependencyProbe(name="mongodb-replica-set", check=hang),),
            timeout_s=10,
        )
    )
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_mongo_readiness_requires_replica_set_transaction_read_write() -> None:
    database = f"kokoro_readiness_test_{uuid.uuid4().hex}"
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    try:
        await check_mongodb_replica_set(
            MongoReadinessSettings(url=MONGO_URL, database=database, timeout_ms=1_000)
        )
        assert await client[database].list_collection_names() == ["ledger"]
        assert await client[database]["ledger"].count_documents({}) == 0
    finally:
        await client.drop_database(database)
        await client.close()


async def test_redis_readiness_exercises_stream_group_claim_and_ack() -> None:
    await check_redis_streams(
        RedisReadinessSettings(url=REDIS_URL, timeout_ms=1_000)
    )


async def test_redis_readiness_uses_private_scratch_namespace_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingRedis:
        def __init__(self) -> None:
            self.stream_names: list[str] = []
            self.deleted: list[str] = []

        async def xgroup_create(
            self, stream: str, _group: str, *, id: str, mkstream: bool
        ) -> None:
            assert id == "0"
            assert mkstream is True
            self.stream_names.append(stream)

        async def xadd(self, stream: str, _fields: Mapping[str, str]) -> str:
            self.stream_names.append(stream)
            return "1-0"

        async def xreadgroup(
            self,
            _group: str,
            _consumer: str,
            streams: Mapping[str, str],
            *,
            count: int,
            block: int,
        ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
            assert count == 1
            assert block == 100
            self.stream_names.extend(streams)
            return [(next(iter(streams)), [("1-0", {"probe": "stream-consumer"})])]

        async def xautoclaim(
            self,
            stream: str,
            _group: str,
            _consumer: str,
            *,
            min_idle_time: int,
            start_id: str,
            count: int,
        ) -> tuple[str, list[tuple[str, dict[str, str]]], list[str]]:
            assert min_idle_time == 0
            assert start_id == "0-0"
            assert count == 1
            self.stream_names.append(stream)
            return ("0-0", [("1-0", {"probe": "stream-consumer"})], [])

        async def xack(self, stream: str, _group: str, _entry_id: str) -> int:
            self.stream_names.append(stream)
            return 1

        async def xpending(self, stream: str, _group: str) -> dict[str, int]:
            self.stream_names.append(stream)
            return {"pending": 0}

        async def delete(self, stream: str) -> None:
            self.deleted.append(stream)

        async def aclose(self) -> None:
            return None

    redis = RecordingRedis()

    def fake_from_url(*_args: object, **_kwargs: object) -> RecordingRedis:
        return redis

    monkeypatch.setattr("kokoro_agent.readiness.from_url", fake_from_url)

    await check_redis_streams(
        RedisReadinessSettings(url="redis://fixture.invalid/0", timeout_ms=100)
    )

    assert redis.stream_names
    assert all(
        stream.startswith("readiness:kokoro-agent:") for stream in redis.stream_names
    )
    assert redis.deleted == [redis.stream_names[0]]


def test_only_exact_authenticated_application_error_counts_as_rpc_ready() -> None:
    expected_authenticated_rpc_result(
        ConnectError(Code.INVALID_ARGUMENT, "execution assembly request invalid"),
        code=Code.INVALID_ARGUMENT,
        message="execution assembly request invalid",
    )


@pytest.mark.parametrize(
    "code",
    [
        Code.UNAUTHENTICATED,
        Code.UNAVAILABLE,
        Code.DEADLINE_EXCEEDED,
        Code.UNIMPLEMENTED,
        Code.PERMISSION_DENIED,
    ],
)
def test_transport_auth_and_unexpected_rpc_errors_are_not_ready(code: Code) -> None:
    with pytest.raises(RuntimeError, match="DEPENDENCY_RPC_NOT_READY"):
        expected_authenticated_rpc_result(
            ConnectError(code, "secret remote detail"),
            code=Code.INVALID_ARGUMENT,
            message="execution assembly request invalid",
        )


def test_same_code_from_wrong_domain_is_not_ready() -> None:
    with pytest.raises(RuntimeError, match="DEPENDENCY_RPC_NOT_READY"):
        expected_authenticated_rpc_result(
            ConnectError(Code.INVALID_ARGUMENT, "wrong service invalid"),
            code=Code.INVALID_ARGUMENT,
            message="execution assembly request invalid",
        )


RPC_SETTINGS = MtlsRpcReadinessSettings(
    url="https://dependency.internal:8443",
    ca_file="/missing/ca.pem",
    cert_file="/missing/client.pem",
    key_file="/missing/client-key.pem",
    timeout_ms=100,
)


class _HubProbeClient:
    async def resolve_execution_assembly(
        self,
        request: hub_wire.ResolveExecutionAssemblyRequest,
        *,
        timeout_ms: int | None = None,
    ) -> hub_wire.ResolveExecutionAssemblyResponse:
        assert request == hub_wire.ResolveExecutionAssemblyRequest()
        assert timeout_ms == 100
        raise ConnectError(Code.INVALID_ARGUMENT, "execution assembly request invalid")


class _ModelProbeClient:
    async def invoke_model(
        self,
        request: model_wire.InvokeModelRequest,
        *,
        timeout_ms: int | None = None,
    ) -> model_wire.InvokeModelResponse:
        assert request == model_wire.InvokeModelRequest()
        assert timeout_ms == 100
        raise ConnectError(Code.INVALID_ARGUMENT, "model request invalid")


class _EvidenceProbeClient:
    async def get_run_durable_checkpoint(
        self,
        request: evidence_wire.GetRunDurableCheckpointRequest,
        *,
        timeout_ms: int | None = None,
    ) -> evidence_wire.GetRunDurableCheckpointResponse:
        assert request.run_id.startswith("readiness-probe-")
        assert timeout_ms == 100
        return evidence_wire.GetRunDurableCheckpointResponse(
            not_found=evidence_wire.DurableExecutionEvidenceNotFound()
        )


class _PresentationProbeClient:
    async def check_active(
        self,
        request: presentation_wire.CheckActiveRequest,
        *,
        timeout_ms: int | None = None,
    ) -> presentation_wire.CheckActiveResponse:
        assert request == presentation_wire.CheckActiveRequest()
        assert timeout_ms == 100
        return presentation_wire.CheckActiveResponse(contract_revision="agent-presentation@v1")


async def test_outbound_rpc_probes_use_side_effect_free_structurally_invalid_requests() -> None:
    await check_hub_runtime_rpc(RPC_SETTINGS, client=_HubProbeClient())
    await check_model_gateway_rpc(RPC_SETTINGS, client=_ModelProbeClient())


async def test_provider_listener_probes_use_real_safe_read_rpcs() -> None:
    await check_evidence_listener(RPC_SETTINGS, client=_EvidenceProbeClient())
    await check_presentation_listener(RPC_SETTINGS, client=_PresentationProbeClient())


async def test_structurally_invalid_probe_success_fails_closed() -> None:
    class UnexpectedHub:
        async def resolve_execution_assembly(
            self,
            request: hub_wire.ResolveExecutionAssemblyRequest,
            *,
            timeout_ms: int | None = None,
        ) -> hub_wire.ResolveExecutionAssemblyResponse:
            return hub_wire.ResolveExecutionAssemblyResponse()

    with pytest.raises(RuntimeError, match="DEPENDENCY_RPC_PROBE_UNEXPECTED_SUCCESS"):
        await check_hub_runtime_rpc(RPC_SETTINGS, client=UnexpectedHub())


def test_process_probe_sets_match_real_dependency_surfaces() -> None:
    mongo = MongoReadinessSettings(url=MONGO_URL, database="kokoro", timeout_ms=100)
    redis = RedisReadinessSettings(url=REDIS_URL, timeout_ms=100)

    worker = ProcessReadinessSettings(
        role="worker",
        timeout_ms=100,
        mongo=mongo,
        redis=redis,
        hub=RPC_SETTINGS,
        model_gateway=RPC_SETTINGS,
    )
    evidence = ProcessReadinessSettings(
        role="evidence",
        timeout_ms=100,
        mongo=mongo,
        listener=RPC_SETTINGS,
    )
    presentation = ProcessReadinessSettings(
        role="presentation",
        timeout_ms=100,
        mongo=mongo,
        listener=RPC_SETTINGS,
    )

    assert tuple(probe.name for probe in process_dependency_probes(worker)) == (
        "mongodb-replica-set",
        "redis-streams",
        "hub-runtime-mtls-rpc",
        "model-gateway-mtls-rpc",
    )
    assert tuple(probe.name for probe in process_dependency_probes(evidence)) == (
        "mongodb-replica-set",
        "evidence-connect-mtls-listener",
    )
    assert tuple(probe.name for probe in process_dependency_probes(presentation)) == (
        "mongodb-replica-set",
        "presentation-connect-mtls-listener",
    )


def test_process_readiness_settings_reject_partial_or_cross_role_dependencies() -> None:
    mongo = MongoReadinessSettings(url=MONGO_URL, database="kokoro", timeout_ms=100)

    with pytest.raises(ValueError):
        ProcessReadinessSettings(role="worker", timeout_ms=100, mongo=mongo)
    with pytest.raises(ValueError):
        ProcessReadinessSettings(
            role="evidence",
            timeout_ms=100,
            mongo=mongo,
            redis=RedisReadinessSettings(url=REDIS_URL, timeout_ms=100),
            listener=RPC_SETTINGS,
        )


def test_app_config_builds_closed_readiness_settings_for_each_process() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_AGENT_READINESS_TIMEOUT_MS": "700",
            "KOKORO_HUB_RPC_URL": "https://hub.internal:4251",
            "KOKORO_HUB_RPC_SERVER_NAME": "hub.internal",
            "KOKORO_HUB_RPC_CA_FILE": "/run/secrets/hub-ca.pem",
            "KOKORO_HUB_RPC_CERT_FILE": "/run/secrets/hub-client.pem",
            "KOKORO_HUB_RPC_KEY_FILE": "/run/secrets/hub-client-key.pem",
            "KOKORO_MODEL_GATEWAY_URL": "https://model.internal:8446",
            "KOKORO_MODEL_GATEWAY_CA_FILE": "/run/secrets/model-ca.pem",
            "KOKORO_MODEL_GATEWAY_CERT_FILE": "/run/secrets/model-client.pem",
            "KOKORO_MODEL_GATEWAY_KEY_FILE": "/run/secrets/model-client-key.pem",
            "KOKORO_AGENT_EVIDENCE_READINESS_URL": "https://evidence.internal:8443",
            "KOKORO_AGENT_EVIDENCE_READINESS_CA_FILE": "/run/secrets/evidence-ca.pem",
            "KOKORO_AGENT_EVIDENCE_READINESS_CERT_FILE": "/run/secrets/evidence-client.pem",
            "KOKORO_AGENT_EVIDENCE_READINESS_KEY_FILE": "/run/secrets/evidence-client-key.pem",
            "KOKORO_AGENT_PRESENTATION_READINESS_URL": "https://presentation.internal:8444",
            "KOKORO_AGENT_PRESENTATION_READINESS_CA_FILE": "/run/secrets/presentation-ca.pem",
            "KOKORO_AGENT_PRESENTATION_READINESS_CERT_FILE": "/run/secrets/presentation-client.pem",
            "KOKORO_AGENT_PRESENTATION_READINESS_KEY_FILE": "/run/secrets/presentation-client-key.pem",
        }
    )

    assert config.worker_readiness.role == "worker"
    assert config.worker_readiness.timeout_ms == 700
    assert config.evidence_readiness.role == "evidence"
    assert config.evidence_readiness.listener is not None
    assert config.presentation_readiness.role == "presentation"
    assert config.presentation_readiness.listener is not None

    with pytest.raises(ValueError, match="READINESS_WORKER_CONFIGURATION_REQUIRED"):
        config.model_copy(update={"hub_rpc_server_name": "other.internal"}).worker_readiness


@pytest.mark.parametrize(
    "property_name",
    ["worker_readiness", "evidence_readiness", "presentation_readiness"],
)
def test_app_config_readiness_fails_closed_when_role_material_is_missing(
    property_name: str,
) -> None:
    with pytest.raises(ValueError, match="READINESS_.*_CONFIGURATION_REQUIRED"):
        getattr(AppConfig.from_env({}), property_name)


@pytest.mark.parametrize(
    ("module_name", "settings_attribute"),
    [
        ("kokoro_agent.worker.main", "worker_readiness"),
        ("kokoro_agent.evidence.main", "evidence_readiness"),
        ("kokoro_agent.presentation.main", "presentation_readiness"),
    ],
)
def test_process_entrypoint_readiness_failure_is_nonzero_and_never_serves(
    module_name: str,
    settings_attribute: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = __import__(module_name, fromlist=["main"])
    settings = object()
    config = type("Config", (), {settings_attribute: settings})()

    async def failed(_settings: object) -> ReadinessResult:
        assert _settings is settings
        return ReadinessResult(
            ready=False,
            failed_dependencies=("mongodb-replica-set",),
        )

    async def forbidden_serve(*_args: object) -> None:
        raise AssertionError("readiness command must not enter the long-running process")

    def configured(_source: Mapping[str, str]) -> object:
        return config

    def suppress_log(*_args: object) -> None:
        return None

    monkeypatch.setattr(module.AppConfig, "from_env", configured)
    monkeypatch.setattr(module, "check_process_readiness", failed, raising=False)
    monkeypatch.setattr(module, "_serve", forbidden_serve)
    if hasattr(module, "log_config_summary"):
        monkeypatch.setattr(module, "log_config_summary", suppress_log)

    with pytest.raises(SystemExit) as exit_info:
        module.main(["--readiness"])

    assert exit_info.value.code == 1


@pytest.mark.parametrize(
    ("module_name", "settings_attribute"),
    [
        ("kokoro_agent.worker.main", "worker_readiness"),
        ("kokoro_agent.evidence.main", "evidence_readiness"),
        ("kokoro_agent.presentation.main", "presentation_readiness"),
    ],
)
def test_process_entrypoint_readiness_recovers_to_zero(
    module_name: str,
    settings_attribute: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = __import__(module_name, fromlist=["main"])
    settings = object()
    config = type("Config", (), {settings_attribute: settings})()

    async def ready(_settings: object) -> ReadinessResult:
        assert _settings is settings
        return ReadinessResult(ready=True, failed_dependencies=())

    async def forbidden_serve(*_args: object) -> None:
        raise AssertionError("readiness command must not enter the long-running process")

    def configured(_source: Mapping[str, str]) -> object:
        return config

    def suppress_log(*_args: object) -> None:
        return None

    monkeypatch.setattr(module.AppConfig, "from_env", configured)
    monkeypatch.setattr(module, "check_process_readiness", ready, raising=False)
    monkeypatch.setattr(module, "_serve", forbidden_serve)
    if hasattr(module, "log_config_summary"):
        monkeypatch.setattr(module, "log_config_summary", suppress_log)

    module.main(["--readiness"])
