"""HTTP owner-interface acceptance against real PostgreSQL and Redis fixtures."""

from __future__ import annotations

import asyncio
import os
import threading
import uuid
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass

import httpx
import psycopg
import pytest
from pydantic import JsonValue, SecretStr, TypeAdapter
from psycopg import sql

from kokoro_agent.chat.models import ChatEventDraft, ChatMessageDraft, ChatProjection
from kokoro_agent.chat.store import ChatStoreSettings, make_chat_store
from kokoro_agent.contract import (
    ExecutionIdentity,
    IdentityRef,
    RunCompleted,
    RunCompletedPayload,
    RunInput,
    RunRequest,
    RunStarted,
    RunStartedPayload,
    REQUESTS_STREAM,
    run_control_stream,
    run_events_stream,
)
from kokoro_agent.execution.scope import runtime_namespace
from kokoro_agent.http.server import create_http_server
from kokoro_agent.infrastructure.postgres_run_repository import DEFAULT_LEASE_TTL_S, RunRepositorySettings, make_run_repository
from kokoro_agent.infrastructure.postgres import connect_pg
from kokoro_agent.streams.factory import StreamSettings
from kokoro_agent.streams.redis import RedisStream

_DATABASE_URL = os.environ.get(
    "KOKORO_AGENT_DATABASE_URL", "postgresql://127.0.0.1/postgres"
)
_REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
_INTERNAL_SECRET = "acceptance-internal-secret"
_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


@dataclass(frozen=True)
class _AcceptanceConfig:
    stream: StreamSettings
    run_repository: RunRepositorySettings
    database_url: str
    database_schema: str
    internal_secret_agent: SecretStr | None


@dataclass(frozen=True)
class _AcceptanceState:
    config: _AcceptanceConfig
    redis_url: str


def _json_object(value: object) -> dict[str, JsonValue]:
    return _JSON_OBJECT.validate_python(value)


def _headers(subject: str = "subject") -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_INTERNAL_SECRET}",
        "x-kokoro-tenant-ref": "tenant",
        "x-kokoro-subject-ref": subject,
        "x-kokoro-actor-ref": "actor",
        "x-kokoro-identity-assertion-ref": "assertion",
        "x-request-id": f"request-{uuid.uuid4().hex}",
    }


def _identity(subject: str = "subject") -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref=subject),
        identity_assertion_ref="assertion",
    )


def _launch_body(run_id: str) -> dict[str, JsonValue]:
    return _json_object(
        {
            "request_id": f"request-{run_id}",
            "run_id": run_id,
            "session_id": "session-1",
            "feature_key": "chat",
            "execution_identity": _identity().model_dump(mode="json"),
            "message_id": f"message-{run_id}",
            "content": "hello from acceptance",
        }
    )


def _request(run_id: str, subject: str = "subject") -> RunRequest:
    return RunRequest(
        kind="run.request",
        request_id=f"request-{run_id}",
        run_id=run_id,
        session_id="session-1",
        feature_key="chat",
        execution_identity=_identity(subject),
        input=RunInput(message_id=f"message-{run_id}", content="hello from acceptance"),
    )


async def _require_postgres(database_url: str) -> None:
    try:
        async with await psycopg.AsyncConnection.connect(database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
    except Exception as error:  # noqa: BLE001 - fixture preflight must fail loudly
        raise RuntimeError(f"PostgreSQL required but unreachable at {database_url}") from error


async def _require_redis(redis_url: str) -> None:
    port = RedisStream(redis_url, block_ms=100)
    try:
        await asyncio.wait_for(port.read_all(f"kokoro-acceptance-probe:{uuid.uuid4().hex}"), 2.0)
    except Exception as error:  # noqa: BLE001 - fixture preflight must fail loudly
        raise RuntimeError(f"Redis required but unreachable at {redis_url}") from error
    finally:
        await port.aclose()


@pytest.fixture
async def acceptance_state() -> AsyncIterator[_AcceptanceState]:
    """Create one isolated Agent-owned PostgreSQL schema and verify both services first."""
    await _require_postgres(_DATABASE_URL)
    await _require_redis(_REDIS_URL)
    schema = f"kokoro_acceptance_{uuid.uuid4().hex}"
    config = _AcceptanceConfig(
        stream=StreamSettings(redis_url=_REDIS_URL),
        run_repository=RunRepositorySettings(
            database_url=_DATABASE_URL,
            schema_name=schema,
            lease_ttl_ms=DEFAULT_LEASE_TTL_S * 1000,
        ),
        database_url=_DATABASE_URL,
        database_schema=schema,
        internal_secret_agent=SecretStr(_INTERNAL_SECRET),
    )
    try:
        async with make_run_repository(config.run_repository):
            pass
        async with make_chat_store(
            ChatStoreSettings(database_url=_DATABASE_URL, schema_name=schema)
        ):
            pass
        yield _AcceptanceState(config=config, redis_url=_REDIS_URL)
    finally:
        async with connect_pg(_DATABASE_URL) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
                )


@pytest.fixture
async def http_client(
    acceptance_state: _AcceptanceState,
) -> AsyncIterator[httpx.AsyncClient]:
    server = create_http_server(acceptance_state.config, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, name="agent-http-acceptance", daemon=True)
    thread.start()
    port = int(server.server_address[1])
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", timeout=10.0
        ) as client:
            yield client
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


async def _seed_claimed_run(state: _AcceptanceState, request: RunRequest) -> None:
    async with make_run_repository(state.config.run_repository) as run_repository:
        assert await run_repository.try_claim(request, "acceptance-test") is True


async def _seed_chat(state: _AcceptanceState, request: RunRequest) -> None:
    namespace = runtime_namespace(request.execution_identity)
    async with make_chat_store(
        ChatStoreSettings(database_url=state.config.database_url, schema_name=state.config.database_schema)
    ) as chat:
        await chat.append(
            ChatProjection(
                event=ChatEventDraft(
                    namespace=namespace,
                    session_id=request.session_id,
                    run_id=request.run_id,
                    source_index=0,
                    event_type="run.started",
                    payload_json='{"status":"running"}',
                    created_at=1,
                ),
                message=ChatMessageDraft(
                    chat_message_id=f"message-{request.run_id}",
                    namespace=namespace,
                    session_id=request.session_id,
                    run_id=request.run_id,
                    role="user",
                    content=request.input.content,
                    status="completed",
                    created_at=1,
                    updated_at=1,
                ),
            )
        )


async def _seed_events(state: _AcceptanceState, run_id: str) -> None:
    port = RedisStream(state.redis_url)
    started = RunStarted(
        kind="run.started",
        run_id=run_id,
        index=0,
        timestamp=1,
        payload=RunStartedPayload(),
    )
    completed = RunCompleted(
        kind="run.completed",
        run_id=run_id,
        index=1,
        timestamp=2,
        payload=RunCompletedPayload(status="completed", token_usage=None),
    )
    try:
        for event in (started, completed):
            await port.publish(
                run_events_stream(run_id),
                _json_object(event.model_dump(mode="json", exclude_none=True)),
                maxlen=100,
            )
    finally:
        await port.aclose()


async def _read_matching(
    state: _AcceptanceState, stream: str, run_id: str
) -> list[dict[str, JsonValue]]:
    port = RedisStream(state.redis_url)
    try:
        items = await port.read_all(stream)
    finally:
        await port.aclose()
    return [_json_object(item.event) for item in items if item.event.get("run_id") == run_id]


def _nested(payload: Mapping[str, JsonValue], key: str) -> dict[str, JsonValue]:
    return _json_object(payload[key])


@pytest.mark.asyncio
async def test_health_and_ready_are_real_http_contracts(
    http_client: httpx.AsyncClient,
) -> None:
    health = await http_client.get("/healthz")
    assert health.status_code == 200
    assert _json_object(health.json()) == {"status": "ok", "service": "kokoro-agent"}

    ready = await http_client.get("/readyz", headers=_headers())
    assert ready.status_code == 200
    assert _json_object(ready.json()) == {"status": "ready", "service": "kokoro-agent"}


@pytest.mark.asyncio
async def test_launch_is_durable_and_idempotent_over_http(
    acceptance_state: _AcceptanceState,
    http_client: httpx.AsyncClient,
) -> None:
    run_id = f"launch-{uuid.uuid4().hex}"
    body = _launch_body(run_id)

    first = await http_client.post("/v1/runs", headers=_headers(), json=body)
    assert first.status_code == 202
    first_data = _nested(_json_object(first.json()), "data")
    assert first_data["run_id"] == run_id
    assert first_data["replayed"] is False

    # Once the worker has claimed the durable intent, a retry must reuse the
    # receipt without publishing a second worker envelope.
    async with make_run_repository(acceptance_state.config.run_repository) as run_repository:
        assert await run_repository.claim_dispatch(run_id, "acceptance-worker") is True

    second = await http_client.post("/v1/runs", headers=_headers(), json=body)
    assert second.status_code == 202
    second_data = _nested(_json_object(second.json()), "data")
    assert second_data["replayed"] is True

    published = await _read_matching(acceptance_state, REQUESTS_STREAM, run_id)
    assert len(published) == 1
    assert published[0]["kind"] == "run.request"


@pytest.mark.asyncio
async def test_auth_and_invalid_launch_fail_with_stable_http_errors(
    http_client: httpx.AsyncClient,
) -> None:
    denied = await http_client.post("/v1/runs", json=_launch_body("denied"))
    assert denied.status_code == 401
    denied_error = _nested(_json_object(denied.json()), "error")
    assert denied_error["code"] == "service_auth_failed"

    invalid = await http_client.post("/v1/runs", headers=_headers(), json={})
    assert invalid.status_code == 400
    invalid_error = _nested(_json_object(invalid.json()), "error")
    assert invalid_error["code"] == "invalid_launch_request"


@pytest.mark.asyncio
async def test_control_and_evidence_use_real_redis_and_postgres_state(
    acceptance_state: _AcceptanceState,
    http_client: httpx.AsyncClient,
) -> None:
    run_id = f"control-{uuid.uuid4().hex}"
    request = _request(run_id)
    await _seed_claimed_run(acceptance_state, request)
    await _seed_events(acceptance_state, run_id)

    control = await http_client.post(
        f"/v1/runs/{run_id}/control",
        headers={**_headers(), "Idempotency-Key": "command-1"},
        json={"kind": "run.cancel", "session_id": "session-1"},
    )
    assert control.status_code == 202
    control_data = _nested(_json_object(control.json()), "data")
    assert control_data["status"] == "pending"
    assert control_data["command_id"] == "command-1"
    assert control_data["replayed"] is False

    replay = await http_client.post(
        f"/v1/runs/{run_id}/control",
        headers={**_headers(), "Idempotency-Key": "command-1"},
        json={"kind": "run.cancel", "session_id": "session-1"},
    )
    assert replay.status_code == 202
    replay_data = _nested(_json_object(replay.json()), "data")
    assert replay_data["status"] == "pending"
    assert replay_data["replayed"] is True

    conflict = await http_client.post(
        f"/v1/runs/{run_id}/control",
        headers={**_headers(), "Idempotency-Key": "command-1"},
        json={
            "kind": "run.steer",
            "session_id": "session-1",
            "message_id": "message-2",
            "content": "different command",
        },
    )
    assert conflict.status_code == 409
    assert _nested(_json_object(conflict.json()), "error")["code"] == "command_digest_mismatch"

    control_frames = await _read_matching(
        acceptance_state, run_control_stream(run_id), run_id
    )
    assert len(control_frames) == 2
    assert control_frames[0]["kind"] == "run.cancel"

    evidence = await http_client.get(
        f"/v1/runs/{run_id}/events?after_seq=0&limit=10",
        headers=_headers(),
    )
    assert evidence.status_code == 200
    evidence_data = _nested(_json_object(evidence.json()), "data")
    events = evidence_data["events"]
    assert isinstance(events, list)
    assert len(events) == 1
    assert _json_object(events[0])["kind"] == "run.completed"
    assert evidence_data["next_seq"] == 1
    assert evidence_data["terminal"] is True


@pytest.mark.asyncio
async def test_history_and_replay_are_identity_scoped_over_http(
    acceptance_state: _AcceptanceState,
    http_client: httpx.AsyncClient,
) -> None:
    run_id = f"history-{uuid.uuid4().hex}"
    request = _request(run_id)
    await _seed_chat(acceptance_state, request)

    history = await http_client.get(
        "/v1/sessions/session-1/messages", headers=_headers()
    )
    assert history.status_code == 200
    history_data = _nested(_json_object(history.json()), "data")
    messages = history_data["messages"]
    assert isinstance(messages, list)
    assert len(messages) == 1
    assert _json_object(messages[0])["content"] == "hello from acceptance"

    replay = await http_client.get("/v1/sessions/session-1/events", headers=_headers())
    assert replay.status_code == 200
    replay_data = _nested(_json_object(replay.json()), "data")
    events = replay_data["events"]
    assert isinstance(events, list)
    assert len(events) == 1
    assert _json_object(events[0])["event_type"] == "run.started"

    foreign = await http_client.get(
        "/v1/sessions/session-1/messages", headers=_headers("another-subject")
    )
    assert foreign.status_code == 200
    foreign_data = _nested(_json_object(foreign.json()), "data")
    assert foreign_data["messages"] == []
