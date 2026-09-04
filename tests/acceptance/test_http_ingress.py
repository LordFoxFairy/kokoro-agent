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
from support.fakes import FakeBus

from kokoro_agent.application.chat.mappers import wire_epoch_millis_to_utc
from kokoro_agent.domain.chat.models import ChatEventDraft, ChatMessageDraft, ChatProjection
from kokoro_agent.infrastructure.postgres_chat_repository import (
    PostgresChatRepository,
    PostgresChatRepositorySettings,
    make_chat_repository,
)
from kokoro_agent.protocol import (
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
from kokoro_agent.domain.run.scope import runtime_namespace
from kokoro_agent.execution.events import RunEmitter, message_delta_payload
from kokoro_agent.interfaces.http.server import create_http_server
from kokoro_agent.infrastructure.postgres_run_repository import (
    DEFAULT_LEASE_TTL_S,
    PostgresRunRepository,
    RunRepositorySettings,
    make_run_repository,
)
from kokoro_agent.infrastructure.postgres import connect_pg
from kokoro_agent.infrastructure.schema import (
    CHAT_EVENTS_TABLE,
    CHAT_MESSAGES_TABLE,
    CHAT_SEQUENCES_TABLE,
    RUN_RECEIPT_MANIFESTS_TABLE,
    RUN_RECEIPTS_TABLE,
    apply_agent_schema,
)
from kokoro_agent.streams.factory import StreamSettings
from kokoro_agent.streams.redis import RedisStream

_DATABASE_URL = os.environ.get(
    "KOKORO_AGENT_DATABASE_URL",
    "postgresql://kokoro@127.0.0.1:55433/kokoro_worker_agent?password=kokoro",
)
_REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:56380/9")
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
        raise RuntimeError(
            f"PostgreSQL required but unreachable at {database_url}"
        ) from error


async def _require_redis(redis_url: str) -> None:
    port = RedisStream(redis_url, block_ms=100)
    try:
        await asyncio.wait_for(
            port.read_all(f"kokoro-acceptance-probe:{uuid.uuid4().hex}"), 2.0
        )
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
        async with connect_pg(_DATABASE_URL) as connection:
            await apply_agent_schema(connection, schema, require_blank=True)
        async with make_run_repository(config.run_repository):
            pass
        async with make_chat_repository(
            PostgresChatRepositorySettings(
                database_url=_DATABASE_URL, schema_name=schema
            )
        ):
            pass
        yield _AcceptanceState(config=config, redis_url=_REDIS_URL)
    finally:
        async with connect_pg(_DATABASE_URL) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    )
                )


@pytest.fixture
async def http_client(
    acceptance_state: _AcceptanceState,
) -> AsyncIterator[httpx.AsyncClient]:
    server = create_http_server(acceptance_state.config, "127.0.0.1", 0)
    thread = threading.Thread(
        target=server.serve_forever, name="agent-http-acceptance", daemon=True
    )
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
        await run_repository.enqueue_dispatch(
            request,
            runtime_namespace(request.execution_identity),
            f"acceptance:{request.run_id}",
        )
        assert (
            await run_repository.claim_dispatch(request, "acceptance-test") is not None
        )


async def _seed_chat(state: _AcceptanceState, request: RunRequest) -> None:
    namespace = runtime_namespace(request.execution_identity)
    async with make_chat_repository(
        PostgresChatRepositorySettings(
            database_url=state.config.database_url,
            schema_name=state.config.database_schema,
        )
    ) as chat:
        await chat.append(
            ChatProjection(
                event=ChatEventDraft(
                    tenant_id=request.execution_identity.tenant_ref,
                    namespace=namespace,
                    session_id=request.session_id,
                    run_id=request.run_id,
                    source_index=0,
                    event_type="run.started",
                    payload_json='{"status":"running"}',
                    created_at=wire_epoch_millis_to_utc(1),
                ),
                message=ChatMessageDraft(
                    chat_message_id=f"message-{request.run_id}",
                    tenant_id=request.execution_identity.tenant_ref,
                    namespace=namespace,
                    session_id=request.session_id,
                    run_id=request.run_id,
                    role="user",
                    content=request.input.content,
                    status="completed",
                    created_at=wire_epoch_millis_to_utc(1),
                    updated_at=wire_epoch_millis_to_utc(1),
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
    return [
        _json_object(item.event) for item in items if item.event.get("run_id") == run_id
    ]


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
    async with make_run_repository(
        acceptance_state.config.run_repository
    ) as run_repository:
        assert (
            await run_repository.claim_dispatch(_request(run_id), "acceptance-worker")
            is not None
        )

    second = await http_client.post("/v1/runs", headers=_headers(), json=body)
    assert second.status_code == 202
    second_data = _nested(_json_object(second.json()), "data")
    assert second_data["replayed"] is True

    published = await _read_matching(acceptance_state, REQUESTS_STREAM, run_id)
    assert len(published) == 1
    assert published[0]["kind"] == "run.request"


@pytest.mark.asyncio
async def test_pending_dispatch_is_replayable_from_postgres_without_redis_frame(
    acceptance_state: _AcceptanceState,
) -> None:
    run_id = f"orphan-dispatch-{uuid.uuid4().hex}"
    request = _request(run_id)

    async with make_run_repository(
        acceptance_state.config.run_repository
    ) as run_repository:
        await run_repository.enqueue_dispatch(
            request,
            runtime_namespace(request.execution_identity),
            f"acceptance:{run_id}",
        )

        pending = await run_repository.list_pending_dispatches()
        assert request in pending

        assert (
            await run_repository.claim_dispatch(request, "acceptance-worker")
            is not None
        )
        assert await run_repository.get_request(run_id) == request
        assert await run_repository.claim_dispatch(request, "other-worker") is None
        assert request not in await run_repository.list_pending_dispatches()


@pytest.mark.asyncio
async def test_postgres_lease_generation_fences_stale_same_owner_worker(
    acceptance_state: _AcceptanceState,
) -> None:
    """A restarted process may reuse its worker name; generation must still fence it."""

    clock_ms = [1_000]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    current_request = _request(f"lease-generation-{uuid.uuid4().hex}")

    first = await repository.try_claim(current_request, "same-worker-name")
    assert first is not None
    clock_ms[0] += 11
    assert await repository.renew(current_request.run_id, first) is False
    reclaimed = await repository.reclaim_expired("same-worker-name")
    assert len(reclaimed) == 1
    second = reclaimed[0].lease

    assert second.generation == first.generation + 1
    assert await repository.renew(current_request.run_id, first) is False
    assert await repository.try_mark_terminal(current_request.run_id, first) is False
    assert await repository.renew(current_request.run_id, second) is True
    assert await repository.try_mark_terminal(current_request.run_id, second) is True


@pytest.mark.asyncio
async def test_postgres_execution_effects_require_current_lease_generation(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [10_000]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    current_request = _request(f"effect-fence-{uuid.uuid4().hex}")

    stale = await repository.try_claim(current_request, "reused-worker-name")
    assert stale is not None
    await repository.add_steer(current_request.run_id, "steer-1", "keep me")
    clock_ms[0] += 11
    reclaimed = await repository.reclaim_expired("reused-worker-name")
    current = reclaimed[0].lease

    assert await repository.add_tokens(current_request.run_id, stale, 3) is None
    assert await repository.add_usage(current_request.run_id, stale, 5, 7) is None
    assert (
        await repository.put_tool_result(
            current_request.run_id, stale, "tool-1", "stale", False
        )
        is None
    )
    assert (
        await repository.journal_tool_started(
            current_request.run_id, stale, "call-1", "write_file"
        )
        is False
    )
    assert (
        await repository.bind_sandbox_id(
            current_request.run_id,
            stale,
            expected_sandbox_id=None,
            sandbox_id="sandbox-old",
            backend_kind="custom",
            teardown_ref="fixtures.sandbox:destroy",
        )
        is None
    )
    assert (
        await repository.ack_steers(current_request.run_id, stale, ["steer-1"]) is False
    )
    assert await repository.peek_steers(current_request.run_id) == [
        ("steer-1", "keep me")
    ]
    assert (
        await repository.stage_critical_frame(
            current_request.run_id,
            stale,
            "run.started",
            clock_ms[0],
            "{}",
            terminal=False,
        )
        is None
    )
    projection = ChatProjection(
        event=ChatEventDraft(
            tenant_id=current_request.execution_identity.tenant_ref,
            namespace=runtime_namespace(current_request.execution_identity),
            session_id=current_request.session_id,
            run_id=current_request.run_id,
            source_index=0,
            event_type="run.started",
            payload_json="{}",
            created_at=wire_epoch_millis_to_utc(clock_ms[0]),
        )
    )
    chat_repository = PostgresChatRepository(
        acceptance_state.config.database_url,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await chat_repository.setup()
    assert await chat_repository.append_fenced(projection, stale, mode="active") is None
    assert (
        await chat_repository.append_fenced(projection, current, mode="active")
        is not None
    )

    assert await repository.add_tokens(current_request.run_id, current, 3) == 3
    assert await repository.add_usage(current_request.run_id, current, 5, 7) == (5, 7)
    assert await repository.put_tool_result(
        current_request.run_id, current, "tool-1", "current", False
    ) == ("current", False)
    assert (
        await repository.journal_tool_started(
            current_request.run_id, current, "call-1", "write_file"
        )
        is True
    )
    assert (
        await repository.bind_sandbox_id(
            current_request.run_id,
            current,
            expected_sandbox_id=None,
            sandbox_id="sandbox-new",
            backend_kind="custom",
            teardown_ref="fixtures.sandbox:destroy",
        )
        == "sandbox-new"
    )
    assert (
        await repository.ack_steers(current_request.run_id, current, ["steer-1"])
        is True
    )
    assert (
        await repository.stage_critical_frame(
            current_request.run_id,
            current,
            "run.started",
            clock_ms[0],
            "{}",
            terminal=False,
        )
        is not None
    )
    assert await repository.peek_steers(current_request.run_id) == []


@pytest.mark.asyncio
async def test_chat_fence_distinguishes_active_work_from_current_generation(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [20_000]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    chat_repository = PostgresChatRepository(
        acceptance_state.config.database_url,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    await chat_repository.setup()
    current_request = _request(f"chat-fence-mode-{uuid.uuid4().hex}")
    lease = await repository.try_claim(current_request, "chat-worker")
    assert lease is not None
    clock_ms[0] += 11
    projection = ChatProjection(
        event=ChatEventDraft(
            tenant_id=current_request.execution_identity.tenant_ref,
            namespace=runtime_namespace(current_request.execution_identity),
            session_id=current_request.session_id,
            run_id=current_request.run_id,
            source_index=0,
            event_type="run.failed",
            payload_json="{}",
            created_at=wire_epoch_millis_to_utc(clock_ms[0]),
        )
    )

    assert await chat_repository.append_fenced(projection, lease, mode="active") is None
    assert (
        await chat_repository.append_fenced(
            projection, lease, mode="current_generation"
        )
        is not None
    )


@pytest.mark.asyncio
async def test_chat_projection_and_sequences_commit_atomically(
    acceptance_state: _AcceptanceState,
) -> None:
    schema = acceptance_state.config.database_schema
    repository = PostgresChatRepository(
        acceptance_state.config.database_url,
        schema=schema,
    )
    await repository.setup()
    run_id = f"atomic-chat-{uuid.uuid4().hex}"
    namespace = "tenant:atomic"
    session_id = "session-atomic"
    message_id = f"message-{uuid.uuid4().hex}"
    projection = ChatProjection(
        event=ChatEventDraft(
            tenant_id="tenant",
            namespace=namespace,
            session_id=session_id,
            run_id=run_id,
            source_index=0,
            chat_message_id=message_id,
            event_type="assistant.completed",
            payload_json='{"content":"atomic"}',
            created_at=wire_epoch_millis_to_utc(30_000),
        ),
        message=ChatMessageDraft(
            chat_message_id=message_id,
            tenant_id="tenant",
            namespace=namespace,
            session_id=session_id,
            run_id=run_id,
            role="assistant",
            content="atomic",
            status="completed",
            created_at=wire_epoch_millis_to_utc(30_000),
            updated_at=wire_epoch_millis_to_utc(30_000),
        ),
    )

    async with connect_pg(acceptance_state.config.database_url) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    """
                    CREATE FUNCTION {}.reject_atomic_chat_message()
                    RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        RAISE EXCEPTION 'forced chat projection rollback';
                    END;
                    $$
                    """
                ).format(sql.Identifier(schema))
            )
            await cursor.execute(
                sql.SQL(
                    """
                    CREATE TRIGGER reject_atomic_chat_message
                    BEFORE INSERT ON {}.{}
                    FOR EACH ROW EXECUTE FUNCTION {}.reject_atomic_chat_message()
                    """
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(CHAT_MESSAGES_TABLE),
                    sql.Identifier(schema),
                )
            )

    with pytest.raises(psycopg.errors.RaiseException, match="forced chat"):
        await repository.append(projection)

    async with connect_pg(acceptance_state.config.database_url) as connection:
        async with connection.cursor() as cursor:
            for table in (
                CHAT_EVENTS_TABLE,
                CHAT_MESSAGES_TABLE,
                CHAT_SEQUENCES_TABLE,
            ):
                await cursor.execute(
                    sql.SQL("SELECT count(*) AS row_count FROM {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
                row = await cursor.fetchone()
                assert row is not None
                assert int(row["row_count"]) == 0
            await cursor.execute(
                sql.SQL("DROP TRIGGER reject_atomic_chat_message ON {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(CHAT_MESSAGES_TABLE)
                )
            )
            await cursor.execute(
                sql.SQL("DROP FUNCTION {}.reject_atomic_chat_message()").format(
                    sql.Identifier(schema)
                )
            )

    committed = await repository.append(projection)
    assert committed.seq == 1
    history = await repository.history("tenant", namespace, session_id)
    assert len(history) == 1
    assert history[0].seq == 1


@pytest.mark.asyncio
async def test_chat_sequence_commit_order_cannot_overtake_lower_sequence(
    acceptance_state: _AcceptanceState,
) -> None:
    schema = acceptance_state.config.database_schema
    repository = PostgresChatRepository(
        acceptance_state.config.database_url,
        schema=schema,
    )
    await repository.setup()
    namespace = "tenant:ordered"
    session_id = f"session-{uuid.uuid4().hex}"
    run_id = f"ordered-chat-{uuid.uuid4().hex}"

    def projection(source_index: int) -> ChatProjection:
        return ChatProjection(
            event=ChatEventDraft(
                tenant_id="tenant",
                namespace=namespace,
                session_id=session_id,
                run_id=run_id,
                source_index=source_index,
                event_type="assistant.delta",
                payload_json=f'{{"index":{source_index}}}',
                created_at=wire_epoch_millis_to_utc(31_000 + source_index),
            )
        )

    async with connect_pg(acceptance_state.config.database_url) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    "INSERT INTO {}.{} (kind, tenant_id, namespace, session_id, seq) "
                    "VALUES ('event', %s, %s, %s, 0)"
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(CHAT_SEQUENCES_TABLE),
                ),
                ("tenant", namespace, session_id),
            )
            await cursor.execute(
                sql.SQL(
                    """
                    CREATE FUNCTION {}.delay_first_chat_event()
                    RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        IF NEW.source_index = 0 THEN
                            PERFORM pg_sleep(0.25);
                        END IF;
                        RETURN NEW;
                    END;
                    $$
                    """
                ).format(sql.Identifier(schema))
            )
            await cursor.execute(
                sql.SQL(
                    """
                    CREATE TRIGGER delay_first_chat_event
                    BEFORE INSERT ON {}.{}
                    FOR EACH ROW EXECUTE FUNCTION {}.delay_first_chat_event()
                    """
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(CHAT_EVENTS_TABLE),
                    sql.Identifier(schema),
                )
            )

    completion_order: list[int] = []

    async def append(source_index: int) -> None:
        await repository.append(projection(source_index))
        completion_order.append(source_index)

    first = asyncio.create_task(append(0))
    await asyncio.sleep(0.05)
    second = asyncio.create_task(append(1))
    await asyncio.gather(first, second)

    replay = await repository.replay("tenant", namespace, session_id)
    assert completion_order == [0, 1]
    assert [(event.source_index, event.seq) for event in replay] == [(0, 1), (1, 2)]

    async with connect_pg(acceptance_state.config.database_url) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                sql.SQL("DROP TRIGGER delay_first_chat_event ON {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(CHAT_EVENTS_TABLE)
                )
            )
            await cursor.execute(
                sql.SQL("DROP FUNCTION {}.delay_first_chat_event()").format(
                    sql.Identifier(schema)
                )
            )


@pytest.mark.asyncio
async def test_usage_is_idempotent_per_lease_generation(
    acceptance_state: _AcceptanceState,
) -> None:
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
        schema=acceptance_state.config.database_schema,
    )
    await repository.setup()
    current_request = _request(f"usage-segment-{uuid.uuid4().hex}")
    lease = await repository.try_claim(current_request, "usage-worker")
    assert lease is not None
    assert await repository.try_mark_terminal(current_request.run_id, lease) is True

    assert await repository.add_usage(current_request.run_id, lease, 5, 7) == (5, 7)
    assert await repository.add_usage(current_request.run_id, lease, 5, 7) == (5, 7)
    with pytest.raises(RuntimeError, match="usage.*identity|identity.*usage"):
        await repository.add_usage(current_request.run_id, lease, 6, 7)


@pytest.mark.asyncio
async def test_active_effect_blocks_lease_reclaim_until_effect_finishes(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [40_000]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    current_request = _request(f"effect-linearization-{uuid.uuid4().hex}")
    lease = await repository.try_claim(current_request, "old-worker")
    assert lease is not None
    effect_started = asyncio.Event()
    release_effect = asyncio.Event()

    async def effect() -> None:
        effect_started.set()
        await release_effect.wait()

    effect_task = asyncio.create_task(
        repository.execute_active_effect(current_request.run_id, lease, effect)
    )
    await asyncio.wait_for(effect_started.wait(), timeout=1)
    clock_ms[0] += 11
    reclaim_task = asyncio.create_task(repository.reclaim_expired("new-worker"))
    await asyncio.sleep(0.05)
    assert reclaim_task.done() is False

    release_effect.set()
    assert await asyncio.wait_for(effect_task, timeout=1) is True
    reclaimed = await asyncio.wait_for(reclaim_task, timeout=1)
    assert len(reclaimed) == 1
    assert reclaimed[0].lease.generation == lease.generation + 1


@pytest.mark.asyncio
async def test_sandbox_binding_is_compare_and_swap(
    acceptance_state: _AcceptanceState,
) -> None:
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
        schema=acceptance_state.config.database_schema,
    )
    await repository.setup()
    current_request = _request(f"sandbox-cas-{uuid.uuid4().hex}")
    lease = await repository.try_claim(current_request, "sandbox-worker")
    assert lease is not None

    assert (
        await repository.bind_sandbox_id(
            current_request.run_id,
            lease,
            expected_sandbox_id=None,
            sandbox_id="A",
            backend_kind="custom",
            teardown_ref="fixtures.sandbox:destroy",
        )
        == "A"
    )
    assert (
        await repository.bind_sandbox_id(
            current_request.run_id,
            lease,
            expected_sandbox_id=None,
            sandbox_id="B",
            backend_kind="custom",
            teardown_ref="fixtures.sandbox:destroy",
        )
        == "A"
    )
    assert (
        await repository.bind_sandbox_id(
            current_request.run_id,
            lease,
            expected_sandbox_id="A",
            sandbox_id="C",
            backend_kind="custom",
            teardown_ref="fixtures.sandbox:destroy",
        )
        == "A"
    )
    assert await repository.pause(current_request.run_id, lease) is True
    replacement_lease = await repository.adopt(
        current_request.run_id, "replacement-worker"
    )
    assert replacement_lease is not None
    assert (
        await repository.bind_sandbox_id(
            current_request.run_id,
            replacement_lease,
            expected_sandbox_id="A",
            sandbox_id="C",
            backend_kind="custom",
            teardown_ref="fixtures.sandbox:destroy",
        )
        == "C"
    )
    assert await repository.get_sandbox_id(current_request.run_id) == "C"


@pytest.mark.asyncio
async def test_terminal_cleanup_intent_is_atomic_and_blocks_retention(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [60_000]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    current_request = _request(f"sandbox-cleanup-{uuid.uuid4().hex}")
    lease = await repository.try_claim(current_request, "sandbox-worker")
    assert lease is not None
    assert (
        await repository.bind_sandbox_id(
            current_request.run_id,
            lease,
            expected_sandbox_id=None,
            sandbox_id="custom-authoritative",
            backend_kind="custom",
            teardown_ref="fixtures.sandbox:destroy",
        )
        == "custom-authoritative"
    )

    assert await repository.try_mark_terminal(current_request.run_id, lease) is True
    claimed = await repository.claim_sandbox_cleanups(
        "cleanup-worker", run_id=current_request.run_id, limit=10, lease_ms=1_000
    )
    assert len(claimed) == 1
    intent = claimed[0]
    assert intent.run_id == current_request.run_id
    assert intent.lease_generation == lease.generation
    assert intent.backend_kind == "custom"
    assert intent.sandbox_id == "custom-authoritative"
    assert intent.teardown_ref == "fixtures.sandbox:destroy"
    assert intent.attempt_count == 1

    assert await repository.purge_terminal(0) == 0
    assert await repository.complete_sandbox_cleanup(intent.cleanup_id) is True
    assert await repository.purge_terminal(0) == 1


@pytest.mark.asyncio
async def test_emitter_recovers_index_reserved_by_queued_critical_frame(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [50_000]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    current_request = _request(f"outbox-index-{uuid.uuid4().hex}")
    stale = await repository.try_claim(current_request, "old-worker")
    assert stale is not None
    staged = await repository.stage_critical_frame(
        current_request.run_id,
        stale,
        "run.started",
        clock_ms[0],
        "{}",
        terminal=False,
    )
    assert staged is not None
    async with connect_pg(acceptance_state.config.database_url) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    """
                INSERT INTO {} (
                    run_id, durable_seq, event_id, status, reason, created_at
                ) VALUES (%s, %s, %s, 'persisted', NULL,
                          to_timestamp(%s / 1000.0))
                """
                ).format(
                    sql.Identifier(
                        acceptance_state.config.database_schema,
                        RUN_RECEIPTS_TABLE,
                    )
                ),
                (
                    current_request.run_id,
                    staged.durable_seq,
                    staged.event_id,
                    clock_ms[0],
                ),
            )
            await cursor.execute(
                sql.SQL(
                    """
                INSERT INTO {} (
                    run_id, persisted_seq, projected_seq, consumed_seq,
                    producer_close_requested, producer_closed, updated_at
                ) VALUES (%s, %s, %s, 0, FALSE, FALSE,
                          to_timestamp(%s / 1000.0))
                """
                ).format(
                    sql.Identifier(
                        acceptance_state.config.database_schema,
                        RUN_RECEIPT_MANIFESTS_TABLE,
                    )
                ),
                (
                    current_request.run_id,
                    staged.durable_seq,
                    staged.durable_seq,
                    clock_ms[0],
                ),
            )
    reconciled = await repository.reconcile_receipts(current_request.run_id)
    assert reconciled.consumed_through == staged.durable_seq

    clock_ms[0] += 11
    current = (await repository.reclaim_expired("new-worker"))[0].lease
    bus = FakeBus()

    emitter = await RunEmitter.attach(
        bus,
        current_request.run_id,
        outbox=repository,
        lease=current,
    )

    assert emitter.at_start is False
    payload = message_delta_payload("continued", segment_id="segment")
    assert payload is not None
    await emitter.emit(payload)
    published = bus.run_events(current_request.run_id)
    assert len(published) == 1
    assert published[0].index == 1


@pytest.mark.asyncio
async def test_concurrent_critical_and_live_events_allocate_unique_monotonic_indices(
    acceptance_state: _AcceptanceState,
) -> None:
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
        schema=acceptance_state.config.database_schema,
    )
    await repository.setup()
    current_request = _request(f"outbox-index-race-{uuid.uuid4().hex}")
    lease = await repository.try_claim(current_request, "worker")
    assert lease is not None
    bus = FakeBus()
    critical, live = await asyncio.gather(
        RunEmitter.attach(
            bus,
            current_request.run_id,
            outbox=repository,
            lease=lease,
        ),
        RunEmitter.attach(
            bus,
            current_request.run_id,
            outbox=repository,
            lease=lease,
        ),
    )
    delta = message_delta_payload("live", segment_id="segment")
    assert delta is not None

    await asyncio.gather(critical.emit(RunStartedPayload()), live.emit(delta))

    published = bus.run_events(current_request.run_id)
    assert sorted(event.index for event in published) == [0, 1]
    assert await repository.next_event_index(current_request.run_id) == 2


@pytest.mark.asyncio
async def test_dispatch_claim_conflict_keeps_durable_intent_pending(
    acceptance_state: _AcceptanceState,
) -> None:
    """The dispatch CAS and lease insert are one all-or-nothing transaction."""

    current_request = _request(f"dispatch-conflict-{uuid.uuid4().hex}")
    namespace = runtime_namespace(current_request.execution_identity)
    async with make_run_repository(
        acceptance_state.config.run_repository
    ) as repository:
        await repository.enqueue_dispatch(
            current_request,
            namespace,
            f"acceptance:{current_request.run_id}",
        )
        existing = await repository.try_claim(current_request, "existing-worker")
        assert existing is not None

        assert await repository.claim_dispatch(current_request, "late-worker") is None
        assert current_request in await repository.list_pending_dispatches()


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
    assert (
        _nested(_json_object(conflict.json()), "error")["code"]
        == "command_digest_mismatch"
    )

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

    hidden_evidence = await http_client.get(
        f"/v1/runs/{run_id}/events?after_seq=0&limit=10",
        headers=_headers("other-subject"),
    )
    assert hidden_evidence.status_code == 404
    assert (
        _nested(_json_object(hidden_evidence.json()), "error")["code"]
        == "run_not_found"
    )

    hidden_control = await http_client.post(
        f"/v1/runs/{run_id}/control",
        headers={**_headers("other-subject"), "Idempotency-Key": "command-hidden"},
        json={"kind": "run.cancel", "session_id": "session-1"},
    )
    assert hidden_control.status_code == 404
    assert (
        _nested(_json_object(hidden_control.json()), "error")["code"] == "run_not_found"
    )


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
