"""HTTP owner-interface acceptance against real PostgreSQL and Redis fixtures."""

from __future__ import annotations

from support.fakes import read_unpaused_interaction, settled_state_callback

from support.fakes import finish_run

from support.fakes import repository_terminal_callback

import asyncio
import json
import os
import threading
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any, LiteralString

import httpx
import psycopg
import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import StructuredTool
from pydantic import BaseModel as PydanticBaseModel
from pydantic import JsonValue, SecretStr, TypeAdapter
from psycopg import sql
from support.fakes import (
    FakeAgent,
    FakeBus,
    text_run,
    usage_recorder,
)
from support.deepagents import create_test_deep_agent
from support.local_fake import LocalFakeChatModel

from kokoro_agent.clients.system import ModelResolutionError
from kokoro_agent.application.chat.mappers import wire_epoch_millis_to_utc
from kokoro_agent.domain.chat.models import (
    ChatEventDraft,
    ChatEventRecord,
    ChatMessageDraft,
    ChatProjection,
)
from kokoro_agent.infrastructure.postgres_chat_repository import (
    PostgresChatRepository,
    PostgresChatRepositorySettings,
    make_chat_repository,
)
from kokoro_agent.protocol import (
    ExecutionIdentity,
    IdentityRef,
    MessageDeltaPayload,
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
from kokoro_agent.agent_factory import AgentHandle
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.worker.supervisor import RunSupervisor
from kokoro_agent.domain.run.scope import runtime_namespace
from kokoro_agent.execution.events import (
    RunEmitter,
    message_completed_payload,
    message_delta_payload,
)
from kokoro_agent.execution.failures import run_failed_payload
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.interfaces.http.execution_proof_jwks import ExecutionProofJwksState
from kokoro_agent.interfaces.http.server import create_http_server
from kokoro_agent.infrastructure.postgres_run_repository import (
    DEFAULT_LEASE_TTL_S,
    PostgresRunRepository,
    RunRepositorySettings,
    make_run_repository,
)
from kokoro_agent.infrastructure.postgres import connect_pg
from kokoro_agent.infrastructure.schema import (
    RUN_CLAIMS_TABLE,
    CHAT_EVENTS_TABLE,
    CHAT_MESSAGES_TABLE,
    CHAT_SEQUENCES_TABLE,
    RUN_OUTBOX_TABLE,
    RUN_RECEIPT_MANIFESTS_TABLE,
    RUN_RECEIPTS_TABLE,
    apply_agent_schema,
)
from kokoro_agent.streams.factory import StreamSettings
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.streams.protocol import StreamItem

_DATABASE_URL = os.environ.get(
    "KOKORO_AGENT_DATABASE_URL",
    "postgresql://kokoro:kokoro@127.0.0.1:55433/kokoro_worker_agent",
)
_REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:56380/9")
_INTERNAL_SECRET = "acceptance-internal-secret"
_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


class _LookupArgs(PydanticBaseModel):
    pass


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


def _launch_body(run_id: str, refs: tuple[str, ...] = ()) -> dict[str, JsonValue]:
    return _json_object(
        {
            "request_id": f"request-{run_id}",
            "run_id": run_id,
            "session_id": "session-1",
            "feature_key": "chat",
            "selected_skill_source_refs": list(refs),
            "message_id": f"message-{run_id}",
            "content": "hello from acceptance",
        }
    )


def _request(
    run_id: str, subject: str = "subject", refs: tuple[str, ...] = ()
) -> RunRequest:
    return RunRequest(
        kind="run.request",
        request_id=f"request-{run_id}",
        run_id=run_id,
        session_id="session-1",
        feature_key="chat",
        selected_skill_source_refs=refs,
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
    server = create_http_server(
        acceptance_state.config,
        "127.0.0.1",
        0,
        execution_proof_jwks=ExecutionProofJwksState(
            available=True,
            body=(
                b'{"keys":[{"alg":"EdDSA","crv":"Ed25519",'
                b'"kid":"acceptance","kty":"OKP","use":"sig",'
                b'"x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}]}'
            ),
        ),
    )
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
    chat_message_id = f"message-{request.run_id}"
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
                    chat_message_id=chat_message_id,
                    event_type="run.started",
                    payload_json='{"status":"running"}',
                    created_at=wire_epoch_millis_to_utc(1),
                ),
                message=ChatMessageDraft(
                    chat_message_id=chat_message_id,
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
    refs = ("skill:revision-a", "skill:revision-b")
    body = _launch_body(run_id, refs)

    first = await http_client.post("/v1/runs", headers=_headers(), json=body)
    assert first.status_code == 202
    first_data = _nested(_json_object(first.json()), "data")
    assert first_data["run_id"] == run_id
    assert first_data["replayed"] is False
    expected_request = _request(run_id, refs=refs)

    # Once the worker has claimed the durable intent, a retry must reuse the
    # receipt without publishing a second worker envelope.
    async with make_run_repository(
        acceptance_state.config.run_repository
    ) as run_repository:
        assert expected_request in await run_repository.list_pending_dispatches()
        assert (
            await run_repository.claim_dispatch(expected_request, "acceptance-worker")
            is not None
        )
        assert await run_repository.get_request(run_id) == expected_request

    second = await http_client.post("/v1/runs", headers=_headers(), json=body)
    assert second.status_code == 202
    second_data = _nested(_json_object(second.json()), "data")
    assert second_data["replayed"] is True

    changed = _launch_body(run_id, ("skill:revision-b", "skill:revision-a"))
    conflict = await http_client.post("/v1/runs", headers=_headers(), json=changed)
    assert conflict.status_code == 409
    assert _nested(_json_object(conflict.json()), "error")["code"] == (
        "run_identity_conflict"
    )

    published = await _read_matching(acceptance_state, REQUESTS_STREAM, run_id)
    assert len(published) == 1
    assert published[0]["kind"] == "run.request"
    assert published[0]["selected_skill_source_refs"] == list(refs)


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

    clock_ms = [int(time.time() * 1000)]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    current_request = _request(f"lease-generation-{uuid.uuid4().hex}")

    first = await repository.try_claim(current_request, "same-worker-name")
    assert first is not None
    clock_ms[0] += 10_001
    assert await repository.renew(current_request.run_id, first) is False
    reclaimed = await repository.reclaim_expired("same-worker-name")
    assert len(reclaimed) == 1
    second = reclaimed[0].lease

    assert second.generation == first.generation + 1
    assert await repository.renew(current_request.run_id, first) is False
    assert await finish_run(repository, current_request.run_id, first) is False
    assert await repository.renew(current_request.run_id, second) is True
    assert await finish_run(repository, current_request.run_id, second) is True


@pytest.mark.asyncio
async def test_postgres_execution_effects_require_current_lease_generation(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [int(time.time() * 1000)]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    current_request = _request(f"effect-fence-{uuid.uuid4().hex}")

    stale = await repository.try_claim(current_request, "reused-worker-name")
    assert stale is not None
    await repository.add_steer(current_request.run_id, "steer-1", "keep me")
    clock_ms[0] += 10_001
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
    clock_ms = [int(time.time() * 1000)]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    chat_repository = PostgresChatRepository(
        acceptance_state.config.database_url,
        schema=acceptance_state.config.database_schema,
    )
    await repository.setup()
    await chat_repository.setup()
    current_request = _request(f"chat-fence-mode-{uuid.uuid4().hex}")
    lease = await repository.try_claim(current_request, "chat-worker")
    assert lease is not None
    clock_ms[0] += 10_001
    async with await psycopg.AsyncConnection.connect(
        acceptance_state.config.database_url
    ) as conn:
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=%s"
            ).format(
                sql.Identifier(
                    acceptance_state.config.database_schema, RUN_CLAIMS_TABLE
                )
            ),
            (current_request.run_id,),
        )
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
    assert await repository.add_usage(current_request.run_id, lease, 5, 7) == (5, 7)
    assert await finish_run(repository, current_request.run_id, lease) is True
    assert await repository.add_usage(current_request.run_id, lease, 5, 7) == (5, 7)
    with pytest.raises(RuntimeError, match="usage.*identity|identity.*usage"):
        await repository.add_usage(current_request.run_id, lease, 6, 7)


@pytest.mark.asyncio
async def test_live_publish_does_not_hold_run_lock_while_network_waits(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [int(time.time() * 1000)]
    config = acceptance_state.config
    repository = PostgresRunRepository(
        config.database_url,
        ttl_ms=10_000,
        schema=config.database_schema,
        clock=lambda: clock_ms[0],
    )
    current_request = _request(f"live-without-db-lock-{uuid.uuid4().hex}")
    lease = await repository.try_claim(current_request, "old-worker")
    assert lease is not None
    effect_started, release_effect = asyncio.Event(), asyncio.Event()

    class PausedBus(FakeBus):
        async def publish(
            self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
        ) -> StreamItem:
            effect_started.set()
            await release_effect.wait()
            return await super().publish(stream, event, maxlen=maxlen)

    chat = PostgresChatRepository(config.database_url, config.database_schema)
    emitter = await RunEmitter.attach(
        PausedBus(),
        current_request.run_id,
        outbox=repository,
        lease=lease,
        chat_repository=chat,
        tenant_id=current_request.execution_identity.tenant_ref,
        namespace=runtime_namespace(current_request.execution_identity),
        session_id=current_request.session_id,
    )
    task = asyncio.create_task(
        emitter.emit(MessageDeltaPayload(segment_id="segment", delta="hello"))
    )
    try:
        await asyncio.wait_for(effect_started.wait(), timeout=1)
        clock_ms[0] += 10_001
        reclaimed = await asyncio.wait_for(
            repository.reclaim_expired("new-worker"), timeout=1
        )
        assert len(reclaimed) == 1
        assert reclaimed[0].lease.generation == lease.generation + 1
    finally:
        release_effect.set()
        await task


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
    clock_ms = [int(time.time() * 1000)]
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

    assert await finish_run(repository, current_request.run_id, lease) is True
    async with connect_pg(acceptance_state.config.database_url) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now"
            )
            database_clock = await cursor.fetchone()
            assert database_clock is not None
            clock_ms[0] = int(database_clock["now"])
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

    clock_ms[0] = int(time.time() * 1000) + 1
    assert await repository.purge_terminal(0) == 0
    assert await repository.complete_sandbox_cleanup(intent.cleanup_id) is True
    assert await repository.purge_terminal(0) == 1


@pytest.mark.asyncio
async def test_emitter_recovers_index_reserved_by_queued_critical_frame(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [int(time.time() * 1000)]
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
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

    clock_ms[0] += 10_001
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

    old_body = _launch_body(f"old-bff-{uuid.uuid4().hex}")
    del old_body["selected_skill_source_refs"]
    old_body["trace"] = {"pinned_skills": ["skill:revision-a"]}
    old_response = await http_client.post("/v1/runs", headers=_headers(), json=old_body)
    assert old_response.status_code == 400
    assert _nested(_json_object(old_response.json()), "error")["code"] == (
        "invalid_launch_request"
    )


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


@pytest.mark.asyncio
async def test_empty_final_segment_persists_and_replays_after_tool(
    acceptance_state: _AcceptanceState,
    http_client: httpx.AsyncClient,
) -> None:
    from kokoro_agent.infrastructure.checkpoints import (
        CheckpointSettings,
        make_checkpointer,
    )
    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.protocol.events import ChatInteractionState

    run_id = f"empty-final-{uuid.uuid4().hex}"
    namespace = runtime_namespace(_identity())
    stream_name = run_events_stream(run_id)
    stream = RedisStream(acceptance_state.redis_url)
    settings = PostgresChatRepositorySettings(
        database_url=acceptance_state.config.database_url,
        schema_name=acceptance_state.config.database_schema,
    )
    try:
        async with (
            make_run_repository(acceptance_state.config.run_repository) as runs,
            make_chat_repository(settings) as chat,
            make_checkpointer(
                CheckpointSettings(
                    database_url=acceptance_state.config.database_url,
                    schema_name=acceptance_state.config.database_schema,
                )
            ) as saver,
        ):
            request = _request(run_id)
            thread_id = RunScope.of(request).scoped_thread_id
            await runs.enqueue_dispatch(request, namespace, f"acceptance:{run_id}")
            lease = await runs.claim_dispatch(request, "empty-final-worker")
            assert lease is not None
            emitter = await RunEmitter.attach(
                stream,
                run_id,
                outbox=runs,
                lease=lease,
                tenant_id="tenant",
                namespace=namespace,
                session_id="session-1",
                chat_repository=chat,
            )

            def lookup() -> str:
                return "done"

            tool = StructuredTool(
                name="lookup",
                description="deterministic lookup",
                args_schema=_LookupArgs,
                func=lookup,
            )
            agent = create_test_deep_agent(
                model=LocalFakeChatModel.with_script(
                    [
                        AIMessage(
                            content="draft",
                            id="draft-segment",
                            tool_calls=[
                                {
                                    "name": "lookup",
                                    "args": {},
                                    "id": "tool-1",
                                    "type": "tool_call",
                                }
                            ],
                        ),
                        AIMessage(
                            content="",
                            id="tool-segment",
                            tool_calls=[
                                {
                                    "name": "lookup",
                                    "args": {},
                                    "id": "tool-2",
                                    "type": "tool_call",
                                }
                            ],
                        ),
                        AIMessage(content="", id="final-segment"),
                    ]
                ),
                tools=[tool],
                system_prompt="x",
                subagents=[],
                checkpointer=saver,
                permissions=[],
                interrupt_on={},
            )
            await invoke_once(
                emitter,
                agent,
                thread_id,
                {"messages": [HumanMessage(content="go")]},
                approval_tool_names=frozenset(),
                source_for=lambda _name: "runtime-custom",
                finalize_terminal=repository_terminal_callback(
                    runs, stream, run_id, lease
                ),
                record_usage=usage_recorder()[0],
                on_native_settled=settled_state_callback(agent, thread_id),
            )

        # 重新打开 SQL repository，证据来自落库后的 replay 而非进程内缓存。
        async with make_chat_repository(settings) as reopened:
            replay = await reopened.replay("tenant", namespace, "session-1")
            history = await reopened.history("tenant", namespace, "session-1")
        completed = [
            (event.seq, json.loads(event.payload_json)["content"])
            for event in replay
            if event.event_type == "assistant.completed"
        ]
        assert [content for _seq, content in completed] == ["draft", "", ""]
        assert [seq for seq, _content in completed] == sorted(
            seq for seq, _content in completed
        )
        assert [message.content for message in history] == ["draft", "", ""]
        replay_by_index = {event.source_index: event.seq for event in replay}
        response = await http_client.get(
            "/v1/sessions/session-1/events", headers=_headers()
        )
        assert response.status_code == 200
        page = _nested(_json_object(response.json()), "data")
        http_events = page["events"]
        assert isinstance(http_events, list)
        completed_http: list[str] = []
        http_seq_by_index: dict[int, int] = {}
        for item in http_events:
            record = _json_object(item)
            source_index, seq = record["source_index"], record["seq"]
            assert isinstance(source_index, int) and isinstance(seq, int)
            http_seq_by_index[source_index] = seq
            if record["event_type"] != "assistant.completed":
                continue
            payload_json = record["payload_json"]
            assert isinstance(payload_json, str)
            content = json.loads(payload_json)["content"]
            assert isinstance(content, str)
            completed_http.append(content)
        assert completed_http == ["draft", "", ""]
        assert http_seq_by_index == replay_by_index
        wire = await stream.read_all(stream_name)
        assert [event.event_type for event in replay[-2:]] == [
            "interaction.state",
            "run.completed",
        ]
        interaction = replay[-2]
        terminal_state = ChatInteractionState.model_validate_json(
            interaction.payload_json
        )
        assert terminal_state.model_dump(mode="json") == {
            "interaction_revision": 1,
            "pause_revision": 0,
            "pause_ref": None,
            "phase": "terminal",
            "groups": [],
            "action_result": None,
        }
        assert replay[-1].seq == interaction.seq + 1
        assert replay[-1].source_index == interaction.source_index + 1
        wire_indices: list[int] = []
        for item in wire:
            index = item.event["index"]
            assert isinstance(index, int)
            wire_indices.append(index)
        assert wire_indices == sorted(set(wire_indices))
        assert interaction.source_index not in wire_indices
        assert sorted([*wire_indices, interaction.source_index]) == list(
            range(len(wire) + 1)
        )
        assert wire_indices[-1] == replay[-1].source_index
        assert wire[-1].event["kind"] == "run.completed"
        assert [item.event["kind"] for item in wire].count("message.delta") == 1
        ordered = [
            (item.event["kind"], item.event["payload"])
            for item in wire
            if item.event["kind"]
            in {"message.completed", "tool.invoked", "tool.returned"}
        ]
        assert [kind for kind, _payload in ordered] == [
            "message.completed",
            "tool.invoked",
            "tool.returned",
            "message.completed",
            "tool.invoked",
            "tool.returned",
            "message.completed",
        ]
        completed_indices: list[int] = []
        for item in wire:
            if item.event["kind"] != "message.completed":
                continue
            index = item.event["index"]
            assert isinstance(index, int)
            completed_indices.append(index)
        assert [replay_by_index[index] for index in completed_indices] == [
            seq for seq, _content in completed
        ]
    finally:
        await stream.delete(stream_name)
        await stream.aclose()


@pytest.mark.asyncio
async def test_stale_lease_cannot_publish_or_persist_empty_completion(
    acceptance_state: _AcceptanceState,
) -> None:
    clock_ms = [int(time.time() * 1000)]
    run_id = f"stale-empty-final-{uuid.uuid4().hex}"
    namespace = runtime_namespace(_identity())
    repository = PostgresRunRepository(
        acceptance_state.config.database_url,
        ttl_ms=10_000,
        schema=acceptance_state.config.database_schema,
        clock=lambda: clock_ms[0],
    )
    await repository.setup()
    request = _request(run_id)
    await repository.enqueue_dispatch(request, namespace, f"acceptance:{run_id}")
    stale = await repository.claim_dispatch(request, "reused-worker")
    assert stale is not None
    stream_name = run_events_stream(run_id)
    stream = RedisStream(acceptance_state.redis_url)
    chat = PostgresChatRepository(
        acceptance_state.config.database_url,
        schema=acceptance_state.config.database_schema,
    )
    await chat.setup()
    try:
        stale_emitter = await RunEmitter.attach(
            stream,
            run_id,
            outbox=repository,
            lease=stale,
            tenant_id="tenant",
            namespace=namespace,
            session_id="session-1",
            chat_repository=chat,
        )
        clock_ms[0] += 10_001
        reclaimed = await repository.reclaim_expired("reused-worker")
        assert len(reclaimed) == 1
        current = reclaimed[0].lease
        assert current.generation == stale.generation + 1

        await stale_emitter.emit(
            message_completed_payload("", segment_id="stale-empty")
        )
        assert await repository.next_event_index(run_id) == 0
        assert await stream.read_all(stream_name) == []
        assert await chat.replay("tenant", namespace, "session-1") == ()

        current_emitter = await RunEmitter.attach(
            stream,
            run_id,
            outbox=repository,
            lease=current,
            tenant_id="tenant",
            namespace=namespace,
            session_id="session-1",
            chat_repository=chat,
        )
        await current_emitter.emit(
            message_completed_payload("", segment_id="current-empty")
        )
        assert await repository.next_event_index(run_id) == 1
        replay = await chat.replay("tenant", namespace, "session-1")
        assert len(replay) == 1
        assert json.loads(replay[0].payload_json)["content"] == ""
        wire = await stream.read_all(stream_name)
        assert len(wire) == 1
        assert wire[0].event["kind"] == "message.completed"
        assert wire[0].event["index"] == replay[0].source_index == 0
    finally:
        await stream.delete(stream_name)
        await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", [True, False])
async def test_safe_model_failure_is_durable_and_replayed_over_http(
    acceptance_state: _AcceptanceState,
    http_client: httpx.AsyncClient,
    retryable: bool,
) -> None:
    from kokoro_agent.protocol.events import ChatInteractionState

    expected_interaction: dict[str, JsonValue] = {
        "interaction_revision": 1,
        "pause_revision": 0,
        "pause_ref": None,
        "phase": "terminal",
        "groups": [],
        "action_result": None,
    }
    run_id = f"safe-failure-{uuid.uuid4().hex}"
    current_request = _request(run_id)
    namespace = runtime_namespace(current_request.execution_identity)
    stream_name = run_events_stream(run_id)
    stream = RedisStream(acceptance_state.redis_url)
    settings = PostgresChatRepositorySettings(
        database_url=acceptance_state.config.database_url,
        schema_name=acceptance_state.config.database_schema,
    )
    expected_run = {"code": "model_unavailable", "retryable": retryable}
    expected_chat = {"status": "failed", **expected_run}
    error = ModelResolutionError("MODEL_UNAVAILABLE", retryable=retryable)
    error.args = ("SENTINEL_PROVIDER_TOKEN_PASSWORD",)
    try:
        async with (
            make_run_repository(acceptance_state.config.run_repository) as runs,
            make_chat_repository(settings) as chat,
        ):
            await runs.enqueue_dispatch(
                current_request, namespace, f"acceptance:{run_id}"
            )
            lease = await runs.claim_dispatch(current_request, "safe-failure-worker")
            assert lease is not None
            from kokoro_agent.domain.run.models import (
                ExecutionTerminalAuthority,
                RunTerminalOutcome,
            )
            from kokoro_agent.execution.events import outbox_wire_event

            payload = run_failed_payload(error, code="assembly_failed")
            assert payload.model_dump() == expected_run
            authority = ExecutionTerminalAuthority(lease=lease)
            outcome = RunTerminalOutcome(payload=payload, usage=None)
            committed = await runs.finalize_terminal(run_id, authority, outcome, ())
            assert committed.status == "committed"
            assert len(committed.retained_frames) == 1
            # A repeated terminal request reads the same fact, never stages a second row.
            replayed = await runs.finalize_terminal(run_id, authority, outcome, ())
            assert replayed.status == "replayed"
            assert replayed.retained_frames == committed.retained_frames
            for frame in committed.retained_frames:
                await runs.verify_terminal_frame(frame)
                await stream.publish(stream_name, outbox_wire_event(frame), maxlen=100)
                await runs.mark_critical_published(run_id, frame.durable_seq)
            assert (
                await runs.finalize_terminal(run_id, authority, outcome, ())
            ).retained_frames == ()
            first_replay = await chat.replay("tenant", namespace, "session-1")
            assert [
                (event.event_type, event.seq, event.source_index)
                for event in first_replay
            ] == [("interaction.state", 1, 0), ("run.failed", 2, 1)]
            assert await runs.is_terminal(run_id)

        # Read durable owner outbox bytes independently of the emitter instance.
        async with connect_pg(acceptance_state.config.database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(
                        "SELECT durable_seq, status, index_value, kind, payload_json "
                        "FROM {} WHERE run_id = %s ORDER BY durable_seq"
                    ).format(
                        sql.Identifier(
                            acceptance_state.config.database_schema, RUN_OUTBOX_TABLE
                        )
                    ),
                    (run_id,),
                )
                rows = await cursor.fetchall()
        assert [
            (row["durable_seq"], row["status"], row["index_value"]) for row in rows
        ] == [(1, "published", 1)]
        outbox_payloads: list[str] = []
        for row in rows:
            assert row["kind"] == "run.failed"
            outbox_payload = row["payload_json"]
            assert isinstance(outbox_payload, str)
            assert json.loads(outbox_payload) == expected_run
            outbox_payloads.append(outbox_payload)

        async with make_chat_repository(settings) as reopened:
            persisted = await reopened.replay("tenant", namespace, "session-1")
        assert persisted == first_replay
        assert [
            (event.event_type, event.seq, event.source_index) for event in persisted
        ] == [("interaction.state", 1, 0), ("run.failed", 2, 1)]
        assert (
            ChatInteractionState.model_validate_json(
                persisted[0].payload_json
            ).model_dump(mode="json")
            == expected_interaction
        )
        assert json.loads(persisted[1].payload_json) == expected_chat

        wire = await stream.read_all(stream_name)
        assert len(wire) == 1
        assert wire[0].event["kind"] == "run.failed"
        assert wire[0].event["payload"] == expected_run
        assert (
            persisted[1].source_index
            == wire[0].event["index"]
            == rows[0]["index_value"]
        )

        evidence = await http_client.get(
            f"/v1/runs/{run_id}/events?after_seq=-1&limit=10", headers=_headers()
        )
        assert evidence.status_code == 200
        evidence_data = _nested(_json_object(evidence.json()), "data")
        assert evidence_data["terminal"] is True
        events = evidence_data["events"]
        assert isinstance(events, list) and len(events) == 1
        assert _json_object(events[0])["payload"] == expected_run

        response = await http_client.get(
            "/v1/sessions/session-1/events", headers=_headers()
        )
        assert response.status_code == 200
        page = _nested(_json_object(response.json()), "data")
        records = page["events"]
        assert isinstance(records, list) and len(records) == 2
        http_records = [_json_object(item) for item in records]
        assert [
            (item["event_type"], item["seq"], item["source_index"])
            for item in http_records
        ] == [("interaction.state", 1, 0), ("run.failed", 2, 1)]
        for item, durable in zip(http_records, persisted, strict=True):
            assert item["run_id"] == run_id
            assert item["session_id"] == current_request.session_id
            assert item["chat_event_id"] == durable.chat_event_id
            assert item["payload_json"] == durable.payload_json
        interaction_json = http_records[0]["payload_json"]
        assert isinstance(interaction_json, str)
        assert (
            ChatInteractionState.model_validate_json(interaction_json).model_dump(
                mode="json"
            )
            == expected_interaction
        )
        record = http_records[1]
        assert record["event_type"] == "run.failed"
        assert record["run_id"] == run_id
        assert record["seq"] == persisted[1].seq
        assert record["source_index"] == wire[0].event["index"]
        raw = record["payload_json"]
        assert isinstance(raw, str)
        assert json.loads(raw) == expected_chat
        for representation in (
            *outbox_payloads,
            *(event.payload_json for event in persisted),
            evidence.text,
            response.text,
        ):
            assert "SENTINEL" not in representation
            assert "ModelResolutionError" not in representation
            assert "error_kind" not in representation
    finally:
        try:
            await stream.delete(stream_name)
        finally:
            await stream.aclose()


@pytest.mark.asyncio
async def test_terminal_chat_write_failure_rolls_back_run_and_outbox(
    acceptance_state: _AcceptanceState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production supervisor/repositories: terminal Chat failure is not a sealed Run."""
    config = acceptance_state.config
    runs = PostgresRunRepository(
        config.database_url, ttl_ms=10_000, schema=config.database_schema
    )
    chat = PostgresChatRepository(config.database_url, schema=config.database_schema)
    current_request = _request(f"terminal-chat-rollback-{uuid.uuid4().hex}")
    agent = FakeAgent(run=text_run("terminal transaction"))
    bus = FakeBus()
    terminal_chat_attempts = 0
    original_append = PostgresChatRepository.append_on_cursor

    async def fail_terminal_chat(
        self: PostgresChatRepository, cur: Any, projection: ChatProjection
    ) -> ChatEventRecord:
        nonlocal terminal_chat_attempts
        if projection.event.event_type in {"run.completed", "run.failed"}:
            terminal_chat_attempts += 1
            await original_append(self, cur, projection)
            raise RuntimeError("injected terminal Chat persistence failure")
        return await original_append(self, cur, projection)

    monkeypatch.setattr(PostgresChatRepository, "append_on_cursor", fail_terminal_chat)

    async def build(_request: RunRequest, _lease: LeaseFence) -> AgentHandle:
        return AgentHandle(runnable=agent, tool_descriptions={})

    supervisor = RunSupervisor(
        interaction_reader=read_unpaused_interaction,
        agent_builder=build,
        run_repository=runs,
        approval_tool_names=lambda _request: frozenset(),
        trace_factory=lambda _request: None,
        source_for=lambda _name: "runtime-custom",
        consumer="terminal-atomic-test",
        chat_repository=chat,
    )
    try:
        namespace = runtime_namespace(current_request.execution_identity)
        await runs.enqueue_dispatch(
            current_request,
            namespace,
            f"acceptance:{current_request.run_id}",
        )
        await supervisor.dispatch(bus, current_request)
        outcomes = await asyncio.gather(
            *tuple(supervisor.tasks.values()), return_exceptions=True
        )
        assert terminal_chat_attempts > 0
        for outcome in outcomes:
            assert outcome is None or (
                isinstance(outcome, RuntimeError)
                and "injected terminal Chat persistence failure" in str(outcome)
            )
        # Use a fresh repository instance, not supervisor's in-memory flags.
        recovered = PostgresRunRepository(
            config.database_url, ttl_ms=10_000, schema=config.database_schema
        )
        assert await recovered.is_terminal(current_request.run_id) is False
        pending = await recovered.list_unpublished_outbox()
        assert not any(
            frame.run_id == current_request.run_id
            and frame.kind in {"run.completed", "run.failed"}
            for frame in pending
        )
        replay = await chat.replay(
            current_request.execution_identity.tenant_ref,
            runtime_namespace(current_request.execution_identity),
            current_request.session_id,
        )
        assert not any(
            event.event_type in {"run.completed", "run.failed"} for event in replay
        )
    finally:
        tasks = tuple(supervisor.tasks.values()) + tuple(
            supervisor.control_listeners.values()
        )
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("matching", [True, False])
async def test_terminal_checks_rejected_receipt_identity_under_run_lock(
    acceptance_state: _AcceptanceState,
    matching: bool,
) -> None:
    from kokoro_agent.domain.run.models import (
        ExecutionTerminalAuthority,
        RunTerminalOutcome,
    )

    config = acceptance_state.config
    repository = PostgresRunRepository(
        config.database_url, ttl_ms=30_000, schema=config.database_schema
    )
    run = _request(f"terminal-rejected-{uuid.uuid4().hex}")
    lease = await repository.try_claim(run, "worker")
    assert lease is not None
    frame = await repository.stage_critical_frame(
        run.run_id, lease, "run.started", int(time.time() * 1000), "{}", terminal=False
    )
    assert frame is not None
    async with await psycopg.AsyncConnection.connect(config.database_url) as conn:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {} (run_id,durable_seq,event_id,status,created_at) VALUES (%s,%s,%s,'rejected',clock_timestamp())"
            ).format(sql.Identifier(config.database_schema, RUN_RECEIPTS_TABLE)),
            (
                run.run_id,
                frame.durable_seq,
                frame.event_id if matching else "wrong-event",
            ),
        )
    result = await repository.finalize_terminal(
        run.run_id,
        ExecutionTerminalAuthority(lease=lease),
        RunTerminalOutcome(
            payload=RunCompletedPayload(status="completed", token_usage=None),
            usage=None,
        ),
        (),
    )
    assert result.status == ("lost" if matching else "committed")
    assert await repository.is_terminal(run.run_id) is (not matching)
    if matching:
        assert await repository.list_unpublished_outbox() != []
        chat = PostgresChatRepository(config.database_url, config.database_schema)
        assert (
            await chat.replay(
                run.execution_identity.tenant_ref,
                runtime_namespace(run.execution_identity),
                run.session_id,
            )
            == ()
        )


@pytest.mark.parametrize(
    "column,value",
    [
        ("session_id", "wrong-session"),
        ("chat_event_id", "wrong-chat-id"),
        ("payload_json", '{"status":"cancelled"}'),
    ],
)
async def test_terminal_recovery_rejects_corrupt_chat_identity(
    acceptance_state: _AcceptanceState,
    column: str,
    value: str,
) -> None:
    from kokoro_agent.domain.run.models import (
        ExecutionTerminalAuthority,
        RunTerminalOutcome,
    )
    from kokoro_agent.infrastructure.schema import CHAT_EVENTS_TABLE

    config = acceptance_state.config
    repository = PostgresRunRepository(
        config.database_url, ttl_ms=30_000, schema=config.database_schema
    )
    run = _request(f"terminal-chat-identity-{uuid.uuid4().hex}")
    lease = await repository.try_claim(run, "worker")
    assert lease is not None
    committed = await repository.finalize_terminal(
        run.run_id,
        ExecutionTerminalAuthority(lease=lease),
        RunTerminalOutcome(
            payload=RunCompletedPayload(status="completed", token_usage=None),
            usage=None,
        ),
        (),
    )
    frame = committed.retained_frames[0]
    await repository.verify_terminal_frame(frame)
    async with await psycopg.AsyncConnection.connect(config.database_url) as conn:
        unaffected_query = sql.SQL(
            "SELECT * FROM {} WHERE run_id=%s AND event_type='interaction.state' ORDER BY source_index"
        ).format(sql.Identifier(config.database_schema, CHAT_EVENTS_TABLE))
        before_cursor = await conn.execute(unaffected_query, (run.run_id,))
        unaffected_before = await before_cursor.fetchall()
        assert len(unaffected_before) == 1
        assert frame.kind == "run.completed"
        changed = await conn.execute(
            sql.SQL(
                "UPDATE {} SET {}=%s WHERE run_id=%s AND source_index=%s AND event_type=%s"
            ).format(
                sql.Identifier(config.database_schema, CHAT_EVENTS_TABLE),
                sql.Identifier(column),
            ),
            (value, run.run_id, frame.index, frame.kind),
        )
        assert changed.rowcount == 1
        after_cursor = await conn.execute(unaffected_query, (run.run_id,))
        assert await after_cursor.fetchall() == unaffected_before
    with pytest.raises(RuntimeError, match="terminal Chat"):
        await repository.verify_terminal_frame(frame)
    assert await repository.list_unpublished_outbox() == [frame]


@pytest.mark.parametrize(
    "tamper", ["kind", "payload", "index", "time", "seq", "counter"]
)
async def test_quarantine_replay_rejects_private_audit_tampering(
    acceptance_state: _AcceptanceState,
    tamper: str,
) -> None:
    from kokoro_agent.domain.run.models import (
        QuarantineTerminalAuthority,
        RunTerminalOutcome,
    )
    from kokoro_agent.protocol import RunFailedPayload

    config = acceptance_state.config
    repository = PostgresRunRepository(
        config.database_url, ttl_ms=30_000, schema=config.database_schema
    )
    run = _request(f"quarantine-audit-{uuid.uuid4().hex}")
    lease = await repository.try_claim(run, "worker")
    assert lease is not None
    offending = await repository.stage_critical_frame(
        run.run_id, lease, "run.started", int(time.time() * 1000), "{}", terminal=False
    )
    assert offending is not None
    async with connect_pg(config.database_url) as conn:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {} (run_id,durable_seq,event_id,status) VALUES (%s,%s,%s,'rejected')"
            ).format(sql.Identifier(config.database_schema, RUN_RECEIPTS_TABLE)),
            (run.run_id, offending.durable_seq, offending.event_id),
        )
        await conn.commit()
    authority = QuarantineTerminalAuthority(
        owner="quarantine", rejected_seq=offending.durable_seq
    )
    outcome = RunTerminalOutcome(
        payload=RunFailedPayload(code="contract_incompatible", retryable=False),
        usage=None,
    )
    committed = await repository.finalize_terminal(run.run_id, authority, outcome, ())
    assert committed.status == "committed" and committed.retained_frames == ()
    assert (
        await repository.finalize_terminal(run.run_id, authority, outcome, ())
    ).status == "replayed"
    assignments: dict[str, LiteralString] = {
        "kind": "kind='run.completed'",
        "payload": "payload_json='{}'",
        "index": "index_value=0",
        "time": "occurred_at=occurred_at+interval '1 second'",
        "seq": "durable_seq=durable_seq+10",
    }
    async with connect_pg(config.database_url) as conn:
        if tamper == "counter":
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET durable_counter=durable_counter+10 WHERE run_id=%s"
                ).format(sql.Identifier(config.database_schema, RUN_CLAIMS_TABLE)),
                (run.run_id,),
            )
        else:
            await conn.execute(
                sql.SQL("UPDATE {} SET {} WHERE run_id=%s AND durable_seq>%s").format(
                    sql.Identifier(config.database_schema, RUN_OUTBOX_TABLE),
                    sql.SQL(assignments[tamper]),
                ),
                (run.run_id, offending.durable_seq),
            )
        await conn.commit()
    replay = await repository.finalize_terminal(run.run_id, authority, outcome, ())
    assert replay.status == "lost" and replay.retained_frames == ()
    chat = PostgresChatRepository(config.database_url, config.database_schema)
    assert (
        await chat.replay(
            run.execution_identity.tenant_ref,
            runtime_namespace(run.execution_identity),
            run.session_id,
        )
        == ()
    )


@pytest.mark.asyncio
async def test_http4_resume_full_identity_and_conflict_receipt_preserve_waiting(
    acceptance_state: _AcceptanceState,
    http_client: httpx.AsyncClient,
) -> None:
    from support.fakes import interaction_pause_fixture
    from kokoro_agent.protocol import RunResume, SubagentSource
    from kokoro_agent.protocol.control import control_request_digest
    from kokoro_agent.infrastructure.schema import RUN_CONTROL_COMMANDS_TABLE

    run_id = f"http4-resume-{uuid.uuid4().hex}"
    launched = await http_client.post(
        "/v1/runs", headers=_headers(), json=_launch_body(run_id)
    )
    assert launched.status_code == 202
    async with make_run_repository(acceptance_state.config.run_repository) as runs:
        request = await runs.get_pending_dispatch(run_id)
        assert request is not None
        lease = await runs.claim_dispatch(request, "http4-worker")
        assert lease is not None
        await runs.record_pause(request, lease, interaction_pause_fixture(request))
        before = await runs.read_interaction(request)
        assert before is not None
        replay_before = await http_client.get(
            "/v1/sessions/session-1/events?after_seq=0", headers=_headers()
        )
        assert replay_before.status_code == 200
        source_before = _nested(_json_object(replay_before.json()), "data")
        body: dict[str, JsonValue] = {
            "kind": "run.resume",
            "session_id": request.session_id,
            "expected_pause_revision": 1,
            "pause_ref": "pause-1",
            "decisions": [{"type": "approve", "item_id": "item-A"}],
        }
        invalid_requests: list[dict[str, JsonValue]] = [
            {**body, "decisions": [{"type": "approve", "tool_id": "call-A"}]},
            {
                **body,
                "decisions": [{"type": "submit", "request_id": "call-A", "value": {}}],
            },
            {
                key: value
                for key, value in body.items()
                if key != "expected_pause_revision"
            },
            {
                **body,
                "decisions": [
                    {"type": "approve", "item_id": "item-A"},
                    {"type": "approve", "item_id": "item-A"},
                ],
            },
        ]
        for index, invalid in enumerate(invalid_requests):
            response = await http_client.post(
                f"/v1/runs/{run_id}/control",
                headers={**_headers(), "Idempotency-Key": f"invalid-{index}"},
                json=invalid,
            )
            assert response.status_code == 400
            assert (
                _nested(_json_object(response.json()), "error")["code"]
                == "invalid_run_control"
            )

        stale_body = {**body, "expected_pause_revision": 2}
        stale = await http_client.post(
            f"/v1/runs/{run_id}/control",
            headers={**_headers(), "Idempotency-Key": "stale"},
            json=stale_body,
        )
        assert stale.status_code == 202  # Admission, not native acceptance.
        frames = await _read_matching(
            acceptance_state, run_control_stream(run_id), run_id
        )
        command = RunResume.model_validate(
            next(frame for frame in frames if frame.get("command_id") == "stale")
        )

        async def build(_request: RunRequest, _lease: LeaseFence) -> AgentHandle:
            raise AssertionError("stale collection must not build")

        def source_for(_name: str) -> SubagentSource:
            raise AssertionError("stale collection must not resolve a peer")

        supervisor = RunSupervisor(
            interaction_reader=read_unpaused_interaction,
            agent_builder=build,
            run_repository=runs,
            approval_tool_names=lambda _request: frozenset(),
            trace_factory=lambda _request: None,
            source_for=source_for,
            consumer="http4-worker",
        )
        # Actual worker's typed acceptance boundary, using the durable HTTP command.
        # No native handle is constructed and no provider is involved.
        await supervisor.dispatch(FakeBus(), command)
        replay = await http_client.post(
            f"/v1/runs/{run_id}/control",
            headers={**_headers(), "Idempotency-Key": "stale"},
            json=stale_body,
        )
        assert replay.status_code == 202
        receipt = _nested(_json_object(replay.json()), "data")
        assert receipt["status"] == "failed" and receipt["replayed"] is True
        assert receipt["error_code"] == "interaction_conflict"
        assert "reason" not in receipt
        assert await runs.read_interaction(request) == before
        assert not await runs.is_terminal(run_id)

        valid = await http_client.post(
            f"/v1/runs/{run_id}/control",
            headers={**_headers(), "Idempotency-Key": "valid"},
            json=body,
        )
        assert valid.status_code == 202
        assert _nested(_json_object(valid.json()), "data")["status"] == "pending"
        replay = await http_client.post(
            f"/v1/runs/{run_id}/control",
            headers={**_headers(), "Idempotency-Key": "valid"},
            json={
                **body,
                "decisions": [{"type": "approve", "item_id": "item-A", "args": None}],
            },
        )
        assert replay.status_code == 202
        assert _nested(_json_object(replay.json()), "data")["replayed"] is True
        assert await runs.read_resume_context(request, "valid") is None
        assert await runs.read_interaction(request) == before

    async with connect_pg(acceptance_state.config.database_url) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    "SELECT command_id, body, request_digest, status, error_code FROM {}.{} WHERE run_id = %s ORDER BY command_id"
                ).format(
                    sql.Identifier(acceptance_state.config.database_schema),
                    sql.Identifier(RUN_CONTROL_COMMANDS_TABLE),
                ),
                (run_id,),
            )
            rows = await cursor.fetchall()
    assert len(rows) == 2  # Invalid legacy/missing/duplicate requests made zero rows.
    for row in rows:
        command_id, stored_body, digest = (
            row["command_id"],
            row["body"],
            row["request_digest"],
        )
        status, error_code = row["status"], row["error_code"]
        parsed = RunResume.model_validate_json(stored_body)
        assert stored_body.encode("utf-8") == parsed.model_dump_json().encode("utf-8")
        assert digest == parsed.request_digest == control_request_digest(parsed)
        if command_id == "stale":
            assert (status, error_code) == ("failed", "interaction_conflict")
        else:
            assert command_id == "valid" and (status, error_code) == ("admitted", None)
    replay_after = await http_client.get(
        "/v1/sessions/session-1/events?after_seq=0", headers=_headers()
    )
    assert replay_after.status_code == 200
    assert _nested(_json_object(replay_after.json()), "data") == source_before
