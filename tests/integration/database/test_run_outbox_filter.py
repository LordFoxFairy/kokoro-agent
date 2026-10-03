"""The PostgreSQL outbox filter preserves queued-only replay behavior."""

from __future__ import annotations

from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest


def _request(run_id: str) -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        session_id="session-outbox-filter",
        feature_key="chat",
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant-outbox-filter",
            actor=IdentityRef(kind="user", opaque_ref="actor-outbox-filter"),
            subject=IdentityRef(kind="user", opaque_ref="subject-outbox-filter"),
            identity_assertion_ref="assertion-outbox-filter",
        ),
        input=RunInput(message_id=f"message-{run_id}", content="outbox filter"),
    )


async def test_list_unpublished_outbox_returns_only_queued_rows(
    run_repository: RunRepository,
) -> None:
    request = _request("outbox-filter")
    lease = await run_repository.try_claim(request, "outbox-filter-worker")
    assert lease is not None

    first = await run_repository.stage_critical_frame(
        request.run_id,
        lease,
        "message.delta",
        1_700_000_000_000,
        '{"delta":"first"}',
        terminal=False,
    )
    second = await run_repository.stage_critical_frame(
        request.run_id,
        lease,
        "message.delta",
        1_700_000_000_001,
        '{"delta":"second"}',
        terminal=False,
    )
    assert first is not None
    assert second is not None

    await run_repository.mark_critical_published(request.run_id, first.durable_seq)

    unpublished = await run_repository.list_unpublished_outbox()
    assert [
        (frame.run_id, frame.durable_seq, frame.payload_json) for frame in unpublished
    ] == [(request.run_id, second.durable_seq, '{"delta":"second"}')]


async def test_started_competing_connections_and_lost_ack_reuse_original_frame(
    run_repository: RunRepository,
) -> None:
    import asyncio

    request = _request("started-competition")
    lease = await run_repository.try_claim(request, "worker-start")
    assert lease is not None
    assert await run_repository.reserve_event_index(request.run_id, lease) == 0
    # Each PostgreSQL stage call opens its own connection/transaction. An ACK
    # dropped by the caller is reproduced by retrying without publishing first.
    first, second = await asyncio.gather(
        *[
            run_repository.stage_critical_frame(
                request.run_id, lease, "run.started", timestamp, "{}", terminal=False
            )
            for timestamp in (1_700_000_000_000, 1_700_000_000_999)
        ]
    )
    assert first is not None and second is not None
    assert first.event_id == second.event_id
    assert first.index == second.index == 1
    assert first.timestamp == second.timestamp
    assert first.durable_seq == second.durable_seq
    assert sum([first.newly_staged, second.newly_staged]) == 1
    await run_repository.mark_critical_published(request.run_id, first.durable_seq)
    replay = await run_repository.stage_critical_frame(
        request.run_id, lease, "run.started", 1_700_000_002_000, "{}", terminal=False
    )
    assert replay is not None
    assert replay.event_id == first.event_id
    assert replay.timestamp == first.timestamp
    assert replay.published is True and replay.newly_staged is False
    assert await run_repository.next_event_index(request.run_id) == 2


def _todo_projection(request: RunRequest, source_index: int, *, empty: bool = False):
    from datetime import UTC, datetime

    from kokoro_agent.domain.chat.projection import project_chat_fact
    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.protocol import TodoUpdatedPayload

    projection = project_chat_fact(
        tenant_id=request.execution_identity.tenant_ref,
        namespace=RunScope.of(request).namespace,
        session_id=request.session_id,
        run_id=request.run_id,
        source_index=source_index,
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        payload=TodoUpdatedPayload.model_validate(
            {"todos": [] if empty else [{"content": "检查 🦊", "status": "pending"}]}
        ),
    )
    assert projection is not None
    return projection


async def _todo_storage_snapshot(
    database_url: str, schema: str, run_id: str
) -> tuple[tuple[str, ...], ...]:
    """Independent SQL observations, including counters hidden by max(seq)."""
    from kokoro_agent.infrastructure.postgres import connect_pg, qualified
    from kokoro_agent.infrastructure.schema import (
        CHAT_EVENTS_TABLE,
        CHAT_SEQUENCES_TABLE,
        RUN_CLAIMS_TABLE,
    )
    from kokoro_agent.infrastructure.sql import execute_sql, fetch_all

    snapshots: list[tuple[str, ...]] = []
    async with connect_pg(database_url) as conn, conn.cursor() as cur:
        for table, ordering in (
            (CHAT_EVENTS_TABLE, "chat_event_id"),
            (CHAT_SEQUENCES_TABLE, "kind, tenant_id, namespace, session_id"),
        ):
            await execute_sql(
                cur,
                f"SELECT row_to_json(t)::text AS value FROM {qualified(schema, table)} t ORDER BY {ordering}",
            )
            snapshots.append(tuple(str(row["value"]) for row in await fetch_all(cur)))
        await execute_sql(
            cur,
            "SELECT event_index_counter, durable_counter FROM {} WHERE run_id=%s".format(
                qualified(schema, RUN_CLAIMS_TABLE)
            ),
            (run_id,),
        )
        snapshots.append(
            tuple(
                f"{row['event_index_counter']}:{row['durable_counter']}"
                for row in await fetch_all(cur)
            )
        )
    return tuple(snapshots)


async def _wait_for_todo_lock_waiters(
    database_url: str, blocker_pid: int, expected: int
) -> None:
    """Observe actual backends, including a row-lock queue, not task scheduling."""
    import asyncio

    from kokoro_agent.infrastructure.postgres import connect_pg
    from kokoro_agent.infrastructure.sql import execute_sql, fetch_all

    async with (
        asyncio.timeout(5),
        connect_pg(database_url) as observer,
        observer.cursor() as cur,
    ):
        while True:
            await execute_sql(
                cur,
                """WITH RECURSIVE waiters AS (
                SELECT pid FROM pg_stat_activity
                WHERE datname=current_database() AND %s=ANY(pg_blocking_pids(pid))
                UNION
                SELECT a.pid FROM pg_stat_activity a JOIN waiters w
                ON w.pid=ANY(pg_blocking_pids(a.pid))
                WHERE a.datname=current_database()
            ) SELECT DISTINCT pid FROM waiters""",
                (blocker_pid,),
            )
            if len(await fetch_all(cur)) == expected:
                return


async def test_todo_complete_ordered_snapshot_then_empty_replays_from_fresh_repository(
    run_repository: RunRepository, run_chat_database_url: str, run_chat_schema: str
) -> None:
    from collections.abc import Mapping
    from datetime import UTC, datetime

    from pydantic import JsonValue
    from support.fakes import FakeBus

    from kokoro_agent.domain.chat.models import ChatEventRecord, chat_event_id
    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.execution.events import RunEmitter
    from kokoro_agent.infrastructure.postgres_chat_repository import (
        PostgresChatRepository,
    )
    from kokoro_agent.protocol import TodoUpdatedPayload, agent_event_adapter
    from kokoro_agent.streams.protocol import StreamItem

    request = _request("todo-ordered-empty")
    scope = RunScope.of(request)
    tenant = request.execution_identity.tenant_ref
    lease = await run_repository.try_claim(request, "todo-writer")
    assert lease is not None
    repository = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    observed: list[tuple[ChatEventRecord, ...]] = []

    class CommitObservingBus(FakeBus):
        async def publish(
            self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
        ) -> StreamItem:
            # Separate connection must see commit before live send. Assert outside
            # publish because the emitter deliberately catches live exceptions.
            reader = PostgresChatRepository(run_chat_database_url, run_chat_schema)
            observed.append(
                await reader.replay(tenant, scope.namespace, scope.session_id)
            )
            return await super().publish(stream, event, maxlen=maxlen)

    bus = CommitObservingBus()
    emitter = await RunEmitter.attach(
        bus,
        request.run_id,
        outbox=run_repository,
        lease=lease,
        tenant_id=tenant,
        namespace=scope.namespace,
        session_id=scope.session_id,
        chat_repository=repository,
    )
    expected = (
        '{"todos":[{"content":"检查 🦊","status":"completed"},'
        '{"content":"第二步","status":"in_progress"},'
        '{"content":"检查 🦊","status":"pending"}]}'
    ).encode("utf-8")
    await emitter.emit(TodoUpdatedPayload.model_validate_json(expected))
    await emitter.emit(TodoUpdatedPayload(todos=[]))
    fresh = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    await fresh.setup()
    records = await fresh.replay(tenant, scope.namespace, scope.session_id)
    assert [record.seq for record in records] == [1, 2]
    assert [record.payload_json.encode("utf-8") for record in records] == [
        expected,
        b'{"todos":[]}',
    ]
    assert observed == [records[:1], records]
    live = [agent_event_adapter.validate_python(item[1]) for item in bus.published]
    assert len(live) == 2
    for record, event in zip(records, live, strict=True):
        assert record.event_type == event.kind == "todo.updated"
        assert (
            record.tenant_id,
            record.namespace,
            record.session_id,
            record.run_id,
        ) == (tenant, scope.namespace, scope.session_id, request.run_id)
        assert record.source_index == event.index
        assert record.chat_event_id == chat_event_id(
            scope.namespace, request.run_id, event.index
        )
        assert record.chat_message_id is None
        assert record.created_at == datetime.fromtimestamp(
            event.timestamp / 1000, tz=UTC
        )
        assert isinstance(event.payload, TodoUpdatedPayload)
        assert event.payload.canonical_bytes() == record.payload_json.encode("utf-8")
    assert len({record.chat_event_id for record in records}) == 2
    assert (
        await fresh.replay(tenant, scope.namespace, scope.session_id, after_seq=1)
        == records[1:]
    )
    assert await fresh.watermark(tenant, scope.namespace, scope.session_id) == 2


async def test_todo_competing_connections_and_lost_ack_preserve_identity_and_sequence(
    run_repository: RunRepository, run_chat_database_url: str, run_chat_schema: str
) -> None:
    import asyncio

    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.infrastructure.postgres import connect_pg, qualified
    from kokoro_agent.infrastructure.postgres_chat_repository import (
        PostgresChatRepository,
    )
    from kokoro_agent.infrastructure.schema import RUN_CLAIMS_TABLE
    from kokoro_agent.infrastructure.sql import execute_sql, fetch_one

    request = _request("todo-concurrent-retry")
    scope = RunScope.of(request)
    tenant = request.execution_identity.tenant_ref
    lease = await run_repository.try_claim(request, "todo-writer")
    assert lease is not None
    index = await run_repository.reserve_event_index(request.run_id, lease)
    assert index is not None
    projection = _todo_projection(request, index)
    repositories = [
        PostgresChatRepository(run_chat_database_url, run_chat_schema) for _ in range(2)
    ]
    async with connect_pg(run_chat_database_url) as blocker, blocker.cursor() as cur:
        await execute_sql(cur, "BEGIN")
        await execute_sql(cur, "SELECT pg_backend_pid() AS pid")
        backend = await fetch_one(cur)
        assert backend is not None
        await execute_sql(
            cur,
            "SELECT run_id FROM {} WHERE run_id=%s FOR UPDATE".format(
                qualified(run_chat_schema, RUN_CLAIMS_TABLE)
            ),
            (request.run_id,),
        )
        tasks = [
            asyncio.create_task(repo.append_fenced(projection, lease, mode="active"))
            for repo in repositories
        ]
        try:
            await _wait_for_todo_lock_waiters(
                run_chat_database_url, int(backend["pid"]), 2
            )
            await blocker.commit()
            async with asyncio.timeout(5):
                first, second = await asyncio.gather(*tasks)
        finally:
            await blocker.rollback()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert first is not None and first == second
    assert first.seq == 1 and first.source_index == index
    before = await _todo_storage_snapshot(
        run_chat_database_url, run_chat_schema, request.run_id
    )
    fresh = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    # Caller lost the committed acknowledgement: retry the immutable draft.
    assert await fresh.append_fenced(projection, lease, mode="active") == first
    assert await fresh.replay(tenant, scope.namespace, scope.session_id) == (first,)
    assert (
        await _todo_storage_snapshot(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        == before
    )
    next_index = await run_repository.reserve_event_index(request.run_id, lease)
    assert next_index is not None and next_index == index + 1
    next_record = await fresh.append_fenced(
        _todo_projection(request, next_index, empty=True), lease, mode="active"
    )
    assert next_record is not None and next_record.seq == 2


async def test_todo_payload_time_and_session_drift_roll_back_every_counter(
    run_repository: RunRepository, run_chat_database_url: str, run_chat_schema: str
) -> None:
    from datetime import timedelta

    import pytest

    from kokoro_agent.domain.chat.repositories import ChatIdentityConflict
    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.infrastructure.postgres_chat_repository import (
        PostgresChatRepository,
    )

    request = _request("todo-identity-drift")
    scope = RunScope.of(request)
    tenant = request.execution_identity.tenant_ref
    lease = await run_repository.try_claim(request, "todo-writer")
    assert lease is not None
    repository = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    assert await run_repository.reserve_event_index(request.run_id, lease) == 0
    projection = _todo_projection(request, 0)
    original = await repository.append_fenced(projection, lease, mode="active")
    assert original is not None
    before = await _todo_storage_snapshot(
        run_chat_database_url, run_chat_schema, request.run_id
    )
    for changed in (
        {"payload_json": '{"todos":[]}'},
        {"created_at": projection.event.created_at + timedelta(milliseconds=1)},
        {"session_id": "session-drift"},
    ):
        drift = projection.model_copy(
            update={"event": projection.event.model_copy(update=changed)}
        )
        with pytest.raises(ChatIdentityConflict):
            await repository.append_fenced(drift, lease, mode="active")
        assert (
            await _todo_storage_snapshot(
                run_chat_database_url, run_chat_schema, request.run_id
            )
            == before
        )
        fresh = PostgresChatRepository(run_chat_database_url, run_chat_schema)
        assert await fresh.replay(tenant, scope.namespace, scope.session_id) == (
            original,
        )
        assert (
            await fresh.watermark(tenant, scope.namespace, scope.session_id)
            == original.seq
        )
        assert await fresh.replay(tenant, scope.namespace, "session-drift") == ()
        assert await fresh.watermark(tenant, scope.namespace, "session-drift") == 0


async def test_todo_wrong_owner_old_generation_and_terminal_reject_new_facts(
    run_repository: RunRepository, run_chat_database_url: str, run_chat_schema: str
) -> None:
    from support.fakes import finish_run

    from kokoro_agent.domain.run.models import LeaseFence
    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.infrastructure.postgres import connect_pg, qualified
    from kokoro_agent.infrastructure.postgres_chat_repository import (
        PostgresChatRepository,
    )
    from kokoro_agent.infrastructure.schema import RUN_CLAIMS_TABLE
    from kokoro_agent.infrastructure.sql import execute_sql

    request = _request("todo-fence-rejections")
    scope = RunScope.of(request)
    tenant = request.execution_identity.tenant_ref
    lease = await run_repository.try_claim(request, "todo-writer")
    assert lease is not None
    repository = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    assert await run_repository.reserve_event_index(request.run_id, lease) == 0
    original = await repository.append_fenced(
        _todo_projection(request, 0), lease, mode="active"
    )
    assert original is not None
    new_fact = _todo_projection(request, 1, empty=True)
    before = await _todo_storage_snapshot(
        run_chat_database_url, run_chat_schema, request.run_id
    )
    assert (
        await repository.append_fenced(
            new_fact,
            LeaseFence(owner="wrong-owner", generation=lease.generation),
            mode="active",
        )
        is None
    )
    assert (
        await _todo_storage_snapshot(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        == before
    )
    assert (
        await repository.watermark(tenant, scope.namespace, scope.session_id)
        == original.seq
    )

    # Fault setup only; production recovery owns the generation change.
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "UPDATE {} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=%s".format(
                qualified(run_chat_schema, RUN_CLAIMS_TABLE)
            ),
            (request.run_id,),
        )
    reclaimed = await run_repository.reclaim_expired("todo-writer")
    assert len(reclaimed) == 1
    current = reclaimed[0].lease
    assert current.owner == lease.owner and current.generation > lease.generation
    before = await _todo_storage_snapshot(
        run_chat_database_url, run_chat_schema, request.run_id
    )
    assert await repository.append_fenced(new_fact, lease, mode="active") is None
    assert (
        await _todo_storage_snapshot(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        == before
    )
    assert (
        await repository.watermark(tenant, scope.namespace, scope.session_id)
        == original.seq
    )

    assert await finish_run(run_repository, request.run_id, current)
    terminal_records = await repository.replay(
        tenant, scope.namespace, scope.session_id
    )
    watermark = await repository.watermark(tenant, scope.namespace, scope.session_id)
    before = await _todo_storage_snapshot(
        run_chat_database_url, run_chat_schema, request.run_id
    )
    terminal_index = await run_repository.next_event_index(request.run_id)
    assert (
        await repository.append_fenced(
            _todo_projection(request, terminal_index, empty=True),
            current,
            mode="active",
        )
        is None
    )
    fresh = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    assert (
        await fresh.replay(tenant, scope.namespace, scope.session_id)
        == terminal_records
    )
    assert await fresh.watermark(tenant, scope.namespace, scope.session_id) == watermark
    assert (
        await _todo_storage_snapshot(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        == before
    )


async def test_todo_lease_expiring_while_waiting_for_run_lock_rejects_append(
    run_repository: RunRepository, run_chat_database_url: str, run_chat_schema: str
) -> None:
    import asyncio

    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.infrastructure.postgres import connect_pg, qualified
    from kokoro_agent.infrastructure.postgres_chat_repository import (
        PostgresChatRepository,
    )
    from kokoro_agent.infrastructure.schema import RUN_CLAIMS_TABLE
    from kokoro_agent.infrastructure.sql import execute_sql, fetch_one

    request = _request("todo-expiry-lock-wait")
    scope = RunScope.of(request)
    tenant = request.execution_identity.tenant_ref
    lease = await run_repository.try_claim(request, "todo-writer")
    assert lease is not None
    repository = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    assert await run_repository.reserve_event_index(request.run_id, lease) == 0
    original = await repository.append_fenced(
        _todo_projection(request, 0), lease, mode="active"
    )
    assert original is not None
    before = await _todo_storage_snapshot(
        run_chat_database_url, run_chat_schema, request.run_id
    )
    table = qualified(run_chat_schema, RUN_CLAIMS_TABLE)
    async with connect_pg(run_chat_database_url) as blocker, blocker.cursor() as cur:
        await execute_sql(
            cur,
            f"UPDATE {table} SET lease_expires_at=clock_timestamp()+interval '2 seconds' WHERE run_id=%s RETURNING lease_expires_at",
            (request.run_id,),
        )
        expires = await fetch_one(cur)
        assert expires is not None
        await execute_sql(cur, "BEGIN")
        await execute_sql(
            cur,
            f"SELECT run_id FROM {table} WHERE run_id=%s FOR UPDATE",
            (request.run_id,),
        )
        await execute_sql(cur, "SELECT pg_backend_pid() AS pid")
        backend = await fetch_one(cur)
        assert backend is not None
        task = asyncio.create_task(
            repository.append_fenced(
                _todo_projection(request, 1, empty=True), lease, mode="active"
            )
        )
        try:
            await _wait_for_todo_lock_waiters(
                run_chat_database_url, int(backend["pid"]), 1
            )
            async with (
                asyncio.timeout(5),
                connect_pg(run_chat_database_url) as observer,
                observer.cursor() as probe,
            ):
                await execute_sql(
                    probe,
                    "SELECT xact_start < %s AS started_before_expiry FROM pg_stat_activity WHERE datname=current_database() AND %s=ANY(pg_blocking_pids(pid))",
                    (expires["lease_expires_at"], backend["pid"]),
                )
                waiting = await fetch_one(probe)
                assert waiting is not None and waiting["started_before_expiry"] is True
                while True:
                    await execute_sql(
                        probe,
                        "SELECT clock_timestamp() >= %s AS expired",
                        (expires["lease_expires_at"],),
                    )
                    clock = await fetch_one(probe)
                    assert clock is not None
                    if clock["expired"]:
                        break
                    # Poll cadence only; catalog wait and DB clock prove both facts.
                    await asyncio.sleep(0.01)
            assert not task.done()
            await blocker.commit()
            async with asyncio.timeout(5):
                assert await task is None
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    fresh = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    assert await fresh.replay(tenant, scope.namespace, scope.session_id) == (original,)
    assert (
        await fresh.watermark(tenant, scope.namespace, scope.session_id) == original.seq
    )
    assert (
        await _todo_storage_snapshot(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        == before
    )


async def test_todo_live_publish_failure_keeps_committed_fact_replayable(
    run_repository: RunRepository, run_chat_database_url: str, run_chat_schema: str
) -> None:
    from collections.abc import Mapping

    from pydantic import JsonValue
    from support.fakes import FakeBus

    from kokoro_agent.domain.chat.models import ChatEventRecord
    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.execution.events import RunEmitter
    from kokoro_agent.infrastructure.postgres_chat_repository import (
        PostgresChatRepository,
    )
    from kokoro_agent.protocol import TodoUpdatedPayload
    from kokoro_agent.streams.protocol import StreamItem

    request = _request("todo-live-failure")
    scope = RunScope.of(request)
    tenant = request.execution_identity.tenant_ref
    lease = await run_repository.try_claim(request, "todo-writer")
    assert lease is not None
    observed: list[tuple[ChatEventRecord, ...]] = []

    class FailedLiveBus(FakeBus):
        async def publish(
            self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
        ) -> StreamItem:
            reader = PostgresChatRepository(run_chat_database_url, run_chat_schema)
            observed.append(
                await reader.replay(tenant, scope.namespace, scope.session_id)
            )
            raise OSError("injected live boundary failure")

    # Only live boundary is fake; this is not Redis integration evidence.
    bus = FailedLiveBus()
    repository = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    emitter = await RunEmitter.attach(
        bus,
        request.run_id,
        outbox=run_repository,
        lease=lease,
        tenant_id=tenant,
        namespace=scope.namespace,
        session_id=scope.session_id,
        chat_repository=repository,
    )
    expected = '{"todos":[{"content":"已落盘 🦊","status":"in_progress"}]}'.encode(
        "utf-8"
    )
    await emitter.emit(TodoUpdatedPayload.model_validate_json(expected))
    fresh = PostgresChatRepository(run_chat_database_url, run_chat_schema)
    records = await fresh.replay(tenant, scope.namespace, scope.session_id)
    assert len(records) == 1 and observed == [records]
    assert bus.published == []
    record = records[0]
    assert record.event_type == "todo.updated" and record.seq == 1
    assert record.payload_json.encode("utf-8") == expected
    assert await fresh.watermark(tenant, scope.namespace, scope.session_id) == 1
    before = await _todo_storage_snapshot(
        run_chat_database_url, run_chat_schema, request.run_id
    )
    assert (
        await PostgresChatRepository(run_chat_database_url, run_chat_schema).replay(
            tenant, scope.namespace, scope.session_id
        )
        == records
    )
    assert (
        await _todo_storage_snapshot(
            run_chat_database_url, run_chat_schema, request.run_id
        )
        == before
    )
