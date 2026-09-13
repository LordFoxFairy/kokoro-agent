"""Real PostgreSQL and controlled lifecycle tests for proof lease reads."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta, tzinfo
import os
import re
import time
import traceback
from types import TracebackType
from typing import Any, Self
import uuid

import psycopg
from psycopg import conninfo, sql
from psycopg.rows import dict_row
import pytest

from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.execution.execution_proof_supplier import (
    CurrentLeaseObservation,
    ExecutionProofUnavailableError,
)
from kokoro_agent.infrastructure.postgres_execution_proof_lease import (
    CLEANUP_ALLOWANCE_SECONDS,
    PostgresExecutionProofLeaseConfig,
    PostgresExecutionProofLeaseReader,
)
from kokoro_agent.infrastructure.schema import apply_agent_schema


DATABASE_URL = os.environ.get(
    "KOKORO_AGENT_DATABASE_URL",
    "postgresql://kokoro:kokoro@127.0.0.1:55433/kokoro_worker_agent",
)
MAX_SAFE_INTEGER = 9_007_199_254_740_991
TEST_SCHEDULER_TOLERANCE = 3.0


class _Cursor:
    def __init__(
        self,
        *,
        row: object,
        stall_stage: str | None = None,
        release: asyncio.Event | None = None,
    ) -> None:
        self.row = row
        self.stall_stage = stall_stage
        self.release = release
        self.sql_text = ""
        self.params: object = None
        self.stage_calls: list[str] = []

    async def _stage(self, name: str) -> None:
        self.stage_calls.append(name)
        if self.stall_stage == name:
            assert self.release is not None
            await self.release.wait()

    async def __aenter__(self) -> Self:
        await self._stage("enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._stage("exit")

    async def execute(self, query: object, params: object = None) -> None:
        self.sql_text = str(query)
        self.params = params
        await self._stage("execute")

    async def fetchone(self) -> object:
        await self._stage("fetch")
        return self.row


class _Connection:
    def __init__(
        self,
        cursor: _Cursor,
        *,
        close_release: asyncio.Event | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self._cursor = cursor
        self._close_release = close_release
        self._close_error = close_error
        self.close_calls = 0
        self.closed = False

    def cursor(self) -> _Cursor:
        return self._cursor

    async def close(self) -> None:
        self.close_calls += 1
        if self._close_release is not None:
            await self._close_release.wait()
        if self._close_error is not None:
            raise self._close_error
        self.closed = True


class _ProcessControlSignal(BaseException):
    pass


@pytest.mark.parametrize(
    "kwargs",
    [
        {"database_url": "", "schema_name": "valid"},
        {"database_url": "postgresql://host/db\x00", "schema_name": "valid"},
        {"database_url": "not a conninfo", "schema_name": "valid"},
        {"database_url": "postgresql://host/db", "schema_name": "bad-name"},
        {"database_url": "postgresql://host/db", "schema_name": "é"},
        {"database_url": "postgresql://host/db", "schema_name": "a" * 64},
        {
            "database_url": "postgresql://host/db",
            "schema_name": "valid",
            "total_deadline_seconds": True,
        },
        {
            "database_url": "postgresql://host/db",
            "schema_name": "valid",
            "total_deadline_seconds": float("inf"),
        },
        {
            "database_url": "postgresql://host/db",
            "schema_name": "valid",
            "total_deadline_seconds": 0,
        },
        {
            "database_url": "postgresql://host/db",
            "schema_name": "valid",
            "total_deadline_seconds": 2.0001,
        },
    ],
)
def test_config_rejects_invalid_values_without_exposing_them(
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(ExecutionProofUnavailableError) as caught:
        PostgresExecutionProofLeaseConfig(**kwargs)

    rendered = repr(caught.value) + "".join(traceback.format_exception(caught.value))
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "host" not in rendered
    assert "bad-name" not in rendered


def test_config_is_exact_frozen_redacted_and_defaults_to_two_seconds() -> None:
    config = PostgresExecutionProofLeaseConfig(
        database_url="postgresql://host/db", schema_name="proof_schema"
    )

    assert config.total_deadline_seconds == 2.0
    assert "host" not in repr(config)
    assert "proof_schema" not in repr(config)
    with pytest.raises((AttributeError, TypeError)):
        setattr(config, "schema_name", "mutated")


def test_reader_rejects_config_subclass_without_leaking_fields() -> None:
    class ConfigSubclass(PostgresExecutionProofLeaseConfig):
        pass

    config = ConfigSubclass(
        database_url="postgresql://URL_SENTINEL/db",
        schema_name="SCHEMA_SENTINEL",
    )
    with pytest.raises(ExecutionProofUnavailableError) as caught:
        PostgresExecutionProofLeaseReader(config)
    rendered = repr(caught.value) + "".join(traceback.format_exception(caught.value))
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "URL_SENTINEL" not in rendered
    assert "SCHEMA_SENTINEL" not in rendered


@pytest.mark.asyncio
@pytest.mark.parametrize("stall_stage", ["enter", "execute", "fetch", "exit"])
async def test_statement_stages_share_total_deadline_close_then_cancel(
    monkeypatch: pytest.MonkeyPatch, stall_stage: str
) -> None:
    release = asyncio.Event()
    cursor = _Cursor(
        row=(
            datetime.now(UTC),
            datetime.now(UTC) + timedelta(seconds=10),
        ),
        stall_stage=stall_stage,
        release=release,
    )
    connection = _Connection(cursor)

    async def connect_direct(database_url: str) -> _Connection:
        assert database_url == "postgresql://host/db"
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(ExecutionProofUnavailableError):
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert connection.close_calls == 1
    assert connection.closed
    assert not [
        task for task in asyncio.all_tasks() if task is not asyncio.current_task()
    ]


@pytest.mark.asyncio
async def test_connection_wait_is_in_total_deadline_and_has_no_connection_to_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        started.set()
        await asyncio.Event().wait()
        raise AssertionError

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(ExecutionProofUnavailableError):
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert started.is_set()
    assert not [
        task for task in asyncio.all_tasks() if task is not asyncio.current_task()
    ]


@pytest.mark.asyncio
async def test_single_and_double_external_cancel_preserve_first_object_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for cancel_count in (1, 2):
        release = asyncio.Event()
        cursor = _Cursor(row=None, stall_stage="execute", release=release)
        connection = _Connection(cursor)

        async def connect_direct(database_url: str) -> _Connection:
            del database_url
            return connection

        monkeypatch.setattr(
            "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
            connect_direct,
        )
        reader = PostgresExecutionProofLeaseReader(
            PostgresExecutionProofLeaseConfig(
                database_url="postgresql://host/db", schema_name="proof_schema"
            )
        )
        task = asyncio.create_task(
            reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )
        )
        while "execute" not in cursor.stage_calls:
            await asyncio.sleep(0)
        first = asyncio.CancelledError("FIRST_CANCEL")
        task.cancel(first)
        if cancel_count == 2:
            await asyncio.sleep(0)
            task.cancel("SECOND_CANCEL")
        with pytest.raises(asyncio.CancelledError) as caught:
            async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
                await task
        assert caught.value.args == (first,)
        assert connection.close_calls == 1
        assert connection.closed
        assert not [
            live
            for live in asyncio.all_tasks()
            if live is not asyncio.current_task() and not live.done()
        ]


@pytest.mark.asyncio
async def test_cancel_during_close_is_not_swallowed_and_close_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for cancel_count in (1, 2):
        close_release = asyncio.Event()
        database_now = datetime.now(UTC)
        connection = _Connection(
            _Cursor(row=(database_now, database_now + timedelta(seconds=10))),
            close_release=close_release,
        )

        async def connect_direct(database_url: str) -> _Connection:
            del database_url
            return connection

        monkeypatch.setattr(
            "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
            connect_direct,
        )
        reader = PostgresExecutionProofLeaseReader(
            PostgresExecutionProofLeaseConfig(
                database_url="postgresql://host/db", schema_name="proof_schema"
            )
        )
        task = asyncio.create_task(
            reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )
        )
        while connection.close_calls == 0:
            await asyncio.sleep(0)
        task.cancel("FIRST_CLOSE_CANCEL")
        if cancel_count == 2:
            await asyncio.sleep(0)
            task.cancel("SECOND_CLOSE_CANCEL")
        close_release.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
                await task
        assert caught.value.args == ("FIRST_CLOSE_CANCEL",)
        assert connection.close_calls == 1
        assert connection.closed
        assert not [
            live
            for live in asyncio.all_tasks()
            if live is not asyncio.current_task() and not live.done()
        ]


@pytest.mark.asyncio
async def test_cleanup_budget_failure_leaves_no_background_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_release = asyncio.Event()
    database_now = datetime.now(UTC)
    connection = _Connection(
        _Cursor(row=(database_now, database_now + timedelta(seconds=10))),
        close_release=close_release,
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )
    started = time.monotonic()
    with pytest.raises(ExecutionProofUnavailableError) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    elapsed = time.monotonic() - started
    assert 0.09 <= elapsed <= CLEANUP_ALLOWANCE_SECONDS + 0.10
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert connection.close_calls == 1
    assert not connection.closed
    assert not [
        live
        for live in asyncio.all_tasks()
        if live is not asyncio.current_task() and not live.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("stall_stage", ["execute", "fetch"])
@pytest.mark.parametrize("close_mode", ["raise", "hang"])
async def test_close_failure_never_skips_active_operation_settlement(
    monkeypatch: pytest.MonkeyPatch, stall_stage: str, close_mode: str
) -> None:
    operation_release = asyncio.Event()
    close_release = asyncio.Event() if close_mode == "hang" else None
    close_error = RuntimeError("CLOSE_SENTINEL") if close_mode == "raise" else None
    cursor = _Cursor(
        row=(datetime.now(UTC), datetime.now(UTC) + timedelta(seconds=10)),
        stall_stage=stall_stage,
        release=operation_release,
    )
    connection = _Connection(
        cursor,
        close_release=close_release,
        close_error=close_error,
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(ExecutionProofUnavailableError):
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("stall_stage", ["execute", "fetch"])
@pytest.mark.parametrize("cancel_count", [1, 2])
async def test_external_cancel_during_hung_close_still_settles_active_operation(
    monkeypatch: pytest.MonkeyPatch, stall_stage: str, cancel_count: int
) -> None:
    operation_release = asyncio.Event()
    cursor = _Cursor(row=None, stall_stage=stall_stage, release=operation_release)
    connection = _Connection(
        cursor,
        close_release=asyncio.Event(),
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )
    task = asyncio.create_task(
        reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )
    )
    while stall_stage not in cursor.stage_calls:
        await asyncio.sleep(0)
    task.cancel("FIRST_CANCEL")
    while connection.close_calls == 0:
        await asyncio.sleep(0)
    if cancel_count == 2:
        task.cancel("SECOND_CANCEL")

    with pytest.raises(asyncio.CancelledError) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await task

    assert caught.value.args == ("FIRST_CANCEL",)
    assert not [
        owned
        for owned in asyncio.all_tasks()
        if owned is not asyncio.current_task() and not owned.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("close_mode", ["raise", "hang"])
async def test_connect_race_close_failure_settles_acquisition_task(
    monkeypatch: pytest.MonkeyPatch, close_mode: str
) -> None:
    close_release = asyncio.Event() if close_mode == "hang" else None
    close_error = RuntimeError("CLOSE_SENTINEL") if close_mode == "raise" else None
    connection = _Connection(
        _Cursor(row=None), close_release=close_release, close_error=close_error
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return connection
        raise AssertionError

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(ExecutionProofUnavailableError):
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("late_outcome", ["connection", "exception"])
async def test_second_cleanup_cancel_collects_late_acquisition_outcome(
    monkeypatch: pytest.MonkeyPatch, late_outcome: str
) -> None:
    cancel_count = 0
    connection = _Connection(_Cursor(row=None))

    async def connect_direct(database_url: str) -> _Connection:
        nonlocal cancel_count
        del database_url
        while True:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancel_count += 1
                if cancel_count == 1:
                    continue
                if late_outcome == "connection":
                    return connection
                raise RuntimeError("LATE_CONNECT_SENTINEL") from None

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(ExecutionProofUnavailableError) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert cancel_count == 2
    assert connection.close_calls == (1 if late_outcome == "connection" else 0)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["execute", "exit"])
@pytest.mark.parametrize("close_mode", ["success", "raise", "hang", "signal"])
async def test_process_control_base_exception_always_cleans_owned_connection(
    monkeypatch: pytest.MonkeyPatch, failure_stage: str, close_mode: str
) -> None:
    signal = _ProcessControlSignal(f"PROCESS_CONTROL_{failure_stage}")
    close_signal = _ProcessControlSignal("SECONDARY_CLOSE_SIGNAL")

    class ProcessControlCursor(_Cursor):
        async def execute(self, query: object, params: object = None) -> None:
            if failure_stage == "execute":
                raise signal
            await super().execute(query, params)

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            if failure_stage == "exit":
                raise signal
            await super().__aexit__(exc_type, exc, tb)

    database_now = datetime.now(UTC)
    connection = _Connection(
        ProcessControlCursor(row=(database_now, database_now + timedelta(seconds=10))),
        close_release=asyncio.Event() if close_mode == "hang" else None,
        close_error=(
            RuntimeError("CLOSE_SENTINEL")
            if close_mode == "raise"
            else close_signal
            if close_mode == "signal"
            else None
        ),
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(_ProcessControlSignal) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert caught.value is signal
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["execute", "fetch"])
@pytest.mark.parametrize("primary_kind", ["process_control", "cancelled"])
@pytest.mark.parametrize("close_mode", ["success", "raise", "hang"])
async def test_operation_primary_beats_secondary_cursor_exit_signal(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    primary_kind: str,
    close_mode: str,
) -> None:
    primary: BaseException
    if primary_kind == "cancelled":
        primary = asyncio.CancelledError(f"PRIMARY_{failure_stage}")
    else:
        primary = _ProcessControlSignal(f"PRIMARY_{failure_stage}")
    secondary = _ProcessControlSignal("SECONDARY_EXIT")

    class PrimaryThenExitCursor(_Cursor):
        async def _raise_at_stage(self, stage: str) -> None:
            self.stage_calls.append(stage)
            if failure_stage == stage:
                raise primary

        async def execute(self, query: object, params: object = None) -> None:
            self.sql_text = str(query)
            self.params = params
            await self._raise_at_stage("execute")

        async def fetchone(self) -> object:
            await self._raise_at_stage("fetch")
            return self.row

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            del exc_type, exc, tb
            self.stage_calls.append("exit")
            raise secondary

    database_now = datetime.now(UTC)
    cursor = PrimaryThenExitCursor(
        row=(database_now, database_now + timedelta(seconds=10))
    )
    connection = _Connection(
        cursor,
        close_release=asyncio.Event() if close_mode == "hang" else None,
        close_error=(RuntimeError("CLOSE_SENTINEL") if close_mode == "raise" else None),
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(type(primary)) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert caught.value is primary
    assert "exit" in cursor.stage_calls
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("close_mode", ["success", "raise", "hang"])
async def test_decode_process_control_primary_beats_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch, close_mode: str
) -> None:
    primary = _ProcessControlSignal("PRIMARY_DECODE")

    class SignalTimezone(tzinfo):
        def utcoffset(self, value: datetime | None) -> timedelta:
            del value
            raise primary

        def dst(self, value: datetime | None) -> timedelta:
            del value
            return timedelta(0)

    database_now = datetime(2026, 1, 1, tzinfo=SignalTimezone())
    connection = _Connection(
        _Cursor(row=(database_now, database_now + timedelta(seconds=10))),
        close_release=asyncio.Event() if close_mode == "hang" else None,
        close_error=(RuntimeError("CLOSE_SENTINEL") if close_mode == "raise" else None),
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(_ProcessControlSignal) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert caught.value is primary
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
async def test_cleanup_only_process_control_signal_is_rethrown_after_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_signal = _ProcessControlSignal("CLOSE_ONLY_SIGNAL")
    database_now = datetime.now(UTC)
    connection = _Connection(
        _Cursor(row=(database_now, database_now + timedelta(seconds=10))),
        close_error=close_signal,
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(_ProcessControlSignal) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert caught.value is close_signal
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["execute", "fetch"])
async def test_cursor_exit_process_control_beats_ordinary_operation_failure(
    monkeypatch: pytest.MonkeyPatch, failure_stage: str
) -> None:
    exit_signal = _ProcessControlSignal("EXIT_SIGNAL")

    class OrdinaryThenSignalCursor(_Cursor):
        async def execute(self, query: object, params: object = None) -> None:
            if failure_stage == "execute":
                raise RuntimeError("ORDINARY_EXECUTE")
            await super().execute(query, params)

        async def fetchone(self) -> object:
            if failure_stage == "fetch":
                raise RuntimeError("ORDINARY_FETCH")
            return await super().fetchone()

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            del exc_type, exc, tb
            raise exit_signal

    connection = _Connection(OrdinaryThenSignalCursor(row=None))

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(_ProcessControlSignal) as caught:
        await reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )

    assert caught.value is exit_signal
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["execute", "fetch"])
async def test_public_close_process_control_beats_ordinary_operation_failure(
    monkeypatch: pytest.MonkeyPatch, failure_stage: str
) -> None:
    close_signal = _ProcessControlSignal("CLOSE_SIGNAL")

    class OrdinaryFailureCursor(_Cursor):
        async def execute(self, query: object, params: object = None) -> None:
            if failure_stage == "execute":
                raise RuntimeError("ORDINARY_EXECUTE")
            await super().execute(query, params)

        async def fetchone(self) -> object:
            if failure_stage == "fetch":
                raise RuntimeError("ORDINARY_FETCH")
            return await super().fetchone()

    connection = _Connection(OrdinaryFailureCursor(row=None), close_error=close_signal)

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(_ProcessControlSignal) as caught:
        await reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )

    assert caught.value is close_signal
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("close_mode", ["success", "raise", "hang"])
async def test_active_task_settlement_process_control_beats_deadline_failure(
    monkeypatch: pytest.MonkeyPatch, close_mode: str
) -> None:
    settlement_signal = _ProcessControlSignal("ACTIVE_SETTLEMENT_SIGNAL")

    class CancelResistantCursor(_Cursor):
        async def execute(self, query: object, params: object = None) -> None:
            self.sql_text = str(query)
            self.params = params
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise settlement_signal

    connection = _Connection(
        CancelResistantCursor(row=None),
        close_release=asyncio.Event() if close_mode == "hang" else None,
        close_error=(RuntimeError("CLOSE_SENTINEL") if close_mode == "raise" else None),
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(_ProcessControlSignal) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert caught.value is settlement_signal
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
async def test_cleanup_process_control_beats_cleanup_period_external_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_release = asyncio.Event()
    close_signal = _ProcessControlSignal("CLOSE_SIGNAL")
    database_now = datetime.now(UTC)
    connection = _Connection(
        _Cursor(row=(database_now, database_now + timedelta(seconds=10))),
        close_release=close_release,
        close_error=close_signal,
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )
    task = asyncio.create_task(
        reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )
    )
    while connection.close_calls == 0:
        await asyncio.sleep(0)
    task.cancel("CLEANUP_EXTERNAL_CANCEL")
    close_release.set()

    with pytest.raises(_ProcessControlSignal) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await task

    assert caught.value is close_signal
    assert connection.close_calls == 1
    assert not [
        owned
        for owned in asyncio.all_tasks()
        if owned is not asyncio.current_task() and not owned.done()
    ]


@pytest.mark.asyncio
async def test_public_close_child_cancelled_error_is_internal_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    internal_cancel = asyncio.CancelledError("INTERNAL_CLOSE_CANCEL")
    database_now = datetime.now(UTC)
    connection = _Connection(
        _Cursor(row=(database_now, database_now + timedelta(seconds=10))),
        close_error=internal_cancel,
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(ExecutionProofUnavailableError) as caught:
        await reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )

    assert caught.value is not internal_cancel
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["execute", "fetch"])
async def test_external_cancel_during_exit_beats_ordinary_operation_failure(
    monkeypatch: pytest.MonkeyPatch, failure_stage: str
) -> None:
    exit_started = asyncio.Event()

    class OrdinaryThenBlockingExitCursor(_Cursor):
        async def execute(self, query: object, params: object = None) -> None:
            if failure_stage == "execute":
                raise RuntimeError("ORDINARY_EXECUTE")
            await super().execute(query, params)

        async def fetchone(self) -> object:
            if failure_stage == "fetch":
                raise RuntimeError("ORDINARY_FETCH")
            return await super().fetchone()

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            del exc_type, exc, tb
            exit_started.set()
            await asyncio.Event().wait()

    connection = _Connection(OrdinaryThenBlockingExitCursor(row=None))

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )
    task = asyncio.create_task(
        reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )
    )
    await exit_started.wait()
    marker = asyncio.CancelledError("CANCEL_DURING_EXIT")
    task.cancel(marker)

    with pytest.raises(asyncio.CancelledError) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await task

    assert caught.value.args == (marker,)
    assert connection.close_calls == 1
    assert not [
        owned
        for owned in asyncio.all_tasks()
        if owned is not asyncio.current_task() and not owned.done()
    ]


@pytest.mark.asyncio
async def test_cleanup_process_control_collection_is_first_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_first = _ProcessControlSignal("CLOSE_FIRST")
    active_second = _ProcessControlSignal("ACTIVE_SECOND")

    class CancelResistantCursor(_Cursor):
        async def execute(self, query: object, params: object = None) -> None:
            self.sql_text = str(query)
            self.params = params
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise active_second

    connection = _Connection(CancelResistantCursor(row=None), close_error=close_first)

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(_ProcessControlSignal) as caught:
        async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
            await reader.observe_current_lease(
                run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
            )

    assert caught.value is close_first
    assert connection.close_calls == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
async def test_decode_crossing_logical_deadline_cannot_commit_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DelayedTimezone(tzinfo):
        def utcoffset(self, value: datetime | None) -> timedelta:
            del value
            time.sleep(0.03)
            return timedelta(0)

        def dst(self, value: datetime | None) -> timedelta:
            del value
            return timedelta(0)

    slow_utc = DelayedTimezone()
    database_now = datetime(2026, 1, 1, tzinfo=slow_utc)
    connection = _Connection(
        _Cursor(row=(database_now, database_now + timedelta(seconds=10)))
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(ExecutionProofUnavailableError):
        await reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )


@pytest.mark.asyncio
async def test_decode_finishing_exactly_at_logical_deadline_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExactDeadlineClock:
        values = iter([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.02])

        @classmethod
        def monotonic(cls) -> float:
            return next(cls.values, 100.02)

    database_now = datetime.now(UTC)
    connection = _Connection(
        _Cursor(row=(database_now, database_now + timedelta(seconds=10)))
    )

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease.time",
        ExactDeadlineClock,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db",
            schema_name="proof_schema",
            total_deadline_seconds=0.02,
        )
    )

    with pytest.raises(ExecutionProofUnavailableError):
        await reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )


@pytest.mark.asyncio
async def test_dependency_cancelled_error_object_is_propagated_by_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = asyncio.CancelledError("DEPENDENCY_CANCEL")

    class CancellingCursor(_Cursor):
        async def execute(self, query: object, params: object = None) -> None:
            del query, params
            raise cancelled

    connection = _Connection(CancellingCursor(row=None))

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(asyncio.CancelledError) as caught:
        await reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=1)
        )

    assert caught.value is cancelled
    assert connection.close_calls == 1
    assert connection.closed


@pytest.mark.asyncio
async def test_one_parameterized_statement_and_exact_row_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_now = datetime.now(UTC)
    cursor = _Cursor(row=(database_now, database_now + timedelta(seconds=10)))
    connection = _Connection(cursor)

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    observation = await reader.observe_current_lease(
        run_id="run-1", fence=LeaseFence(owner="owner-1", generation=7)
    )

    assert observation == CurrentLeaseObservation(
        database_now=database_now,
        lease_expires_at=database_now + timedelta(seconds=10),
    )
    assert cursor.stage_calls == ["enter", "execute", "fetch", "exit"]
    assert "WITH db_clock AS MATERIALIZED" in cursor.sql_text
    assert cursor.sql_text.count("clock_timestamp()") == 1
    assert "lease_expires_at > db_clock.database_now" in cursor.sql_text
    assert cursor.params == ("run-1", "owner-1", 7, MAX_SAFE_INTEGER)
    assert connection.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        (),
        (datetime.now(UTC),),
        (datetime.now(UTC), datetime.now(UTC), "extra"),
        ("bad", datetime.now(UTC)),
        (datetime.now(UTC), "bad"),
        (datetime.now(), datetime.now(UTC)),
        (datetime.now(UTC), datetime.now()),
    ],
)
async def test_wrong_row_shape_or_time_fails_closed(
    monkeypatch: pytest.MonkeyPatch, row: object
) -> None:
    connection = _Connection(_Cursor(row=row))

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )

    with pytest.raises(ExecutionProofUnavailableError):
        await reader.observe_current_lease(
            run_id="run-1", fence=LeaseFence(owner="owner-1", generation=7)
        )
    assert connection.close_calls == 1


@pytest.mark.asyncio
async def test_adapter_failure_chain_traceback_and_repr_are_redacted(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class ExplodingCursor(_Cursor):
        async def execute(self, query: object, params: object = None) -> None:
            del query, params
            try:
                raise RuntimeError("INNER_SQL_SENTINEL")
            except RuntimeError as inner:
                raise RuntimeError("OUTER_SQL_SENTINEL") from inner

    connection = _Connection(ExplodingCursor(row=None))

    async def connect_direct(database_url: str) -> _Connection:
        del database_url
        return connection

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    config = PostgresExecutionProofLeaseConfig(
        database_url="postgresql://URL_SENTINEL/db",
        schema_name="SCHEMA_SENTINEL",
    )
    reader = PostgresExecutionProofLeaseReader(config)
    run_id = "RUN_SENTINEL"
    owner = "OWNER_SENTINEL"
    with pytest.raises(ExecutionProofUnavailableError) as caught:
        await reader.observe_current_lease(
            run_id=run_id, fence=LeaseFence(owner=owner, generation=7)
        )

    rendered = "\n".join(
        (
            repr(config),
            repr(reader),
            repr(caught.value),
            "".join(traceback.format_exception(caught.value)),
            caplog.text,
        )
    )
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    for secret in (
        "INNER_SQL_SENTINEL",
        "OUTER_SQL_SENTINEL",
        "URL_SENTINEL",
        "SCHEMA_SENTINEL",
        run_id,
        owner,
        "clock_timestamp",
    ):
        assert secret not in rendered


@pytest.mark.asyncio
async def test_invalid_run_or_fence_never_connects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def connect_direct(database_url: str) -> _Connection:
        nonlocal calls
        calls += 1
        raise AssertionError(database_url)

    monkeypatch.setattr(
        "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
        connect_direct,
    )
    reader = PostgresExecutionProofLeaseReader(
        PostgresExecutionProofLeaseConfig(
            database_url="postgresql://host/db", schema_name="proof_schema"
        )
    )
    bad_fences: list[Any] = [
        object(),
        {"owner": "owner", "generation": 1},
        LeaseFence(owner="owner", generation=1),
    ]
    object.__setattr__(bad_fences[-1], "generation", True)

    for run_id, fence in [("", LeaseFence(owner="owner", generation=1))] + [
        ("run", fence) for fence in bad_fences
    ]:
        with pytest.raises(ExecutionProofUnavailableError):
            await reader.observe_current_lease(
                run_id=run_id,
                fence=fence,
            )
    assert calls == 0


async def _bounded(awaitable: Any) -> Any:
    async with asyncio.timeout(TEST_SCHEDULER_TOLERANCE):
        return await awaitable


async def _connect(
    *,
    application_name: str | None = None,
    mapping_rows: bool = False,
    autocommit: bool = True,
) -> psycopg.AsyncConnection[Any]:
    options: dict[str, object] = {"autocommit": autocommit}
    if application_name is not None:
        options["application_name"] = application_name
    if mapping_rows:
        options["row_factory"] = dict_row
    connect: Any = psycopg.AsyncConnection.connect
    return await _bounded(connect(DATABASE_URL, **options))


async def _query(
    connection: psycopg.AsyncConnection[Any],
    query: object,
    params: object = None,
    *,
    fetch: str = "none",
) -> object:
    manager = connection.cursor()
    cursor = await _bounded(manager.__aenter__())
    error: BaseException | None = None
    try:
        execute: Any = cursor.execute
        if params is None:
            await _bounded(execute(query))
        else:
            await _bounded(execute(query, params))
        if fetch == "one":
            return await _bounded(cursor.fetchone())
        if fetch == "all":
            return await _bounded(cursor.fetchall())
        return None
    except BaseException as caught:
        error = caught
        raise
    finally:
        await _bounded(
            manager.__aexit__(
                type(error) if error is not None else None,
                error,
                error.__traceback__ if error is not None else None,
            )
        )


async def _install_canonical_schema(schema: str) -> None:
    admin: psycopg.AsyncConnection[Any] | None = None
    try:
        admin = await _connect(mapping_rows=True)
        await _bounded(apply_agent_schema(admin, schema, require_blank=True))
    finally:
        if admin is not None:
            await _bounded(admin.close())


def _exact_datetime_row(value: object) -> datetime:
    match value:
        case (datetime() as result,) if (
            type(value) is tuple and type(result) is datetime
        ):
            return result
        case _:
            raise AssertionError("expected one exact datetime row")


async def _database_clock(
    connection: psycopg.AsyncConnection[Any],
) -> datetime:
    row = await _query(connection, "SELECT clock_timestamp()", fetch="one")
    return _exact_datetime_row(row)


async def _wait_for_locking_reader(
    observer: psycopg.AsyncConnection[Any],
    *,
    application_name: str,
    schema: str,
) -> int:
    deadline = time.monotonic() + TEST_SCHEDULER_TOLERANCE
    while time.monotonic() < deadline:
        rows = await _query(
            observer,
            """
            SELECT pid
              FROM pg_stat_activity
             WHERE datname = current_database()
               AND usename = current_user
               AND application_name = %s
               AND query LIKE %s
               AND wait_event_type = 'Lock'
            """,
            (application_name, f'%"{schema}"%kokoro_agent_run%'),
            fetch="all",
        )
        match rows:
            case [(int() as pid,)] if type(rows) is list and type(pid) is int:
                return pid
            case _:
                pass
        await asyncio.sleep(0.01)
    raise AssertionError("reader backend did not reach the exact table lock")


async def _wait_for_pid_gone(observer: psycopg.AsyncConnection[Any], pid: int) -> None:
    deadline = time.monotonic() + TEST_SCHEDULER_TOLERANCE
    while time.monotonic() < deadline:
        row = await _query(
            observer,
            "SELECT 1 FROM pg_stat_activity WHERE pid = %s",
            (pid,),
            fetch="one",
        )
        if row is None:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("owned reader backend did not disappear")


async def _wait_for_application_gone(
    observer: psycopg.AsyncConnection[Any], application_name: str
) -> None:
    deadline = time.monotonic() + TEST_SCHEDULER_TOLERANCE
    while time.monotonic() < deadline:
        row = await _query(
            observer,
            "SELECT 1 FROM pg_stat_activity WHERE application_name = %s",
            (application_name,),
            fetch="one",
        )
        if row is None:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("owned reader application backend did not disappear")


async def _cleanup_real_case(
    *,
    schema: str,
    application_names: tuple[str, ...],
    tasks: tuple[asyncio.Task[Any] | None, ...],
    blockers: tuple[psycopg.AsyncConnection[Any] | None, ...],
    connections: tuple[psycopg.AsyncConnection[Any] | None, ...],
    reader_pids: tuple[int | None, ...],
) -> None:
    errors: list[BaseException] = []
    # Release every blocker before cancelling local reader tasks so public
    # connection close can finish without a test-owned lock extending cleanup.
    for blocker in blockers:
        if blocker is None or blocker.closed:
            continue
        try:
            await _bounded(blocker.rollback())
        except BaseException as error:
            errors.append(error)
    for task in tasks:
        if task is None:
            continue
        if not task.done():
            task.cancel()
        try:
            await _bounded(asyncio.gather(task, return_exceptions=True))
        except BaseException as error:
            errors.append(error)
    for connection in (*blockers, *connections):
        if connection is None or connection.closed:
            continue
        try:
            await _bounded(connection.close())
        except BaseException as error:
            errors.append(error)

    dropper: psycopg.AsyncConnection[Any] | None = None
    try:
        dropper = await _connect()
        try:
            await _query(
                dropper,
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                ),
            )
        except BaseException as error:
            errors.append(error)
        try:
            row = await _query(
                dropper,
                "SELECT 1 FROM pg_namespace WHERE nspname = %s",
                (schema,),
                fetch="one",
            )
            if row is not None:
                errors.append(AssertionError("temporary schema still exists"))
        except BaseException as error:
            errors.append(error)
    except BaseException as error:
        errors.append(error)
    finally:
        if dropper is not None:
            try:
                await _bounded(dropper.close())
            except BaseException as error:
                errors.append(error)

    # PID/application disappearance is observed only after all testcase
    # connections are closed and the schema was dropped, using a fresh session.
    inspector: psycopg.AsyncConnection[Any] | None = None
    try:
        inspector = await _connect()
        for pid in reader_pids:
            if pid is None:
                continue
            try:
                await _wait_for_pid_gone(inspector, pid)
            except BaseException as error:
                errors.append(error)
        for application_name in application_names:
            try:
                await _wait_for_application_gone(inspector, application_name)
            except BaseException as error:
                errors.append(error)
    except BaseException as error:
        errors.append(error)
    finally:
        if inspector is not None:
            try:
                await _bounded(inspector.close())
            except BaseException as error:
                errors.append(error)
    if errors:
        raise AssertionError(
            "real PostgreSQL cleanup failed: "
            + "; ".join(type(error).__name__ for error in errors)
        )


def _reader_dsn(application_name: str) -> str:
    return conninfo.make_conninfo(DATABASE_URL, application_name=application_name)


async def _pre_statement_clock_mutant(
    *, schema: str, application_name: str, captured_now: datetime
) -> datetime | None:
    connection: psycopg.AsyncConnection[Any] | None = None
    try:
        connection = await _connect(application_name=application_name)
        row = await _query(
            connection,
            sql.SQL(
                "SELECT lease_expires_at FROM {}.kokoro_agent_run "
                "WHERE run_id=%s AND owner=%s AND lease_generation=%s "
                "AND lease_generation BETWEEN 1 AND %s "
                "AND lease_expires_at > %s AND terminal=FALSE"
            ).format(sql.Identifier(schema)),
            ("race", "owner", 7, MAX_SAFE_INTEGER, captured_now),
            fetch="one",
        )
        if row is None:
            return None
        return _exact_datetime_row(row)
    finally:
        if connection is not None:
            await _bounded(connection.close())


@pytest.mark.asyncio
async def test_real_postgres_current_mismatch_aba_and_exact_cleanup() -> None:
    schema = f"proof_{uuid.uuid4().hex}"
    app_name = f"a2c_matrix_{uuid.uuid4().hex}"
    observer: psycopg.AsyncConnection[Any] | None = None
    try:
        await _install_canonical_schema(schema)
        observer = await _connect()
        await _query(
            observer,
            sql.SQL(
                "INSERT INTO {}.kokoro_agent_run "
                "(run_id, tenant_id, owner, lease_generation, lease_expires_at, terminal) "
                "VALUES "
                "('current','tenant','owner',7,clock_timestamp()+interval '30 sec',FALSE),"
                "('aba','tenant','owner',7,clock_timestamp()+interval '30 sec',FALSE),"
                "('paused','tenant','owner',7,NULL,FALSE),"
                "('terminal','tenant','owner',7,clock_timestamp()+interval '30 sec',TRUE),"
                "('expired','tenant','owner',7,clock_timestamp()-interval '1 sec',FALSE),"
                "('unsafe','tenant','owner',9007199254740992,clock_timestamp()+interval '30 sec',FALSE)"
            ).format(sql.Identifier(schema)),
        )
        reader = PostgresExecutionProofLeaseReader(
            PostgresExecutionProofLeaseConfig(
                database_url=_reader_dsn(app_name), schema_name=schema
            )
        )
        current = await reader.observe_current_lease(
            run_id="current", fence=LeaseFence(owner="owner", generation=7)
        )
        assert current is not None and current.lease_expires_at > current.database_now
        aba_seven = await reader.observe_current_lease(
            run_id="aba", fence=LeaseFence(owner="owner", generation=7)
        )
        assert aba_seven is not None
        await _query(
            observer,
            sql.SQL(
                "UPDATE {}.kokoro_agent_run SET lease_generation=%s "
                "WHERE run_id=%s AND owner=%s AND lease_generation=%s"
            ).format(sql.Identifier(schema)),
            (8, "aba", "owner", 7),
        )
        assert (
            await reader.observe_current_lease(
                run_id="aba", fence=LeaseFence(owner="owner", generation=7)
            )
            is None
        )
        assert (
            await reader.observe_current_lease(
                run_id="aba", fence=LeaseFence(owner="owner", generation=8)
            )
            is not None
        )
        for run_id, fence in (
            ("missing", LeaseFence(owner="owner", generation=7)),
            ("current", LeaseFence(owner="wrong", generation=7)),
            ("current", LeaseFence(owner="owner", generation=8)),
            ("paused", LeaseFence(owner="owner", generation=7)),
            ("terminal", LeaseFence(owner="owner", generation=7)),
            ("expired", LeaseFence(owner="owner", generation=7)),
            ("unsafe", LeaseFence(owner="owner", generation=MAX_SAFE_INTEGER)),
        ):
            assert (
                await reader.observe_current_lease(run_id=run_id, fence=fence) is None
            )
    finally:
        await _cleanup_real_case(
            schema=schema,
            application_names=(app_name,),
            tasks=(),
            blockers=(),
            connections=(observer,),
            reader_pids=(),
        )
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]


@pytest.mark.asyncio
async def test_real_blocked_race_defeats_pre_statement_app_clock_mutant() -> None:
    schema = f"proof_{uuid.uuid4().hex}"
    production_app = f"a2c_race_prod_{uuid.uuid4().hex}"
    mutant_app = f"a2c_race_mutant_{uuid.uuid4().hex}"
    blocker: psycopg.AsyncConnection[Any] | None = None
    observer: psycopg.AsyncConnection[Any] | None = None
    production_task: asyncio.Task[CurrentLeaseObservation | None] | None = None
    mutant_task: asyncio.Task[datetime | None] | None = None
    production_pid: int | None = None
    mutant_pid: int | None = None
    try:
        await _install_canonical_schema(schema)
        blocker = await _connect(autocommit=False)
        observer = await _connect()
        await _query(
            blocker,
            sql.SQL(
                "INSERT INTO {}.kokoro_agent_run "
                "(run_id, tenant_id, owner, lease_generation, lease_expires_at, terminal) "
                "VALUES ('race','tenant','owner',7,"
                "clock_timestamp()+interval '0.700 sec',FALSE)"
            ).format(sql.Identifier(schema)),
        )
        await _bounded(blocker.commit())
        expiry_row = await _query(
            observer,
            sql.SQL(
                "SELECT lease_expires_at FROM {}.kokoro_agent_run WHERE run_id=%s"
            ).format(sql.Identifier(schema)),
            ("race",),
            fetch="one",
        )
        expiry = _exact_datetime_row(expiry_row)
        await _query(
            blocker,
            sql.SQL("LOCK TABLE {}.kokoro_agent_run IN ACCESS EXCLUSIVE MODE").format(
                sql.Identifier(schema)
            ),
        )
        captured_now = await _database_clock(observer)
        assert captured_now < expiry
        reader = PostgresExecutionProofLeaseReader(
            PostgresExecutionProofLeaseConfig(
                database_url=_reader_dsn(production_app), schema_name=schema
            )
        )
        production_task = asyncio.create_task(
            reader.observe_current_lease(
                run_id="race", fence=LeaseFence(owner="owner", generation=7)
            )
        )
        mutant_task = asyncio.create_task(
            _pre_statement_clock_mutant(
                schema=schema,
                application_name=mutant_app,
                captured_now=captured_now,
            )
        )
        production_pid, mutant_pid = await asyncio.gather(
            _wait_for_locking_reader(
                observer, application_name=production_app, schema=schema
            ),
            _wait_for_locking_reader(
                observer, application_name=mutant_app, schema=schema
            ),
        )
        assert production_pid != mutant_pid
        while await _database_clock(observer) <= expiry:
            await asyncio.sleep(0.01)
        await _bounded(blocker.rollback())  # release the single shared lock once
        production_result, mutant_result = await _bounded(
            asyncio.gather(production_task, mutant_task)
        )
        assert production_result is None
        assert type(mutant_result) is datetime and mutant_result == expiry
        print(
            "A2C_PG_RACE "
            f"schema={schema} production_pid={production_pid} mutant_pid={mutant_pid}"
        )
    finally:
        await _cleanup_real_case(
            schema=schema,
            application_names=(production_app, mutant_app),
            tasks=(production_task, mutant_task),
            blockers=(blocker,),
            connections=(observer,),
            reader_pids=(production_pid, mutant_pid),
        )
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]
    print(f"A2C_PG_RACE_CLEANUP schema={schema} dropped=true pids=zero")


@pytest.mark.asyncio
async def test_real_locked_reader_calls_public_close_once_and_drops_exact_pid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = f"proof_{uuid.uuid4().hex}"
    app_name = f"a2c_public_close_{uuid.uuid4().hex}"
    blocker: psycopg.AsyncConnection[Any] | None = None
    observer: psycopg.AsyncConnection[Any] | None = None
    underlying: psycopg.AsyncConnection[Any] | None = None
    reader_task: asyncio.Task[CurrentLeaseObservation | None] | None = None
    reader_pid: int | None = None
    method_started: float | None = None
    method_elapsed: float | None = None
    try:
        await _install_canonical_schema(schema)
        blocker = await _connect(autocommit=False)
        observer = await _connect()
        underlying = await _connect(application_name=app_name)
        await _query(
            blocker,
            sql.SQL(
                "INSERT INTO {}.kokoro_agent_run "
                "(run_id, tenant_id, owner, lease_generation, lease_expires_at, terminal) "
                "VALUES ('race','tenant','owner',7,"
                "clock_timestamp()+interval '30 sec',FALSE)"
            ).format(sql.Identifier(schema)),
        )
        await _bounded(blocker.commit())
        await _query(
            blocker,
            sql.SQL("LOCK TABLE {}.kokoro_agent_run IN ACCESS EXCLUSIVE MODE").format(
                sql.Identifier(schema)
            ),
        )

        class CountingPublicConnection:
            def __init__(self) -> None:
                self.close_calls = 0

            def cursor(self) -> object:
                assert underlying is not None
                return underlying.cursor()

            async def close(self) -> None:
                self.close_calls += 1
                assert underlying is not None
                await underlying.close()

        counted_connection = CountingPublicConnection()

        async def connect_direct(database_url: str) -> CountingPublicConnection:
            del database_url
            return counted_connection

        monkeypatch.setattr(
            "kokoro_agent.infrastructure.postgres_execution_proof_lease._connect_direct",
            connect_direct,
        )
        reader = PostgresExecutionProofLeaseReader(
            PostgresExecutionProofLeaseConfig(
                database_url=_reader_dsn(app_name),
                schema_name=schema,
                total_deadline_seconds=0.05,
            )
        )
        method_started = time.monotonic()
        reader_task = asyncio.create_task(
            reader.observe_current_lease(
                run_id="race", fence=LeaseFence(owner="owner", generation=7)
            )
        )
        reader_pid = await _wait_for_locking_reader(
            observer, application_name=app_name, schema=schema
        )
        with pytest.raises(ExecutionProofUnavailableError):
            await _bounded(reader_task)
        method_elapsed = time.monotonic() - method_started
        assert counted_connection.close_calls == 1
        assert method_elapsed <= 0.05 + CLEANUP_ALLOWANCE_SECONDS + 0.10
        print(
            "A2C_PG_PUBLIC_CLOSE "
            f"schema={schema} pid={reader_pid} close_calls=1 "
            f"method_elapsed={method_elapsed:.6f}"
        )
    finally:
        await _cleanup_real_case(
            schema=schema,
            application_names=(app_name,),
            tasks=(reader_task,),
            blockers=(blocker,),
            connections=(observer, underlying),
            reader_pids=(reader_pid,),
        )
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]
    print(f"A2C_PG_PUBLIC_CLOSE_CLEANUP schema={schema} dropped=true pid=zero")


@pytest.mark.asyncio
async def test_real_table_lock_crosses_work_deadline_and_backend_is_discarded() -> None:
    schema = f"proof_{uuid.uuid4().hex}"
    app_name = f"a2c_deadline_{uuid.uuid4().hex}"
    blocker: psycopg.AsyncConnection[Any] | None = None
    observer: psycopg.AsyncConnection[Any] | None = None
    reader_task: asyncio.Task[CurrentLeaseObservation | None] | None = None
    reader_pid: int | None = None
    method_started: float | None = None
    method_elapsed: float | None = None
    try:
        await _install_canonical_schema(schema)
        blocker = await _connect(autocommit=False)
        observer = await _connect()
        await _query(
            blocker,
            sql.SQL(
                "INSERT INTO {}.kokoro_agent_run "
                "(run_id, tenant_id, owner, lease_generation, lease_expires_at, terminal) "
                "VALUES ('race','tenant','owner',7,"
                "clock_timestamp()+interval '30 sec',FALSE)"
            ).format(sql.Identifier(schema)),
        )
        await _bounded(blocker.commit())
        await _query(
            blocker,
            sql.SQL("LOCK TABLE {}.kokoro_agent_run IN ACCESS EXCLUSIVE MODE").format(
                sql.Identifier(schema)
            ),
        )
        reader = PostgresExecutionProofLeaseReader(
            PostgresExecutionProofLeaseConfig(
                database_url=_reader_dsn(app_name),
                schema_name=schema,
                total_deadline_seconds=0.05,
            )
        )
        method_started = time.monotonic()
        reader_task = asyncio.create_task(
            reader.observe_current_lease(
                run_id="race", fence=LeaseFence(owner="owner", generation=7)
            )
        )
        reader_pid = await _wait_for_locking_reader(
            observer, application_name=app_name, schema=schema
        )
        with pytest.raises(ExecutionProofUnavailableError):
            await _bounded(reader_task)
        method_elapsed = time.monotonic() - method_started
        assert method_elapsed <= 0.05 + CLEANUP_ALLOWANCE_SECONDS + 0.10
        await _bounded(blocker.rollback())
        await _wait_for_pid_gone(observer, reader_pid)
        print(
            f"A2C_PG_DEADLINE schema={schema} application={app_name} "
            f"pid={reader_pid} method_elapsed={method_elapsed:.6f}"
        )
    finally:
        await _cleanup_real_case(
            schema=schema,
            application_names=(app_name,),
            tasks=(reader_task,),
            blockers=(blocker,),
            connections=(observer,),
            reader_pids=(reader_pid,),
        )
    assert not [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    ]
    print(f"A2C_PG_DEADLINE_CLEANUP schema={schema} dropped=true pid=zero")


def test_cleanup_allowance_is_independent_quarter_second_budget() -> None:
    assert CLEANUP_ALLOWANCE_SECONDS == 0.25
    assert re.fullmatch(r"0\.25", str(CLEANUP_ALLOWANCE_SECONDS))
