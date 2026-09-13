"""Direct PostgreSQL statement-time execution-proof lease reader."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
import math
import re
import time
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.execution.execution_proof_profile import MAX_SAFE_INTEGER
from kokoro_agent.execution.execution_proof_supplier import (
    CurrentLeaseObservation,
    ExecutionProofUnavailableError,
)
from kokoro_agent.infrastructure.postgres import qualified
from kokoro_agent.infrastructure.schema import RUN_CLAIMS_TABLE


CLEANUP_ALLOWANCE_SECONDS = 0.25
_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$", re.ASCII)
_ERROR_MESSAGE = "execution proof lease is unavailable"


class _WorkDeadline(RuntimeError):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class PostgresExecutionProofLeaseConfig:
    """Validated connection target and bounded logical database-work budget."""

    database_url: str = field(repr=False)
    schema_name: str = field(repr=False)
    total_deadline_seconds: float = 2.0

    def __post_init__(self) -> None:
        failed = False
        deadline = 0.0
        try:
            if (
                type(self.database_url) is not str
                or not self.database_url
                or "\x00" in self.database_url
            ):
                raise ValueError
            conninfo_to_dict(self.database_url)
            if (
                type(self.schema_name) is not str
                or _SCHEMA_NAME.fullmatch(self.schema_name) is None
            ):
                raise ValueError
            if type(self.total_deadline_seconds) not in {int, float}:
                raise ValueError
            deadline = float(self.total_deadline_seconds)
            if not math.isfinite(deadline) or not 0 < deadline <= 2.0:
                raise ValueError
        except Exception:
            failed = True
        if failed:
            raise ExecutionProofUnavailableError(_ERROR_MESSAGE) from None
        object.__setattr__(self, "total_deadline_seconds", deadline)

    def __repr__(self) -> str:
        return (
            "PostgresExecutionProofLeaseConfig(database_url=<redacted>, "
            "schema_name=<redacted>, "
            f"total_deadline_seconds={self.total_deadline_seconds!r})"
        )


@dataclass(slots=True)
class _OperationState:
    connection: Any | None = None
    active_task: asyncio.Task[Any] | None = None
    acquiring_connection: bool = False
    owned_tasks: set[asyncio.Task[Any]] = field(default_factory=lambda: set())


async def _connect_direct(database_url: str) -> psycopg.AsyncConnection[Any]:
    return await psycopg.AsyncConnection.connect(database_url)


class PostgresExecutionProofLeaseReader:
    """Read one current lease using a database statement-time clock."""

    __slots__ = ("_config",)

    def __init__(self, config: PostgresExecutionProofLeaseConfig) -> None:
        failed = False
        snapshot: PostgresExecutionProofLeaseConfig | None = None
        try:
            snapshot = _config_snapshot(config)
        except Exception:
            failed = True
        if failed or snapshot is None:
            raise ExecutionProofUnavailableError(_ERROR_MESSAGE) from None
        self._config = snapshot

    def __repr__(self) -> str:
        return "PostgresExecutionProofLeaseReader(<redacted>)"

    async def observe_current_lease(
        self, *, run_id: str, fence: LeaseFence
    ) -> CurrentLeaseObservation | None:
        failed = False
        cancelled: asyncio.CancelledError | None = None
        process_control: BaseException | None = None
        result: CurrentLeaseObservation | None = None
        state = _OperationState()
        cleanup_ok = False
        cleanup_cancelled: asyncio.CancelledError | None = None
        cleanup_process_control: BaseException | None = None
        try:
            config = _config_snapshot(self._config)
            run_snapshot, fence_snapshot = _input_snapshot(run_id, fence)
            deadline = time.monotonic() + config.total_deadline_seconds
            state.acquiring_connection = True
            connection = await _await_step(
                _connect_direct(config.database_url), deadline=deadline, state=state
            )
            state.acquiring_connection = False
            state.connection = connection
            cursor_manager = connection.cursor()
            cursor = await _await_step(
                cursor_manager.__aenter__(), deadline=deadline, state=state
            )
            cursor_error: BaseException | None = None
            exit_error: BaseException | None = None
            row: object = None
            try:
                query = _statement(config.schema_name)
                await _await_step(
                    cursor.execute(
                        query,
                        (
                            run_snapshot,
                            fence_snapshot.owner,
                            fence_snapshot.generation,
                            MAX_SAFE_INTEGER,
                        ),
                    ),
                    deadline=deadline,
                    state=state,
                )
                row = await _await_step(
                    cursor.fetchone(), deadline=deadline, state=state
                )
            except BaseException as error:
                cursor_error = error
            if state.active_task is None:
                try:
                    await _await_step(
                        cursor_manager.__aexit__(
                            type(cursor_error) if cursor_error is not None else None,
                            cursor_error,
                            (
                                cursor_error.__traceback__
                                if cursor_error is not None
                                else None
                            ),
                        ),
                        deadline=deadline,
                        state=state,
                    )
                except BaseException as error:
                    exit_error = error
            if cursor_error is not None and (
                isinstance(cursor_error, asyncio.CancelledError)
                or _is_process_control(cursor_error)
            ):
                raise cursor_error
            if exit_error is not None and (
                isinstance(exit_error, asyncio.CancelledError)
                or _is_process_control(exit_error)
            ):
                raise exit_error
            if cursor_error is not None:
                raise cursor_error
            if exit_error is not None:
                raise exit_error
            result = _decode_row(row)
            if time.monotonic() >= deadline:
                raise _WorkDeadline
        except asyncio.CancelledError as error:
            cancelled = error
        except Exception:
            failed = True
        except BaseException as error:
            process_control = error
        finally:
            try:
                (
                    cleanup_ok,
                    cleanup_cancelled,
                    cleanup_process_control,
                ) = await _bounded_cleanup(state)
            except asyncio.CancelledError as error:
                cleanup_cancelled = error
            except BaseException:
                cleanup_ok = False
        if process_control is not None:
            raise process_control
        if cancelled is not None:
            raise cancelled
        if cleanup_process_control is not None:
            raise cleanup_process_control
        if cleanup_cancelled is not None:
            raise cleanup_cancelled
        if failed:
            raise ExecutionProofUnavailableError(_ERROR_MESSAGE) from None
        if not cleanup_ok:
            raise ExecutionProofUnavailableError(_ERROR_MESSAGE) from None
        return result


def _config_snapshot(value: object) -> PostgresExecutionProofLeaseConfig:
    if type(value) is not PostgresExecutionProofLeaseConfig:
        raise ValueError
    return PostgresExecutionProofLeaseConfig(
        database_url=value.database_url,
        schema_name=value.schema_name,
        total_deadline_seconds=value.total_deadline_seconds,
    )


def _input_snapshot(run_id: object, fence: object) -> tuple[str, LeaseFence]:
    if type(run_id) is not str or not run_id or type(fence) is not LeaseFence:
        raise ValueError
    owner = fence.owner
    generation = fence.generation
    if type(owner) is not str or not owner:
        raise ValueError
    if type(generation) is not int or not 1 <= generation <= MAX_SAFE_INTEGER:
        raise ValueError
    return run_id, LeaseFence(owner=owner, generation=generation)


async def _await_step(
    awaitable: Any, *, deadline: float, state: _OperationState
) -> Any:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise _WorkDeadline
    task = asyncio.ensure_future(awaitable)
    state.owned_tasks.add(task)
    state.active_task = task
    done, _ = await asyncio.wait({task}, timeout=remaining)
    if not done:
        raise _WorkDeadline
    state.active_task = None
    state.owned_tasks.discard(task)
    return task.result()


async def _bounded_cleanup(
    state: _OperationState,
) -> tuple[bool, asyncio.CancelledError | None, BaseException | None]:
    deadline = time.monotonic() + CLEANUP_ALLOWANCE_SECONDS
    cleanup = asyncio.create_task(_cleanup(state, deadline=deadline))
    state.owned_tasks.add(cleanup)
    cancelled: asyncio.CancelledError | None = None
    while not cleanup.done():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            cleanup.cancel()
            for owned in tuple(state.owned_tasks):
                if owned is not asyncio.current_task() and not owned.done():
                    owned.cancel()
            break
        try:
            await asyncio.wait({cleanup}, timeout=remaining)
        except asyncio.CancelledError as error:
            if cancelled is None:
                cancelled = error
            continue
    if not cleanup.done():
        cleanup.cancel()
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as error:
            if cleanup.done() and cleanup.cancelled():
                break
            if cancelled is None:
                cancelled = error
            continue
        except BaseException:
            break
    state.owned_tasks.discard(cleanup)
    try:
        cleanup.result()
    except asyncio.CancelledError:
        return False, cancelled, None
    except Exception:
        return False, cancelled, None
    except BaseException as error:
        return False, cancelled, error
    return True, cancelled, None


async def _cleanup(state: _OperationState, *, deadline: float) -> None:
    connection: Any = state.connection
    active = state.active_task
    acquiring_connection = state.acquiring_connection
    close_attempted = False
    cleanup_ok = True
    process_control: BaseException | None = None
    try:
        if connection is None and acquiring_connection and active is not None:
            result = await _cancel_and_collect(active, state=state)
            active = None
            if _is_process_control(result) and process_control is None:
                process_control = result
            elif not isinstance(result, BaseException):
                connection = result
                state.connection = connection
        if connection is not None:
            close_attempted = True
            close_error = await _close_once(connection, state=state, deadline=deadline)
            if _is_process_control(close_error) and process_control is None:
                process_control = close_error
            elif close_error is not None:
                cleanup_ok = False
    except BaseException as error:
        if _is_process_control(error) and process_control is None:
            process_control = error
        else:
            cleanup_ok = False
    finally:
        if active is not None:
            result = await _cancel_and_collect(active, state=state)
            active = None
            if _is_process_control(result) and process_control is None:
                process_control = result
            elif (
                connection is None
                and acquiring_connection
                and not isinstance(result, BaseException)
            ):
                connection = result
                state.connection = connection
        if connection is not None and not close_attempted:
            close_attempted = True
            close_error = await _close_once(connection, state=state, deadline=deadline)
            if _is_process_control(close_error) and process_control is None:
                process_control = close_error
            elif close_error is not None:
                cleanup_ok = False
        state.active_task = None
        state.acquiring_connection = False
        state.connection = None
    if process_control is not None:
        raise process_control
    if not cleanup_ok:
        raise RuntimeError


async def _cancel_and_collect(
    task: asyncio.Task[Any], *, state: _OperationState
) -> Any:
    if not task.done():
        task.cancel()
    result = await _collect_owned_task(task)
    state.owned_tasks.discard(task)
    return result


async def _close_once(
    connection: Any, *, state: _OperationState, deadline: float
) -> BaseException | None:
    try:

        async def public_close() -> None:
            await connection.close()

        close_task = asyncio.create_task(public_close())
    except BaseException as error:
        return error
    state.owned_tasks.add(close_task)
    try:
        # Let the one public close call start even when a late connection arrives
        # at the cleanup deadline; no private driver fallback or retry is used.
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass
        remaining = max(0.0, deadline - time.monotonic())
        if not close_task.done() and remaining > 0:
            try:
                await asyncio.wait({close_task}, timeout=remaining)
            except asyncio.CancelledError:
                pass
        if not close_task.done():
            close_task.cancel()
        result = await _collect_owned_task(close_task)
        return result if isinstance(result, BaseException) else None
    finally:
        state.owned_tasks.discard(close_task)


def _is_process_control(value: object) -> bool:
    return isinstance(value, BaseException) and not isinstance(
        value, (Exception, asyncio.CancelledError)
    )


async def _collect_owned_task(task: asyncio.Task[Any]) -> Any:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
        except BaseException:
            break
    if task.cancelled():
        return asyncio.CancelledError()
    try:
        return task.result()
    except BaseException as error:
        return error


def _statement(schema_name: str) -> str:
    table = qualified(schema_name, RUN_CLAIMS_TABLE)
    return f"""
        WITH db_clock AS MATERIALIZED (
            SELECT clock_timestamp() AS database_now
        )
        SELECT db_clock.database_now, run.lease_expires_at
          FROM db_clock
          JOIN {table} AS run ON run.run_id = %s
         WHERE run.owner = %s
           AND run.lease_generation = %s
           AND run.lease_generation BETWEEN 1 AND %s
           AND run.lease_expires_at IS NOT NULL
           AND run.lease_expires_at > db_clock.database_now
           AND run.terminal = FALSE
    """


def _decode_row(row: object) -> CurrentLeaseObservation | None:
    if row is None:
        return None
    match row:
        case (datetime() as database_now, datetime() as lease_expires_at) if (
            type(row) is tuple
            and type(database_now) is datetime
            and type(lease_expires_at) is datetime
        ):
            pass
        case _:
            raise ValueError
    if database_now.tzinfo is None or lease_expires_at.tzinfo is None:
        raise ValueError
    database_utc = database_now.astimezone(UTC)
    expiry_utc = lease_expires_at.astimezone(UTC)
    if expiry_utc <= database_utc:
        raise ValueError
    return CurrentLeaseObservation(
        database_now=database_utc,
        lease_expires_at=expiry_utc,
    )


__all__ = [
    "CLEANUP_ALLOWANCE_SECONDS",
    "PostgresExecutionProofLeaseConfig",
    "PostgresExecutionProofLeaseReader",
]
