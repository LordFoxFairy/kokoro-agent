"""SQL adapter boundary tests: dynamic driver calls stay typed at one edge."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one


@pytest.mark.parametrize("cancel_during_connect", [False, True])
@pytest.mark.parametrize("previous_cancel_handled", [False, True])
async def test_connection_readiness_never_grants_work_after_cancellation(
    monkeypatch: pytest.MonkeyPatch,
    cancel_during_connect: bool,
    previous_cancel_handled: bool,
) -> None:
    from kokoro_agent.infrastructure import postgres

    connection = Mock()
    connection.close = AsyncMock()
    entered = asyncio.Event()
    ready = asyncio.Event()
    body_entered = False

    async def connect(_: str) -> object:
        entered.set()
        try:
            await ready.wait()
        except asyncio.CancelledError:
            # Model the observed driver readiness race: a connection is returned
            # even though the caller's task still has a pending cancellation.
            return connection
        return connection

    monkeypatch.setattr(postgres, "connect_dict_row", connect)

    async def consume() -> None:
        nonlocal body_entered
        if previous_cancel_handled:
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                pass
            # Explicit finally/cleanup work may acquire a resource after the
            # earlier cancellation was handled without calling uncancel().
            assert current.cancelling() == 1
        async with postgres.connect_pg("postgresql://fixture"):
            body_entered = True

    task = asyncio.create_task(consume())
    await entered.wait()
    if cancel_during_connect:
        task.cancel()
    else:
        ready.set()
    result = (await asyncio.gather(task, return_exceptions=True))[0]
    assert body_entered is (not cancel_during_connect)
    if cancel_during_connect:
        assert isinstance(result, asyncio.CancelledError)
    else:
        assert result is None
    connection.close.assert_awaited_once_with()


class _Cursor:
    def __init__(self, rows: list[object]) -> None:
        self.calls: list[tuple[str, object | None]] = []
        self._rows = rows

    async def execute(self, query: str, params: object | None = None) -> None:
        self.calls.append((query, params))

    async def fetchone(self) -> object | None:
        return self._rows[0] if self._rows else None

    async def fetchall(self) -> list[object]:
        return self._rows


@pytest.mark.asyncio
async def test_execute_sql_preserves_parameter_binding() -> None:
    cursor = _Cursor([])

    await execute_sql(cursor, "SELECT $1", ("value",))
    await execute_sql(cursor, "SELECT 1")

    assert cursor.calls == [("SELECT $1", ("value",)), ("SELECT 1", None)]


@pytest.mark.asyncio
async def test_fetch_helpers_normalize_mapping_rows() -> None:
    cursor = _Cursor([{"id": 1}, {"id": 2}])

    assert await fetch_one(cursor) == {"id": 1}
    assert await fetch_all(cursor) == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_fetch_helpers_reject_tuple_rows() -> None:
    cursor = _Cursor([(1,)])

    with pytest.raises(TypeError, match="dict_row"):
        await fetch_one(cursor)
    with pytest.raises(TypeError, match="dict_row"):
        await fetch_all(cursor)
