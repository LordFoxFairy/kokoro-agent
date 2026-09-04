"""SQL adapter boundary tests: dynamic driver calls stay typed at one edge."""

from __future__ import annotations

import pytest

from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one


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
