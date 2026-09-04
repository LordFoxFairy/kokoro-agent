"""Unit coverage for the run-context outbox SQL boundary."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest

import kokoro_agent.infrastructure.postgres_run_context as context_module
from kokoro_agent.infrastructure.postgres_run_context import (
    OutboxFilter,
    PostgresRunRepositoryContext,
)


class _Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []

    async def __aenter__(self) -> "_Cursor":
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    async def execute(self, query: str, params: object | None = None) -> None:
        self.calls.append((query, params))

    async def fetchall(self) -> list[dict[str, object]]:
        return []


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor


@pytest.mark.asyncio
async def test_fetch_outbox_binds_the_queued_status_parameter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _Cursor()
    connection = _Connection(cursor)

    @asynccontextmanager
    async def fake_connect(_: str) -> AsyncGenerator[_Connection, None]:
        yield connection

    monkeypatch.setattr(context_module, "connect_pg", fake_connect)
    context = PostgresRunRepositoryContext(
        "postgresql://fixture", ttl_ms=1_000, schema="fixture"
    )

    assert await context.fetch_outbox(OutboxFilter.QUEUED) == []

    assert len(cursor.calls) == 1
    query, params = cursor.calls[0]
    assert "WHERE status = %s" in query
    assert "status = 'queued'" not in query
    assert params == ("queued",)


def test_outbox_filter_does_not_accept_a_sql_fragment() -> None:
    assert OutboxFilter("queued") is OutboxFilter.QUEUED
    with pytest.raises(ValueError):
        OutboxFilter("status = 'queued'")
