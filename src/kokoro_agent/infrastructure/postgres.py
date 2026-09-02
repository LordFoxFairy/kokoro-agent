"""PostgreSQL 连接与 schema 约定（stage1：durable state 走 PG，streams 仍走 Redis）。"""

# psycopg's current stubs reject the runtime driver's dynamically composed SQL
# and dict-row factory; ruff plus the integration contract tests cover these
# adapter boundaries while the rest of the worker remains strict-checked.
# pyright: reportCallIssue=false, reportArgumentType=false, reportReturnType=false

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

DEFAULT_PG_SCHEMA = "kokoro_agent"


@asynccontextmanager
async def connect_pg(database_url: str) -> AsyncGenerator[psycopg.AsyncConnection[Any], None]:
    conn = await psycopg.AsyncConnection.connect(
        database_url,
        autocommit=True,
        row_factory=dict_row,
    )
    try:
        yield conn
    finally:
        await conn.close()


def qualified(schema: str, name: str) -> str:
    return f'"{_quote_ident(schema)}"."{_quote_ident(name)}"'


async def ensure_schema(conn: psycopg.AsyncConnection[Any], schema: str) -> None:
    async with conn.cursor() as cur:
        await cur.execute(f"CREATE SCHEMA IF NOT EXISTS \"{_quote_ident(schema)}\"")


def _quote_ident(value: str) -> str:
    return value.replace('"', '""')
