"""Typed boundary for psycopg's dynamically composed async row API.

The installed psycopg stubs intentionally accept only literal SQL and tuple rows,
while Agent repositories use validated schema identifiers and ``dict_row`` at
runtime.  This module is the single, narrow interop boundary: callers still own
parameterization and identifier validation, and returned rows are normalized to
an explicit mapping shape.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import psycopg
from psycopg.rows import dict_row


async def connect_dict_row(database_url: str) -> psycopg.AsyncConnection[Any]:
    """Open the repository connection with the runtime's dict-row factory.

    psycopg's published generic signature currently models only tuple rows for
    this overload.  The dynamic call is deliberately isolated here so the
    rest of the infrastructure layer can retain concrete connection types.
    """

    connect: Any = psycopg.AsyncConnection.connect
    return await connect(database_url, autocommit=True, row_factory=dict_row)


async def execute_sql(cursor: object, query: str, params: object | None = None) -> None:
    """Execute parameterized SQL through the runtime-typed psycopg surface."""

    dynamic_cursor: Any = cursor
    if params is None:
        await dynamic_cursor.execute(query)
    else:
        await dynamic_cursor.execute(query, params)


async def fetch_one(cursor: object) -> dict[str, Any] | None:
    """Fetch one dict row and fail closed if the configured row factory drifts."""

    dynamic_cursor: Any = cursor
    row: Any = await dynamic_cursor.fetchone()
    if row is None:
        return None
    return _row_mapping(row)


async def fetch_all(cursor: object) -> list[dict[str, Any]]:
    """Fetch all rows through the same explicit dict-row boundary."""

    dynamic_cursor: Any = cursor
    rows: Any = await dynamic_cursor.fetchall()
    return [_row_mapping(row) for row in rows]


def _row_mapping(row: Any) -> dict[str, Any]:
    """Normalize the runtime ``dict_row`` result at one explicit interop edge."""

    mapping_type: Any = Mapping
    if not isinstance(row, mapping_type):
        return _reject_row()
    # The psycopg row factory is a runtime mapping, while its stubs expose an
    # unresolved key/value pair.  Keep that uncertainty inside this adapter.
    items: Any = row.items()
    return {str(key): value for key, value in items}


def _reject_row() -> dict[str, Any]:
    raise TypeError("Agent PostgreSQL adapters require dict_row")


__all__ = ["connect_dict_row", "execute_sql", "fetch_all", "fetch_one"]
