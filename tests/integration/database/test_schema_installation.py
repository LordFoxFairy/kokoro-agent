"""The operator command installs one exact canonical schema and rejects drift."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
from psycopg import sql

from kokoro_agent.cli import apply_database_schema
from kokoro_agent.infrastructure.postgres import connect_pg
from kokoro_agent.infrastructure.schema import AGENT_TABLES, verify_agent_schema


DATABASE_URL = os.environ.get(
    "KOKORO_AGENT_DATABASE_URL",
    "postgresql://kokoro@127.0.0.1:55433/kokoro_worker_agent?password=kokoro",
)


@pytest.mark.asyncio
async def test_apply_schema_installs_only_a_blank_namespace() -> None:
    schema = f"kokoro_schema_command_{uuid4().hex}"
    environment = {
        "KOKORO_AGENT_DATABASE_URL": DATABASE_URL,
        "KOKORO_AGENT_DATABASE_SCHEMA": schema,
    }
    try:
        await apply_database_schema(environment)
        async with connect_pg(DATABASE_URL) as connection:
            await verify_agent_schema(connection, schema)
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    """,
                    (schema,),
                )
                tables = {str(row["table_name"]) for row in await cursor.fetchall()}
        assert tables == set(AGENT_TABLES)

        with pytest.raises(RuntimeError, match="requires an empty namespace"):
            await apply_database_schema(environment)
    finally:
        async with connect_pg(DATABASE_URL) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    )
                )
