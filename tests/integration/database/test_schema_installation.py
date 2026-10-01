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
    "postgresql://kokoro:kokoro@127.0.0.1:55433/kokoro_worker_agent",
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


@pytest.mark.parametrize(
    "mutation",
    [
        "DROP COLUMN assembly_recipe_bytes",
        "ALTER COLUMN assembly_recipe_bytes TYPE text USING encode(assembly_recipe_bytes, 'hex')",
        "ALTER COLUMN assembly_recipe_fingerprint SET NOT NULL",
        "ALTER COLUMN assembly_recipe_fingerprint SET DEFAULT repeat('0', 64)",
        "DROP CONSTRAINT ck_kokoro_agent_run_static_recipe",
        "DROP CONSTRAINT ck_kokoro_agent_run_static_recipe, ADD CONSTRAINT ck_kokoro_agent_run_static_recipe CHECK (assembly_recipe_bytes IS NULL OR octet_length(assembly_recipe_bytes) > 0)",
    ],
)
async def test_profile_schema_catalog_drift_rejected(
    run_chat_schema: str, run_chat_database_url: str, mutation: str
) -> None:
    from kokoro_agent.infrastructure.schema import SchemaNotReadyError
    from kokoro_agent.infrastructure.postgres import qualified
    from kokoro_agent.infrastructure.sql import execute_sql

    async with connect_pg(run_chat_database_url) as connection:
        await verify_agent_schema(connection, run_chat_schema)
        async with connection.cursor() as cursor:
            # The test owns this random schema; expressions are parametrized cases,
            # never external input. A bytea->text type change must drop its check.
            if "TYPE text" in mutation:
                await execute_sql(
                    cursor,
                    "ALTER TABLE {} DROP CONSTRAINT ck_kokoro_agent_run_static_recipe".format(
                        qualified(run_chat_schema, "kokoro_agent_run")
                    ),
                )
            await execute_sql(
                cursor,
                "ALTER TABLE {} {}".format(
                    qualified(run_chat_schema, "kokoro_agent_run"), mutation
                ),
            )
        with pytest.raises(SchemaNotReadyError):
            await verify_agent_schema(connection, run_chat_schema)
