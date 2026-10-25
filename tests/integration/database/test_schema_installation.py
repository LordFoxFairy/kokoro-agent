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


@pytest.mark.parametrize(
    ("table", "mutation"),
    [
        ("kokoro_agent_model_call_attempt", "DROP COLUMN final_request_digest"),
        (
            "kokoro_agent_model_call_attempt",
            "ALTER COLUMN final_request_digest DROP NOT NULL",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "ALTER COLUMN final_request_digest SET DEFAULT repeat('0',64)",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "ALTER COLUMN final_request_binding_version DROP NOT NULL",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "ALTER COLUMN final_request_binding_version SET DEFAULT 1",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP CONSTRAINT ck_model_attempt_final_request",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP CONSTRAINT ck_model_attempt_final_request, ADD CONSTRAINT ck_model_attempt_final_request CHECK (TRUE)",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP COLUMN final_request_binding_version",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "ALTER COLUMN final_request_binding_version TYPE bigint",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "ALTER COLUMN final_request_digest TYPE varchar(64)",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP CONSTRAINT ck_model_attempt_final_request, ADD CONSTRAINT ck_model_attempt_final_request CHECK (final_request_binding_version = 2)",
        ),
        (
            "kokoro_agent_run",
            "DROP CONSTRAINT ck_kokoro_agent_run_model_usage",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP CONSTRAINT ck_model_attempt_state",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP CONSTRAINT ck_model_attempt_state, "
            "ADD CONSTRAINT ck_model_attempt_state CHECK (TRUE)",
        ),
        (
            "kokoro_agent_model_usage_evidence",
            "DROP CONSTRAINT ck_model_evidence_revision",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP CONSTRAINT uq_model_attempt_call_ordinal",
        ),
        (
            "kokoro_agent_model_usage_evidence",
            "DROP CONSTRAINT uq_model_evidence_attempt_revision",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP COLUMN authorization_digest",
        ),
        (
            "kokoro_agent_model_call_attempt",
            "DROP CONSTRAINT kokoro_agent_model_call_attempt_pkey",
        ),
    ],
)
async def test_a1_model_evidence_catalog_drift_rejected(
    run_chat_schema: str,
    run_chat_database_url: str,
    table: str,
    mutation: str,
) -> None:
    from kokoro_agent.infrastructure.postgres import qualified
    from kokoro_agent.infrastructure.schema import SchemaNotReadyError
    from kokoro_agent.infrastructure.sql import execute_sql

    async with connect_pg(run_chat_database_url) as connection:
        await verify_agent_schema(connection, run_chat_schema)
        async with connection.cursor() as cursor:
            await execute_sql(
                cursor,
                "ALTER TABLE {} {}".format(qualified(run_chat_schema, table), mutation),
            )
        with pytest.raises(SchemaNotReadyError):
            await verify_agent_schema(connection, run_chat_schema)


@pytest.mark.parametrize(
    "column",
    [
        "execution_bound_artifact_version",
        "execution_bound_ref",
        "execution_bound_revision",
        "execution_bound_bytes",
        "execution_bound_digest",
        "execution_bound_valid_until",
    ],
)
@pytest.mark.parametrize("change", ["nullable", "default", "missing"])
async def test_bound_six_columns_are_clean_slate_required_without_defaults(
    run_chat_schema: str, run_chat_database_url: str, column: str, change: str
) -> None:
    from kokoro_agent.infrastructure.schema import SchemaNotReadyError
    from kokoro_agent.infrastructure.postgres import qualified
    from kokoro_agent.infrastructure.sql import execute_sql

    table = qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
    if change == "nullable":
        mutation = f"ALTER COLUMN {column} DROP NOT NULL"
    elif change == "missing":
        mutation = f"DROP COLUMN {column} CASCADE"
    else:
        defaults = {
            "execution_bound_revision": "1",
            "execution_bound_bytes": "decode('7b7d','hex')",
            "execution_bound_valid_until": "CURRENT_TIMESTAMP(3)",
        }
        mutation = f"ALTER COLUMN {column} SET DEFAULT {defaults.get(column, chr(39) + 'test' + chr(39))}"
    async with connect_pg(run_chat_database_url) as connection:
        await verify_agent_schema(connection, run_chat_schema)
        async with connection.cursor() as cursor:
            await execute_sql(cursor, f"ALTER TABLE {table} {mutation}")
        with pytest.raises(SchemaNotReadyError):
            await verify_agent_schema(connection, run_chat_schema)


@pytest.mark.parametrize(
    "mutation",
    [
        "DROP CONSTRAINT uq_model_attempt_execution_bound",
        "DROP CONSTRAINT ck_model_attempt_execution_bound",
        "DROP CONSTRAINT ck_model_attempt_execution_bound, ADD CONSTRAINT ck_model_attempt_execution_bound CHECK (TRUE)",
    ],
)
async def test_bound_identity_unique_and_check_are_verified(
    run_chat_schema: str, run_chat_database_url: str, mutation: str
) -> None:
    from kokoro_agent.infrastructure.schema import SchemaNotReadyError
    from kokoro_agent.infrastructure.postgres import qualified
    from kokoro_agent.infrastructure.sql import execute_sql

    async with connect_pg(run_chat_database_url) as connection:
        async with connection.cursor() as cursor:
            await execute_sql(
                cursor,
                f"ALTER TABLE {qualified(run_chat_schema, 'kokoro_agent_model_call_attempt')} {mutation}",
            )
        with pytest.raises(SchemaNotReadyError):
            await verify_agent_schema(connection, run_chat_schema)
