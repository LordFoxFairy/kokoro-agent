"""Install and verify the Agent-owned canonical PostgreSQL schema."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import psycopg

from kokoro_agent.distribution_assets import canonical_ddl_path, read_canonical_ddl
from kokoro_agent.infrastructure.postgres import ensure_schema

RUN_CLAIMS_TABLE = "kokoro_agent_run"
RUN_DISPATCHES_TABLE = "kokoro_agent_run_dispatch"
RUN_DLQ_TABLE = "kokoro_agent_run_dlq"
RUN_OUTBOX_TABLE = "kokoro_agent_run_outbox"
RUN_RECEIPTS_TABLE = "kokoro_agent_run_receipt"
RUN_RECEIPT_MANIFESTS_TABLE = "kokoro_agent_run_receipt_manifest"
RUN_CHECKPOINT_OBSERVATIONS_TABLE = "kokoro_agent_run_checkpoint_observation"
RUN_CONTROL_COMMANDS_TABLE = "kokoro_agent_run_control_command"
RUN_STEERS_TABLE = "kokoro_agent_run_steer"
RUN_USAGE_SEGMENTS_TABLE = "kokoro_agent_run_usage_segment"
SANDBOX_CLEANUP_INTENTS_TABLE = "kokoro_agent_sandbox_cleanup_intent"
TOOL_RESULTS_TABLE = "kokoro_agent_tool_result"
TOOL_JOURNAL_TABLE = "kokoro_agent_tool_journal"
CHAT_MESSAGES_TABLE = "kokoro_agent_chat_message"
CHAT_EVENTS_TABLE = "kokoro_agent_chat_event"
CHAT_SEQUENCES_TABLE = "kokoro_agent_chat_sequence"
CHAT_SESSIONS_TABLE = "kokoro_agent_chat_session"
MEMORY_TABLE = "kokoro_agent_memory"
CHECKPOINT_TABLES = ("checkpoints", "checkpoint_blobs", "checkpoint_writes")

AGENT_TABLES = (
    RUN_CLAIMS_TABLE,
    RUN_DISPATCHES_TABLE,
    RUN_DLQ_TABLE,
    RUN_OUTBOX_TABLE,
    RUN_RECEIPTS_TABLE,
    RUN_RECEIPT_MANIFESTS_TABLE,
    RUN_CONTROL_COMMANDS_TABLE,
    RUN_CHECKPOINT_OBSERVATIONS_TABLE,
    RUN_STEERS_TABLE,
    RUN_USAGE_SEGMENTS_TABLE,
    SANDBOX_CLEANUP_INTENTS_TABLE,
    TOOL_RESULTS_TABLE,
    TOOL_JOURNAL_TABLE,
    CHAT_MESSAGES_TABLE,
    CHAT_EVENTS_TABLE,
    CHAT_SEQUENCES_TABLE,
    CHAT_SESSIONS_TABLE,
    MEMORY_TABLE,
    *CHECKPOINT_TABLES,
)


class SchemaNotReadyError(RuntimeError):
    """The configured database schema has not been installed completely."""


def canonical_schema_path() -> Path:
    """Return the sole DDL asset in a source checkout or installed distribution."""

    return canonical_ddl_path()


def canonical_schema_sql() -> str:
    return read_canonical_ddl(canonical_schema_path())


async def apply_agent_schema(
    conn: psycopg.AsyncConnection[Any],
    schema: str,
    *,
    require_blank: bool,
) -> None:
    """Install the current V1 schema into one validated empty namespace."""

    schema_sql = canonical_schema_sql()
    await ensure_schema(conn, schema)
    async with conn.transaction():
        async with conn.cursor() as cur:
            # psycopg's stubs only accept LiteralString/Template while this
            # validated canonical file is intentionally loaded at runtime.
            dynamic_cursor: Any = cur
            if require_blank:
                await cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """,
                    (schema,),
                )
                existing = [str(row["table_name"]) for row in await cur.fetchall()]
                if existing:
                    raise RuntimeError(
                        "canonical schema requires an empty namespace; found: "
                        + ", ".join(existing)
                    )
            await dynamic_cursor.execute(
                f'SET LOCAL search_path TO "{_quote_ident(schema)}", public, pg_catalog'
            )
            await dynamic_cursor.execute(schema_sql)


async def verify_agent_schema(conn: psycopg.AsyncConnection[Any], schema: str) -> None:
    """Fail loudly when runtime starts against an incomplete deployment schema."""

    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (schema,),
        )
        actual = {str(row["table_name"]) for row in await cur.fetchall()}
    missing = sorted(set(AGENT_TABLES) - actual)
    if missing:
        raise SchemaNotReadyError(
            "Agent canonical schema is incomplete; missing tables: "
            + ", ".join(missing)
        )

    async with conn.cursor() as cur:
        await cur.execute(
            """SELECT column_name, data_type, is_nullable, column_default
               FROM information_schema.columns
               WHERE table_schema = %s AND table_name = 'kokoro_agent_run'
                 AND column_name IN ('assembly_recipe_bytes', 'assembly_recipe_fingerprint')
               ORDER BY column_name""",
            (schema,),
        )
        columns = [
            tuple(
                row[key]
                for key in ("column_name", "data_type", "is_nullable", "column_default")
            )
            for row in await cur.fetchall()
        ]
        if columns != [
            ("assembly_recipe_bytes", "bytea", "YES", None),
            ("assembly_recipe_fingerprint", "text", "YES", None),
        ]:
            raise SchemaNotReadyError(
                "Agent static recipe columns differ from canonical schema"
            )
        await cur.execute(
            """SELECT pg_get_constraintdef(c.oid, true) AS definition,
                      c.convalidated, c.connoinherit
               FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
               JOIN pg_namespace n ON n.oid = t.relnamespace
               WHERE n.nspname = %s AND t.relname = 'kokoro_agent_run'
                 AND c.conname = 'ck_kokoro_agent_run_static_recipe' AND c.contype = 'c'""",
            (schema,),
        )
        constraints = await cur.fetchall()
        expected = (
            "CHECK (assembly_recipe_bytes IS NULL AND assembly_recipe_fingerprint IS NULL OR "
            "assembly_recipe_bytes IS NOT NULL AND assembly_recipe_fingerprint IS NOT NULL AND "
            "octet_length(assembly_recipe_bytes) >= 1 AND octet_length(assembly_recipe_bytes) <= 8388608 AND "
            "assembly_recipe_fingerprint ~ '^[0-9a-f]{64}$'::text)"
        )
        if (
            len(constraints) != 1
            or constraints[0]["definition"] != expected
            or constraints[0]["convalidated"] is not True
            or constraints[0]["connoinherit"] is not False
        ):
            raise SchemaNotReadyError(
                "Agent static recipe constraint differs from canonical schema"
            )

    await _verify_interaction_schema(conn, schema)


async def _verify_interaction_schema(
    conn: psycopg.AsyncConnection[Any], schema: str
) -> None:
    """Check exact HITL columns and catalog definitions, without mutating schema."""
    run_columns = {
        "interaction_revision": ("bigint", "NO", "0", None),
        "interaction_phase": ("text", "NO", "'active'::text", None),
        "interaction_source_index": ("bigint", "YES", None, None),
        "pause_revision": ("bigint", "NO", "0", None),
        "pause_ref": ("text", "YES", None, None),
        "pending_groups_json": ("jsonb", "NO", "'[]'::jsonb", None),
        "pause_snapshot_json": ("jsonb", "YES", None, None),
        "pause_collection_digest": ("text", "YES", None, None),
        "interaction_command_id": ("text", "YES", None, None),
    }
    command_types = {
        "resume_pause_revision": "bigint",
        "resume_pause_ref": "text",
        "resume_decisions_bytes": "bytea",
        "resume_decisions_digest": "text",
        "resume_intent_status": "text",
        "resume_pause_snapshot_json": "jsonb",
        "resume_pause_collection_digest": "text",
        "resume_accepted_revision": "bigint",
        "resume_accepted_source_index": "bigint",
        "resume_attempt_id": "text",
        "resume_attempt_generation": "bigint",
        "resume_dispatch_plan_json": "jsonb",
        "resume_accepted_at": "timestamp with time zone",
        "resume_started_at": "timestamp with time zone",
        "resume_observation_digest": "text",
        "resume_result_kind": "text",
        "resume_result_revision": "bigint",
        "resume_result_source_index": "bigint",
        "resume_probe_progress_digest": "text",
        "resume_probe_quiescence_json": "jsonb",
        "resume_probe_last_read_id": "text",
        "resume_probe_checked_at": "timestamp with time zone",
    }
    command_columns: dict[str, tuple[str, str, str | None, int | None]] = {
        name: (kind, "YES", None, 3 if kind == "timestamp with time zone" else None)
        for name, kind in command_types.items()
    }
    command_columns["resume_probe_count"] = ("smallint", "NO", "0", None)
    observation_columns = {
        "run_id": ("text", "NO", None, None),
        "observation_digest": ("text", "NO", None, None),
        "generation": ("bigint", "NO", None, None),
        "command_id": ("text", "YES", None, None),
        "attempt_id": ("text", "YES", None, None),
        "kind": ("text", "NO", None, None),
        "disposition": ("text", "NO", None, None),
        "evidence_bytes": ("bytea", "NO", None, None),
        "probe_read_id": ("text", "YES", None, None),
        "created_at": ("timestamp with time zone", "NO", "clock_timestamp()", 3),
    }
    checks = {
        RUN_CLAIMS_TABLE: {
            "ck_run_interaction_revision": "CHECK (pause_revision >= 0 AND interaction_revision >= pause_revision)",
            "ck_run_interaction_phase": "CHECK ((interaction_phase = ANY (ARRAY['active'::text, 'waiting'::text, 'resuming'::text, 'terminal'::text])) AND terminal = (interaction_phase = 'terminal'::text))",
            "ck_run_interaction_collection": "CHECK (jsonb_typeof(pending_groups_json) = 'array'::text AND CASE WHEN interaction_phase = ANY (ARRAY['waiting'::text, 'resuming'::text]) THEN jsonb_array_length(pending_groups_json) > 0 ELSE jsonb_array_length(pending_groups_json) = 0 END)",
            "ck_run_pause_identity": "CHECK (pause_revision = 0 AND pause_ref IS NULL AND pause_snapshot_json IS NULL AND pause_collection_digest IS NULL OR pause_revision > 0 AND pause_ref IS NOT NULL AND length(pause_ref) > 0 AND pause_snapshot_json IS NOT NULL AND jsonb_typeof(pause_snapshot_json) = 'object'::text AND pause_collection_digest IS NOT NULL AND pause_collection_digest ~ '^[0-9a-f]{64}$'::text)",
            "ck_run_interaction_command": "CHECK ((interaction_command_id IS NULL OR length(interaction_command_id) > 0) AND (interaction_phase <> 'resuming'::text OR interaction_command_id IS NOT NULL))",
            "ck_run_interaction_source": "CHECK ((interaction_source_index IS NULL OR interaction_source_index >= 0) AND (interaction_revision <> 0 OR interaction_source_index IS NULL) AND ((interaction_phase <> ALL (ARRAY['waiting'::text, 'resuming'::text])) OR interaction_source_index IS NOT NULL))",
        },
        RUN_CONTROL_COMMANDS_TABLE: {
            "ck_control_resume_intent": "CHECK (resume_intent_status IS NULL AND num_nonnulls(resume_pause_revision, resume_pause_ref, resume_decisions_bytes, resume_decisions_digest, resume_pause_snapshot_json, resume_pause_collection_digest, resume_accepted_revision, resume_accepted_source_index, resume_attempt_id, resume_attempt_generation, resume_dispatch_plan_json, resume_accepted_at, resume_started_at) = 0 OR resume_intent_status IS NOT NULL AND (resume_intent_status = ANY (ARRAY['accepted'::text, 'dispatch_started'::text, 'native_observed'::text, 'unknown'::text, 'reconciled'::text, 'terminal'::text])) AND num_nonnulls(resume_pause_revision, resume_pause_ref, resume_decisions_bytes, resume_decisions_digest, resume_pause_snapshot_json, resume_pause_collection_digest, resume_accepted_revision, resume_accepted_source_index, resume_accepted_at) = 9 AND resume_pause_revision > 0 AND length(resume_pause_ref) > 0 AND octet_length(resume_decisions_bytes) >= 1 AND octet_length(resume_decisions_bytes) <= 8388608 AND resume_decisions_digest ~ '^[0-9a-f]{64}$'::text AND resume_pause_collection_digest ~ '^[0-9a-f]{64}$'::text AND jsonb_typeof(resume_pause_snapshot_json) = 'object'::text AND resume_accepted_revision > resume_pause_revision AND resume_accepted_source_index >= 0)",
            "ck_control_resume_attempt": "CHECK (num_nonnulls(resume_attempt_id, resume_attempt_generation, resume_dispatch_plan_json, resume_started_at) = 0 AND (resume_intent_status IS NULL OR (resume_intent_status = ANY (ARRAY['accepted'::text, 'terminal'::text]))) OR num_nonnulls(resume_attempt_id, resume_attempt_generation, resume_dispatch_plan_json, resume_started_at) = 4 AND resume_intent_status IS NOT NULL AND (resume_intent_status = ANY (ARRAY['dispatch_started'::text, 'native_observed'::text, 'unknown'::text, 'reconciled'::text, 'terminal'::text])) AND length(resume_attempt_id) > 0 AND resume_attempt_generation > 0 AND jsonb_typeof(resume_dispatch_plan_json) = 'object'::text AND resume_started_at >= resume_accepted_at)",
        },
    }
    checks[RUN_CONTROL_COMMANDS_TABLE].update(
        {
            "ck_control_resume_observation": "CHECK ((resume_observation_digest IS NULL OR resume_observation_digest ~ '^[0-9a-f]{64}$'::text) AND (resume_intent_status IS DISTINCT FROM 'native_observed'::text OR resume_observation_digest IS NOT NULL))",
            "ck_control_resume_result": "CHECK (num_nonnulls(resume_result_kind, resume_result_revision, resume_result_source_index) = 0 OR num_nonnulls(resume_result_kind, resume_result_revision, resume_result_source_index) = 3 AND (resume_result_kind = ANY (ARRAY['accepted'::text, 'native_consumed'::text, 'validation_failed'::text, 'unknown'::text, 'cancelled'::text])) AND resume_result_revision > 0 AND resume_result_source_index >= 0 AND resume_intent_status IS NOT NULL)",
            "ck_control_resume_probe": "CHECK (resume_probe_count >= 0 AND resume_probe_count <= 3 AND (num_nonnulls(resume_probe_progress_digest, resume_probe_quiescence_json, resume_probe_last_read_id, resume_probe_checked_at) = 0 AND resume_probe_count = 0 OR num_nonnulls(resume_probe_progress_digest, resume_probe_quiescence_json, resume_probe_last_read_id, resume_probe_checked_at) = 4 AND resume_probe_progress_digest ~ '^[0-9a-f]{64}$'::text AND length(resume_probe_last_read_id) > 0 AND jsonb_typeof(resume_probe_quiescence_json) = 'object'::text AND resume_attempt_id IS NOT NULL))",
        }
    )
    checks[RUN_CHECKPOINT_OBSERVATIONS_TABLE] = {
        "ck_checkpoint_observation_identity": "CHECK (length(run_id) > 0 AND generation > 0 AND observation_digest ~ '^[0-9a-f]{64}$'::text)",
        "ck_checkpoint_observation_target": "CHECK ((command_id IS NULL AND attempt_id IS NULL OR command_id IS NOT NULL AND attempt_id IS NOT NULL AND length(command_id) > 0 AND length(attempt_id) > 0) AND (kind <> 'pause'::text OR command_id IS NULL) AND ((kind <> ALL (ARRAY['resume'::text, 'probe'::text])) OR command_id IS NOT NULL))",
        "ck_checkpoint_observation_kind": "CHECK ((kind = ANY (ARRAY['pause'::text, 'read'::text, 'resume'::text, 'probe'::text])) AND (disposition = ANY (ARRAY['current'::text, 'audit'::text])))",
        "ck_checkpoint_observation_bytes": "CHECK (octet_length(evidence_bytes) >= 1 AND octet_length(evidence_bytes) <= 8388608)",
        "ck_checkpoint_observation_probe": "CHECK (kind = 'probe'::text AND probe_read_id IS NOT NULL AND length(probe_read_id) > 0 OR kind <> 'probe'::text AND probe_read_id IS NULL)",
    }
    async with conn.cursor() as cur:
        for table, expected in (
            (RUN_CLAIMS_TABLE, run_columns),
            (RUN_CONTROL_COMMANDS_TABLE, command_columns),
            (RUN_CHECKPOINT_OBSERVATIONS_TABLE, observation_columns),
        ):
            await cur.execute(
                """SELECT column_name,data_type,is_nullable,column_default,datetime_precision
                FROM information_schema.columns WHERE table_schema=%s AND table_name=%s
                  AND column_name=ANY(%s)""",
                (schema, table, list(expected)),
            )
            actual = {
                row["column_name"]: (
                    row["data_type"],
                    row["is_nullable"],
                    row["column_default"],
                    row["datetime_precision"],
                )
                for row in await cur.fetchall()
            }
            if actual != expected:
                raise SchemaNotReadyError("Agent interaction columns differ: " + table)
            await cur.execute(
                """SELECT c.conname,pg_get_constraintdef(c.oid,true) AS definition,c.convalidated,c.connoinherit
                FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
                WHERE n.nspname=%s AND t.relname=%s AND c.contype='c' AND c.conname=ANY(%s)""",
                (schema, table, list(checks[table])),
            )
            found = {row["conname"]: row for row in await cur.fetchall()}
            for name, definition in checks[table].items():
                row = found.get(name)
                if (
                    row is None
                    or not row["convalidated"]
                    or row["connoinherit"]
                    or _sql_tokens(row["definition"]) != _sql_tokens(definition)
                ):
                    raise SchemaNotReadyError(
                        "Agent interaction constraint differs: " + name
                    )
        await cur.execute(
            """SELECT i.relname,x.indisunique,x.indisvalid,x.indisready,x.indnatts,x.indnkeyatts,
            pg_get_indexdef(x.indexrelid,1,true) AS first_key,pg_get_indexdef(x.indexrelid,2,true) AS second_key,
            pg_get_expr(x.indpred,x.indrelid,true) AS predicate,am.amname
            FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class t ON t.oid=x.indrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace JOIN pg_am am ON am.oid=i.relam
            WHERE n.nspname=%s AND t.relname=%s AND i.relname=ANY(%s)""",
            (
                schema,
                RUN_CONTROL_COMMANDS_TABLE,
                ["uq_control_resume_pause", "uq_control_resume_attempt"],
            ),
        )
        indexes = {row["relname"]: row for row in await cur.fetchall()}
        for name, key, predicate in (
            (
                "uq_control_resume_pause",
                "resume_pause_revision",
                "resume_intent_status IS NOT NULL",
            ),
            (
                "uq_control_resume_attempt",
                "resume_attempt_id",
                "resume_attempt_id IS NOT NULL",
            ),
        ):
            row = indexes.get(name)
            if (
                row is None
                or not all(row[k] for k in ("indisunique", "indisvalid", "indisready"))
                or row["indnatts"] != 2
                or row["indnkeyatts"] != 2
                or row["first_key"] != "run_id"
                or row["second_key"] != key
                or row["amname"] != "btree"
                or _sql_tokens(row["predicate"]) != _sql_tokens(predicate)
            ):
                raise SchemaNotReadyError("Agent interaction index differs: " + name)

        # Catalog shape is checked, not inferred from a familiar index name.
        for table, name, unique, keys, predicate in (
            (
                RUN_CHECKPOINT_OBSERVATIONS_TABLE,
                "kokoro_agent_run_checkpoint_observation_pkey",
                True,
                ["run_id", "observation_digest"],
                None,
            ),
            (
                RUN_CHECKPOINT_OBSERVATIONS_TABLE,
                "uq_checkpoint_observation_probe",
                True,
                ["run_id", "command_id", "attempt_id", "probe_read_id"],
                "kind = 'probe'::text",
            ),
            (
                RUN_CHECKPOINT_OBSERVATIONS_TABLE,
                "idx_checkpoint_observation_target",
                False,
                [
                    "run_id",
                    "command_id",
                    "attempt_id",
                    "generation",
                    "observation_digest",
                ],
                None,
            ),
            (
                RUN_CONTROL_COMMANDS_TABLE,
                "idx_control_resume_unsettled",
                False,
                ["run_id", "command_id"],
                "resume_intent_status = ANY (ARRAY['accepted'::text, 'dispatch_started'::text, 'native_observed'::text, 'unknown'::text])",
            ),
        ):
            await cur.execute(
                """SELECT x.indisunique,x.indisvalid,x.indisready,x.indnatts,x.indnkeyatts,
                ARRAY(SELECT pg_get_indexdef(x.indexrelid,k,true) FROM generate_series(1,x.indnkeyatts) AS k) AS keys,
                pg_get_expr(x.indpred,x.indrelid,true) AS predicate,am.amname
                FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class t ON t.oid=x.indrelid
                JOIN pg_namespace n ON n.oid=t.relnamespace JOIN pg_am am ON am.oid=i.relam
                WHERE n.nspname=%s AND t.relname=%s AND i.relname=%s""",
                (schema, table, name),
            )
            row = await cur.fetchone()
            if (
                row is None
                or row["indisunique"] != unique
                or not row["indisvalid"]
                or not row["indisready"]
                or row["keys"] != keys
                or row["indnatts"] != len(keys)
                or row["indnkeyatts"] != len(keys)
                or row["amname"] != "btree"
                or (row["predicate"] is None) != (predicate is None)
                or (
                    predicate is not None
                    and _sql_tokens(row["predicate"]) != _sql_tokens(predicate)
                )
            ):
                raise SchemaNotReadyError("Agent interaction index differs: " + name)


def _sql_tokens(value: str) -> list[str]:
    # Ignore formatting only, never parentheses, string values or operators.
    return re.findall(
        r"'(?:''|[^'])*'|[a-zA-Z_][a-zA-Z_0-9]*|[0-9]+|::|>=|<=|<>|\S", value
    )


def _quote_ident(value: str) -> str:
    return value.replace('"', '""')


__all__ = [
    "AGENT_TABLES",
    "CHAT_EVENTS_TABLE",
    "CHAT_MESSAGES_TABLE",
    "CHAT_SEQUENCES_TABLE",
    "CHAT_SESSIONS_TABLE",
    "CHECKPOINT_TABLES",
    "MEMORY_TABLE",
    "RUN_CLAIMS_TABLE",
    "RUN_CONTROL_COMMANDS_TABLE",
    "RUN_DISPATCHES_TABLE",
    "RUN_DLQ_TABLE",
    "RUN_OUTBOX_TABLE",
    "RUN_RECEIPT_MANIFESTS_TABLE",
    "RUN_RECEIPTS_TABLE",
    "RUN_STEERS_TABLE",
    "RUN_USAGE_SEGMENTS_TABLE",
    "SANDBOX_CLEANUP_INTENTS_TABLE",
    "SchemaNotReadyError",
    "TOOL_JOURNAL_TABLE",
    "TOOL_RESULTS_TABLE",
    "apply_agent_schema",
    "canonical_schema_path",
    "canonical_schema_sql",
    "verify_agent_schema",
]
