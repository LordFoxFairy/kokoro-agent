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
MODEL_CALL_ATTEMPTS_TABLE = "kokoro_agent_model_call_attempt"
MODEL_USAGE_EVIDENCE_TABLE = "kokoro_agent_model_usage_evidence"
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
    MODEL_CALL_ATTEMPTS_TABLE,
    MODEL_USAGE_EVIDENCE_TABLE,
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
    await _verify_model_evidence_schema(conn, schema)


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


async def _verify_model_evidence_schema(
    conn: psycopg.AsyncConnection[Any], schema: str
) -> None:
    """Check exact A1 columns and the approved constraint/index inventory."""

    run_columns = {
        "model_attempt_count": ("bigint", "NO", "0", None),
        "model_dispatched_attempt_count": ("bigint", "NO", "0", None),
        "model_sealed_attempt_count": ("bigint", "NO", "0", None),
        "model_unknown_attempt_count": ("bigint", "NO", "0", None),
        "model_usage_completeness": ("text", "NO", "'none'::text", None),
    }
    attempt_types = {
        "attempt_id": "uuid",
        "tenant_id": "text",
        "run_id": "text",
        "call_id": "uuid",
        "attempt_ordinal": "bigint",
        "lease_owner": "text",
        "lease_generation": "bigint",
        "attempt_binding_bytes": "bytea",
        "attempt_binding_digest": "text",
        "planned_bytes": "bytea",
        "planned_digest": "text",
        "final_request_binding_version": "integer",
        "final_request_digest": "text",
        "authorization_bytes": "bytea",
        "authorization_digest": "text",
        "authorization_state": "text",
        "status": "text",
        "prepared_at": "timestamp with time zone",
        "dispatch_started_at": "timestamp with time zone",
        "sealed_at": "timestamp with time zone",
        "latest_revision": "bigint",
        "latest_event_id": "uuid",
        "latest_evidence_digest": "text",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
    }
    evidence_types = {
        "event_id": "uuid",
        "tenant_id": "text",
        "run_id": "text",
        "attempt_id": "uuid",
        "call_id": "uuid",
        "revision": "bigint",
        "predecessor_event_id": "uuid",
        "predecessor_digest": "text",
        "evidence_bytes": "bytea",
        "evidence_digest": "text",
        "execution_outcome": "text",
        "usage_state": "text",
        "totals_state": "text",
        "classification_state": "text",
        "sealed": "boolean",
        "observed_at": "timestamp with time zone",
        "created_at": "timestamp with time zone",
    }
    nullable_attempt = {
        "dispatch_started_at",
        "sealed_at",
        "latest_event_id",
        "latest_evidence_digest",
    }
    nullable_evidence = {"predecessor_event_id", "predecessor_digest"}
    expected_columns = {
        RUN_CLAIMS_TABLE: run_columns,
        MODEL_CALL_ATTEMPTS_TABLE: {
            name: (
                kind,
                "YES" if name in nullable_attempt else "NO",
                "0"
                if name == "latest_revision"
                else (
                    "CURRENT_TIMESTAMP(3)"
                    if name in {"created_at", "updated_at"}
                    else None
                ),
                3 if kind == "timestamp with time zone" else None,
            )
            for name, kind in attempt_types.items()
        },
        MODEL_USAGE_EVIDENCE_TABLE: {
            name: (
                kind,
                "YES" if name in nullable_evidence else "NO",
                "CURRENT_TIMESTAMP(3)" if name == "created_at" else None,
                3 if kind == "timestamp with time zone" else None,
            )
            for name, kind in evidence_types.items()
        },
    }
    check_names = {
        RUN_CLAIMS_TABLE: {"ck_kokoro_agent_run_model_usage"},
        MODEL_CALL_ATTEMPTS_TABLE: {
            "ck_model_attempt_final_request",
            "ck_model_attempt_identity",
            "ck_model_attempt_bytes",
            "ck_model_attempt_latest",
            "ck_model_attempt_state",
            "ck_model_attempt_times",
        },
        MODEL_USAGE_EVIDENCE_TABLE: {
            "ck_model_evidence_identity",
            "ck_model_evidence_revision",
            "ck_model_evidence_bytes",
            "ck_model_evidence_state",
        },
    }
    canonical_checks = {
        "ck_kokoro_agent_run_model_usage": "CHECK (model_attempt_count >= 0 AND "
        "model_dispatched_attempt_count >= 0 AND "
        "model_dispatched_attempt_count <= "
        "model_attempt_count AND "
        "model_sealed_attempt_count >= 0 AND "
        "model_sealed_attempt_count <= model_attempt_count "
        "AND model_unknown_attempt_count >= 0 AND "
        "model_unknown_attempt_count <= "
        "model_sealed_attempt_count AND "
        "(model_usage_completeness = ANY "
        "(ARRAY['none'::text, 'complete'::text, "
        "'partial'::text, 'unknown'::text])) AND "
        "(model_usage_completeness = 'none'::text AND "
        "model_attempt_count = 0 AND "
        "model_dispatched_attempt_count = 0 AND "
        "model_sealed_attempt_count = 0 AND "
        "model_unknown_attempt_count = 0 OR "
        "(model_usage_completeness = ANY "
        "(ARRAY['complete'::text, 'partial'::text])) AND "
        "model_attempt_count > 0 AND "
        "model_sealed_attempt_count = model_attempt_count "
        "AND model_unknown_attempt_count = 0 OR "
        "model_usage_completeness = 'unknown'::text AND "
        "model_attempt_count > 0 AND "
        "(model_sealed_attempt_count < model_attempt_count "
        "OR model_unknown_attempt_count > 0)))",
        "ck_model_attempt_final_request": "CHECK (final_request_binding_version = 1 AND "
        "final_request_digest ~ '^[0-9a-f]{64}$'::text)",
        "ck_model_attempt_bytes": "CHECK (octet_length(attempt_binding_bytes) >= 1 AND "
        "octet_length(attempt_binding_bytes) <= 65536 AND "
        "octet_length(planned_bytes) >= 1 AND "
        "octet_length(planned_bytes) <= 65536 AND "
        "octet_length(authorization_bytes) >= 1 AND "
        "octet_length(authorization_bytes) <= 65536 AND "
        "attempt_binding_digest ~ '^[0-9a-f]{64}$'::text AND "
        "planned_digest ~ '^[0-9a-f]{64}$'::text AND "
        "authorization_digest ~ '^[0-9a-f]{64}$'::text AND "
        "(latest_evidence_digest IS NULL OR latest_evidence_digest "
        "~ '^[0-9a-f]{64}$'::text))",
        "ck_model_attempt_identity": "CHECK (length(TRIM(BOTH FROM tenant_id)) > 0 AND "
        "length(run_id) > 0 AND length(lease_owner) > 0 AND "
        "attempt_ordinal > 0 AND lease_generation > 0)",
        "ck_model_attempt_latest": "CHECK (latest_revision = 0 AND latest_event_id IS NULL "
        "AND latest_evidence_digest IS NULL AND sealed_at IS NULL "
        "OR latest_revision > 0 AND latest_event_id IS NOT NULL "
        "AND latest_evidence_digest IS NOT NULL AND sealed_at IS "
        "NOT NULL)",
        "ck_model_attempt_state": "CHECK (status = 'prepared'::text AND authorization_state = "
        "'not_requested'::text AND dispatch_started_at IS NULL AND "
        "latest_revision = 0 OR status = "
        "'authorization_pending'::text AND authorization_state = "
        "'requested_unknown'::text AND dispatch_started_at IS NULL "
        "AND latest_revision = 0 OR status = "
        "'authorization_denied'::text AND authorization_state = "
        "'denied'::text AND dispatch_started_at IS NULL AND "
        "latest_revision > 0 OR status = 'authorized'::text AND "
        "authorization_state = 'granted'::text AND "
        "dispatch_started_at IS NULL AND latest_revision = 0 OR "
        "status = 'dispatch_started'::text AND authorization_state "
        "= 'granted'::text AND dispatch_started_at IS NOT NULL AND "
        "latest_revision = 0 OR status = 'observed'::text AND "
        "authorization_state = 'granted'::text AND "
        "dispatch_started_at IS NOT NULL AND latest_revision > 0 OR "
        "status = 'unknown'::text AND authorization_state = "
        "'granted'::text AND dispatch_started_at IS NOT NULL AND "
        "latest_revision > 0 OR status = 'not_dispatched'::text AND "
        "(authorization_state = ANY (ARRAY['not_requested'::text, "
        "'requested_unknown'::text, 'granted'::text])) AND "
        "dispatch_started_at IS NULL AND latest_revision > 0)",
        "ck_model_attempt_times": "CHECK ((dispatch_started_at IS NULL OR dispatch_started_at "
        ">= prepared_at) AND (sealed_at IS NULL OR sealed_at >= "
        "prepared_at) AND (sealed_at IS NULL OR dispatch_started_at "
        "IS NULL OR sealed_at >= dispatch_started_at))",
        "ck_model_evidence_bytes": "CHECK (octet_length(evidence_bytes) >= 1 AND "
        "octet_length(evidence_bytes) <= 65536)",
        "ck_model_evidence_identity": "CHECK (length(TRIM(BOTH FROM tenant_id)) > 0 AND "
        "length(run_id) > 0 AND revision > 0 AND "
        "evidence_digest ~ '^[0-9a-f]{64}$'::text AND "
        "(predecessor_digest IS NULL OR predecessor_digest ~ "
        "'^[0-9a-f]{64}$'::text))",
        "ck_model_evidence_revision": "CHECK (revision = 1 AND predecessor_event_id IS NULL "
        "AND predecessor_digest IS NULL OR revision > 1 AND "
        "predecessor_event_id IS NOT NULL AND "
        "predecessor_digest IS NOT NULL)",
        "ck_model_evidence_state": "CHECK (sealed = true AND (execution_outcome = ANY "
        "(ARRAY['not_dispatched'::text, 'completed'::text, "
        "'failed'::text, 'cancelled'::text, 'unknown'::text])) AND "
        "(usage_state = ANY (ARRAY['normalized'::text, "
        "'unresolved'::text])) AND (totals_state = ANY "
        "(ARRAY['known_zero'::text, 'known_nonzero'::text, "
        "'unknown'::text])) AND (classification_state = ANY "
        "(ARRAY['complete'::text, 'partial'::text, "
        "'unknown'::text])) AND (usage_state <> 'unresolved'::text "
        "OR totals_state = 'unknown'::text AND "
        "classification_state = 'unknown'::text) AND (usage_state "
        "<> 'normalized'::text OR (totals_state = ANY "
        "(ARRAY['known_zero'::text, 'known_nonzero'::text])) AND "
        "classification_state = 'partial'::text OR totals_state = "
        "'unknown'::text AND classification_state = "
        "'unknown'::text) AND (execution_outcome <> "
        "'not_dispatched'::text OR usage_state = "
        "'unresolved'::text) AND (execution_outcome <> "
        "'unknown'::text OR totals_state = 'unknown'::text AND "
        "classification_state = 'unknown'::text))",
    }
    primary_keys = {
        MODEL_CALL_ATTEMPTS_TABLE: (
            "kokoro_agent_model_call_attempt_pkey",
            ["attempt_id"],
        ),
        MODEL_USAGE_EVIDENCE_TABLE: (
            "kokoro_agent_model_usage_evidence_pkey",
            ["event_id"],
        ),
    }
    indexes = (
        (
            MODEL_CALL_ATTEMPTS_TABLE,
            "uq_model_attempt_call_ordinal",
            True,
            ["run_id", "call_id", "attempt_ordinal"],
            None,
        ),
        (
            MODEL_CALL_ATTEMPTS_TABLE,
            "uq_model_attempt_binding",
            True,
            ["tenant_id", "run_id", "attempt_binding_digest"],
            None,
        ),
        (
            MODEL_CALL_ATTEMPTS_TABLE,
            "ix_model_attempt_unsealed",
            False,
            ["run_id", "status", "attempt_id"],
            "status = ANY (ARRAY['prepared'::text, 'authorization_pending'::text, 'authorized'::text, 'dispatch_started'::text])",
        ),
        (
            MODEL_USAGE_EVIDENCE_TABLE,
            "uq_model_evidence_attempt_revision",
            True,
            ["attempt_id", "revision"],
            None,
        ),
        (
            MODEL_USAGE_EVIDENCE_TABLE,
            "uq_model_evidence_attempt_digest",
            True,
            ["attempt_id", "evidence_digest"],
            None,
        ),
        (
            MODEL_USAGE_EVIDENCE_TABLE,
            "ix_model_evidence_run",
            False,
            ["run_id", "attempt_id", "revision"],
            None,
        ),
    )

    async with conn.cursor() as cur:
        for table, expected in expected_columns.items():
            await cur.execute(
                """SELECT column_name,data_type,is_nullable,column_default,datetime_precision
                FROM information_schema.columns WHERE table_schema=%s AND table_name=%s
                  AND column_name=ANY(%s)""",
                (schema, table, list(expected)),
            )
            actual = {
                row["column_name"]: tuple(
                    row[key]
                    for key in (
                        "data_type",
                        "is_nullable",
                        "column_default",
                        "datetime_precision",
                    )
                )
                for row in await cur.fetchall()
            }
            if actual != expected:
                raise SchemaNotReadyError(
                    "Agent model evidence columns differ: " + table
                )
            await cur.execute(
                """SELECT c.conname,pg_get_constraintdef(c.oid,true) AS definition,
                c.convalidated,c.connoinherit
                FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
                JOIN pg_namespace n ON n.oid=t.relnamespace
                WHERE n.nspname=%s AND t.relname=%s AND c.contype='c'
                  AND c.conname=ANY(%s)""",
                (schema, table, list(check_names[table])),
            )
            found = {row["conname"]: row for row in await cur.fetchall()}
            if set(found) != check_names[table] or any(
                not row["convalidated"] or row["connoinherit"] for row in found.values()
            ):
                raise SchemaNotReadyError(
                    "Agent model evidence checks differ: " + table
                )
            for name, row in found.items():
                definition = str(row["definition"])
                if _sql_tokens(definition) != _sql_tokens(canonical_checks[name]):
                    raise SchemaNotReadyError(
                        "Agent model evidence check differs: " + name
                    )

            if table in primary_keys:
                primary_name, primary_columns = primary_keys[table]
                await cur.execute(
                    """SELECT c.conname,c.convalidated,
                    ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum,ord)
                          JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum
                          ORDER BY k.ord) AS columns
                    FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
                    JOIN pg_namespace n ON n.oid=t.relnamespace
                    WHERE n.nspname=%s AND t.relname=%s AND c.contype='p'""",
                    (schema, table),
                )
                primary = await cur.fetchone()
                if (
                    primary is None
                    or primary["conname"] != primary_name
                    or not primary["convalidated"]
                    or primary["columns"] != primary_columns
                ):
                    raise SchemaNotReadyError(
                        "Agent model evidence primary key differs: " + table
                    )

        for table, name, unique, keys, predicate in indexes:
            await cur.execute(
                """SELECT x.indisunique,x.indisvalid,x.indisready,x.indnatts,x.indnkeyatts,
                ARRAY(SELECT pg_get_indexdef(x.indexrelid,k,true)
                      FROM generate_series(1,x.indnkeyatts) AS k) AS keys,
                pg_get_expr(x.indpred,x.indrelid,true) AS predicate,am.amname
                FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid
                JOIN pg_class t ON t.oid=x.indrelid
                JOIN pg_namespace n ON n.oid=t.relnamespace
                JOIN pg_am am ON am.oid=i.relam
                WHERE n.nspname=%s AND t.relname=%s AND i.relname=%s""",
                (schema, table, name),
            )
            row = await cur.fetchone()
            if (
                row is None
                or row["indisunique"] != unique
                or not row["indisvalid"]
                or not row["indisready"]
                or row["indnatts"] != len(keys)
                or row["indnkeyatts"] != len(keys)
                or row["keys"] != keys
                or row["amname"] != "btree"
                or (row["predicate"] is None) != (predicate is None)
                or (
                    predicate is not None
                    and _sql_tokens(row["predicate"]) != _sql_tokens(predicate)
                )
            ):
                raise SchemaNotReadyError("Agent model evidence index differs: " + name)


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
    "MODEL_CALL_ATTEMPTS_TABLE",
    "MODEL_USAGE_EVIDENCE_TABLE",
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
