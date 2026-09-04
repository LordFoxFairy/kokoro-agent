"""Canonical PostgreSQL schema for Agent-owned durable execution state.

Kokoro V1 installs only the current schema into an empty database. The runtime
repository neither migrates nor rewrites historical tables.
"""

# psycopg's async cursor stubs model only literal/template queries while this
# module intentionally formats validated schema-qualified identifiers.
# pyright: reportCallIssue=false, reportArgumentType=false

from __future__ import annotations

from typing import Any

import psycopg

from kokoro_agent.infrastructure.postgres import ensure_schema, qualified

RUN_CLAIMS_TABLE = "kokoro_agent_runs"
RUN_DISPATCHES_TABLE = "kokoro_agent_run_dispatches"
RUN_DLQ_TABLE = "kokoro_agent_run_dlq"
RUN_OUTBOX_TABLE = "kokoro_agent_run_outbox"
RUN_RECEIPTS_TABLE = "kokoro_agent_run_receipts"
RUN_RECEIPT_MANIFESTS_TABLE = "kokoro_agent_run_receipt_manifests"
RUN_CONTROL_COMMANDS_TABLE = "kokoro_agent_run_control_commands"
RUN_STEERS_TABLE = "kokoro_agent_run_steers"
RUN_USAGE_SEGMENTS_TABLE = "kokoro_agent_run_usage_segments"
SANDBOX_CLEANUP_INTENTS_TABLE = "kokoro_agent_sandbox_cleanup_intents"
TOOL_RESULTS_TABLE = "kokoro_agent_tool_results"
TOOL_JOURNAL_TABLE = "kokoro_agent_tool_journal"


def schema_statements(schema: str) -> tuple[str, ...]:
    """Return the complete canonical schema for one validated namespace."""

    return (
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text PRIMARY KEY,
            request_json text,
            owner text,
            lease_generation bigint NOT NULL DEFAULT 0,
            lease_expires_at bigint,
            terminal boolean NOT NULL DEFAULT FALSE,
            terminal_at bigint,
            durable_counter bigint NOT NULL DEFAULT 0,
            event_index_counter bigint NOT NULL DEFAULT 0,
            terminal_fence_seq bigint,
            token_total bigint NOT NULL DEFAULT 0,
            usage_input_total bigint NOT NULL DEFAULT 0,
            usage_output_total bigint NOT NULL DEFAULT 0,
            sandbox_id text,
            sandbox_generation bigint,
            sandbox_backend_kind text,
            sandbox_teardown_ref text,
            CONSTRAINT ck_kokoro_agent_runs_sandbox_binding CHECK (
                (sandbox_id IS NULL
                    AND sandbox_generation IS NULL
                    AND sandbox_backend_kind IS NULL
                    AND sandbox_teardown_ref IS NULL)
                OR
                (sandbox_id IS NOT NULL
                    AND sandbox_generation IS NOT NULL
                    AND sandbox_backend_kind IN ('docker', 'e2b', 'custom')
                    AND sandbox_teardown_ref IS NOT NULL)
            )
        )
        """.format(qualified(schema, RUN_CLAIMS_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            cleanup_id text PRIMARY KEY,
            run_id text NOT NULL,
            lease_generation bigint NOT NULL CHECK (lease_generation >= 1),
            backend_kind text NOT NULL CHECK (backend_kind IN ('docker', 'e2b', 'custom')),
            sandbox_id text NOT NULL,
            teardown_ref text NOT NULL,
            status text NOT NULL CHECK (status IN ('pending', 'processing', 'completed')),
            cleanup_owner text,
            attempt_count bigint NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            next_attempt_at bigint NOT NULL,
            last_error text,
            created_at bigint NOT NULL,
            updated_at bigint NOT NULL,
            CONSTRAINT uq_kokoro_agent_sandbox_cleanup_resource
                UNIQUE (run_id, lease_generation, backend_kind, sandbox_id)
        )
        """.format(qualified(schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
        """
        CREATE INDEX IF NOT EXISTS ix_kokoro_agent_sandbox_cleanup_due
        ON {} (status, next_attempt_at, created_at, cleanup_id)
        """.format(qualified(schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text PRIMARY KEY,
            session_id text NOT NULL,
            namespace text NOT NULL,
            request_json text NOT NULL,
            fence text NOT NULL,
            status text NOT NULL CHECK (status IN ('pending', 'claimed')),
            claimed_by text,
            created_at bigint NOT NULL,
            updated_at bigint NOT NULL
        )
        """.format(qualified(schema, RUN_DISPATCHES_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            raw_hash text PRIMARY KEY,
            source text NOT NULL,
            reason text NOT NULL,
            at bigint NOT NULL
        )
        """.format(qualified(schema, RUN_DLQ_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text NOT NULL,
            durable_seq bigint NOT NULL,
            event_id text NOT NULL UNIQUE,
            kind text NOT NULL,
            status text NOT NULL,
            index_value bigint,
            timestamp bigint,
            payload_json text,
            published_at bigint,
            PRIMARY KEY (run_id, durable_seq)
        )
        """.format(qualified(schema, RUN_OUTBOX_TABLE)),
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_kokoro_agent_run_outbox_run_index
        ON {} (run_id, index_value)
        WHERE index_value IS NOT NULL AND status <> 'superseded'
        """.format(qualified(schema, RUN_OUTBOX_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text NOT NULL,
            durable_seq bigint NOT NULL,
            event_id text NOT NULL,
            status text NOT NULL,
            reason text,
            created_at bigint NOT NULL,
            PRIMARY KEY (run_id, durable_seq)
        )
        """.format(qualified(schema, RUN_RECEIPTS_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text PRIMARY KEY,
            persisted_seq bigint NOT NULL DEFAULT 0,
            projected_seq bigint NOT NULL DEFAULT 0,
            consumed_seq bigint NOT NULL DEFAULT 0,
            producer_close_requested boolean NOT NULL DEFAULT FALSE,
            producer_closed boolean NOT NULL DEFAULT FALSE,
            updated_at bigint NOT NULL
        )
        """.format(qualified(schema, RUN_RECEIPT_MANIFESTS_TABLE)),
        """
        -- This is the single Agent-owned control command ledger. It contains
        -- admission and worker delivery state, not a chat message/event
        -- projection, and is never exposed to BFF or Web.
        CREATE TABLE IF NOT EXISTS {} (
            run_id text NOT NULL,
            command_id text NOT NULL,
            request_digest text NOT NULL,
            fingerprint text,
            status text NOT NULL CHECK (status IN (
                'admitted', 'persisted', 'applied', 'succeeded', 'failed', 'superseded'
            )),
            body text NOT NULL,
            error_code text,
            created_at bigint NOT NULL,
            updated_at bigint NOT NULL,
            PRIMARY KEY (run_id, command_id)
        )
        """.format(qualified(schema, RUN_CONTROL_COMMANDS_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text NOT NULL,
            message_id text NOT NULL,
            content text NOT NULL,
            created_at bigint NOT NULL,
            PRIMARY KEY (run_id, message_id)
        )
        """.format(qualified(schema, RUN_STEERS_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text NOT NULL,
            lease_generation bigint NOT NULL,
            input_tokens bigint NOT NULL CHECK (input_tokens >= 0),
            output_tokens bigint NOT NULL CHECK (output_tokens >= 0),
            created_at bigint NOT NULL,
            PRIMARY KEY (run_id, lease_generation)
        )
        """.format(qualified(schema, RUN_USAGE_SEGMENTS_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text NOT NULL,
            tool_id text NOT NULL,
            result text NOT NULL,
            is_error boolean NOT NULL,
            PRIMARY KEY (run_id, tool_id)
        )
        """.format(qualified(schema, TOOL_RESULTS_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text NOT NULL,
            tool_call_id text NOT NULL,
            name text NOT NULL,
            status text NOT NULL,
            result text NOT NULL,
            is_error boolean NOT NULL,
            PRIMARY KEY (run_id, tool_call_id)
        )
        """.format(qualified(schema, TOOL_JOURNAL_TABLE)),
    )


async def ensure_run_repository_schema(
    conn: psycopg.AsyncConnection[Any], schema: str
) -> None:
    """Create the canonical Agent execution tables for a fresh database."""

    await ensure_schema(conn, schema)
    async with conn.cursor() as cur:
        for statement in schema_statements(schema):
            await cur.execute(statement)


__all__ = [
    "RUN_CLAIMS_TABLE",
    "RUN_DISPATCHES_TABLE",
    "RUN_DLQ_TABLE",
    "RUN_CONTROL_COMMANDS_TABLE",
    "RUN_OUTBOX_TABLE",
    "RUN_RECEIPT_MANIFESTS_TABLE",
    "RUN_RECEIPTS_TABLE",
    "RUN_STEERS_TABLE",
    "RUN_USAGE_SEGMENTS_TABLE",
    "SANDBOX_CLEANUP_INTENTS_TABLE",
    "TOOL_JOURNAL_TABLE",
    "TOOL_RESULTS_TABLE",
    "schema_statements",
    "ensure_run_repository_schema",
]
