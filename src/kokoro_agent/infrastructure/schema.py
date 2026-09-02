"""Canonical PostgreSQL schema for Agent-owned durable execution state.

The Agent database is created from this schema only.  Schema evolution belongs
in an explicit migration before a new application version is deployed; the
runtime repository does not inspect or rewrite unknown historical tables.
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
            lease_expires_at bigint,
            terminal boolean NOT NULL DEFAULT FALSE,
            terminal_at bigint,
            durable_counter bigint NOT NULL DEFAULT 0,
            terminal_fence_seq bigint,
            token_total bigint NOT NULL DEFAULT 0,
            usage_input_total bigint NOT NULL DEFAULT 0,
            usage_output_total bigint NOT NULL DEFAULT 0,
            sandbox_id text
        )
        """.format(qualified(schema, RUN_CLAIMS_TABLE)),
        """
        CREATE TABLE IF NOT EXISTS {} (
            run_id text PRIMARY KEY,
            session_id text NOT NULL,
            namespace text NOT NULL,
            fence text NOT NULL,
            status text NOT NULL,
            deadline_at bigint NOT NULL,
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
    "TOOL_JOURNAL_TABLE",
    "TOOL_RESULTS_TABLE",
    "schema_statements",
    "ensure_run_repository_schema",
]
