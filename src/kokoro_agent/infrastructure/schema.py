"""Install and verify the Agent-owned canonical PostgreSQL schema."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

import psycopg

from kokoro_agent.infrastructure.postgres import ensure_schema

RUN_CLAIMS_TABLE = "kokoro_agent_run"
RUN_DISPATCHES_TABLE = "kokoro_agent_run_dispatch"
RUN_DLQ_TABLE = "kokoro_agent_run_dlq"
RUN_OUTBOX_TABLE = "kokoro_agent_run_outbox"
RUN_RECEIPTS_TABLE = "kokoro_agent_run_receipt"
RUN_RECEIPT_MANIFESTS_TABLE = "kokoro_agent_run_receipt_manifest"
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

    source_path = Path(__file__).resolve().parents[3] / "database" / "schema.sql"
    if source_path.is_file():
        return source_path
    installed_path = Path(sys.prefix) / "share" / "kokoro-agent" / "schema.sql"
    if installed_path.is_file():
        return installed_path
    raise FileNotFoundError(
        "canonical database/schema.sql is not present in the source checkout "
        "or installed share/kokoro-agent asset"
    )


def canonical_schema_sql() -> str:
    return canonical_schema_path().read_text(encoding="utf-8")


async def apply_agent_schema(
    conn: psycopg.AsyncConnection[Any],
    schema: str,
    *,
    require_blank: bool,
) -> None:
    """Install the current V1 schema into one validated empty namespace."""

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
            await dynamic_cursor.execute(canonical_schema_sql())


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
