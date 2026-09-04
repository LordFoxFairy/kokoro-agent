"""Canonical database authority and clean-slate invariants."""

from __future__ import annotations

import re
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_SCHEMA = REPOSITORY_ROOT / "database" / "schema.sql"
SOURCE_ROOT = REPOSITORY_ROOT / "src" / "kokoro_agent"


def _schema() -> str:
    return CANONICAL_SCHEMA.read_text(encoding="utf-8")


def test_database_schema_is_the_only_project_owned_ddl_authority() -> None:
    assert CANONICAL_SCHEMA.is_file()
    assert not (REPOSITORY_ROOT / "database" / "migrations").exists()

    ddl_sources = [
        path.relative_to(REPOSITORY_ROOT).as_posix()
        for path in SOURCE_ROOT.rglob("*.py")
        if "CREATE TABLE" in path.read_text(encoding="utf-8").upper()
    ]
    assert ddl_sources == []


def test_database_schema_is_clean_slate_utc_and_application_related() -> None:
    schema = _schema()
    upper = schema.upper()

    assert "FOREIGN KEY" not in upper
    assert "REFERENCES" not in upper
    assert "ALTER TABLE" not in upper
    assert "CHECKPOINT_MIGRATIONS" not in upper
    assert "TIMESTAMP WITHOUT TIME ZONE" not in upper
    assert "TIMESTAMPTZ(3)" in upper
    assert not re.search(r"\b(?:CREATED|UPDATED|TERMINAL|EXPIRES|PUBLISHED)_AT\s+BIGINT\b", upper)


def test_database_schema_contains_every_agent_owned_durable_surface() -> None:
    schema = _schema().lower()
    expected_tables = {
        "kokoro_agent_run",
        "kokoro_agent_run_dispatch",
        "kokoro_agent_run_outbox",
        "kokoro_agent_run_receipt",
        "kokoro_agent_run_receipt_manifest",
        "kokoro_agent_run_control_command",
        "kokoro_agent_run_steer",
        "kokoro_agent_run_usage_segment",
        "kokoro_agent_run_dlq",
        "kokoro_agent_sandbox_cleanup_intent",
        "kokoro_agent_tool_result",
        "kokoro_agent_tool_journal",
        "kokoro_agent_chat_session",
        "kokoro_agent_chat_event",
        "kokoro_agent_chat_message",
        "kokoro_agent_chat_sequence",
        "kokoro_agent_memory",
        "checkpoints",
        "checkpoint_blobs",
        "checkpoint_writes",
    }

    for table in expected_tables:
        assert f"create table if not exists {table}" in schema

    assert "tenant_id" in schema
