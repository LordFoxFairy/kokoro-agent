"""Static architecture checks for the Chat persistence boundary."""

from __future__ import annotations

import re
from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_chat_schema_and_queries_use_explicit_tenant_predicates() -> None:
    root = _root()
    schema = (root / "database" / "schema.sql").read_text(encoding="utf-8")
    repository = (
        root / "src" / "kokoro_agent" / "infrastructure" / "postgres_chat_repository.py"
    ).read_text(encoding="utf-8")

    for table in (
        "kokoro_agent_chat_session",
        "kokoro_agent_chat_event",
        "kokoro_agent_chat_message",
        "kokoro_agent_chat_sequence",
    ):
        start = schema.index(f"CREATE TABLE IF NOT EXISTS {table}")
        end = schema.find("\n);", start)
        assert end > start
        assert re.search(r"\btenant_id\s+TEXT\s+NOT\s+NULL", schema[start:end])

    assert "tenant_id = %s" in repository
    assert "WHERE tenant_id = %s AND namespace = %s" in repository
    assert "extract(epoch FROM created_at)" not in repository
    assert "to_timestamp(%s / 1000.0)" not in repository


def test_chat_has_an_explicit_wire_to_domain_mapper_without_changing_wire_schema() -> (
    None
):
    root = _root()
    mapper = root / "src" / "kokoro_agent" / "application" / "chat" / "mappers.py"
    assert mapper.is_file()
    source = mapper.read_text(encoding="utf-8")
    assert "wire_epoch_millis_to_utc" in source
    assert "utc_to_wire_epoch_millis" in source
    assert "ChatMessageRecord" in source
    assert "ChatMessageView" in source

    openapi = (root / "contract" / "openapi" / "v1" / "openapi.json").read_text(
        encoding="utf-8"
    )
    assert '"created_at": {' in openapi
    assert '"type": "integer"' in openapi
