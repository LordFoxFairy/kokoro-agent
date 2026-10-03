"""Canonical database authority and clean-slate invariants."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from kokoro_agent.infrastructure import schema as installer


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
    assert not re.search(
        r"\b(?:CREATED|UPDATED|TERMINAL|EXPIRES|PUBLISHED)_AT\s+BIGINT\b", upper
    )


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


def test_static_recipe_binding_has_atomic_bounded_canonical_columns() -> None:
    schema = _schema().lower()
    assert re.search(r"assembly_recipe_bytes\s+bytea", schema)
    assert re.search(r"assembly_recipe_fingerprint\s+text", schema)
    assert "ck_kokoro_agent_run_static_recipe" in schema
    assert "8388608" in schema
    assert "^[0-9a-f]{64}$" in schema


# These tests call the real installer with isolated psycopg I/O. Loader faults
# use its existing seam, so no new distribution helper is required.
@pytest.mark.parametrize("failure", ["missing", "digest_drift"])
async def test_installer_rejects_asset_failure_before_schema_creation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    error = (
        FileNotFoundError("canonical DDL asset is missing")
        if failure == "missing"
        else ValueError("canonical DDL asset digest mismatch")
    )
    reads: list[str] = []
    missing_path = tmp_path / "missing-schema.sql"
    original_loader = installer.canonical_schema_sql
    if failure == "missing":
        monkeypatch.setattr(installer, "canonical_schema_path", lambda: missing_path)

    def rejected_sql() -> str:
        reads.append(failure)
        if failure == "missing":
            return original_loader()
        raise error  # Injected digest failure; this test checks ordering, not hashing.

    connection = MagicMock()
    cursor = connection.cursor.return_value.__aenter__.return_value
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    ensure = AsyncMock()
    monkeypatch.setattr(installer, "canonical_schema_sql", rejected_sql)
    monkeypatch.setattr(installer, "ensure_schema", ensure)
    with pytest.raises(type(error)) as caught:
        await installer.apply_agent_schema(
            connection, "agent_asset_test", require_blank=True
        )
    if failure == "missing":
        assert isinstance(caught.value, FileNotFoundError)
        assert caught.value.filename == str(missing_path)
    else:
        assert caught.value is error
    assert reads == [failure]
    assert (ensure.await_count, cursor.execute.await_count) == (0, 0)
    connection.transaction.assert_not_called()


@pytest.mark.parametrize("require_blank", [True, False])
async def test_installer_valid_sql_preserves_original_transaction_control(
    monkeypatch: pytest.MonkeyPatch, require_blank: bool
) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__aenter__.return_value
    events: list[str] = []

    async def record_ensure(conn: object, namespace: str) -> None:
        events.append("ensure")

    async def enter_transaction() -> None:
        events.append("transaction_enter")

    async def exit_transaction(*exception: object) -> None:
        events.append("transaction_exit")

    async def record_execute(statement: str, parameters: object = None) -> None:
        if "information_schema.tables" in statement:
            events.append("blank_check")
        elif statement.startswith("SET LOCAL"):
            events.append("search_path")
        else:
            events.append("ddl")

    connection.transaction.return_value.__aenter__.side_effect = enter_transaction
    connection.transaction.return_value.__aexit__.side_effect = exit_transaction
    cursor.execute = AsyncMock(side_effect=record_execute)
    cursor.fetchall = AsyncMock(return_value=[])
    ensure = AsyncMock(side_effect=record_ensure)
    loader = MagicMock(return_value="SELECT 'canonical bytes';")
    monkeypatch.setattr(installer, "canonical_schema_sql", loader)
    monkeypatch.setattr(installer, "ensure_schema", ensure)
    await installer.apply_agent_schema(
        connection, "agent_asset_test", require_blank=require_blank
    )
    ensure.assert_awaited_once_with(connection, "agent_asset_test")
    connection.transaction.assert_called_once_with()
    transaction = connection.transaction.return_value
    transaction.__aenter__.assert_awaited_once_with()
    transaction.__aexit__.assert_awaited_once_with(None, None, None)
    calls = cursor.execute.await_args_list
    assert len(calls) == (3 if require_blank else 2)
    if require_blank:
        assert "information_schema.tables" in calls[0].args[0]
        assert calls[0].args[1] == ("agent_asset_test",)
        cursor.fetchall.assert_awaited_once_with()
    else:
        cursor.fetchall.assert_not_awaited()
    assert calls[-2].args == (
        'SET LOCAL search_path TO "agent_asset_test", public, pg_catalog',
    )
    assert calls[-1].args == ("SELECT 'canonical bytes';",)
    loader.assert_called_once_with()
    assert events == [
        "ensure",
        "transaction_enter",
        *(["blank_check"] if require_blank else []),
        "search_path",
        "ddl",
        "transaction_exit",
    ]


async def test_installer_captures_validated_sql_before_ensure_and_does_not_reread(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sql_path = tmp_path / "schema.sql"
    initial_sql = "SELECT 'validated bytes';"
    sql_path.write_text(initial_sql, encoding="utf-8")
    connection = MagicMock()
    cursor = connection.cursor.return_value.__aenter__.return_value
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    reads: list[bytes] = []
    real_loader = installer.canonical_schema_sql
    monkeypatch.setattr(installer, "canonical_schema_path", lambda: sql_path)

    def recorded_loader() -> str:
        result = real_loader()
        reads.append(result.encode("utf-8"))
        return result

    async def mutate_after_preflight(conn: object, namespace: str) -> None:
        assert conn is connection and namespace == "agent_asset_test"
        sql_path.write_text("SELECT 'changed after preflight';", encoding="utf-8")

    monkeypatch.setattr(installer, "canonical_schema_sql", recorded_loader)
    monkeypatch.setattr(installer, "ensure_schema", mutate_after_preflight)
    await installer.apply_agent_schema(
        connection, "agent_asset_test", require_blank=True
    )
    assert reads == [initial_sql.encode("utf-8")]
    assert cursor.execute.await_args_list[-1].args == (initial_sql,)


@pytest.mark.parametrize("failure", ["nonempty", "ddl_error"])
async def test_installer_keeps_failure_and_transaction_exit(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__aenter__.return_value
    cursor.fetchall = AsyncMock(
        return_value=[{"table_name": "existing"}] if failure == "nonempty" else []
    )
    ddl = "SELECT 'canonical bytes';"
    ddl_error = RuntimeError("fixture DDL rejected")

    async def execute(statement: str, parameters: object = None) -> None:
        if statement == ddl:
            raise ddl_error

    cursor.execute = AsyncMock(side_effect=execute)
    monkeypatch.setattr(installer, "canonical_schema_sql", lambda: ddl)
    monkeypatch.setattr(installer, "ensure_schema", AsyncMock())
    with pytest.raises(RuntimeError) as caught:
        await installer.apply_agent_schema(
            connection, "agent_asset_test", require_blank=True
        )
    if failure == "nonempty":
        assert "requires an empty namespace; found: existing" in str(caught.value)
        assert cursor.execute.await_count == 1
    else:
        assert caught.value is ddl_error
        assert cursor.execute.await_count == 3
    exit_call = connection.transaction.return_value.__aexit__.await_args
    assert exit_call is not None
    assert exit_call.args[0] is RuntimeError
    assert exit_call.args[1] is caught.value
