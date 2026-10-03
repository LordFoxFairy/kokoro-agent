"""Operator CLI commands keep schema installation explicit and testable."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest

from kokoro_agent import cli


def test_db_apply_schema_uses_the_configured_empty_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[Mapping[str, str]] = []

    async def apply_database_schema(environment: Mapping[str, str]) -> None:
        captured.append(environment)

    monkeypatch.setattr(cli, "apply_database_schema", apply_database_schema)
    environment = {
        "KOKORO_AGENT_DATABASE_URL": "postgresql://fixture/db",
        "KOKORO_AGENT_DATABASE_SCHEMA": "agent_fixture",
    }

    assert cli.main(["db:apply-schema"], environment=environment) == 0
    assert captured == [environment]


@pytest.mark.parametrize("failure", ["missing", "digest_drift"])
@pytest.mark.parametrize("stage", ["initial", "after_connect"])
async def test_schema_operator_checks_assets_before_connecting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str, stage: str
) -> None:
    """Fault-inject the existing loader boundary, not a future helper import."""
    from contextlib import asynccontextmanager
    from collections.abc import AsyncGenerator
    from unittest.mock import AsyncMock, MagicMock

    from kokoro_agent.application import schema as operator
    from kokoro_agent.infrastructure import schema as installer

    error = (
        FileNotFoundError("canonical DDL asset is missing")
        if failure == "missing"
        else ValueError("canonical DDL asset digest mismatch")
    )
    reads: list[str] = []
    connects: list[str] = []
    connection = MagicMock()
    cursor = connection.cursor.return_value.__aenter__.return_value
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    ensure = AsyncMock()

    missing_path = tmp_path / "missing-schema.sql"
    original_loader = installer.canonical_schema_sql
    if failure == "missing":
        monkeypatch.setattr(installer, "canonical_schema_path", lambda: missing_path)

    def rejected_sql() -> str:
        reads.append(failure)
        if stage == "after_connect" and len(reads) == 1:
            return "SELECT 'preflight accepted';"
        if failure == "missing":
            return original_loader()  # Real path/read failure, with no DB I/O.
        raise error  # Digest validation's failure is injected at its existing seam.

    @asynccontextmanager
    async def isolated_connect(url: str) -> AsyncGenerator[MagicMock, None]:
        connects.append(url)
        yield connection

    monkeypatch.setattr(installer, "canonical_schema_sql", rejected_sql)
    # Keep a future direct import of the same existing loader under this fault.
    if getattr(operator, "canonical_schema_sql", None) is original_loader:
        monkeypatch.setattr(operator, "canonical_schema_sql", rejected_sql)
    connect = MagicMock(side_effect=isolated_connect)
    monkeypatch.setattr(operator, "connect_pg", connect)
    monkeypatch.setattr(installer, "ensure_schema", ensure)
    with pytest.raises(type(error)) as caught:
        await operator.apply_database_schema(
            {
                "KOKORO_AGENT_DATABASE_URL": "postgresql://fixture.invalid/unused",
                "KOKORO_AGENT_DATABASE_SCHEMA": "agent_asset_test",
            }
        )
    if failure == "missing":
        assert isinstance(caught.value, FileNotFoundError)
        assert caught.value.filename == str(missing_path)
    else:
        assert caught.value is error
    expected_connects = (
        [] if stage == "initial" else ["postgresql://fixture.invalid/unused"]
    )
    assert reads == [failure] * (1 if stage == "initial" else 2)
    assert (
        connect.call_count,
        connects,
        ensure.await_count,
        cursor.execute.await_count,
    ) == (len(expected_connects), expected_connects, 0, 0)


async def test_schema_operator_valid_asset_precedes_connect_and_keeps_installer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import asynccontextmanager
    from collections.abc import AsyncGenerator
    from unittest.mock import AsyncMock, MagicMock

    from kokoro_agent.application import schema as operator
    from kokoro_agent.infrastructure import schema as installer

    events: list[str] = []
    sql = "SELECT 'validated canonical DDL';"
    connection = MagicMock()
    cursor = connection.cursor.return_value.__aenter__.return_value
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])

    def validated_sql() -> str:
        events.append("asset")
        return sql

    @asynccontextmanager
    async def isolated_connect(url: str) -> AsyncGenerator[MagicMock, None]:
        assert url == "postgresql://fixture.invalid/unused"
        events.append("connect")
        yield connection
        events.append("disconnect")

    async def isolated_ensure(conn: object, namespace: str) -> None:
        assert conn is connection
        assert namespace == "agent_asset_test"
        events.append("ensure")

    original_loader = installer.canonical_schema_sql
    monkeypatch.setattr(installer, "canonical_schema_sql", validated_sql)
    if getattr(operator, "canonical_schema_sql", None) is original_loader:
        monkeypatch.setattr(operator, "canonical_schema_sql", validated_sql)
    monkeypatch.setattr(operator, "connect_pg", isolated_connect)
    monkeypatch.setattr(installer, "ensure_schema", isolated_ensure)
    await operator.apply_database_schema(
        {
            "KOKORO_AGENT_DATABASE_URL": "postgresql://fixture.invalid/unused",
            "KOKORO_AGENT_DATABASE_SCHEMA": "agent_asset_test",
        }
    )
    assert events == ["asset", "connect", "asset", "ensure", "disconnect"]
    assert cursor.execute.await_count == 3
    assert cursor.execute.await_args_list[-1].args == (sql,)
    assert cursor.execute.await_args_list[0].args[1] == ("agent_asset_test",)
    connection.transaction.assert_called_once_with()
