"""Operator CLI commands keep schema installation explicit and testable."""

from __future__ import annotations

from collections.abc import Mapping

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
