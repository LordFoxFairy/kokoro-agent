"""Real native saver display reads; explicit disposable DB, no Redis or model."""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import os
import re
from typing import Any
from urllib.parse import urlsplit

import pytest
from langgraph.checkpoint.base import empty_checkpoint

from kokoro_agent.domain.chat.projection import project_chat_fact
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.infrastructure.checkpoint_display import read_run_display
from kokoro_agent.infrastructure.checkpoints import (
    CheckpointSettings,
    make_checkpointer,
)
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_chat_repository import PostgresChatRepository
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all
from kokoro_agent.protocol import SkillProgressPayload
from support.fakes import request


@pytest.fixture(autouse=True)
def display_fixture_guard() -> None:
    actual = os.environ.get("KOKORO_AGENT_DATABASE_URL", "")
    valid = (
        bool(actual)
        and re.fullmatch(
            r"kokoro_agent_run_display_[0-9a-f]{32}",
            urlsplit(actual).path.removeprefix("/"),
        )
        is not None
    )
    if not valid:
        pytest.fail(
            "explicit disposable Agent display fixture is required", pytrace=False
        )


async def _snapshot(url: str, schema: str) -> str:
    rows: list[object] = []
    async with connect_pg(url) as conn, conn.cursor() as cur:
        for name in (
            "kokoro_agent_chat_event",
            "checkpoints",
            "checkpoint_blobs",
            "checkpoint_writes",
            "kokoro_agent_tool_journal",
        ):
            await execute_sql(
                cur,
                f"SELECT row_to_json(t)::text AS value FROM {qualified(schema, name)} t ORDER BY row_to_json(t)::text",
            )
            rows.extend(await fetch_all(cur))
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


@pytest.mark.asyncio
async def test_native_legacy_display_binds_loader_version_not_later_run_and_never_writes(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
) -> None:
    run = request("display-old").model_copy(
        update={"selected_skill_source_refs": ("skill:alpha",)}
    )
    scope = RunScope.of(run)
    projection = project_chat_fact(
        tenant_id=run.execution_identity.tenant_ref,
        namespace=scope.namespace,
        session_id=run.session_id,
        run_id=run.run_id,
        source_index=0,
        created_at=datetime.now(UTC),
        payload=SkillProgressPayload(
            phase="ready", anchor_source_index=0, source_refs=["skill:alpha"]
        ),
    )
    assert projection is not None
    await chat_repository.append(projection)
    config: Any = {
        "configurable": {"thread_id": scope.scoped_thread_id, "checkpoint_ns": ""}
    }
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        for value, owner, version in [
            ("previous", "previous-run", "1"),
            ("actual-native", run.run_id, "2"),
            ("later-native", "next-run", "3"),
        ]:
            checkpoint = empty_checkpoint()
            checkpoint["channel_versions"] = {"skills_metadata": version}
            checkpoint["channel_values"] = {
                "skills_metadata": [
                    {"name": value, "path": "/.skills/YWxwaGE/SKILL.md"}
                ]
            }
            metadata: Any = {
                "source": "loop",
                "step": 1,
                "parents": {},
                "kokoro_run_id": owner,
            }
            config = await saver.aput(
                config, checkpoint, metadata, {"skills_metadata": version}
            )
    before = await _snapshot(run_chat_database_url, run_chat_schema)
    result = await read_run_display(run_chat_database_url, run_chat_schema, run)
    assert result.model_dump(mode="json")["display_skills"] == [
        {
            "source_ref": "skill:alpha",
            "name": "actual-native",
            "virtual_path": "/.skills/YWxwaGE/SKILL.md",
        }
    ]
    assert result.tool_paths == []
    foreign = run.model_copy(update={"session_id": "foreign-session"})
    assert (
        await read_run_display(run_chat_database_url, run_chat_schema, foreign)
    ).display_skills == []
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [32, 33])
async def test_native_locator_budget_is_enforced_without_truncating_display(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
    count: int,
) -> None:
    run = request("display-budget").model_copy(
        update={"selected_skill_source_refs": ("skill:alpha",)}
    )
    scope = RunScope.of(run)
    projection = project_chat_fact(
        tenant_id=run.execution_identity.tenant_ref,
        namespace=scope.namespace,
        session_id=run.session_id,
        run_id=run.run_id,
        source_index=0,
        created_at=datetime.now(UTC),
        payload=SkillProgressPayload(
            phase="ready", anchor_source_index=0, source_refs=["skill:alpha"]
        ),
    )
    assert projection is not None
    await chat_repository.append(projection)
    config: Any = {
        "configurable": {"thread_id": scope.scoped_thread_id, "checkpoint_ns": ""}
    }
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        for index in range(count):
            checkpoint = empty_checkpoint()
            checkpoint["channel_versions"] = {"skills_metadata": str(index + 1)}
            checkpoint["channel_values"] = {
                "skills_metadata": [
                    {"name": "actual", "path": "/.skills/YWxwaGE/SKILL.md"}
                ]
            }
            metadata: Any = {
                "source": "loop",
                "step": index,
                "parents": {},
                "kokoro_run_id": run.run_id,
            }
            config = await saver.aput(
                config, checkpoint, metadata, {"skills_metadata": str(index + 1)}
            )
    before = await _snapshot(run_chat_database_url, run_chat_schema)
    if count == 33:
        with pytest.raises(ValueError, match="locator budget"):
            await read_run_display(run_chat_database_url, run_chat_schema, run)
    else:
        assert (
            await read_run_display(run_chat_database_url, run_chat_schema, run)
        ).display_skills[0].name == "actual"
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before


@pytest.mark.asyncio
async def test_native_all_channel_bytes_are_admitted_before_deserialization(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
) -> None:
    run = request("display-bytes").model_copy(
        update={"selected_skill_source_refs": ("skill:alpha",)}
    )
    scope = RunScope.of(run)
    projection = project_chat_fact(
        tenant_id=run.execution_identity.tenant_ref,
        namespace=scope.namespace,
        session_id=run.session_id,
        run_id=run.run_id,
        source_index=0,
        created_at=datetime.now(UTC),
        payload=SkillProgressPayload(
            phase="ready", anchor_source_index=0, source_refs=["skill:alpha"]
        ),
    )
    assert projection is not None
    await chat_repository.append(projection)
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        checkpoint = empty_checkpoint()
        checkpoint["channel_versions"] = {
            "skills_metadata": "1",
            "unrelated_payload": "1",
        }
        checkpoint["channel_values"] = {
            "skills_metadata": [
                {"name": "actual", "path": "/.skills/YWxwaGE/SKILL.md"}
            ],
            "unrelated_payload": "x" * (8 * 1024 * 1024),
        }
        metadata: Any = {
            "source": "loop",
            "step": 1,
            "parents": {},
            "kokoro_run_id": run.run_id,
        }
        await saver.aput(
            {
                "configurable": {
                    "thread_id": scope.scoped_thread_id,
                    "checkpoint_ns": "",
                }
            },
            checkpoint,
            metadata,
            checkpoint["channel_versions"],
        )
    before = await _snapshot(run_chat_database_url, run_chat_schema)
    with pytest.raises(ValueError, match="read budget"):
        await read_run_display(run_chat_database_url, run_chat_schema, run)
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before


@pytest.mark.asyncio
async def test_irrelevant_model_tool_checkpoints_do_not_consume_skill_locator_budget(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
) -> None:
    run = request("display-many-tools").model_copy(
        update={"selected_skill_source_refs": ("skill:alpha",)}
    )
    scope = RunScope.of(run)
    projection = project_chat_fact(
        tenant_id=run.execution_identity.tenant_ref,
        namespace=scope.namespace,
        session_id=run.session_id,
        run_id=run.run_id,
        source_index=0,
        created_at=datetime.now(UTC),
        payload=SkillProgressPayload(
            phase="ready", anchor_source_index=0, source_refs=["skill:alpha"]
        ),
    )
    assert projection is not None
    await chat_repository.append(projection)
    config: Any = {
        "configurable": {"thread_id": scope.scoped_thread_id, "checkpoint_ns": ""}
    }
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        for index in range(35):
            checkpoint = empty_checkpoint()
            checkpoint["channel_versions"] = {"skills_metadata": "1"}
            checkpoint["channel_values"] = {
                "skills_metadata": [
                    {"name": "native-actual", "path": "/.skills/YWxwaGE/SKILL.md"}
                ]
            }
            metadata: Any = {
                "source": "loop",
                "step": index,
                "parents": {},
                "kokoro_run_id": run.run_id,
            }
            config = await saver.aput(
                config,
                checkpoint,
                metadata,
                {"skills_metadata": "1"} if index == 0 else {},
            )
    before = await _snapshot(run_chat_database_url, run_chat_schema)
    result = await read_run_display(run_chat_database_url, run_chat_schema, run)
    assert result.display_skills[0].name == "native-actual"
    assert result.tool_paths == []
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before
