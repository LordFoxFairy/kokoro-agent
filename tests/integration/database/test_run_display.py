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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "success",
        "foreign_connection",
        "failed_without_native",
        "embedded_token",
        "33_unrelated",
        "33_relevant",
        "32_relevant",
        "large_native",
        "legacy",
        "legacy_other_hash",
        "legacy_wrong_message",
    ],
)
async def test_historical_mcp_only_exact_journal_native_and_frozen_recipe_are_read_without_writes(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
    case: str,
) -> None:
    from langchain_core.messages import AIMessage, ToolMessage, BaseMessage
    from kokoro_agent.execution.runtime_profile import canonical_json
    from kokoro_agent.protocol import ToolInvokedPayload, ToolReturnedPayload

    run = request("historical-mcp")
    scope = RunScope.of(run)
    for index, (call_id, name) in enumerate(
        [
            ("call-success", "mcp_call"),
            ("describe", "mcp_describe_tool"),
            ("call-failed", "mcp_call"),
        ]
    ):
        for offset, payload in enumerate(
            [
                ToolInvokedPayload(
                    segment_id=call_id, tool_id=call_id, name=name, args={}
                ),
                ToolReturnedPayload(
                    segment_id=call_id,
                    tool_id=call_id,
                    name=name,
                    result="",
                    is_error=False,
                ),
            ]
        ):
            projection = project_chat_fact(
                tenant_id=run.execution_identity.tenant_ref,
                namespace=scope.namespace,
                session_id=run.session_id,
                run_id=run.run_id,
                source_index=index * 2 + offset,
                created_at=datetime.now(UTC),
                payload=payload,
                tool_display_code="tool.mcp.describe"
                if name == "mcp_describe_tool"
                else "tool.mcp.call",
            )
            assert projection is not None
            await chat_repository.append(projection)
    source_hash = (
        "a" * 64
        if case == "legacy_other_hash"
        else "f4f1b0c9ff95221cbf619cdf6376e79d8858c07f32b7c33c72f989458417ddea"
    )
    recipe = canonical_json(
        {
            "domain_tag": "kokoro-agent:assembly-recipe:1",
            "feature": {"agents": [{"mcp": [{"connection_id": "connection"}]}]},
            "implementations": {
                "sources": [
                    {
                        "package": "kokoro_agent",
                        "files": [{"path": "mcp/tools.py", "sha256": source_hash}],
                    }
                ]
            },
        }
    )
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_run')} (run_id,tenant_id,request_json,assembly_recipe_bytes,assembly_recipe_fingerprint) VALUES (%s,%s,%s,%s,%s)",
            (
                run.run_id,
                run.execution_identity.tenant_ref,
                run.model_dump_json(),
                recipe,
                hashlib.sha256(recipe).hexdigest(),
            ),
        )
        for call_id, name, status, error, result in [
            (
                "call-success",
                "mcp_call",
                "succeeded",
                False,
                '{"content":[],"isError":false}',
            ),
            (
                "describe",
                "mcp_describe_tool",
                "succeeded",
                False,
                "error: MCP_TOOL_NOT_ALLOWED",
            ),
            ("call-failed", "mcp_call", "failed", True, "private upstream error"),
        ]:
            await execute_sql(
                cur,
                f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_tool_journal')} (run_id,tool_call_id,name,status,is_error,result) VALUES (%s,%s,%s,%s,%s,%s)",
                (run.run_id, call_id, name, status, error, result),
            )
    config: Any = {
        "configurable": {"thread_id": scope.scoped_thread_id, "checkpoint_ns": ""}
    }
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        for index in range(
            34
            if case == "33_unrelated"
            else 33
            if case == "33_relevant"
            else 32
            if case == "32_relevant"
            else 1
        ):
            checkpoint = empty_checkpoint()
            native_metadata: Any = {
                "source": "loop",
                "step": index,
                "parents": {},
                "kokoro_run_id": run.run_id,
            }
            config = await saver.aput(config, checkpoint, native_metadata, {})
            if case == "failed_without_native":
                continue
            if case == "embedded_token":
                message = AIMessage(
                    id="native",
                    content="call-success describe",
                    tool_calls=[
                        {
                            "id": "not-a-real-call",
                            "name": "mcp_call",
                            "args": {"server": "connection", "tool": "fake"},
                        }
                    ],
                )
            elif case == "33_unrelated" and index < 33:
                message = AIMessage(id=f"n-{index}", content="unrelated", tool_calls=[])
            else:
                message = AIMessage(
                    id=f"native-{index}",
                    content="",
                    tool_calls=[
                        {
                            "id": "call-success",
                            "name": "mcp_call",
                            "args": {
                                "server": "other"
                                if case == "foreign_connection"
                                else "connection",
                                "tool": "read_wiki_structure",
                            },
                        },
                        {
                            "id": "describe",
                            "name": "mcp_describe_tool",
                            "args": {
                                "server": "connection",
                                "tool": "read_wiki_structure",
                            },
                        },
                    ],
                )
            if case == "large_native":
                message = message.model_copy(
                    update={"content": "x" * (8 * 1024 * 1024 + 1)}
                )
            native_messages: list[BaseMessage] = [message]
            if case.startswith("legacy"):
                native_messages.append(
                    ToolMessage(
                        content="error: MCP_TOOL_NOT_ALLOWED",
                        name="mcp_describe_tool",
                        tool_call_id="other"
                        if case == "legacy_wrong_message"
                        else "describe",
                    )
                )
            await saver.aput_writes(
                config, [("messages", native_messages)], task_id=f"task-{index}"
            )
    before = await _snapshot(run_chat_database_url, run_chat_schema)
    if case in {"33_relevant", "large_native"}:
        with pytest.raises(ValueError, match="budget"):
            await read_run_display(run_chat_database_url, run_chat_schema, run)
    else:
        result = await read_run_display(run_chat_database_url, run_chat_schema, run)
        facts = result.model_dump(mode="json")["tool_facts"]
        assert sum(fact.get("status") == "failed" for fact in facts) == (
            2 if case == "legacy" else 1
        )
        names = [
            fact["display_detail"]["name"] for fact in facts if "display_detail" in fact
        ]
        assert names == (
            ["read_wiki_structure", "read_wiki_structure"]
            if case
            in {
                "success",
                "33_unrelated",
                "32_relevant",
                "legacy",
                "legacy_other_hash",
                "legacy_wrong_message",
            }
            else []
        )
        assert result.display_skills == result.tool_paths == []
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before


@pytest.mark.asyncio
async def test_late_native_read_file_checkpoint_recovers_exact_skill_path_not_metadata_guess(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
) -> None:
    from langchain_core.messages import AIMessage
    from kokoro_agent.domain.chat.projection import ToolDisplayDetail
    from kokoro_agent.protocol import ToolInvokedPayload

    run = request("late-read-file").model_copy(
        update={"selected_skill_source_refs": ("skill:alpha",)}
    )
    scope = RunScope.of(run)
    for index, payload in enumerate(
        [
            SkillProgressPayload(
                phase="ready", anchor_source_index=0, source_refs=["skill:alpha"]
            ),
            ToolInvokedPayload(
                segment_id="read", tool_id="read", name="read_file", args={}
            ),
        ]
    ):
        projection = project_chat_fact(
            tenant_id=run.execution_identity.tenant_ref,
            namespace=scope.namespace,
            session_id=run.session_id,
            run_id=run.run_id,
            source_index=index,
            created_at=datetime.now(UTC),
            payload=payload,
            tool_display_code="tool.file.read",
            tool_display_detail=ToolDisplayDetail(kind="file", name="SKILL.md")
            if index
            else None,
        )
        assert projection is not None
        await chat_repository.append(projection)
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_tool_journal')} (run_id,tool_call_id,name,status,is_error,result) VALUES (%s,'read','read_file','succeeded',false,'public file')",
            (run.run_id,),
        )
    config: Any = {
        "configurable": {"thread_id": scope.scoped_thread_id, "checkpoint_ns": ""}
    }
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        for index in range(2):
            checkpoint = empty_checkpoint()
            checkpoint["channel_versions"] = {"skills_metadata": "1"}
            checkpoint["channel_values"] = {
                "skills_metadata": [
                    {"name": "actual-native", "path": "/.skills/YWxwaGE/SKILL.md"}
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
            if index:
                msg = AIMessage(
                    id="native",
                    content="",
                    tool_calls=[
                        {
                            "id": "read",
                            "name": "read_file",
                            "args": {"file_path": "/.skills/YWxwaGE/SKILL.md"},
                        }
                    ],
                )
                await saver.aput_writes(
                    config, [("messages", [msg])], task_id="later-tool"
                )
    before = await _snapshot(run_chat_database_url, run_chat_schema)
    result = await read_run_display(run_chat_database_url, run_chat_schema, run)
    assert [item.virtual_path for item in result.tool_paths] == [
        "/.skills/YWxwaGE/SKILL.md"
    ]
    assert [item.name for item in result.display_skills] == ["actual-native"]
    assert result.tool_facts == []
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["running", "terminal", "discover", "foreign_run", "wrong_name", "secret_output"],
)
async def test_private_details_read_exact_native_call_and_journal_without_writes(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
    case: str,
) -> None:
    from langchain_core.messages import AIMessage
    from kokoro_agent.execution.runtime_profile import canonical_json
    from kokoro_agent.protocol import ToolInvokedPayload

    run = request("private-display-" + case)
    scope = RunScope.of(run)
    name = "mcp_list_tools" if case == "discover" else "mcp_call"
    args: dict[str, Any] = (
        {}
        if case == "discover"
        else {
            "server": "connection",
            "tool": "query",
            "arguments": {
                "repository": "owner/repo",
                "Authorization": "Bearer OMIT_ME",
            },
        }
    )
    projection = project_chat_fact(
        tenant_id=run.execution_identity.tenant_ref,
        namespace=scope.namespace,
        session_id=run.session_id,
        run_id=run.run_id,
        source_index=0,
        created_at=datetime.now(UTC),
        payload=ToolInvokedPayload(
            segment_id="call", tool_id="call", name=name, args={}
        ),
        tool_display_code="tool.mcp.discover"
        if case == "discover"
        else "tool.mcp.call",
    )
    assert projection is not None
    await chat_repository.append(projection)
    recipe = canonical_json(
        {
            "domain_tag": "kokoro-agent:assembly-recipe:1",
            "feature": {"agents": [{"mcp": [{"connection_id": "connection"}]}]},
        }
    )
    result = (
        "connection/query — Available tools"
        if case == "discover"
        else json.dumps(
            {
                "content": [
                    {
                        "type": "text",
                        "text": "Bearer OMIT_ME"
                        if case == "secret_output"
                        else "Actual result",
                    }
                ],
                "isError": False,
            }
        )
    )
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_run')} (run_id,tenant_id,request_json,assembly_recipe_bytes,assembly_recipe_fingerprint) VALUES (%s,%s,%s,%s,%s)",
            (
                run.run_id,
                run.execution_identity.tenant_ref,
                run.model_dump_json(),
                recipe,
                hashlib.sha256(recipe).hexdigest(),
            ),
        )
        await execute_sql(
            cur,
            f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_tool_journal')} (run_id,tool_call_id,name,status,is_error,result) VALUES (%s,%s,%s,%s,%s,%s)",
            (
                run.run_id,
                "call",
                "other_tool" if case == "wrong_name" else name,
                "started" if case == "running" else "succeeded",
                False,
                result,
            ),
        )
    config: Any = {
        "configurable": {"thread_id": scope.scoped_thread_id, "checkpoint_ns": ""}
    }
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        metadata: Any = {
            "source": "loop",
            "step": 0,
            "parents": {},
            "kokoro_run_id": "foreign" if case == "foreign_run" else run.run_id,
        }
        config = await saver.aput(config, empty_checkpoint(), metadata, {})
        await saver.aput_writes(
            config,
            [
                (
                    "messages",
                    [
                        AIMessage(
                            id="native",
                            content="",
                            tool_calls=[{"id": "call", "name": name, "args": args}],
                        )
                    ],
                )
            ],
            task_id="tool-call",
        )
    before = await _snapshot(run_chat_database_url, run_chat_schema)
    response = await read_run_display(run_chat_database_url, run_chat_schema, run)
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before
    details = response.model_dump(mode="json")["activity_details"]
    assert "OMIT_ME" not in json.dumps(details)
    if case in {"foreign_run", "wrong_name"}:
        assert details == []
    else:
        assert len(details) == 1 and details[0]["tool_name"] == name
        assert "input_preview" in details[0]
        if case in {"running", "secret_output"}:
            assert "output_preview" not in details[0]
        else:
            assert (
                "Available tools" if case == "discover" else "Actual result"
            ) in details[0]["output_preview"]["text"]
