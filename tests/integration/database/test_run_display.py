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
from kokoro_agent.domain.run.repository import RunRepository
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
        "error_text_is_not_failure",
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
    activity_ids: dict[str, str] = {}
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
            activity_id = json.loads(projection.event.payload_json)["activity_id"]
            assert isinstance(activity_id, str)
            activity_ids[call_id] = activity_id
            await chat_repository.append(projection)
    # The formerly recognized producer must not turn error text into failure facts.
    source_hash = "f4f1b0c9ff95221cbf619cdf6376e79d8858c07f32b7c33c72f989458417ddea"
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
            if case == "error_text_is_not_failure":
                native_messages.append(
                    ToolMessage(
                        content="error: MCP_TOOL_NOT_ALLOWED",
                        name="mcp_describe_tool",
                        tool_call_id="describe",
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
        assert sum(fact.get("status") == "failed" for fact in facts) == 1
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
                "error_text_is_not_failure",
            }
            else []
        )
        if case == "error_text_is_not_failure":
            assert facts == [
                {
                    "activity_id": activity_ids["call-success"],
                    "display_detail": {
                        "kind": "mcp_tool",
                        "name": "read_wiki_structure",
                    },
                },
                {
                    "activity_id": activity_ids["describe"],
                    "display_detail": {
                        "kind": "mcp_tool",
                        "name": "read_wiki_structure",
                    },
                },
                {"activity_id": activity_ids["call-failed"], "status": "failed"},
            ]
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
    [
        "running",
        "terminal",
        "discover",
        "description",
        "foreign_run",
        "wrong_name",
        "secret_output",
        "execute",
        "execute_foreign",
        "execute_wrong_result",
    ],
)
async def test_private_details_read_exact_native_call_and_journal_without_writes(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
    case: str,
) -> None:
    from langchain_core.messages import AIMessage, ToolMessage
    from kokoro_agent.execution.runtime_profile import canonical_json
    from kokoro_agent.protocol import ToolInvokedPayload, ToolReturnedPayload

    run = request("private-display-" + case)
    scope = RunScope.of(run)
    isolated = case.startswith("execute")
    name = (
        "execute"
        if isolated
        else "mcp_list_tools"
        if case == "discover"
        else "mcp_describe_tool"
        if case == "description"
        else "mcp_call"
    )
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
    if isolated:
        args = {"command": "printf 'Actual result\\n'"}
    if case == "description":
        args = {"server": "connection", "tool": "query"}
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
        else "tool.mcp.describe"
        if case == "description"
        else "tool.execution"
        if isolated
        else "tool.mcp.call",
    )
    assert projection is not None
    await chat_repository.append(projection)
    if isolated:
        returned = project_chat_fact(
            tenant_id=run.execution_identity.tenant_ref,
            namespace=scope.namespace,
            session_id=run.session_id,
            run_id=run.run_id,
            source_index=1,
            created_at=datetime.now(UTC),
            payload=ToolReturnedPayload(
                segment_id="call", tool_id="call", name=name, result="", is_error=False
            ),
            tool_display_code="tool.execution",
        )
        assert returned is not None
        await chat_repository.append(returned)
    peer: dict[str, Any] = {"mcp": [{"connection_id": "connection"}]}
    if isolated:
        peer = {
            "mcp": [],
            "backend": {"kind": "docker"},
            "permissions": {"approval_tools": [], "review_tools": []},
            "pause_tools": [],
        }
    recipe = canonical_json(
        {
            "domain_tag": "kokoro-agent:assembly-recipe:1",
            "feature": {"agents": [peer]},
        }
    )
    result = (
        "connection/query — Available tools"
        if case == "discover"
        else 'connection/query\nActual result documentation\n参数 schema：{"type":"object"}'
        if case == "description"
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
    if isolated:
        result = "Actual result"
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
            "kokoro_run_id": "foreign"
            if case in {"foreign_run", "execute_foreign"}
            else run.run_id,
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
                        ),
                        *(
                            [
                                ToolMessage(
                                    name="execute",
                                    tool_call_id="call",
                                    content="Mismatch"
                                    if case == "execute_wrong_result"
                                    else result,
                                )
                            ]
                            if isolated
                            else []
                        ),
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
    if case in {"foreign_run", "wrong_name", "execute_foreign", "execute_wrong_result"}:
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

        if case == "description":
            assert details[0]["tool_description"] == {
                "text": "Actual result documentation",
                "truncated": False,
                "redacted": False,
            }


@pytest.mark.asyncio
async def test_native_file_loop_details_without_journal_survive_reopen_and_stay_read_only(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
) -> None:
    """Actual pinned SDK file tools + native PG saver; the model is an offline script."""
    import deepagents
    from langchain_core.messages import AIMessage, HumanMessage
    from kokoro_agent.execution.runtime_profile import canonical_json
    from kokoro_agent.protocol import ToolInvokedPayload, ToolReturnedPayload
    from support.local_fake import LocalFakeChatModel
    from kokoro_agent.tools.toolset import Toolset

    run = request("native-file-details")
    scope = RunScope.of(run)
    operations = [
        ("write_file", {"file_path": "/notes.md", "content": "Before"}),
        ("ls", {"path": "/"}),
        (
            "edit_file",
            {"file_path": "/notes.md", "old_string": "Before", "new_string": "After"},
        ),
        ("read_file", {"file_path": "/notes.md"}),
        ("glob", {"pattern": "*.md", "path": "/"}),
        ("grep", {"pattern": "After", "path": "/", "output_mode": "content"}),
    ]
    script = [
        AIMessage(
            id=f"model-{index}",
            content="",
            tool_calls=[
                {
                    "id": f"call-{index}",
                    "name": name,
                    "args": args,
                }
            ],
        )
        for index, (name, args) in enumerate(operations)
    ]
    script.append(AIMessage(id="final", content="Done"))
    recipe_value: dict[str, Any] = {
        "domain_tag": "kokoro-agent:assembly-recipe:1",
        "feature": {
            "agents": [
                {
                    "mcp": [],
                    "backend": {"kind": "state"},
                    "permissions": {"approval_tools": [], "review_tools": []},
                    "pause_tools": [],
                }
            ]
        },
    }
    recipe = canonical_json(recipe_value)
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
    # Unrelated journaled tools do not participate in native file previews.
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_tool_journal')} (run_id,tool_call_id,name,status,is_error,result) SELECT %s, 'unrelated-' || n, 'other_tool', 'succeeded', false, 'Other result' FROM generate_series(1,129) n",
            (run.run_id,),
        )
    config: Any = {
        "configurable": {"thread_id": scope.scoped_thread_id, "checkpoint_ns": ""},
        "metadata": {"kokoro_run_id": run.run_id},
    }
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        create: Any = getattr(deepagents, "create_deep_agent")
        graph = create(model=LocalFakeChatModel.with_script(script), checkpointer=saver)
        codes = dict(Toolset.from_tools([]).mounted_display_codes(graph))
        await graph.ainvoke(
            {"messages": [HumanMessage(content="Inspect fixture files")]}, config
        )
    for index, (name, _) in enumerate(operations):
        code = codes.get(name, "tool.execution")
        for offset, payload in enumerate(
            [
                ToolInvokedPayload(
                    segment_id=f"call-{index}",
                    tool_id=f"call-{index}",
                    name=name,
                    args={},
                ),
                ToolReturnedPayload(
                    segment_id=f"call-{index}",
                    tool_id=f"call-{index}",
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
                tool_display_code=code,
            )
            assert projection is not None
            await chat_repository.append(projection)
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        for index in range(40):
            unrelated: Any = {
                "source": "loop",
                "step": 100 + index,
                "parents": {},
                "kokoro_run_id": run.run_id,
            }
            config = await saver.aput(config, empty_checkpoint(), unrelated, {})
            await saver.aput_writes(
                config,
                [
                    (
                        "messages",
                        [
                            AIMessage(
                                id=f"unrelated-{index}",
                                content="Details about unrelated tools and ls in prose",
                            )
                        ],
                    )
                ],
                task_id="other-model",
            )
    before = await _snapshot(run_chat_database_url, run_chat_schema)
    first = await read_run_display(run_chat_database_url, run_chat_schema, run)
    assert [item.tool_name for item in first.activity_details] == [
        name for name, _ in operations
    ]
    for item, (_, args) in zip(first.activity_details, operations, strict=True):
        assert (
            item.input_preview is not None
            and json.loads(item.input_preview.text) == args
        )
        assert item.output_preview is not None
    assert first.activity_details[3].output_preview is not None
    assert "After" in first.activity_details[3].output_preview.text
    assert first.activity_details[1].output_preview is not None
    assert "/notes.md" in first.activity_details[1].output_preview.text
    assert await read_run_display(run_chat_database_url, run_chat_schema, run) == first
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before
    # Same thread has no right to use this Run's calls under another Run identity.
    foreign = run.model_copy(update={"run_id": "foreign-run"})
    assert (
        await read_run_display(run_chat_database_url, run_chat_schema, foreign)
    ).activity_details == []


@pytest.mark.asyncio
async def test_native_state_file_postimages_bind_committed_run_versions(
    run_chat_schema: str,
    run_chat_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Native UTF-8 postimages come from the exact committed step, not its ACK."""
    from typing import cast

    import deepagents
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langchain_core.runnables import RunnableConfig
    from langgraph.checkpoint.base import (
        ChannelVersions,
        Checkpoint,
        CheckpointMetadata,
        CheckpointTuple,
    )

    from kokoro_agent.domain.chat.projection import safe_delivery_display_path
    from support.local_fake import LocalFakeChatModel

    # Candidate path for this probe only; no product output namespace is installed.
    path = "/outputs/probe.txt"
    assert safe_delivery_display_path(path)
    first = "版本一\r\nunchanged tail  \r\n"
    second = "版本二🦉\r\nunchanged tail  \r\n"
    first_run = request("native-file-write")
    second_run = request("native-file-edit")
    scope = RunScope.of(first_run)
    assert RunScope.of(second_run) == scope
    settings = CheckpointSettings(
        database_url=run_chat_database_url, schema_name=run_chat_schema
    )
    steps = (
        (first_run, "write_file", {"file_path": path, "content": first}, first),
        (
            second_run,
            "edit_file",
            {"file_path": path, "old_string": "版本一", "new_string": "版本二🦉"},
            second,
        ),
    )
    locators: dict[str, RunnableConfig] = {}
    tools = {run.run_id: tool for run, tool, _, _ in steps}

    def postimage(parent: CheckpointTuple, tool: str, call_id: str) -> bytes:
        # The original SDK tool's successful result and the files write share
        # a native task, but the result message does not carry the file bytes.
        tasks: set[str] = set()
        writes: list[tuple[str, object]] = []
        for task, channel, value in parent.pending_writes or ():
            if channel == "messages":
                messages: list[object] = (
                    cast(list[object], value) if isinstance(value, list) else [value]
                )
                if any(
                    isinstance(message, ToolMessage)
                    and (message.name, message.tool_call_id, message.status)
                    == (tool, call_id, "success")
                    for message in messages
                ):
                    tasks.add(task)
            if channel == "files" and isinstance(value, dict):
                delta = cast(dict[str, object], value)
                if path in delta:
                    writes.append((task, delta[path]))
        assert len(writes) == 1
        task, file_data = writes[0]
        assert tasks == {task}
        assert isinstance(file_data, dict)
        fields = cast(dict[str, object], file_data)
        assert fields.get("encoding") == "utf-8"
        content = fields.get("content")
        assert isinstance(content, str)
        return content.encode("utf-8")

    async with (
        make_checkpointer(settings) as saver,
        make_checkpointer(settings) as reader,
    ):
        put = saver.aput

        async def observe_put(
            config: RunnableConfig,
            checkpoint: Checkpoint,
            metadata: CheckpointMetadata,
            new_versions: ChannelVersions,
        ) -> RunnableConfig:
            if "files" not in (checkpoint.get("updated_channels") or ()):
                return await put(config, checkpoint, metadata, new_versions)
            config_values = config.get("configurable", {})
            locator_keys = ("thread_id", "checkpoint_ns", "checkpoint_id")
            parent_locator: RunnableConfig = {
                "configurable": {key: config_values[key] for key in locator_keys}
            }
            parent = await reader.aget_tuple(parent_locator)
            assert parent is not None and parent.config == parent_locator
            child: RunnableConfig = {
                "configurable": {
                    **parent_locator.get("configurable", {}),
                    "checkpoint_id": checkpoint["id"],
                }
            }
            run_id = parent.metadata.get("kokoro_run_id")
            assert isinstance(run_id, str) and run_id not in locators
            assert config_values["thread_id"] == scope.scoped_thread_id
            assert config_values["checkpoint_ns"] == ""
            assert parent.metadata.get("kokoro_generation") == 1
            # A second connection sees real durable pending writes, but no
            # committed version exists until the unmodified saver puts the child.
            assert postimage(parent, tools[run_id], run_id)
            assert await reader.aget_tuple(child) is None
            saved = await put(config, checkpoint, metadata, new_versions)
            assert saved == child
            locators[run_id] = saved
            return saved

        monkeypatch.setattr(saver, "aput", observe_put)
        create: Any = getattr(deepagents, "create_deep_agent")
        # The deterministic fixture indexes by all AI messages in this thread,
        # including the first Run's two messages when the second graph opens.
        script = [
            message
            for run, tool, args, _ in steps
            for message in (
                AIMessage(
                    id=f"{run.run_id}-model",
                    content="",
                    tool_calls=[{"id": run.run_id, "name": tool, "args": args}],
                ),
                AIMessage(id=f"{run.run_id}-final", content="Done"),
            )
        ]
        for run, _, _, _ in steps:
            graph = create(
                model=LocalFakeChatModel.with_script(script),
                checkpointer=saver,
            )
            await graph.ainvoke(
                {
                    "messages": [
                        HumanMessage(id=run.input.message_id, content=run.input.content)
                    ]
                },
                {
                    "configurable": {
                        "thread_id": scope.scoped_thread_id,
                        "checkpoint_ns": "",
                    },
                    "metadata": {"kokoro_run_id": run.run_id, "kokoro_generation": 1},
                },
            )

    assert set(locators) == {first_run.run_id, second_run.run_id}
    assert locators[first_run.run_id] != locators[second_run.run_id]
    async with make_checkpointer(settings) as reopened:
        for run, tool, _, expected in steps:
            locator = locators[run.run_id]
            child = await reopened.aget_tuple(locator)
            assert child is not None and child.config == locator
            assert child.parent_config is not None
            parent = await reopened.aget_tuple(child.parent_config)
            assert parent is not None and parent.config == child.parent_config
            for committed in (parent, child):
                config_values = committed.config.get("configurable", {})
                assert config_values.get("thread_id") == scope.scoped_thread_id
                assert config_values.get("checkpoint_ns") == ""
                assert committed.metadata.get("kokoro_run_id") == run.run_id
                assert committed.metadata.get("kokoro_generation") == 1
            assert child.checkpoint["channel_versions"]["files"] != parent.checkpoint[
                "channel_versions"
            ].get("files")
            raw = postimage(parent, tool, run.run_id)
            expected_raw = expected.encode("utf-8")
            assert (len(raw), hashlib.sha256(raw).hexdigest()) == (
                len(expected_raw),
                hashlib.sha256(expected_raw).hexdigest(),
            )


@pytest.mark.asyncio
async def test_state_output_versions_recover_committed_edit_without_borrowing_new_lease(
    run_chat_schema: str,
    run_chat_database_url: str,
    run_repository: RunRepository,
) -> None:
    """One native output, a missed committed edit, then its fenced recovery."""
    from collections.abc import Awaitable, Callable
    from typing import cast

    import deepagents
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langchain_core.runnables import RunnableConfig

    from kokoro_agent.agents.native_profile import bootstrap_native_profiles
    from kokoro_agent.clients.storage import StorageClientError
    from kokoro_agent.domain.run.models import LeaseFence
    from kokoro_agent.protocol import RunRequest
    from kokoro_agent.domain.run.workspace_output import (
        OUTPUT_METADATA,
        OUTPUT_PROOF,
        OUTPUT_ROOT,
        output_path_digest,
    )
    from kokoro_agent.infrastructure.checkpoint_interactions import (
        make_interaction_checkpointer,
    )
    from kokoro_agent.infrastructure.checkpoint_workspace_outputs import (
        CheckpointWorkspaceOutputs,
    )
    from kokoro_agent.infrastructure.schema import WORKSPACE_OUTPUT_VERSIONS_TABLE
    from kokoro_agent.tools.middleware import WorkspaceOutputProofMiddleware
    from support.local_fake import LocalFakeChatModel

    run = request("workspace-output-recovery")
    scope = RunScope.of(run)
    await run_repository.enqueue_dispatch(run, scope.namespace, f"output:{run.run_id}")
    lease = await run_repository.claim_dispatch(run, "workspace-output-first")
    assert lease is not None and lease.generation == 1
    settings = CheckpointSettings(
        database_url=run_chat_database_url, schema_name=run_chat_schema
    )
    path = "/outputs/report.txt"
    first, second = "版本一\r\nunchanged tail  \r\n", "版本二🦉\r\nunchanged tail  \r\n"
    script = [
        AIMessage(
            content="",
            tool_calls=[
                {
                    "id": "file-write",
                    "name": "write_file",
                    "args": {
                        "file_path": path,
                        "content": first,
                    },
                },
                {
                    "id": "private-write",
                    "name": "write_file",
                    "args": {
                        "file_path": "/outputs/.private.txt",
                        "content": "Not public",
                    },
                },
            ],
        ),
        AIMessage(content="First response"),
        AIMessage(
            content="",
            tool_calls=[
                {
                    "id": "file-edit",
                    "name": "edit_file",
                    "args": {
                        "file_path": path,
                        "old_string": "版本一",
                        "new_string": "版本二🦉",
                    },
                }
            ],
        ),
        AIMessage(content="Second response"),
    ]
    config: RunnableConfig = {
        "configurable": {"thread_id": scope.scoped_thread_id, "checkpoint_ns": ""},
        "metadata": {
            "kokoro_run_id": run.run_id,
            "kokoro_generation": lease.generation,
            OUTPUT_METADATA: OUTPUT_ROOT,
        },
    }

    async def versions() -> list[dict[str, object]]:
        table = qualified(run_chat_schema, WORKSPACE_OUTPUT_VERSIONS_TABLE)
        async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
            await execute_sql(
                cur,
                f"SELECT * FROM {table} WHERE run_id=%s ORDER BY checkpoint_id",
                (run.run_id,),
            )
            return await fetch_all(cur)

    def exact_version(row: dict[str, object], content: str, call_id: str) -> None:
        raw = content.encode("utf-8")
        assert (
            row["path"],
            row["source_generation"],
            row["tool_call_id"],
            row["size_bytes"],
            row["content_sha256"],
        ) == (
            path,
            lease.generation,
            call_id,
            len(raw),
            hashlib.sha256(raw).hexdigest(),
        )
        assert row["parent_checkpoint_id"] != row["checkpoint_id"]

    bootstrap_native_profiles()
    create: Any = getattr(deepagents, "create_deep_agent")
    async with (
        make_checkpointer(settings) as saver,
        make_checkpointer(settings) as reader,
    ):
        wrapped = make_interaction_checkpointer(
            saver=saver,
            reader=reader,
            repository=run_repository,
            settings=settings,
        )

        def graph(checkpointer: object) -> Any:
            return create(
                model=LocalFakeChatModel.with_script(script),
                checkpointer=checkpointer,
                middleware=[WorkspaceOutputProofMiddleware(scope.scoped_thread_id)],
            )

        result = await graph(wrapped).ainvoke(
            {"messages": [HumanMessage(id="workspace-first", content="Write report")]},
            config,
        )
        messages = cast(list[object], result["messages"])
        tool_results = {
            message.tool_call_id: message
            for message in messages
            if isinstance(message, ToolMessage)
        }
        metadata = cast(
            dict[str, object], getattr(tool_results["file-write"], "response_metadata")
        )
        assert metadata.get(OUTPUT_PROOF) == output_path_digest(path)
        assert path not in repr(metadata) and first not in repr(metadata)
        assert tool_results["file-write"].artifact is None
        assert OUTPUT_PROOF not in cast(
            dict[str, object],
            getattr(tool_results["private-write"], "response_metadata"),
        )
        (original,) = await versions()
        exact_version(original, first, "file-write")

        # The old process committed the real native edit, but its live
        # callback was lost. Reopen the actual checkpoint without that callback.
        await graph(saver).ainvoke(
            {"messages": [HumanMessage(id="workspace-second", content="Edit report")]},
            config,
        )
        changed = [
            item
            async for item in reader.alist(config)
            if "files" in (item.checkpoint.get("updated_channels") or ())
        ]
        assert len(changed) == 2
        edit = changed[0]
        assert edit.parent_config is not None
        assert await versions() == [original]
        assert await run_repository.pause(run.run_id, lease)
        current = await run_repository.adopt(run.run_id, "workspace-output-second")
        assert current is not None and current.generation == 2
        await CheckpointWorkspaceOutputs(settings, run_repository, reader).capture(
            edit.parent_config,
            edit.config,
        )
        await wrapped.recover_workspace_outputs(run, lease)
        assert await versions() == [original]

        await wrapped.recover_workspace_outputs(run, current)
        prior, recovered = await versions()
        assert prior == original
        exact_version(recovered, second, "file-edit")
        assert recovered["checkpoint_id"] == edit.checkpoint["id"]
        assert recovered["version_id"] != original["version_id"]
        await wrapped.recover_workspace_outputs(run, current)
        assert await versions() == [original, recovered]

        original_run = run

        class CleanStorage:
            next_lease: LeaseFence | None = None
            calls: list[tuple[str, bytes, tuple[str, str] | None]]

            def __init__(self) -> None:
                self.calls = []

            async def upload_workspace_output(
                self,
                run: RunRequest,
                lease: LeaseFence,
                version_id: str,
                path: str,
                content_sha256: str,
                content: bytes,
                selected: tuple[str, str] | None,
                freeze: Callable[[str, str], Awaitable[bool]],
            ) -> str:
                assert run == original_run and path == "/outputs/report.txt"
                assert hashlib.sha256(content).hexdigest() == content_sha256
                if not await run_repository.is_lease_current(run.run_id, lease):
                    raise StorageClientError("WORKSPACE_OUTPUT_LEASE_STALE")
                self.calls.append((version_id, content, selected))
                selection = (f"upload-{version_id}", "s3-exact-version")
                if selected is None:
                    assert await freeze(*selection)
                else:
                    assert selected == selection
                if self.next_lease is None:
                    # Simulate a legitimate CLEAN owner reply arriving after
                    # another worker owns this Run. Only the SQL CAS can stop it.
                    assert await run_repository.pause(run.run_id, lease)
                    self.next_lease = await run_repository.adopt(
                        run.run_id, "workspace-output-third"
                    )
                return f"asset:{version_id}"

        storage = CleanStorage()
        publication = CheckpointWorkspaceOutputs(
            settings, run_repository, reader, storage
        )
        await publication.recover(run, current)
        later = storage.next_lease
        assert later is not None and later.generation == 3
        unpublished = await read_run_display(
            run_chat_database_url, run_chat_schema, run
        )
        assert "workspace_outputs" not in unpublished.model_dump(mode="json")
        await publication.recover(run, later)
        published = await read_run_display(run_chat_database_url, run_chat_schema, run)
        assert [
            (item.version_id, item.virtual_path, item.content_sha256, item.mime_type)
            for item in published.workspace_outputs
        ] == [
            (row["version_id"], path, row["content_sha256"], "text/plain")
            for row in (original, recovered)
        ]
        assert [content for _, content, _ in storage.calls] == [
            first.encode(),
            first.encode(),
            second.encode(),
        ]
        assert [selected is not None for _, _, selected in storage.calls] == [
            False,
            True,
            False,
        ]
        unrelated = await read_run_display(
            run_chat_database_url, run_chat_schema, request("other-output-run")
        )
        assert not unrelated.workspace_outputs
