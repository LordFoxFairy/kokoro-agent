"""Bounded legacy final proof against native PostgreSQL checkpoints; no model service."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
import re
from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import StructuredTool
from pydantic import BaseModel

from kokoro_agent.domain.chat.projection import assistant_message_id, project_chat_fact
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.execution.runtime_profile import canonical_json
from kokoro_agent.execution.runtime_profile_sources import production_manifest
from kokoro_agent.infrastructure.checkpoint_display import (
    DisplayReadBudget,
    read_run_display,
)
from kokoro_agent.infrastructure.checkpoint_final_answer import read_final_message_id
from kokoro_agent.infrastructure.checkpoints import (
    CheckpointSettings,
    make_checkpointer,
)
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_chat_repository import PostgresChatRepository
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.protocol import MessageCompletedPayload, RunCompletedPayload
from support.deepagents import create_test_deep_agent
from support.fakes import request
from support.local_fake import LocalFakeChatModel


async def _snapshot(url: str, schema: str) -> str:
    rows: list[object] = []
    async with connect_pg(url) as conn, conn.cursor() as cur:
        for name in (
            "kokoro_agent_run",
            "kokoro_agent_chat_event",
            "checkpoints",
            "checkpoint_blobs",
            "checkpoint_writes",
        ):
            await execute_sql(
                cur,
                f"SELECT row_to_json(t)::text AS value FROM {qualified(schema, name)} t ORDER BY row_to_json(t)::text",
            )
            rows.extend(await fetch_all(cur))
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


@pytest.mark.asyncio
async def test_bounded_native_final_is_read_only_and_exact_run(
    run_chat_schema: str,
    run_chat_database_url: str,
    chat_repository: PostgresChatRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert re.fullmatch(r"kokoro_test_[0-9a-f]{32}", run_chat_schema)
    run = request("bounded-final")
    scope = RunScope.of(run)
    manifest = production_manifest()
    recipe_value: dict[str, Any] = {
        "domain_tag": "kokoro-agent:assembly-recipe:1",
        "feature": {"agents": [{}]},
        "implementations": {
            "dependencies": [list(pair) for pair in manifest.versions],
            "sources": [manifest.descriptor("assembly"), manifest.descriptor("native")],
        },
    }
    recipe = canonical_json(recipe_value)

    class Args(BaseModel):
        pass

    noop = StructuredTool(
        name="noop", description="no-op", args_schema=Args, func=lambda: "ok"
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
        graph = create_test_deep_agent(
            model=LocalFakeChatModel.with_script(
                [
                    AIMessage(
                        id="progress",
                        content="Checking",
                        tool_calls=[{"id": "call", "name": "noop", "args": {}}],
                    ),
                    AIMessage(id="final", content="The answer"),
                ]
            ),
            tools=[noop],
            subagents=[],
            checkpointer=saver,
            permissions=[],
            interrupt_on={},
        )
        stream = await graph.astream_events(
            {"messages": [HumanMessage(content="go")]},
            version="v3",
            config=config,
            transformers=[],
        )
        async with stream:
            await stream.output()
    for index, payload in enumerate(
        [
            MessageCompletedPayload(segment_id="progress", content="Checking"),
            MessageCompletedPayload(segment_id="final", content="The answer"),
            RunCompletedPayload(status="completed"),
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
        )
        assert projection is not None
        await chat_repository.append(projection)
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_run')} (run_id,tenant_id,request_json,assembly_recipe_bytes,assembly_recipe_fingerprint,terminal,terminal_at,interaction_phase) VALUES (%s,%s,%s,%s,%s,true,%s,'terminal')",
            (
                run.run_id,
                run.execution_identity.tenant_ref,
                run.model_dump_json(),
                recipe,
                hashlib.sha256(recipe).hexdigest(),
                datetime.now(UTC) + timedelta(seconds=1),
            ),
        )

    async def read(budget: DisplayReadBudget | None = None) -> str | None:
        async with connect_pg(run_chat_database_url) as conn:
            async with conn.transaction():
                await execute_sql(
                    conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                await execute_sql(conn, "SET LOCAL statement_timeout = '4500ms'")
                await execute_sql(conn, f'SET LOCAL search_path TO "{run_chat_schema}"')
                return await read_final_message_id(
                    conn, run_chat_schema, run, (budget or DisplayReadBudget()).reserve
                )

    before = await _snapshot(run_chat_database_url, run_chat_schema)
    assert await read() == assistant_message_id(scope.namespace, run.run_id, "final")
    display = await read_run_display(run_chat_database_url, run_chat_schema, run)
    assert display.model_dump()["final_message_id"] == assistant_message_id(
        scope.namespace, run.run_id, "final"
    )
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before
    budget = DisplayReadBudget()
    budget.reserve(8 * 1024 * 1024)
    with pytest.raises(ValueError, match="budget exceeded"):
        await read(budget)
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before

    # Unsupported native formats can trigger saver queries beyond our preflight.
    # Reject them before any native tuple is loaded, then restore only our fixture.
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    async def forbidden_native_read(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("unsupported checkpoint reached native deserialization")

    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"UPDATE {qualified(run_chat_schema, 'checkpoints')} SET checkpoint=jsonb_set(checkpoint,'{{v}}','3') WHERE thread_id=%s AND checkpoint_ns=''",
            (scope.scoped_thread_id,),
        )
    with monkeypatch.context() as patch:
        patch.setattr(AsyncPostgresSaver, "aget_tuple", forbidden_native_read)
        assert await read() is None
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"UPDATE {qualified(run_chat_schema, 'checkpoints')} SET checkpoint=jsonb_set(checkpoint,'{{v}}','4') WHERE thread_id=%s AND checkpoint_ns=''",
            (scope.scoped_thread_id,),
        )
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before

    # Explicit terminal identity is authoritative: no native fallback for new Runs.
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT payload_json FROM {qualified(run_chat_schema, 'kokoro_agent_chat_event')} WHERE run_id=%s AND event_type='run.completed'",
            (run.run_id,),
        )
        terminal = await fetch_one(cur)
        assert terminal is not None
        explicit = json.loads(terminal["payload_json"])
        explicit["final_message_id"] = assistant_message_id(
            scope.namespace, run.run_id, "final"
        )
        await execute_sql(
            cur,
            f"UPDATE {qualified(run_chat_schema, 'kokoro_agent_chat_event')} SET payload_json=%s WHERE run_id=%s AND event_type='run.completed'",
            (json.dumps(explicit), run.run_id),
        )
    with monkeypatch.context() as patch:
        patch.setattr(AsyncPostgresSaver, "aget_tuple", forbidden_native_read)
        terminal_only_budget = DisplayReadBudget()
        terminal_only_budget.reserve(
            8 * 1024 * 1024 - len(json.dumps(explicit).encode())
        )
        assert await read(terminal_only_budget) is None
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"UPDATE {qualified(run_chat_schema, 'kokoro_agent_chat_event')} SET payload_json=%s WHERE run_id=%s AND event_type='run.completed'",
            (terminal["payload_json"], run.run_id),
        )
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before

    # A later Run must not cause a full-thread reconstruction or alter old proof.
    async with make_checkpointer(
        CheckpointSettings(
            database_url=run_chat_database_url, schema_name=run_chat_schema
        )
    ) as saver:
        graph = create_test_deep_agent(
            model=LocalFakeChatModel.with_script(
                [AIMessage(id="other-final", content="Later answer")]
            ),
            tools=[],
            subagents=[],
            checkpointer=saver,
            permissions=[],
            interrupt_on={},
        )
        next_config: Any = {
            "configurable": config["configurable"],
            "metadata": {"kokoro_run_id": "later-run"},
        }
        stream = await graph.astream_events(
            {"messages": [HumanMessage(content="next")]},
            version="v3",
            config=next_config,
            transformers=[],
        )
        async with stream:
            await stream.output()
    before_later = await _snapshot(run_chat_database_url, run_chat_schema)
    assert await read() == assistant_message_id(scope.namespace, run.run_id, "final")
    assert await _snapshot(run_chat_database_url, run_chat_schema) == before_later

    # Even a correct native terminal is insufficient if the public body differs.
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT payload_json FROM {qualified(run_chat_schema, 'kokoro_agent_chat_event')} WHERE run_id=%s AND chat_message_id=%s AND event_type='assistant.completed'",
            (run.run_id, assistant_message_id(scope.namespace, run.run_id, "final")),
        )
        row = await fetch_one(cur)
        assert row is not None
        body = json.loads(row["payload_json"])
        body["content"] = "Different body"
        await execute_sql(
            cur,
            f"UPDATE {qualified(run_chat_schema, 'kokoro_agent_chat_event')} SET payload_json=%s WHERE run_id=%s AND chat_message_id=%s AND event_type='assistant.completed'",
            (
                json.dumps(body),
                run.run_id,
                assistant_message_id(scope.namespace, run.run_id, "final"),
            ),
        )
    assert await read() is None
