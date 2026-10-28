"""Bounded native checkpoint display read; no graph, reducer or model execution."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import BaseMessage
from pydantic import TypeAdapter
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from kokoro_agent.application.run_display import RunDisplay, recover_display
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.protocol import RunRequest


@dataclass(slots=True)
class DisplayReadBudget:
    """Count actual native inputs before the saver fetches/deserializes them."""

    _locators: set[str] = field(default_factory=lambda: set[str]())
    _bytes: int = 0

    def accept(self, locator: str, size: int) -> None:
        if locator in self._locators:
            return
        if (
            size < 0
            or len(self._locators) >= 32
            or self._bytes + size > 8 * 1024 * 1024
        ):
            raise ValueError("native display read budget exceeded")
        self._locators.add(locator)
        self._bytes += size


async def read_run_display(
    database_url: str, schema: str, request: RunRequest
) -> RunDisplay:
    """Called inside the resource's absolute five-second timeout.

    RR read-only covers size preflight and native loads: concurrent checkpoint
    writes cannot grow an already-admitted input. No native delta history call
    is made. Unavailable legacy tool arguments stay unknown.
    """
    scope = RunScope.of(request)
    tenant = request.execution_identity.tenant_ref
    events_table = qualified(schema, "kokoro_agent_chat_event")
    journal_table = qualified(schema, "kokoro_agent_tool_journal")
    checkpoints = qualified(schema, "checkpoints")
    blobs = qualified(schema, "checkpoint_blobs")
    writes = qualified(schema, "checkpoint_writes")
    async with connect_pg(database_url) as conn:
        async with conn.transaction():
            await execute_sql(
                conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            await execute_sql(conn, "SET LOCAL statement_timeout = '4500ms'")
            await execute_sql(
                conn,
                f"SET LOCAL search_path TO {qualified(schema, 'checkpoints').rsplit('.', 1)[0]}",
            )
            async with conn.cursor() as cur:
                args = (tenant, scope.namespace, scope.session_id, request.run_id)
                await execute_sql(
                    cur,
                    f"SELECT COUNT(*) AS count, COALESCE(SUM(octet_length(payload_json)),0) AS bytes FROM {events_table} WHERE tenant_id=%s AND namespace=%s AND session_id=%s AND run_id=%s AND event_type='activity'",
                    args,
                )
                size = await fetch_one(cur)
                if size is None or size["count"] > 1024 or size["bytes"] > 1024 * 1024:
                    raise ValueError("display activity budget exceeded")
                await execute_sql(
                    cur,
                    f"SELECT payload_json FROM {events_table} WHERE tenant_id=%s AND namespace=%s AND session_id=%s AND run_id=%s AND event_type='activity' ORDER BY seq LIMIT 1024",
                    args,
                )
                events = [
                    json.loads(row["payload_json"]) for row in await fetch_all(cur)
                ]
                # First pure projection accepts already-persisted future display.
                result = recover_display(
                    scope=(tenant, scope.namespace, scope.session_id, request.run_id),
                    selected=request.selected_skill_source_refs,
                    events=events,
                    metadata=[],
                    messages=[],
                    journal={},
                )
                ready = any(
                    event.get("activity") == "skill" and event.get("phase") == "ready"
                    for event in events
                )
                if not ready or all(
                    ref in {fact.source_ref for fact in result.display_skills}
                    for ref in request.selected_skill_source_refs
                ):
                    return result
                await execute_sql(
                    cur,
                    f"SELECT tool_call_id,name FROM {journal_table} WHERE run_id=%s ORDER BY tool_call_id LIMIT 129",
                    (request.run_id,),
                )
                journal_rows = await fetch_all(cur)
                if len(journal_rows) > 128:
                    raise ValueError("display journal budget exceeded")
                journal = {row["tool_call_id"]: row["name"] for row in journal_rows}
                # Scalar locators only. Parent comparison proves a new loader
                # version, excluding inherited metadata at the next Run's entry.
                await execute_sql(
                    cur,
                    f"""SELECT checkpoint_id,version,parent_version,parent_known FROM (
                    SELECT DISTINCT ON (c.checkpoint->'channel_versions'->>'skills_metadata')
                    c.checkpoint_id,
                    c.checkpoint->'channel_versions'->>'skills_metadata' AS version,
                    p.checkpoint->'channel_versions'->>'skills_metadata' AS parent_version,
                    TRUE AS parent_known
                    FROM {checkpoints} c LEFT JOIN {checkpoints} p
                      ON p.thread_id=c.thread_id AND p.checkpoint_ns=c.checkpoint_ns
                      AND p.checkpoint_id=c.parent_checkpoint_id
                    WHERE c.thread_id=%s AND c.checkpoint_ns=''
                      AND c.metadata->>'kokoro_run_id'=%s
                      AND c.checkpoint->'channel_versions'->>'skills_metadata' IS NOT NULL
                      AND (c.parent_checkpoint_id IS NULL OR p.checkpoint_id IS NOT NULL)
                      AND c.checkpoint->'channel_versions'->>'skills_metadata'
                        IS DISTINCT FROM p.checkpoint->'channel_versions'->>'skills_metadata'
                    ORDER BY c.checkpoint->'channel_versions'->>'skills_metadata', c.checkpoint_id
                    ) relevant ORDER BY checkpoint_id LIMIT 33""",
                    (scope.scoped_thread_id, request.run_id),
                )
                locators = await fetch_all(cur)
                if len(locators) > 32:
                    raise ValueError("display checkpoint locator budget exceeded")
                budget = DisplayReadBudget()
                metadata: list[dict[str, Any]] = []
                messages: list[BaseMessage] = []
                saver_constructor: Any = AsyncPostgresSaver
                saver: Any = saver_constructor(conn)
                for locator in locators:
                    checkpoint_id = locator["checkpoint_id"]
                    # Saver aget_tuple fetches the whole tuple and referenced
                    # channels/writes: budget ALL those bytes, not just metadata.
                    await execute_sql(
                        cur,
                        f"""SELECT
                      octet_length(c.checkpoint::text)+octet_length(c.metadata::text)
                      +octet_length(c.thread_id)+octet_length(c.checkpoint_ns)+octet_length(c.checkpoint_id)+COALESCE(octet_length(c.parent_checkpoint_id),0)
                      +COALESCE((SELECT SUM(COALESCE(octet_length(b.blob),0)+octet_length(b.type)+octet_length(b.channel))
                        FROM {blobs} b JOIN jsonb_each_text(c.checkpoint->'channel_versions') v
                          ON b.channel=v.key AND b.version=v.value
                        WHERE b.thread_id=c.thread_id AND b.checkpoint_ns=c.checkpoint_ns),0)
                      +COALESCE((SELECT SUM(octet_length(w.blob)+COALESCE(octet_length(w.type),0)+octet_length(w.channel)+octet_length(w.task_id)+octet_length(w.task_path))
                        FROM {writes} w WHERE w.thread_id=c.thread_id AND w.checkpoint_ns=c.checkpoint_ns AND w.checkpoint_id=c.checkpoint_id),0) AS bytes
                      FROM {checkpoints} c WHERE c.thread_id=%s AND c.checkpoint_ns='' AND c.checkpoint_id=%s""",
                        (scope.scoped_thread_id, checkpoint_id),
                    )
                    raw_size = await fetch_one(cur)
                    if raw_size is None:
                        raise ValueError("native checkpoint disappeared")
                    budget.accept(checkpoint_id, int(raw_size["bytes"]))
                    saved = await saver.aget_tuple(
                        {
                            "configurable": {
                                "thread_id": scope.scoped_thread_id,
                                "checkpoint_ns": "",
                                "checkpoint_id": checkpoint_id,
                            }
                        }
                    )
                    if (
                        saved is None
                        or saved.metadata.get("kokoro_run_id") != request.run_id
                    ):
                        raise ValueError("native checkpoint scope differs")
                    if (
                        locator["parent_known"]
                        and locator["version"] is not None
                        and locator["version"] != locator["parent_version"]
                    ):
                        values: Any = saved.checkpoint["channel_values"].get(
                            "skills_metadata", []
                        )
                        if not isinstance(values, list):
                            raise ValueError("native Skill metadata differs")
                        metadata.extend(
                            TypeAdapter(list[dict[str, Any]]).validate_python(values)
                        )
                    # Only native decoded direct messages, never custom Delta
                    # decoding or arbitrary nested args/message exploration.
                    pending: list[tuple[str, str, Any]] = saved.pending_writes or []
                    for _task, channel, value in pending:
                        if channel == "messages":
                            candidates = (
                                TypeAdapter(list[Any]).validate_python(value)
                                if isinstance(value, list)
                                else [value]
                            )
                            messages.extend(
                                item
                                for item in candidates
                                if isinstance(item, BaseMessage)
                            )
                return recover_display(
                    scope=(tenant, scope.namespace, scope.session_id, request.run_id),
                    selected=request.selected_skill_source_refs,
                    events=events,
                    metadata=metadata,
                    messages=messages,
                    journal=journal,
                )
