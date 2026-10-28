"""Bounded native checkpoint display read; no graph, reducer or model execution."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import BaseMessage
from pydantic import TypeAdapter
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from kokoro_agent.application.run_display import RunDisplay, recover_display
from kokoro_agent.application.native_activity_details import (
    NATIVE_FILE_CODES,
    frozen_native_tools,
)
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.domain.run.models import ToolJournalRecord
from kokoro_agent.execution.runtime_profile import canonical_json
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.protocol import RunRequest


@dataclass(slots=True)
class DisplayReadBudget:
    """Count actual native inputs before the saver fetches/deserializes them."""

    _locators: set[str] = field(default_factory=lambda: set[str]())
    _bytes: int = 0

    def reserve(self, size: int) -> None:
        if size < 0 or self._bytes + size > 8 * 1024 * 1024:
            raise ValueError("native display read budget exceeded")
        self._bytes += size

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
    claims = qualified(schema, "kokoro_agent_run")
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
                budget = DisplayReadBudget()
                has_mcp = any(
                    event.get("display_code")
                    in {"tool.mcp.call", "tool.mcp.describe", "tool.mcp.discover"}
                    for event in events
                )
                need_skills = ready and not all(
                    ref in {fact.source_ref for fact in result.display_skills}
                    for ref in request.selected_skill_source_refs
                )
                has_file = bool(
                    ready
                    and request.selected_skill_source_refs
                    and any(
                        event.get("display_code") == "tool.file.read"
                        for event in events
                    )
                )
                has_native = any(
                    event.get("display_code") in set(NATIVE_FILE_CODES.values())
                    for event in events
                )
                if not need_skills and not has_mcp and not has_file and not has_native:
                    return result
                records: dict[str, ToolJournalRecord] = {}
                if has_mcp or has_file:
                    await execute_sql(
                        cur,
                        f"SELECT COUNT(*) AS count, COALESCE(SUM(octet_length(result)+octet_length(name)+octet_length(tool_call_id)),0) AS bytes FROM {journal_table} WHERE run_id=%s",
                        (request.run_id,),
                    )
                    journal_size = await fetch_one(cur)
                    if journal_size is None or journal_size["count"] > 128:
                        raise ValueError("display journal budget exceeded")
                    budget.reserve(int(journal_size["bytes"]))
                    await execute_sql(
                        cur,
                        f"SELECT tool_call_id,name,status,is_error,result FROM {journal_table} WHERE run_id=%s ORDER BY tool_call_id LIMIT 129",
                        (request.run_id,),
                    )
                    journal_rows = await fetch_all(cur)
                    records = {
                        row["tool_call_id"]: ToolJournalRecord(
                            name=row["name"],
                            status=row["status"],
                            is_error=row["is_error"],
                            result=row["result"],
                        )
                        for row in journal_rows
                    }
                journal = {key: row.name for key, row in records.items()}
                connections: tuple[str, ...] = ()
                mcp_source_sha256: str | None = None
                native_tools: tuple[str, ...] = ()
                if has_native or (
                    has_mcp
                    and any(
                        row.name in {"mcp_call", "mcp_describe_tool", "mcp_list_tools"}
                        for row in records.values()
                    )
                ):
                    await execute_sql(
                        cur,
                        f"SELECT octet_length(assembly_recipe_bytes)+COALESCE(octet_length(request_json),0) AS bytes FROM {claims} WHERE run_id=%s AND tenant_id=%s",
                        (request.run_id, tenant),
                    )
                    recipe_size = await fetch_one(cur)
                    if recipe_size is not None and recipe_size["bytes"] is not None:
                        budget.reserve(int(recipe_size["bytes"]))
                        await execute_sql(
                            cur,
                            f"SELECT assembly_recipe_bytes,assembly_recipe_fingerprint,request_json FROM {claims} WHERE run_id=%s AND tenant_id=%s",
                            (request.run_id, tenant),
                        )
                        row = await fetch_one(cur)
                        if (
                            row is None
                            or RunRequest.model_validate_json(row["request_json"])
                            != request
                        ):
                            raise ValueError("display frozen request differs")
                        raw = bytes(row["assembly_recipe_bytes"])
                        if (
                            hashlib.sha256(raw).hexdigest()
                            != row["assembly_recipe_fingerprint"]
                        ):
                            raise ValueError("display frozen recipe digest differs")
                        recipe = TypeAdapter(dict[str, Any]).validate_json(
                            raw, strict=True
                        )
                        if (
                            canonical_json(recipe) != raw
                            or recipe.get("domain_tag")
                            != "kokoro-agent:assembly-recipe:1"
                        ):
                            raise ValueError("display frozen recipe differs")
                        feature = TypeAdapter(dict[str, Any]).validate_python(
                            recipe.get("feature"), strict=True
                        )
                        peers = TypeAdapter(list[dict[str, Any]]).validate_python(
                            feature.get("agents"), strict=True
                        )
                        native_tools = frozen_native_tools(peers) if has_native else ()
                        peer_sets: list[set[str]] = []
                        for peer in peers:
                            refs = TypeAdapter(list[dict[str, str]]).validate_python(
                                peer.get("mcp"), strict=True
                            )
                            if any(set(item) != {"connection_id"} for item in refs):
                                raise ValueError("frozen MCP connection shape differs")
                            peer_sets.append({item["connection_id"] for item in refs})
                        implementations = TypeAdapter(dict[str, Any]).validate_python(
                            recipe.get("implementations", {}), strict=True
                        )
                        sources = TypeAdapter(list[dict[str, Any]]).validate_python(
                            implementations.get("sources", []), strict=True
                        )
                        hashes: set[str] = set()
                        for source in sources:
                            if source.get("package") != "kokoro_agent":
                                continue
                            files = TypeAdapter(list[dict[str, str]]).validate_python(
                                source.get("files", []), strict=True
                            )
                            hashes.update(
                                item["sha256"]
                                for item in files
                                if item.get("path") == "mcp/tools.py"
                            )
                        if len(hashes) > 1:
                            raise ValueError("conflicting frozen MCP implementation")
                        mcp_source_sha256 = next(iter(hashes), None)
                        if peer_sets:
                            common = peer_sets[0].copy()
                            for values in peer_sets[1:]:
                                common.intersection_update(values)
                            connections = tuple(sorted(common))
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
                      AND c.metadata->>'kokoro_run_id'=%s AND %s
                      AND c.checkpoint->'channel_versions'->>'skills_metadata' IS NOT NULL
                      AND (c.parent_checkpoint_id IS NULL OR p.checkpoint_id IS NOT NULL)
                      AND c.checkpoint->'channel_versions'->>'skills_metadata'
                        IS DISTINCT FROM p.checkpoint->'channel_versions'->>'skills_metadata'
                    ORDER BY c.checkpoint->'channel_versions'->>'skills_metadata', c.checkpoint_id
                    ) relevant ORDER BY checkpoint_id LIMIT 33""",
                    (scope.scoped_thread_id, request.run_id, need_skills),
                )
                locators = await fetch_all(cur)
                if len(locators) > 32:
                    raise ValueError("display checkpoint locator budget exceeded")
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
                if (has_mcp and connections) or has_file or native_tools:
                    # Native serializer tokens select candidates, not facts. Match
                    # encoded whole names rather than substrings in prose; typed
                    # call/result and opaque activity identity still prove each row.
                    native_tokens = [
                        saver.serde.dumps_typed(name)[1] for name in native_tools
                    ]
                    await execute_sql(
                        cur,
                        f"""SELECT DISTINCT c.checkpoint_id FROM {checkpoints} c
                      JOIN {writes} w ON w.thread_id=c.thread_id AND w.checkpoint_ns=c.checkpoint_ns AND w.checkpoint_id=c.checkpoint_id
                      WHERE c.thread_id=%s AND c.checkpoint_ns='' AND c.metadata->>'kokoro_run_id'=%s AND w.channel='messages'
                      AND ((w.type='msgpack' AND EXISTS (SELECT 1 FROM unnest(%s::bytea[]) AS candidate(token) WHERE position(candidate.token in w.blob)>0)) OR EXISTS (SELECT 1 FROM {journal_table} j WHERE j.run_id=%s AND ((j.name IN ('mcp_call','mcp_describe_tool','mcp_list_tools') AND %s) OR (j.name='read_file' AND %s)) AND position(convert_to(j.tool_call_id,'UTF8') in w.blob)>0))
                      ORDER BY c.checkpoint_id LIMIT 33""",
                        (
                            scope.scoped_thread_id,
                            request.run_id,
                            native_tokens,
                            request.run_id,
                            bool(has_mcp and connections),
                            has_file,
                        ),
                    )
                    mcp_locators = await fetch_all(cur)
                    if len(mcp_locators) > 32:
                        raise ValueError("display MCP locator budget exceeded")
                    for locator in mcp_locators:
                        checkpoint_id = locator["checkpoint_id"]
                        await execute_sql(
                            cur,
                            f"SELECT COALESCE(SUM(octet_length(blob)+octet_length(type)+octet_length(task_id)+octet_length(channel)),0) AS bytes FROM {writes} WHERE thread_id=%s AND checkpoint_ns='' AND checkpoint_id=%s AND channel='messages'",
                            (scope.scoped_thread_id, checkpoint_id),
                        )
                        size = await fetch_one(cur)
                        budget.accept(checkpoint_id, int(size["bytes"]) if size else 0)
                        await execute_sql(
                            cur,
                            f"SELECT type,blob FROM {writes} WHERE thread_id=%s AND checkpoint_ns='' AND checkpoint_id=%s AND channel='messages' ORDER BY task_id,idx",
                            (scope.scoped_thread_id, checkpoint_id),
                        )
                        for write in await fetch_all(cur):
                            value = saver.serde.loads_typed(
                                (write["type"], bytes(write["blob"]))
                            )
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
                    journal_records=records,
                    mcp_connections=connections,
                    mcp_source_sha256=mcp_source_sha256,
                    native_tools=native_tools,
                )
