"""PostgreSQL adapter for the Agent Chat repository."""

# psycopg's dict-row and dynamic SQL APIs are runtime-typed in the installed
# version; contract tests cover this adapter boundary.

from __future__ import annotations

import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from kokoro_agent.domain.chat.models import (
    ChatEventDraft,
    ChatEventRecord,
    ChatMessageDraft,
    ChatMessageRecord,
    ChatProjection,
    ChatSessionRecord,
    chat_event_id,
)
from kokoro_agent.domain.chat.repositories import ChatFenceMode, ChatIdentityConflict
from kokoro_agent.domain.chat.time import epoch_millis_to_utc, normalize_utc_datetime
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.infrastructure.chat_mappers import (
    chat_event_from_row,
    chat_message_from_row,
    chat_session_from_row,
)
from kokoro_agent.infrastructure.postgres import DEFAULT_PG_SCHEMA, connect_pg, qualified
from kokoro_agent.infrastructure.schema import (
    CHAT_EVENTS_TABLE,
    CHAT_MESSAGES_TABLE,
    CHAT_SEQUENCES_TABLE,
    CHAT_SESSIONS_TABLE,
    RUN_CLAIMS_TABLE,
    verify_agent_schema,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one


class PostgresChatRepositorySettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    database_url: str
    schema_name: str = DEFAULT_PG_SCHEMA


class PostgresChatRepository:
    """Append-only events plus idempotent final-message projection."""

    def __init__(
        self,
        database_url: str,
        schema: str = DEFAULT_PG_SCHEMA,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._database_url = database_url
        self._schema = schema
        # The fence clock belongs to the Run claim table and retains its
        # existing epoch-millisecond injection contract. Chat facts themselves
        # use aware datetime values before reaching PostgreSQL.
        self._clock = clock or _now_ms

    async def setup(self) -> None:
        async with connect_pg(self._database_url) as conn:
            await verify_agent_schema(conn, self._schema)

    async def ensure_session(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        project_ref: str | None,
        title: str,
        updated_at: datetime,
    ) -> ChatSessionRecord:
        _validate_scope(tenant_id, namespace, session_id)
        if not title.strip():
            raise ValueError("title is required")
        if project_ref is not None and not project_ref.strip():
            raise ValueError("project_ref must be non-empty when provided")
        updated_at = normalize_utc_datetime(updated_at)

        table = qualified(self._schema, CHAT_SESSIONS_TABLE)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    f"""
                    SELECT tenant_id, namespace, session_id, project_ref, title,
                           created_at, updated_at
                    FROM {table}
                    WHERE tenant_id = %s AND namespace = %s AND session_id = %s
                    """,
                    (tenant_id, namespace, session_id),
                )
                row = await fetch_one(cur)
                if row is not None:
                    existing = chat_session_from_row(row)
                    if existing.project_ref != project_ref:
                        raise ChatIdentityConflict(
                            f"chat session identity drift for {session_id!r}"
                        )
                    await execute_sql(
                        cur,
                        f"""
                        UPDATE {table}
                        SET updated_at = GREATEST(updated_at, %s)
                        WHERE tenant_id = %s AND namespace = %s AND session_id = %s
                        RETURNING tenant_id, namespace, session_id, project_ref, title,
                                  created_at, updated_at
                        """,
                        (updated_at, tenant_id, namespace, session_id),
                    )
                    updated = await fetch_one(cur)
                    if updated is None:
                        raise RuntimeError("chat session update returned no row")
                    return chat_session_from_row(updated)

                await execute_sql(
                    cur,
                    f"""
                    INSERT INTO {table}
                        (tenant_id, namespace, session_id, project_ref, title,
                         created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, namespace, session_id) DO NOTHING
                    RETURNING tenant_id, namespace, session_id, project_ref, title,
                              created_at, updated_at
                    """,
                    (
                        tenant_id,
                        namespace,
                        session_id,
                        project_ref,
                        title.strip()[:80],
                        updated_at,
                        updated_at,
                    ),
                )
                inserted = await fetch_one(cur)
                if inserted is not None:
                    return chat_session_from_row(inserted)

                await execute_sql(
                    cur,
                    f"""
                    SELECT tenant_id, namespace, session_id, project_ref, title,
                           created_at, updated_at
                    FROM {table}
                    WHERE tenant_id = %s AND namespace = %s AND session_id = %s
                    """,
                    (tenant_id, namespace, session_id),
                )
                raced = await fetch_one(cur)
        if raced is None:
            raise RuntimeError("chat session insert raced and row is missing")
        raced_record = chat_session_from_row(raced)
        if raced_record.project_ref != project_ref:
            raise ChatIdentityConflict(
                f"chat session identity drift for {session_id!r}"
            )
        return raced_record

    async def list_sessions(
        self,
        tenant_id: str,
        namespace: str,
        *,
        project_ref: str | None = None,
        after: tuple[datetime, str] | None = None,
        limit: int = 101,
    ) -> tuple[ChatSessionRecord, ...]:
        _validate_scope(tenant_id, namespace)
        if project_ref is not None and not project_ref.strip():
            raise ValueError("project_ref must be non-empty when provided")
        if limit <= 0 or limit > 1001:
            raise ValueError("limit must be between 1 and 1001")
        if after is not None:
            after_time, after_session = after
            if not after_session.strip():
                raise ValueError("after cursor is invalid")
            after = (normalize_utc_datetime(after_time), after_session)

        clauses = ["tenant_id = %s", "namespace = %s"]
        params: list[object] = [tenant_id, namespace]
        if project_ref is not None:
            clauses.append("project_ref = %s")
            params.append(project_ref)
        if after is not None:
            clauses.append(
                "(updated_at < %s OR (updated_at = %s AND session_id > %s))"
            )
            params.extend([after[0], after[0], after[1]])
        params.append(limit)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    f"""
                    SELECT tenant_id, namespace, session_id, project_ref, title,
                           created_at, updated_at
                    FROM {qualified(self._schema, CHAT_SESSIONS_TABLE)}
                    WHERE {" AND ".join(clauses)}
                    ORDER BY updated_at DESC, session_id ASC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = await fetch_all(cur)
        return tuple(chat_session_from_row(row) for row in rows)

    async def append(self, projection: ChatProjection) -> ChatEventRecord:
        _validate_projection(projection)
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    return await self._append_projection(cur, projection)

    async def append_fenced(
        self,
        projection: ChatProjection,
        lease: LeaseFence,
        *,
        mode: ChatFenceMode,
    ) -> ChatEventRecord | None:
        """Hold the run generation lock until its Chat projection is durable."""

        _validate_projection(projection)
        if mode not in {"active", "current_generation"}:
            raise ValueError(f"unsupported chat fence mode: {mode!r}")
        active_predicate = (
            """
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at > %s
              AND terminal = FALSE
            """
            if mode == "active"
            else ""
        )
        params: tuple[object, ...] = (
            projection.event.run_id,
            lease.owner,
            lease.generation,
        )
        if mode == "active":
            params = (*params, epoch_millis_to_utc(self._clock()))

        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await execute_sql(
                        cur,
                        """
                        SELECT 1
                        FROM {}
                        WHERE run_id = %s
                          AND owner = %s
                          AND lease_generation = %s
                          {}
                        FOR UPDATE
                        """.format(
                            qualified(self._schema, RUN_CLAIMS_TABLE),
                            active_predicate,
                        ),
                        params,
                    )
                    if await fetch_one(cur) is None:
                        return None
                    return await self._append_projection(cur, projection)

    async def save_message(self, message: ChatMessageDraft) -> ChatMessageRecord:
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    return await self._save_message(cur, message)

    async def replay(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        after_seq: int = 0,
        limit: int = 500,
    ) -> tuple[ChatEventRecord, ...]:
        _validate_scope(tenant_id, namespace, session_id)
        _validate_page(after_seq, limit)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT chat_event_id, tenant_id, namespace, session_id, run_id,
                           source_index, chat_message_id, event_type, payload_json,
                           created_at, seq
                    FROM {}
                    WHERE tenant_id = %s AND namespace = %s AND session_id = %s
                      AND seq > %s
                    ORDER BY seq ASC
                    LIMIT %s
                    """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
                    (tenant_id, namespace, session_id, after_seq, limit),
                )
                rows = await fetch_all(cur)
        return tuple(chat_event_from_row(row) for row in rows)

    async def history(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        after_seq: int = 0,
        limit: int = 200,
    ) -> tuple[ChatMessageRecord, ...]:
        _validate_scope(tenant_id, namespace, session_id)
        _validate_page(after_seq, limit)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT chat_message_id, tenant_id, namespace, session_id, run_id,
                           role, content, status, created_at, updated_at, seq
                    FROM {}
                    WHERE tenant_id = %s AND namespace = %s AND session_id = %s
                      AND seq > %s
                    ORDER BY seq ASC
                    LIMIT %s
                    """.format(qualified(self._schema, CHAT_MESSAGES_TABLE)),
                    (tenant_id, namespace, session_id, after_seq, limit),
                )
                rows = await fetch_all(cur)
        return tuple(chat_message_from_row(row) for row in rows)

    async def next_source_index(self, tenant_id: str, namespace: str, run_id: str) -> int:
        _validate_scope(tenant_id, namespace, run_id)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT max(source_index) AS source_index
                    FROM {}
                    WHERE tenant_id = %s AND namespace = %s AND run_id = %s
                    """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
                    (tenant_id, namespace, run_id),
                )
                row = await fetch_one(cur)
        value = row["source_index"] if row is not None else None
        return 0 if value is None else int(value) + 1

    async def watermark(self, tenant_id: str, namespace: str, session_id: str) -> int:
        _validate_scope(tenant_id, namespace, session_id)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT max(seq) AS seq
                    FROM {}
                    WHERE tenant_id = %s AND namespace = %s AND session_id = %s
                    """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
                    (tenant_id, namespace, session_id),
                )
                row = await fetch_one(cur)
        value = row["seq"] if row is not None else None
        return 0 if value is None else int(value)

    async def _append_projection(
        self, cur: Any, projection: ChatProjection
    ) -> ChatEventRecord:
        _validate_projection(projection)
        draft = projection.event
        await self._lock_identity(
            cur,
            f"chat-event:{draft.tenant_id}:{draft.namespace}:{draft.run_id}:{draft.source_index}",
        )
        existing = await self._get_event(
            cur, draft.tenant_id, draft.namespace, draft.run_id, draft.source_index
        )
        if existing is not None:
            _assert_event_identity(existing, draft)
            record = existing
        else:
            seq = await self._next_seq(
                cur, "event", draft.tenant_id, draft.namespace, draft.session_id
            )
            record = ChatEventRecord(
                **draft.model_dump(),
                chat_event_id=chat_event_id(
                    draft.namespace, draft.run_id, draft.source_index
                ),
                seq=seq,
            )
            await execute_sql(
                cur,
                """
                INSERT INTO {} (
                    tenant_id, namespace, session_id, run_id, source_index,
                    chat_message_id, event_type, payload_json, created_at, seq,
                    chat_event_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
                (
                    record.tenant_id,
                    record.namespace,
                    record.session_id,
                    record.run_id,
                    record.source_index,
                    record.chat_message_id,
                    record.event_type,
                    record.payload_json,
                    record.created_at,
                    record.seq,
                    record.chat_event_id,
                ),
            )
        if projection.message is not None:
            await self._save_message(cur, projection.message)
        return record

    async def _save_message(
        self, cur: Any, message: ChatMessageDraft
    ) -> ChatMessageRecord:
        await self._lock_identity(
            cur, f"chat-message:{message.tenant_id}:{message.chat_message_id}"
        )
        existing = await self._get_message(cur, message.tenant_id, message.chat_message_id)
        if existing is not None:
            _assert_message_identity(existing, message)
            return existing
        seq = await self._next_seq(
            cur, "message", message.tenant_id, message.namespace, message.session_id
        )
        record = ChatMessageRecord(**message.model_dump(), seq=seq)
        await execute_sql(
            cur,
            """
            INSERT INTO {} (
                tenant_id, chat_message_id, namespace, session_id, run_id, role,
                content, status, created_at, updated_at, seq
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """.format(qualified(self._schema, CHAT_MESSAGES_TABLE)),
            (
                record.tenant_id,
                record.chat_message_id,
                record.namespace,
                record.session_id,
                record.run_id,
                record.role,
                record.content,
                record.status,
                record.created_at,
                record.updated_at,
                record.seq,
            ),
        )
        return record

    async def _next_seq(
        self, cur: Any, kind: str, tenant_id: str, namespace: str, session_id: str
    ) -> int:
        table = qualified(self._schema, CHAT_SEQUENCES_TABLE)
        await execute_sql(
            cur,
            f"""
            INSERT INTO {table} (kind, tenant_id, namespace, session_id, seq)
            VALUES (%s, %s, %s, %s, 0)
            ON CONFLICT (kind, tenant_id, namespace, session_id) DO NOTHING
            """,
            (kind, tenant_id, namespace, session_id),
        )
        await execute_sql(
            cur,
            f"""
            SELECT seq
            FROM {table}
            WHERE kind = %s AND tenant_id = %s AND namespace = %s AND session_id = %s
            FOR UPDATE
            """,
            (kind, tenant_id, namespace, session_id),
        )
        row = await fetch_one(cur)
        if row is None:
            raise RuntimeError(f"failed to lock {kind} sequence for {session_id!r}")
        seq = int(row["seq"]) + 1
        await execute_sql(
            cur,
            f"""
            UPDATE {table}
            SET seq = %s
            WHERE kind = %s AND tenant_id = %s AND namespace = %s AND session_id = %s
            """,
            (seq, kind, tenant_id, namespace, session_id),
        )
        return seq

    async def _get_event(
        self,
        cur: Any,
        tenant_id: str,
        namespace: str,
        run_id: str,
        source_index: int,
    ) -> ChatEventRecord | None:
        await execute_sql(
            cur,
            """
            SELECT chat_event_id, tenant_id, namespace, session_id, run_id,
                   source_index, chat_message_id, event_type, payload_json,
                   created_at, seq
            FROM {}
            WHERE tenant_id = %s AND namespace = %s AND run_id = %s
              AND source_index = %s
            """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
            (tenant_id, namespace, run_id, source_index),
        )
        row = await fetch_one(cur)
        return None if row is None else chat_event_from_row(row)

    async def _get_message(
        self, cur: Any, tenant_id: str, chat_message_id: str
    ) -> ChatMessageRecord | None:
        await execute_sql(
            cur,
            """
            SELECT chat_message_id, tenant_id, namespace, session_id, run_id,
                   role, content, status, created_at, updated_at, seq
            FROM {}
            WHERE tenant_id = %s AND chat_message_id = %s
            """.format(qualified(self._schema, CHAT_MESSAGES_TABLE)),
            (tenant_id, chat_message_id),
        )
        row = await fetch_one(cur)
        return None if row is None else chat_message_from_row(row)

    async def _lock_identity(self, cur: Any, identity: str) -> None:
        """Serialize duplicate immutable identities before allocating a sequence."""

        await execute_sql(
            cur, "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (identity,)
        )


@asynccontextmanager
async def make_chat_repository(
    settings: PostgresChatRepositorySettings,
) -> AsyncGenerator[PostgresChatRepository, None]:
    repository = PostgresChatRepository(settings.database_url, settings.schema_name)
    await repository.setup()
    try:
        yield repository
    finally:
        pass


def _validate_scope(
    tenant_id: str, namespace: str, resource_id: str | None = None
) -> None:
    if not tenant_id.strip() or not namespace.strip():
        raise ValueError("tenant_id and namespace are required")
    if resource_id is not None and not resource_id.strip():
        raise ValueError("resource identifier is required")


def _validate_projection(projection: ChatProjection) -> None:
    event = projection.event
    if projection.message is not None and (
        event.chat_message_id != projection.message.chat_message_id
        or event.tenant_id != projection.message.tenant_id
        or event.namespace != projection.message.namespace
        or event.session_id != projection.message.session_id
        or event.run_id != projection.message.run_id
    ):
        raise ValueError("chat projection event and message scope must match")


def _validate_page(after_seq: int, limit: int) -> None:
    if after_seq < 0:
        raise ValueError("after_seq must be non-negative")
    if limit <= 0 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _assert_event_identity(record: ChatEventRecord, draft: ChatEventDraft) -> None:
    immutable = record.model_dump(exclude={"chat_event_id", "seq"})
    if immutable != draft.model_dump():
        raise ChatIdentityConflict(
            f"chat event identity drift for {draft.run_id!r} index {draft.source_index}"
        )


def _assert_message_identity(
    record: ChatMessageRecord, draft: ChatMessageDraft
) -> None:
    immutable = record.model_dump(exclude={"seq", "created_at", "updated_at"})
    if immutable != draft.model_dump(exclude={"created_at", "updated_at"}):
        raise ChatIdentityConflict(
            f"chat message identity drift for {draft.chat_message_id!r}"
        )


__all__ = [
    "CHAT_EVENTS_TABLE",
    "CHAT_MESSAGES_TABLE",
    "CHAT_SEQUENCES_TABLE",
    "CHAT_SESSIONS_TABLE",
    "PostgresChatRepository",
    "PostgresChatRepositorySettings",
    "make_chat_repository",
]
