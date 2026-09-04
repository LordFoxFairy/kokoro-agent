"""PostgreSQL adapter for the Agent chat repository."""

# psycopg's dict-row and dynamic SQL APIs are runtime-typed in the installed
# version; contract tests cover this adapter boundary.
# pyright: reportCallIssue=false, reportArgumentType=false, reportReturnType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportIncompatibleMethodOverride=false

from __future__ import annotations

import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Any

from pydantic import BaseModel, ConfigDict

from kokoro_agent.chat.models import (
    ChatEventDraft,
    ChatEventRecord,
    ChatMessageDraft,
    ChatMessageRecord,
    ChatProjection,
    ChatSessionRecord,
    chat_event_id,
)
from kokoro_agent.infrastructure.postgres import (
    DEFAULT_PG_SCHEMA,
    connect_pg,
    qualified,
)
from kokoro_agent.infrastructure.schema import (
    CHAT_EVENTS_TABLE,
    CHAT_MESSAGES_TABLE,
    CHAT_SEQUENCES_TABLE,
    CHAT_SESSIONS_TABLE,
    RUN_CLAIMS_TABLE,
    verify_agent_schema,
)
from kokoro_agent.repositories.chat_repository import (
    ChatFenceMode,
    ChatIdentityConflict,
)
from kokoro_agent.repositories.run_records import LeaseFence
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
        self._clock = clock or _now_ms

    async def setup(self) -> None:
        async with connect_pg(self._database_url) as conn:
            await verify_agent_schema(conn, self._schema)

    async def ensure_session(
        self,
        namespace: str,
        session_id: str,
        *,
        project_ref: str | None,
        title: str,
        updated_at: int,
    ) -> ChatSessionRecord:
        if not namespace.strip() or not session_id.strip() or not title.strip():
            raise ValueError("namespace, session_id, and title are required")
        if project_ref is not None and not project_ref.strip():
            raise ValueError("project_ref must be non-empty when provided")
        if updated_at < 0:
            raise ValueError("updated_at must be non-negative")

        table = qualified(self._schema, CHAT_SESSIONS_TABLE)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT session_id, project_ref, title,
                           (extract(epoch FROM created_at) * 1000)::bigint AS created_at,
                           (extract(epoch FROM updated_at) * 1000)::bigint AS updated_at
                    FROM {table}
                    WHERE namespace = %s AND session_id = %s
                    """,
                    (namespace, session_id),
                )
                row = await cur.fetchone()
                if row is not None:
                    existing = dict(row)
                    if existing["project_ref"] != project_ref:
                        raise ChatIdentityConflict(
                            f"chat session identity drift for {session_id!r}"
                        )
                    await cur.execute(
                        f"""
                        UPDATE {table}
                        SET updated_at = GREATEST(updated_at, to_timestamp(%s / 1000.0))
                        WHERE namespace = %s AND session_id = %s
                        RETURNING session_id, project_ref, title,
                                  (extract(epoch FROM created_at) * 1000)::bigint AS created_at,
                                  (extract(epoch FROM updated_at) * 1000)::bigint AS updated_at
                        """,
                        (updated_at, namespace, session_id),
                    )
                    updated = await cur.fetchone()
                    if updated is None:
                        raise RuntimeError("chat session update returned no row")
                    return ChatSessionRecord(**dict(updated))

                await cur.execute(
                    f"""
                    INSERT INTO {table}
                        (namespace, session_id, project_ref, title, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, to_timestamp(%s / 1000.0),
                            to_timestamp(%s / 1000.0))
                    ON CONFLICT (namespace, session_id) DO NOTHING
                    RETURNING session_id, project_ref, title,
                              (extract(epoch FROM created_at) * 1000)::bigint AS created_at,
                              (extract(epoch FROM updated_at) * 1000)::bigint AS updated_at
                    """,
                    (
                        namespace,
                        session_id,
                        project_ref,
                        title.strip()[:80],
                        updated_at,
                        updated_at,
                    ),
                )
                inserted = await cur.fetchone()
                if inserted is not None:
                    return ChatSessionRecord(**dict(inserted))

                await cur.execute(
                    f"""
                    SELECT session_id, project_ref, title,
                           (extract(epoch FROM created_at) * 1000)::bigint AS created_at,
                           (extract(epoch FROM updated_at) * 1000)::bigint AS updated_at
                    FROM {table}
                    WHERE namespace = %s AND session_id = %s
                    """,
                    (namespace, session_id),
                )
                raced = await cur.fetchone()
        if raced is None:
            raise RuntimeError("chat session insert raced and row is missing")
        raced_record = ChatSessionRecord(**dict(raced))
        if raced_record.project_ref != project_ref:
            raise ChatIdentityConflict(
                f"chat session identity drift for {session_id!r}"
            )
        return raced_record

    async def list_sessions(
        self,
        namespace: str,
        *,
        project_ref: str | None = None,
        after: tuple[int, str] | None = None,
        limit: int = 101,
    ) -> tuple[ChatSessionRecord, ...]:
        if not namespace.strip():
            raise ValueError("namespace is required")
        if project_ref is not None and not project_ref.strip():
            raise ValueError("project_ref must be non-empty when provided")
        if limit <= 0 or limit > 1001:
            raise ValueError("limit must be between 1 and 1001")
        if after is not None and (after[0] < 0 or not after[1].strip()):
            raise ValueError("after cursor is invalid")

        clauses = ["namespace = %s"]
        params: list[object] = [namespace]
        if project_ref is not None:
            clauses.append("project_ref = %s")
            params.append(project_ref)
        if after is not None:
            clauses.append(
                "(updated_at < to_timestamp(%s / 1000.0) OR "
                "(updated_at = to_timestamp(%s / 1000.0) AND session_id > %s))"
            )
            params.extend([after[0], after[0], after[1]])
        params.append(limit)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT session_id, project_ref, title,
                           (extract(epoch FROM created_at) * 1000)::bigint AS created_at,
                           (extract(epoch FROM updated_at) * 1000)::bigint AS updated_at
                    FROM {qualified(self._schema, CHAT_SESSIONS_TABLE)}
                    WHERE {" AND ".join(clauses)}
                    ORDER BY updated_at DESC, session_id ASC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = await cur.fetchall()
        return tuple(ChatSessionRecord(**dict(row)) for row in rows)

    async def append(self, projection: ChatProjection) -> ChatEventRecord:
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
        """Hold the run generation lock until its chat projection is durable."""

        if mode not in {"active", "current_generation"}:
            raise ValueError(f"unsupported chat fence mode: {mode!r}")
        active_predicate = (
            """
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at > to_timestamp(%s / 1000.0)
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
            params = (*params, self._clock())

        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
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
                    if await cur.fetchone() is None:
                        return None
                    return await self._append_projection(cur, projection)

    async def save_message(self, message: ChatMessageDraft) -> ChatMessageRecord:
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    return await self._save_message(cur, message)

    async def replay(
        self, namespace: str, session_id: str, *, after_seq: int = 0, limit: int = 500
    ) -> tuple[ChatEventRecord, ...]:
        _validate_page(after_seq, limit)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT chat_event_id, namespace, session_id, run_id, source_index,
                           chat_message_id, event_type, payload_json,
                           (extract(epoch FROM created_at) * 1000)::bigint AS created_at,
                           seq
                    FROM {}
                    WHERE namespace = %s AND session_id = %s AND seq > %s
                    ORDER BY seq ASC
                    LIMIT %s
                    """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
                    (namespace, session_id, after_seq, limit),
                )
                rows = await cur.fetchall()
        return tuple(ChatEventRecord(**dict(row)) for row in rows)

    async def history(
        self, namespace: str, session_id: str, *, after_seq: int = 0, limit: int = 200
    ) -> tuple[ChatMessageRecord, ...]:
        _validate_page(after_seq, limit)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT chat_message_id, namespace, session_id, run_id, role,
                           content, status,
                           (extract(epoch FROM created_at) * 1000)::bigint AS created_at,
                           (extract(epoch FROM updated_at) * 1000)::bigint AS updated_at,
                           seq
                    FROM {}
                    WHERE namespace = %s AND session_id = %s AND seq > %s
                    ORDER BY seq ASC
                    LIMIT %s
                    """.format(qualified(self._schema, CHAT_MESSAGES_TABLE)),
                    (namespace, session_id, after_seq, limit),
                )
                rows = await cur.fetchall()
        return tuple(ChatMessageRecord(**dict(row)) for row in rows)

    async def next_source_index(self, namespace: str, run_id: str) -> int:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT max(source_index) AS source_index
                    FROM {}
                    WHERE namespace = %s AND run_id = %s
                    """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
                    (namespace, run_id),
                )
                row = await cur.fetchone()
        value = row["source_index"] if row is not None else None
        return 0 if value is None else int(value) + 1

    async def watermark(self, namespace: str, session_id: str) -> int:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT max(seq) AS seq
                    FROM {}
                    WHERE namespace = %s AND session_id = %s
                    """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
                    (namespace, session_id),
                )
                row = await cur.fetchone()
        value = row["seq"] if row is not None else None
        return 0 if value is None else int(value)

    async def _append_projection(
        self, cur: Any, projection: ChatProjection
    ) -> ChatEventRecord:
        draft = projection.event
        await self._lock_identity(
            cur,
            f"chat-event:{draft.namespace}:{draft.run_id}:{draft.source_index}",
        )
        existing = await self._get_event(
            cur, draft.namespace, draft.run_id, draft.source_index
        )
        if existing is not None:
            _assert_event_identity(existing, draft)
            record = existing
        else:
            seq = await self._next_seq(cur, "event", draft.namespace, draft.session_id)
            record = ChatEventRecord(
                **draft.model_dump(),
                chat_event_id=chat_event_id(
                    draft.namespace, draft.run_id, draft.source_index
                ),
                seq=seq,
            )
            await cur.execute(
                """
                INSERT INTO {} (
                    namespace, session_id, run_id, source_index, chat_message_id,
                    event_type, payload_json, created_at, seq, chat_event_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s,
                          to_timestamp(%s / 1000.0), %s, %s)
                """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
                (
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
        await self._lock_identity(cur, f"chat-message:{message.chat_message_id}")
        existing = await self._get_message(cur, message.chat_message_id)
        if existing is not None:
            _assert_message_identity(existing, message)
            return existing
        seq = await self._next_seq(
            cur, "message", message.namespace, message.session_id
        )
        record = ChatMessageRecord(**message.model_dump(), seq=seq)
        await cur.execute(
            """
            INSERT INTO {} (
                chat_message_id, namespace, session_id, run_id, role,
                content, status, created_at, updated_at, seq
            ) VALUES (%s, %s, %s, %s, %s, %s, %s,
                      to_timestamp(%s / 1000.0), to_timestamp(%s / 1000.0), %s)
            """.format(qualified(self._schema, CHAT_MESSAGES_TABLE)),
            (
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
        self, cur: Any, kind: str, namespace: str, session_id: str
    ) -> int:
        table = qualified(self._schema, CHAT_SEQUENCES_TABLE)
        await cur.execute(
            f"""
            INSERT INTO {table} (kind, namespace, session_id, seq)
            VALUES (%s, %s, %s, 0)
            ON CONFLICT (kind, namespace, session_id) DO NOTHING
            """,
            (kind, namespace, session_id),
        )
        await cur.execute(
            f"""
            SELECT seq
            FROM {table}
            WHERE kind = %s AND namespace = %s AND session_id = %s
            FOR UPDATE
            """,
            (kind, namespace, session_id),
        )
        row = await cur.fetchone()
        if row is None:
            raise RuntimeError(f"failed to lock {kind} sequence for {session_id!r}")
        seq = int(row["seq"]) + 1
        await cur.execute(
            f"""
            UPDATE {table}
            SET seq = %s
            WHERE kind = %s AND namespace = %s AND session_id = %s
            """,
            (seq, kind, namespace, session_id),
        )
        return seq

    async def _get_event(
        self, cur: Any, namespace: str, run_id: str, source_index: int
    ) -> ChatEventRecord | None:
        await cur.execute(
            """
            SELECT chat_event_id, namespace, session_id, run_id, source_index,
                   chat_message_id, event_type, payload_json,
                   (extract(epoch FROM created_at) * 1000)::bigint AS created_at, seq
            FROM {}
            WHERE namespace = %s AND run_id = %s AND source_index = %s
            """.format(qualified(self._schema, CHAT_EVENTS_TABLE)),
            (namespace, run_id, source_index),
        )
        row = await cur.fetchone()
        return None if row is None else ChatEventRecord(**dict(row))

    async def _get_message(
        self, cur: Any, chat_message_id: str
    ) -> ChatMessageRecord | None:
        await cur.execute(
            """
            SELECT chat_message_id, namespace, session_id, run_id, role,
                   content, status,
                   (extract(epoch FROM created_at) * 1000)::bigint AS created_at,
                   (extract(epoch FROM updated_at) * 1000)::bigint AS updated_at, seq
            FROM {}
            WHERE chat_message_id = %s
            """.format(qualified(self._schema, CHAT_MESSAGES_TABLE)),
            (chat_message_id,),
        )
        row = await cur.fetchone()
        return None if row is None else ChatMessageRecord(**dict(row))

    async def _lock_identity(self, cur: Any, identity: str) -> None:
        """Serialize duplicate immutable identities before allocating a sequence."""

        await cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (identity,)
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


def _validate_page(after_seq: int, limit: int) -> None:
    if after_seq < 0:
        raise ValueError("after_seq must be non-negative")
    if limit <= 0 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _assert_event_identity(record: ChatEventRecord, draft: ChatEventDraft) -> None:
    immutable = {
        "session_id": record.session_id,
        "namespace": record.namespace,
        "run_id": record.run_id,
        "source_index": record.source_index,
        "chat_message_id": record.chat_message_id,
        "event_type": record.event_type,
        "payload_json": record.payload_json,
        "created_at": record.created_at,
    }
    if immutable != draft.model_dump():
        raise ChatIdentityConflict(
            f"chat event identity drift for {draft.run_id!r} index {draft.source_index}"
        )


def _assert_message_identity(
    record: ChatMessageRecord, draft: ChatMessageDraft
) -> None:
    immutable = {
        "chat_message_id": record.chat_message_id,
        "namespace": record.namespace,
        "session_id": record.session_id,
        "run_id": record.run_id,
        "role": record.role,
        "content": record.content,
        "status": record.status,
    }
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
