"""PostgreSQL adapter for the Agent chat repository."""

# psycopg's dict-row and dynamic SQL APIs are runtime-typed in the installed
# version; contract tests cover this adapter boundary.
# pyright: reportCallIssue=false, reportArgumentType=false, reportReturnType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportIncompatibleMethodOverride=false

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

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
from kokoro_agent.infrastructure.postgres import DEFAULT_PG_SCHEMA, connect_pg, ensure_schema, qualified
from kokoro_agent.repositories.chat_repository import ChatIdentityConflict

CHAT_MESSAGES_COLLECTION = "kokoro_agent_chat_messages"
CHAT_EVENTS_COLLECTION = "kokoro_agent_chat_events"
CHAT_SEQUENCES_COLLECTION = "kokoro_agent_chat_sequences"
CHAT_SESSIONS_COLLECTION = "kokoro_agent_chat_sessions"


class PostgresChatRepositorySettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    database_url: str
    schema_name: str = DEFAULT_PG_SCHEMA


class PostgresChatRepository:
    """Append-only events plus idempotent final-message projection."""

    def __init__(self, database_url: str, schema: str = DEFAULT_PG_SCHEMA) -> None:
        self._database_url = database_url
        self._schema = schema

    async def setup(self) -> None:
        async with connect_pg(self._database_url) as conn:
            await ensure_schema(conn, self._schema)
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        chat_event_id text NOT NULL,
                        namespace text NOT NULL,
                        session_id text NOT NULL,
                        run_id text NOT NULL,
                        source_index bigint NOT NULL,
                        chat_message_id text,
                        event_type text NOT NULL,
                        payload_json text NOT NULL,
                        created_at bigint NOT NULL,
                        seq bigint NOT NULL,
                        PRIMARY KEY (namespace, run_id, source_index),
                        UNIQUE (chat_event_id),
                        UNIQUE (namespace, session_id, seq)
                    )
                    """.format(qualified(self._schema, CHAT_EVENTS_COLLECTION))
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        chat_message_id text PRIMARY KEY,
                        namespace text NOT NULL,
                        session_id text NOT NULL,
                        run_id text NOT NULL,
                        role text NOT NULL,
                        content text NOT NULL,
                        status text NOT NULL,
                        created_at bigint NOT NULL,
                        updated_at bigint NOT NULL,
                        seq bigint NOT NULL,
                        UNIQUE (namespace, session_id, seq)
                    )
                    """.format(qualified(self._schema, CHAT_MESSAGES_COLLECTION))
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        kind text NOT NULL,
                        namespace text NOT NULL,
                        session_id text NOT NULL,
                        seq bigint NOT NULL,
                        PRIMARY KEY (kind, namespace, session_id)
                    )
                    """.format(qualified(self._schema, CHAT_SEQUENCES_COLLECTION))
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        namespace text NOT NULL,
                        session_id text NOT NULL,
                        project_ref text,
                        title text NOT NULL,
                        created_at bigint NOT NULL,
                        updated_at bigint NOT NULL,
                        PRIMARY KEY (namespace, session_id)
                    )
                    """.format(qualified(self._schema, CHAT_SESSIONS_COLLECTION))
                )

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

        table = qualified(self._schema, CHAT_SESSIONS_COLLECTION)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT session_id, project_ref, title, created_at, updated_at
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
                        SET updated_at = GREATEST(updated_at, %s)
                        WHERE namespace = %s AND session_id = %s
                        RETURNING session_id, project_ref, title, created_at, updated_at
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
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (namespace, session_id) DO NOTHING
                    RETURNING session_id, project_ref, title, created_at, updated_at
                    """,
                    (namespace, session_id, project_ref, title.strip()[:80], updated_at, updated_at),
                )
                inserted = await cur.fetchone()
                if inserted is not None:
                    return ChatSessionRecord(**dict(inserted))

                await cur.execute(
                    f"""
                    SELECT session_id, project_ref, title, created_at, updated_at
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
            raise ChatIdentityConflict(f"chat session identity drift for {session_id!r}")
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
            clauses.append("(updated_at < %s OR (updated_at = %s AND session_id > %s))")
            params.extend([after[0], after[0], after[1]])
        params.append(limit)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT session_id, project_ref, title, created_at, updated_at
                    FROM {qualified(self._schema, CHAT_SESSIONS_COLLECTION)}
                    WHERE {' AND '.join(clauses)}
                    ORDER BY updated_at DESC, session_id ASC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = await cur.fetchall()
        return tuple(ChatSessionRecord(**dict(row)) for row in rows)

    async def append(self, projection: ChatProjection) -> ChatEventRecord:
        draft = projection.event
        existing = await self._get_event(draft.namespace, draft.run_id, draft.source_index)
        if existing is not None:
            record = existing
            _assert_event_identity(record, draft)
        else:
            seq = await self._next_seq("event", draft.namespace, draft.session_id)
            record = ChatEventRecord(
                **draft.model_dump(),
                chat_event_id=chat_event_id(draft.namespace, draft.run_id, draft.source_index),
                seq=seq,
            )
            async with connect_pg(self._database_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO {} (
                            namespace, session_id, run_id, source_index, chat_message_id,
                            event_type, payload_json, created_at, seq, chat_event_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (namespace, run_id, source_index) DO NOTHING
                        RETURNING chat_event_id
                        """.format(qualified(self._schema, CHAT_EVENTS_COLLECTION)),
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
                    if await cur.fetchone() is None:
                        existing = await self._get_event(
                            draft.namespace, draft.run_id, draft.source_index
                        )
                        if existing is None:
                            raise RuntimeError("chat event insert raced and row is missing")
                        _assert_event_identity(existing, draft)
                        record = existing
        if projection.message is not None:
            await self.save_message(projection.message)
        return record

    async def save_message(self, message: ChatMessageDraft) -> ChatMessageRecord:
        existing = await self._get_message(message.chat_message_id)
        if existing is not None:
            _assert_message_identity(existing, message)
            return existing
        seq = await self._next_seq("message", message.namespace, message.session_id)
        record = ChatMessageRecord(**message.model_dump(), seq=seq)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (
                        chat_message_id, namespace, session_id, run_id, role,
                        content, status, created_at, updated_at, seq
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chat_message_id) DO NOTHING
                    RETURNING chat_message_id
                    """.format(qualified(self._schema, CHAT_MESSAGES_COLLECTION)),
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
                if await cur.fetchone() is None:
                    existing = await self._get_message(message.chat_message_id)
                    if existing is None:
                        raise RuntimeError("chat message insert raced and row is missing")
                    _assert_message_identity(existing, message)
                    return existing
        return record

    async def replay(
        self, namespace: str, session_id: str, *, after_seq: int = 0, limit: int = 500
    ) -> tuple[ChatEventRecord, ...]:
        _validate_page(after_seq, limit)
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT chat_event_id, namespace, session_id, run_id, source_index,
                           chat_message_id, event_type, payload_json, created_at, seq
                    FROM {}
                    WHERE namespace = %s AND session_id = %s AND seq > %s
                    ORDER BY seq ASC
                    LIMIT %s
                    """.format(qualified(self._schema, CHAT_EVENTS_COLLECTION)),
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
                           content, status, created_at, updated_at, seq
                    FROM {}
                    WHERE namespace = %s AND session_id = %s AND seq > %s
                    ORDER BY seq ASC
                    LIMIT %s
                    """.format(qualified(self._schema, CHAT_MESSAGES_COLLECTION)),
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
                    """.format(qualified(self._schema, CHAT_EVENTS_COLLECTION)),
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
                    """.format(qualified(self._schema, CHAT_EVENTS_COLLECTION)),
                    (namespace, session_id),
                )
                row = await cur.fetchone()
        value = row["seq"] if row is not None else None
        return 0 if value is None else int(value)

    async def _next_seq(self, kind: str, namespace: str, session_id: str) -> int:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} AS current_sequence (kind, namespace, session_id, seq)
                    VALUES (%s, %s, %s, 1)
                    ON CONFLICT (kind, namespace, session_id)
                    DO UPDATE SET seq = current_sequence.seq + 1
                    RETURNING seq
                    """.format(qualified(self._schema, CHAT_SEQUENCES_COLLECTION)),
                    (kind, namespace, session_id),
                )
                row = await cur.fetchone()
        if row is None:
            raise RuntimeError(f"failed to allocate {kind} sequence for {session_id!r}")
        return int(row["seq"])

    async def _get_event(
        self, namespace: str, run_id: str, source_index: int
    ) -> ChatEventRecord | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT chat_event_id, namespace, session_id, run_id, source_index,
                           chat_message_id, event_type, payload_json, created_at, seq
                    FROM {}
                    WHERE namespace = %s AND run_id = %s AND source_index = %s
                    """.format(qualified(self._schema, CHAT_EVENTS_COLLECTION)),
                    (namespace, run_id, source_index),
                )
                row = await cur.fetchone()
        return None if row is None else ChatEventRecord(**dict(row))

    async def _get_message(self, chat_message_id: str) -> ChatMessageRecord | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT chat_message_id, namespace, session_id, run_id, role,
                           content, status, created_at, updated_at, seq
                    FROM {}
                    WHERE chat_message_id = %s
                    """.format(qualified(self._schema, CHAT_MESSAGES_COLLECTION)),
                    (chat_message_id,),
                )
                row = await cur.fetchone()
        return None if row is None else ChatMessageRecord(**dict(row))


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
    "CHAT_EVENTS_COLLECTION",
    "CHAT_MESSAGES_COLLECTION",
    "CHAT_SEQUENCES_COLLECTION",
    "CHAT_SESSIONS_COLLECTION",
    "PostgresChatRepository",
    "PostgresChatRepositorySettings",
    "make_chat_repository",
]
