"""PostgreSQL persistence for GA-owned chat messages and replay events."""

# psycopg's dict-row and dynamic SQL APIs are runtime-typed in the installed
# version; contract tests cover this adapter boundary.
# pyright: reportCallIssue=false, reportArgumentType=false, reportReturnType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportIncompatibleMethodOverride=false

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from kokoro_agent.chat.models import (
    ChatEventDraft,
    ChatEventRecord,
    ChatMessageDraft,
    ChatMessageRecord,
    ChatProjection,
    chat_event_id,
)
from kokoro_agent.storage.postgres import DEFAULT_PG_SCHEMA, connect_pg, ensure_schema, qualified

CHAT_MESSAGES_COLLECTION = "kokoro_agent_chat_messages"
CHAT_EVENTS_COLLECTION = "kokoro_agent_chat_events"
CHAT_SEQUENCES_COLLECTION = "kokoro_agent_chat_sequences"


class ChatStoreSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    database_url: str
    schema_name: str = DEFAULT_PG_SCHEMA


class ChatIdentityConflict(RuntimeError):
    """A stable chat identity was reused for different immutable content."""


class ChatStore(Protocol):
    async def append(self, projection: ChatProjection) -> ChatEventRecord: ...

    async def save_message(self, message: ChatMessageDraft) -> ChatMessageRecord: ...

    async def replay(
        self, namespace: str, session_id: str, *, after_seq: int = 0, limit: int = 500
    ) -> tuple[ChatEventRecord, ...]: ...

    async def history(
        self, namespace: str, session_id: str, *, after_seq: int = 0, limit: int = 200
    ) -> tuple[ChatMessageRecord, ...]: ...

    async def next_source_index(self, namespace: str, run_id: str) -> int: ...

    async def watermark(self, namespace: str, session_id: str) -> int: ...


class PgChatStore:
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
                    INSERT INTO {} (kind, namespace, session_id, seq)
                    VALUES (%s, %s, %s, 1)
                    ON CONFLICT (kind, namespace, session_id)
                    DO UPDATE SET seq = seq + 1
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
async def make_chat_store(
    settings: ChatStoreSettings,
) -> AsyncGenerator[PgChatStore, None]:
    store = PgChatStore(settings.database_url, settings.schema_name)
    await store.setup()
    try:
        yield store
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
    "ChatStore",
    "ChatIdentityConflict",
    "ChatStoreSettings",
    "PgChatStore",
    "make_chat_store",
]
