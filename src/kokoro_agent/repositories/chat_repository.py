"""Repository port for Agent-owned user-visible chat facts.

The interface is transport- and database-neutral. PostgreSQL SQL lives in
``infrastructure.postgres_chat_repository``; chat projections and services
consume this port instead of a concrete database adapter.
"""

from __future__ import annotations

from typing import Protocol

from kokoro_agent.chat.models import (
    ChatEventRecord,
    ChatMessageDraft,
    ChatMessageRecord,
    ChatProjection,
    ChatSessionRecord,
)


class ChatIdentityConflict(RuntimeError):
    """A stable chat identity was reused for different immutable content."""


class ChatRepository(Protocol):
    async def ensure_session(
        self,
        namespace: str,
        session_id: str,
        *,
        project_ref: str | None,
        title: str,
        updated_at: int,
    ) -> ChatSessionRecord: ...

    async def list_sessions(
        self,
        namespace: str,
        *,
        project_ref: str | None = None,
        after: tuple[int, str] | None = None,
        limit: int = 101,
    ) -> tuple[ChatSessionRecord, ...]: ...

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


__all__ = ["ChatIdentityConflict", "ChatRepository"]
