"""Repository port for Agent-owned user-visible chat facts.

The interface is transport- and database-neutral. PostgreSQL SQL lives in
``infrastructure.postgres_chat_repository``; chat projections and services
consume this port instead of a concrete database adapter.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol

from kokoro_agent.domain.chat.models import (
    ChatEventRecord,
    ChatMessageDraft,
    ChatMessageRecord,
    ChatProjection,
    ChatSessionRecord,
)
from kokoro_agent.domain.run.models import LeaseFence


class ChatIdentityConflict(RuntimeError):
    """A stable chat identity was reused for different immutable content."""


ChatFenceMode = Literal["active", "current_generation"]


class ChatRepository(Protocol):
    async def ensure_session(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        project_ref: str | None,
        title: str,
        updated_at: datetime,
    ) -> ChatSessionRecord: ...

    async def list_sessions(
        self,
        tenant_id: str,
        namespace: str,
        *,
        project_ref: str | None = None,
        after: tuple[datetime, str] | None = None,
        limit: int = 101,
    ) -> tuple[ChatSessionRecord, ...]: ...

    async def append(self, projection: ChatProjection) -> ChatEventRecord: ...

    async def append_fenced(
        self,
        projection: ChatProjection,
        lease: LeaseFence,
        *,
        mode: ChatFenceMode,
    ) -> ChatEventRecord | None: ...

    async def save_message(self, message: ChatMessageDraft) -> ChatMessageRecord: ...

    async def replay(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        after_seq: int = 0,
        limit: int = 500,
    ) -> tuple[ChatEventRecord, ...]: ...

    async def history(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        after_seq: int = 0,
        limit: int = 200,
    ) -> tuple[ChatMessageRecord, ...]: ...

    async def next_source_index(
        self, tenant_id: str, namespace: str, run_id: str
    ) -> int: ...

    async def watermark(
        self, tenant_id: str, namespace: str, session_id: str
    ) -> int: ...


__all__ = ["ChatFenceMode", "ChatIdentityConflict", "ChatRepository"]
