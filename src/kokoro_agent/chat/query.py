"""Identity-scoped query facade for GA-owned user-visible chat facts."""

from __future__ import annotations

import base64
import binascii
import json
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from kokoro_agent.chat.models import ChatEventType, ChatMessageStatus, ChatRole, ChatSessionRecord
from kokoro_agent.chat.store import ChatStore
from kokoro_agent.contract import ExecutionIdentity
from kokoro_agent.execution.scope import runtime_namespace


class _StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class ChatQueryRequest(_StrictModel):
    execution_identity: ExecutionIdentity
    session_id: str
    after_seq: Annotated[int, Field(ge=0)] = 0
    limit: Annotated[int, Field(gt=0, le=1000)] = 200


class ChatMessageView(_StrictModel):
    chat_message_id: str
    session_id: str
    run_id: str
    role: ChatRole
    content: str
    status: ChatMessageStatus
    seq: int
    created_at: int
    updated_at: int


class ChatEventView(_StrictModel):
    chat_event_id: str
    session_id: str
    run_id: str
    source_index: int
    chat_message_id: str | None = None
    event_type: ChatEventType
    payload_json: str
    seq: int
    created_at: int


class ChatHistoryPage(_StrictModel):
    messages: tuple[ChatMessageView, ...]
    next_seq: int


class ChatReplayPage(_StrictModel):
    events: tuple[ChatEventView, ...]
    next_seq: int
    watermark: int


class ChatSessionListRequest(_StrictModel):
    execution_identity: ExecutionIdentity
    project_ref: Annotated[str, Field(min_length=1)] | None = None
    cursor: str | None = None
    limit: Annotated[int, Field(gt=0, le=100)] = 50


class ChatSessionView(_StrictModel):
    session_id: str
    project_ref: str | None = None
    title: str
    created_at: int
    updated_at: int


class ChatSessionListPage(_StrictModel):
    sessions: tuple[ChatSessionView, ...]
    next_cursor: str | None = None


class _SessionCursor(_StrictModel):
    updated_at: int = Field(ge=0)
    session_id: str = Field(min_length=1)


class ChatQuery:
    """Derive GA namespace from trusted identity; callers never submit it."""

    def __init__(self, store: ChatStore) -> None:
        self._store = store

    async def ensure_session(
        self,
        identity: ExecutionIdentity,
        session_id: str,
        *,
        project_ref: str | None,
        title: str,
        updated_at: int,
    ) -> ChatSessionRecord:
        return await self._store.ensure_session(
            runtime_namespace(identity),
            session_id,
            project_ref=project_ref,
            title=title,
            updated_at=updated_at,
        )

    async def list_sessions(self, request: ChatSessionListRequest) -> ChatSessionListPage:
        after = _decode_cursor(request.cursor) if request.cursor else None
        records = await self._store.list_sessions(
            runtime_namespace(request.execution_identity),
            project_ref=request.project_ref,
            after=after,
            limit=request.limit + 1,
        )
        page = records[: request.limit]
        has_more = len(records) > request.limit
        return ChatSessionListPage(
            sessions=tuple(ChatSessionView.model_validate(record.model_dump()) for record in page),
            next_cursor=_encode_cursor(page[-1].updated_at, page[-1].session_id)
            if has_more and page
            else None,
        )

    async def history(self, request: ChatQueryRequest) -> ChatHistoryPage:
        namespace = runtime_namespace(request.execution_identity)
        records = await self._store.history(
            namespace,
            request.session_id,
            after_seq=request.after_seq,
            limit=request.limit,
        )
        messages = tuple(
            ChatMessageView.model_validate(record.model_dump(exclude={"namespace"}))
            for record in records
        )
        return ChatHistoryPage(
            messages=messages,
            next_seq=messages[-1].seq if messages else request.after_seq,
        )

    async def replay(self, request: ChatQueryRequest) -> ChatReplayPage:
        namespace = runtime_namespace(request.execution_identity)
        records = await self._store.replay(
            namespace,
            request.session_id,
            after_seq=request.after_seq,
            limit=request.limit,
        )
        events = tuple(
            ChatEventView.model_validate(record.model_dump(exclude={"namespace"}))
            for record in records
        )
        return ChatReplayPage(
            events=events,
            next_seq=events[-1].seq if events else request.after_seq,
            watermark=await self._store.watermark(namespace, request.session_id),
        )


__all__ = [
    "ChatEventView",
    "ChatHistoryPage",
    "ChatMessageView",
    "ChatQueryRequest",
    "ChatQuery",
    "ChatReplayPage",
    "ChatSessionListPage",
    "ChatSessionListRequest",
    "ChatSessionView",
]


def _encode_cursor(updated_at: int, session_id: str) -> str:
    raw = json.dumps(
        {"session_id": session_id, "updated_at": updated_at},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(value: str) -> tuple[int, str]:
    padded = value + "=" * ((4 - len(value) % 4) % 4)
    try:
        parsed = json.loads(base64.urlsafe_b64decode(padded).decode())
    except (UnicodeDecodeError, json.JSONDecodeError, binascii.Error) as error:
        raise ValueError("invalid_cursor") from error
    try:
        cursor = _SessionCursor.model_validate(parsed)
    except ValidationError as error:
        raise ValueError("invalid_cursor") from error
    return cursor.updated_at, cursor.session_id
