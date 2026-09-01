"""Identity-scoped query facade for GA-owned user-visible chat facts."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.chat.models import ChatEventType, ChatMessageStatus, ChatRole
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


class ChatQuery:
    """Derive GA namespace from trusted identity; callers never submit it."""

    def __init__(self, store: ChatStore) -> None:
        self._store = store

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
]
