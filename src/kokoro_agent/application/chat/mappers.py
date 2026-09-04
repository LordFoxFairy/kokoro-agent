"""Explicit mappers between Chat wire timestamps, domain facts, and views."""

from __future__ import annotations

from datetime import datetime

from kokoro_agent.application.chat.dto import (
    ChatEventView,
    ChatMessageView,
    ChatSessionView,
)
from kokoro_agent.domain.chat.models import (
    ChatEventRecord,
    ChatMessageRecord,
    ChatSessionRecord,
)
from kokoro_agent.domain.chat.time import epoch_millis_to_utc, normalize_utc_datetime


def wire_epoch_millis_to_utc(value: int) -> datetime:
    """Map the unchanged Agent epoch-millisecond wire field to a UTC instant."""

    return epoch_millis_to_utc(value)


def utc_to_wire_epoch_millis(value: datetime) -> int:
    """Map a UTC-aware Chat/domain instant back to the internal wire shape."""

    milliseconds = int(normalize_utc_datetime(value).timestamp() * 1000)
    if milliseconds < 0:
        raise ValueError("wire timestamp must be non-negative")
    return milliseconds


def chat_session_to_view(record: ChatSessionRecord) -> ChatSessionView:
    """Hide persistence scope while preserving the v1 session response shape."""

    return ChatSessionView(
        session_id=record.session_id,
        project_ref=record.project_ref,
        title=record.title,
        created_at=utc_to_wire_epoch_millis(record.created_at),
        updated_at=utc_to_wire_epoch_millis(record.updated_at),
    )


def chat_message_to_view(record: ChatMessageRecord) -> ChatMessageView:
    """Hide tenant and namespace from the transport response."""

    return ChatMessageView(
        chat_message_id=record.chat_message_id,
        session_id=record.session_id,
        run_id=record.run_id,
        role=record.role,
        content=record.content,
        status=record.status,
        seq=record.seq,
        created_at=utc_to_wire_epoch_millis(record.created_at),
        updated_at=utc_to_wire_epoch_millis(record.updated_at),
    )


def chat_event_to_view(record: ChatEventRecord) -> ChatEventView:
    """Hide tenant and namespace from the transport response."""

    return ChatEventView(
        chat_event_id=record.chat_event_id,
        session_id=record.session_id,
        run_id=record.run_id,
        source_index=record.source_index,
        chat_message_id=record.chat_message_id,
        event_type=record.event_type,
        payload_json=record.payload_json,
        seq=record.seq,
        created_at=utc_to_wire_epoch_millis(record.created_at),
    )


__all__ = [
    "chat_event_to_view",
    "chat_message_to_view",
    "chat_session_to_view",
    "utc_to_wire_epoch_millis",
    "wire_epoch_millis_to_utc",
]
