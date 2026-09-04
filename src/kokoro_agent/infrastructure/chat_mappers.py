"""Strict PostgreSQL-row mappers for the Chat persistence boundary."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from kokoro_agent.domain.chat.models import (
    ChatEventRecord,
    ChatEventType,
    ChatMessageRecord,
    ChatSessionRecord,
    ChatMessageStatus,
    ChatRole,
)
from kokoro_agent.domain.chat.time import normalize_utc_datetime

Row = Mapping[str, object]


def _value(row: Row, key: str) -> object:
    try:
        return row[key]
    except KeyError as error:
        raise RuntimeError(f"Chat row is missing {key!r}") from error


def _text(row: Row, key: str) -> str:
    value = _value(row, key)
    if not isinstance(value, str):
        raise TypeError(f"Chat row field {key!r} must be text")
    return value


def _nullable_text(row: Row, key: str) -> str | None:
    value = _value(row, key)
    if value is not None and not isinstance(value, str):
        raise TypeError(f"Chat row field {key!r} must be text or NULL")
    return value


def _integer(row: Row, key: str) -> int:
    value = _value(row, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"Chat row field {key!r} must be an integer")
    return value


def _timestamp(row: Row, key: str) -> datetime:
    value = _value(row, key)
    if not isinstance(value, datetime):
        raise TypeError(f"Chat row field {key!r} must be a datetime")
    return normalize_utc_datetime(value)


def _event_type(row: Row, key: str) -> ChatEventType:
    value = _text(row, key)
    if value == "run.started":
        return value
    if value == "assistant.delta":
        return value
    if value == "assistant.completed":
        return value
    if value == "activity":
        return value
    if value == "interaction":
        return value
    if value == "delivery":
        return value
    if value == "run.completed":
        return value
    if value == "run.failed":
        return value
    raise TypeError(f"Chat row field {key!r} has unsupported value")


def _role(row: Row, key: str) -> ChatRole:
    value = _text(row, key)
    if value == "user":
        return value
    if value == "assistant":
        return value
    raise TypeError(f"Chat row field {key!r} has unsupported value")


def _status(row: Row, key: str) -> ChatMessageStatus:
    value = _text(row, key)
    if value == "completed":
        return value
    if value == "failed":
        return value
    raise TypeError(f"Chat row field {key!r} has unsupported value")


def chat_session_from_row(row: Row) -> ChatSessionRecord:
    return ChatSessionRecord(
        tenant_id=_text(row, "tenant_id"),
        namespace=_text(row, "namespace"),
        session_id=_text(row, "session_id"),
        project_ref=_nullable_text(row, "project_ref"),
        title=_text(row, "title"),
        created_at=_timestamp(row, "created_at"),
        updated_at=_timestamp(row, "updated_at"),
    )


def chat_event_from_row(row: Row) -> ChatEventRecord:
    return ChatEventRecord(
        chat_event_id=_text(row, "chat_event_id"),
        tenant_id=_text(row, "tenant_id"),
        namespace=_text(row, "namespace"),
        session_id=_text(row, "session_id"),
        run_id=_text(row, "run_id"),
        source_index=_integer(row, "source_index"),
        chat_message_id=_nullable_text(row, "chat_message_id"),
        event_type=_event_type(row, "event_type"),
        payload_json=_text(row, "payload_json"),
        created_at=_timestamp(row, "created_at"),
        seq=_integer(row, "seq"),
    )


def chat_message_from_row(row: Row) -> ChatMessageRecord:
    return ChatMessageRecord(
        chat_message_id=_text(row, "chat_message_id"),
        tenant_id=_text(row, "tenant_id"),
        namespace=_text(row, "namespace"),
        session_id=_text(row, "session_id"),
        run_id=_text(row, "run_id"),
        role=_role(row, "role"),
        content=_text(row, "content"),
        status=_status(row, "status"),
        created_at=_timestamp(row, "created_at"),
        updated_at=_timestamp(row, "updated_at"),
        seq=_integer(row, "seq"),
    )


__all__ = [
    "chat_event_from_row",
    "chat_message_from_row",
    "chat_session_from_row",
]
