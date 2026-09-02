"""Application DTOs for the Agent chat query surface.

These models are the service boundary between the HTTP ingress and the chat
repository.  They intentionally do not know about HTTP response envelopes or
the PostgreSQL adapter.
"""

from __future__ import annotations

import base64
import binascii
import json
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from kokoro_agent.chat.models import ChatEventType, ChatMessageStatus, ChatRole
from kokoro_agent.contract import ExecutionIdentity


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


def encode_session_cursor(updated_at: int, session_id: str) -> str:
    raw = json.dumps(
        {"session_id": session_id, "updated_at": updated_at},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_session_cursor(value: str) -> tuple[int, str]:
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


__all__ = [
    "ChatEventView",
    "ChatHistoryPage",
    "ChatMessageView",
    "ChatQueryRequest",
    "ChatReplayPage",
    "ChatSessionListPage",
    "ChatSessionListRequest",
    "ChatSessionView",
    "decode_session_cursor",
    "encode_session_cursor",
]
