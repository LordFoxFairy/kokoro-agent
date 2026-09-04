"""Strict GA chat fact shapes, separate from LangChain native state."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, Self
from uuid import NAMESPACE_URL, uuid5

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StringConstraints, model_validator

from kokoro_agent.domain.chat.time import normalize_utc_datetime

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
UtcDateTime = Annotated[datetime, AfterValidator(normalize_utc_datetime)]
ChatRole = Literal["user", "assistant"]
ChatMessageStatus = Literal["completed", "failed"]
ChatEventType = Literal[
    "run.started",
    "assistant.delta",
    "assistant.completed",
    "activity",
    "interaction",
    "delivery",
    "run.completed",
    "run.failed",
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class ChatMessageDraft(_StrictModel):
    chat_message_id: NonEmptyStr
    tenant_id: NonEmptyStr
    namespace: NonEmptyStr
    session_id: NonEmptyStr
    run_id: NonEmptyStr
    role: ChatRole
    content: str
    status: ChatMessageStatus
    created_at: UtcDateTime
    updated_at: UtcDateTime


class ChatMessageRecord(ChatMessageDraft):
    seq: Annotated[int, Field(ge=1)]


class ChatSessionRecord(_StrictModel):
    """Durable identity-scoped session metadata used by the Chat list query."""

    tenant_id: NonEmptyStr
    namespace: NonEmptyStr
    session_id: NonEmptyStr
    project_ref: NonEmptyStr | None = None
    title: NonEmptyStr
    created_at: UtcDateTime
    updated_at: UtcDateTime


class ChatEventDraft(_StrictModel):
    tenant_id: NonEmptyStr
    namespace: NonEmptyStr
    session_id: NonEmptyStr
    run_id: NonEmptyStr
    source_index: Annotated[int, Field(ge=0)]
    chat_message_id: NonEmptyStr | None = None
    event_type: ChatEventType
    payload_json: str
    created_at: UtcDateTime


class ChatEventRecord(ChatEventDraft):
    chat_event_id: NonEmptyStr
    seq: Annotated[int, Field(ge=1)]


class ChatProjection(_StrictModel):
    event: ChatEventDraft
    message: ChatMessageDraft | None = None

    @model_validator(mode="after")
    def validate_lineage(self) -> Self:
        message = self.message
        if message is not None and (
            message.tenant_id != self.event.tenant_id
            or message.namespace != self.event.namespace
            or message.session_id != self.event.session_id
            or message.run_id != self.event.run_id
            or self.event.chat_message_id != message.chat_message_id
        ):
            raise ValueError("chat projection event and message scope must match")
        return self


def assistant_message_id(namespace: str, run_id: str, native_segment_id: str) -> str:
    """Create a stable GA ID without reusing LangChain's native message ID."""

    return f"msg_{uuid5(NAMESPACE_URL, f'ga-chat/{namespace}/{run_id}/{native_segment_id}').hex}"


def chat_event_id(namespace: str, run_id: str, source_index: int) -> str:
    """Create an idempotent GA event ID for one stable execution event index."""

    return f"cev_{uuid5(NAMESPACE_URL, f'ga-chat/{namespace}/{run_id}/event/{source_index}').hex}"


__all__ = [
    "ChatEventDraft",
    "ChatEventRecord",
    "ChatEventType",
    "ChatMessageDraft",
    "ChatMessageRecord",
    "ChatSessionRecord",
    "ChatProjection",
    "UtcDateTime",
    "assistant_message_id",
    "chat_event_id",
    "normalize_utc_datetime",
]
