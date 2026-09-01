"""In-process ChatStore used by hermetic worker and emitter tests."""

from __future__ import annotations

from kokoro_agent.chat.models import (
    ChatEventRecord,
    ChatMessageDraft,
    ChatMessageRecord,
    ChatProjection,
    chat_event_id,
)
from kokoro_agent.chat.store import ChatIdentityConflict


class FakeChatStore:
    def __init__(self, order: list[str] | None = None) -> None:
        self.order = order
        self.records: list[ChatEventRecord] = []
        self.messages: dict[str, ChatMessageRecord] = {}

    async def append(self, projection: ChatProjection) -> ChatEventRecord:
        if self.order is not None:
            self.order.append("chat")
        for existing in self.records:
            if (
                existing.run_id == projection.event.run_id
                and existing.source_index == projection.event.source_index
            ):
                if (
                    existing.event_type != projection.event.event_type
                    or existing.payload_json != projection.event.payload_json
                    or existing.session_id != projection.event.session_id
                ):
                    raise ChatIdentityConflict("fake chat event identity drift")
                return existing
        record = ChatEventRecord(
            **projection.event.model_dump(),
            chat_event_id=chat_event_id(
                projection.event.namespace, projection.event.run_id, projection.event.source_index
            ),
            seq=len(self.records) + 1,
        )
        self.records.append(record)
        if projection.message is not None:
            await self.save_message(projection.message)
        return record

    async def save_message(self, message: ChatMessageDraft) -> ChatMessageRecord:
        existing = self.messages.get(message.chat_message_id)
        if existing is not None:
            if (
                existing.session_id != message.session_id
                or existing.run_id != message.run_id
                or existing.role != message.role
                or existing.content != message.content
                or existing.status != message.status
            ):
                raise ChatIdentityConflict("fake chat message identity drift")
            return existing
        record = ChatMessageRecord(
            **message.model_dump(), seq=len(self.messages) + 1
        )
        self.messages[record.chat_message_id] = record
        return record

    async def replay(
        self, namespace: str, session_id: str, *, after_seq: int = 0, limit: int = 500
    ) -> tuple[ChatEventRecord, ...]:
        return tuple(
            event
            for event in self.records
            if event.namespace == namespace and event.session_id == session_id and event.seq > after_seq
        )[:limit]

    async def history(
        self, namespace: str, session_id: str, *, after_seq: int = 0, limit: int = 200
    ) -> tuple[ChatMessageRecord, ...]:
        return tuple(
            message
            for message in self.messages.values()
            if message.namespace == namespace and message.session_id == session_id and message.seq > after_seq
        )[:limit]

    async def next_source_index(self, namespace: str, run_id: str) -> int:
        indices = [event.source_index for event in self.records if event.namespace == namespace and event.run_id == run_id]
        return max(indices, default=-1) + 1

    async def watermark(self, namespace: str, session_id: str) -> int:
        return max(
            (event.seq for event in self.records if event.namespace == namespace and event.session_id == session_id),
            default=0,
        )


__all__ = ["FakeChatStore"]
