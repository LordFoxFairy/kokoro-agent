"""In-process ChatRepository used by hermetic worker and emitter tests."""

from __future__ import annotations

from datetime import datetime

from kokoro_agent.domain.chat.models import (
    ChatEventRecord,
    ChatMessageDraft,
    ChatMessageRecord,
    ChatProjection,
    ChatSessionRecord,
    chat_event_id,
)
from kokoro_agent.domain.chat.repositories import (
    ChatFenceMode,
    ChatIdentityConflict,
)
from kokoro_agent.domain.run.models import LeaseFence


class FakeChatRepository:
    def __init__(self, order: list[str] | None = None) -> None:
        self.order = order
        self.records: list[ChatEventRecord] = []
        self.messages: dict[tuple[str, str], ChatMessageRecord] = {}
        self.sessions: dict[tuple[str, str, str], ChatSessionRecord] = {}

    async def ensure_session(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        project_ref: str | None,
        title: str,
        updated_at: datetime,
    ) -> ChatSessionRecord:
        key = (tenant_id, namespace, session_id)
        existing = self.sessions.get(key)
        if existing is not None:
            if existing.project_ref != project_ref:
                raise ChatIdentityConflict("fake chat session identity drift")
            if updated_at <= existing.updated_at:
                return existing
            updated = existing.model_copy(update={"updated_at": updated_at})
            self.sessions[key] = updated
            return updated
        record = ChatSessionRecord(
            tenant_id=tenant_id,
            namespace=namespace,
            session_id=session_id,
            project_ref=project_ref,
            title=title.strip()[:80],
            created_at=updated_at,
            updated_at=updated_at,
        )
        self.sessions[key] = record
        return record

    async def list_sessions(
        self,
        tenant_id: str,
        namespace: str,
        *,
        project_ref: str | None = None,
        after: tuple[datetime, str] | None = None,
        limit: int = 101,
    ) -> tuple[ChatSessionRecord, ...]:
        records = [
            record
            for (record_tenant, record_namespace, _), record in self.sessions.items()
            if record_tenant == tenant_id
            and record_namespace == namespace
            and (project_ref is None or record.project_ref == project_ref)
        ]
        records.sort(key=lambda record: (-record.updated_at.timestamp(), record.session_id))
        if after is not None:
            records = [
                record
                for record in records
                if record.updated_at < after[0]
                or (record.updated_at == after[0] and record.session_id > after[1])
            ]
        return tuple(records[:limit])

    async def append(self, projection: ChatProjection) -> ChatEventRecord:
        if self.order is not None:
            self.order.append("chat")
        for existing in self.records:
            if (
                existing.tenant_id == projection.event.tenant_id
                and existing.namespace == projection.event.namespace
                and existing.run_id == projection.event.run_id
                and existing.source_index == projection.event.source_index
            ):
                if (
                    existing.event_type != projection.event.event_type
                    or existing.payload_json != projection.event.payload_json
                    or existing.session_id != projection.event.session_id
                    or existing.created_at != projection.event.created_at
                ):
                    raise ChatIdentityConflict("fake chat event identity drift")
                return existing
        seq = (
            max(
                (
                    event.seq
                    for event in self.records
                    if event.tenant_id == projection.event.tenant_id
                    and event.namespace == projection.event.namespace
                    and event.session_id == projection.event.session_id
                ),
                default=0,
            )
            + 1
        )
        record = ChatEventRecord(
            **projection.event.model_dump(),
            chat_event_id=chat_event_id(
                projection.event.namespace,
                projection.event.run_id,
                projection.event.source_index,
            ),
            seq=seq,
        )
        self.records.append(record)
        if projection.message is not None:
            await self.save_message(projection.message)
        return record

    async def append_fenced(
        self,
        projection: ChatProjection,
        lease: LeaseFence,
        *,
        mode: ChatFenceMode,
    ) -> ChatEventRecord | None:
        del lease, mode
        return await self.append(projection)

    async def save_message(self, message: ChatMessageDraft) -> ChatMessageRecord:
        key = (message.tenant_id, message.chat_message_id)
        existing = self.messages.get(key)
        if existing is not None:
            if (
                existing.namespace != message.namespace
                or existing.session_id != message.session_id
                or existing.run_id != message.run_id
                or existing.role != message.role
                or existing.content != message.content
                or existing.status != message.status
            ):
                raise ChatIdentityConflict("fake chat message identity drift")
            return existing
        seq = (
            max(
                (
                    item.seq
                    for item in self.messages.values()
                    if item.tenant_id == message.tenant_id
                    and item.namespace == message.namespace
                    and item.session_id == message.session_id
                ),
                default=0,
            )
            + 1
        )
        record = ChatMessageRecord(**message.model_dump(), seq=seq)
        self.messages[key] = record
        return record

    async def replay(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        after_seq: int = 0,
        limit: int = 500,
    ) -> tuple[ChatEventRecord, ...]:
        events = sorted(
            (
                event
                for event in self.records
                if event.tenant_id == tenant_id
                and event.namespace == namespace
                and event.session_id == session_id
                and event.seq > after_seq
            ),
            key=lambda event: event.seq,
        )
        return tuple(events[:limit])

    async def history(
        self,
        tenant_id: str,
        namespace: str,
        session_id: str,
        *,
        after_seq: int = 0,
        limit: int = 200,
    ) -> tuple[ChatMessageRecord, ...]:
        messages = sorted(
            (
                message
                for message in self.messages.values()
                if message.tenant_id == tenant_id
                and message.namespace == namespace
                and message.session_id == session_id
                and message.seq > after_seq
            ),
            key=lambda message: message.seq,
        )
        return tuple(messages[:limit])

    async def next_source_index(self, tenant_id: str, namespace: str, run_id: str) -> int:
        indices = [
            event.source_index
            for event in self.records
            if event.tenant_id == tenant_id
            and event.namespace == namespace
            and event.run_id == run_id
        ]
        return max(indices, default=-1) + 1

    async def watermark(self, tenant_id: str, namespace: str, session_id: str) -> int:
        return max(
            (
                event.seq
                for event in self.records
                if event.tenant_id == tenant_id
                and event.namespace == namespace
                and event.session_id == session_id
            ),
            default=0,
        )


__all__ = ["FakeChatRepository"]
