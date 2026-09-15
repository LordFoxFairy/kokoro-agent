"""Application service for identity-scoped chat queries."""

from __future__ import annotations

from kokoro_agent.application.chat.dto import (
    ChatHistoryPage,
    ChatQueryRequest,
    ChatReplayPage,
    ChatSessionListPage,
    ChatSessionListRequest,
    decode_session_cursor,
    encode_session_cursor,
)
from kokoro_agent.application.chat.mappers import (
    chat_event_to_view,
    chat_message_to_view,
    chat_session_to_view,
    utc_to_wire_epoch_millis,
    wire_epoch_millis_to_utc,
)
from kokoro_agent.domain.chat.models import ChatSessionRecord
from kokoro_agent.domain.chat.repositories import ChatRepository
from kokoro_agent.domain.run.scope import runtime_namespace
from kokoro_agent.protocol import ExecutionIdentity


class ChatService:
    """Apply identity scoping and map repository records to API views."""

    def __init__(self, repository: ChatRepository) -> None:
        self._repository = repository

    async def ensure_session(
        self,
        identity: ExecutionIdentity,
        session_id: str,
        *,
        project_ref: str | None,
        title: str,
        updated_at: int,
    ) -> ChatSessionRecord:
        return await self._repository.ensure_session(
            identity.tenant_ref,
            runtime_namespace(identity),
            session_id,
            project_ref=project_ref,
            title=title,
            updated_at=wire_epoch_millis_to_utc(updated_at),
        )

    async def list_sessions(
        self, request: ChatSessionListRequest
    ) -> ChatSessionListPage:
        decoded = decode_session_cursor(request.cursor) if request.cursor else None
        after = (
            (wire_epoch_millis_to_utc(decoded[0]), decoded[1])
            if decoded is not None
            else None
        )
        namespace = runtime_namespace(request.execution_identity)
        records = await self._repository.list_sessions(
            request.execution_identity.tenant_ref,
            namespace,
            project_ref=request.project_ref,
            after=after,
            limit=request.limit + 1,
        )
        page = records[: request.limit]
        has_more = len(records) > request.limit
        return ChatSessionListPage(
            sessions=tuple(chat_session_to_view(record) for record in page),
            next_cursor=encode_session_cursor(
                utc_to_wire_epoch_millis(page[-1].updated_at), page[-1].session_id
            )
            if has_more and page
            else None,
        )

    async def history(self, request: ChatQueryRequest) -> ChatHistoryPage:
        namespace = runtime_namespace(request.execution_identity)
        records = await self._repository.history(
            request.execution_identity.tenant_ref,
            namespace,
            request.session_id,
            after_seq=request.after_seq,
            limit=request.limit,
        )
        messages = tuple(chat_message_to_view(record) for record in records)
        return ChatHistoryPage(
            messages=messages,
            next_seq=messages[-1].seq if messages else request.after_seq,
        )

    async def replay(self, request: ChatQueryRequest) -> ChatReplayPage:
        namespace = runtime_namespace(request.execution_identity)
        records = await self._repository.replay(
            request.execution_identity.tenant_ref,
            namespace,
            request.session_id,
            after_seq=request.after_seq,
            limit=request.limit,
        )
        events = tuple(chat_event_to_view(record) for record in records)
        return ChatReplayPage(
            events=events,
            next_seq=events[-1].seq if events else request.after_seq,
            watermark=await self._repository.watermark(
                request.execution_identity.tenant_ref, namespace, request.session_id
            ),
        )


__all__ = ["ChatService"]
