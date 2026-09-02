"""Application service for identity-scoped chat queries."""

from __future__ import annotations

from kokoro_agent.chat.models import ChatSessionRecord
from kokoro_agent.repositories.chat_repository import ChatRepository
from kokoro_agent.contract import ExecutionIdentity
from kokoro_agent.execution.scope import runtime_namespace
from kokoro_agent.services.chat_dto import (
    ChatEventView,
    ChatHistoryPage,
    ChatMessageView,
    ChatQueryRequest,
    ChatReplayPage,
    ChatSessionListPage,
    ChatSessionListRequest,
    ChatSessionView,
    decode_session_cursor,
    encode_session_cursor,
)


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
            runtime_namespace(identity),
            session_id,
            project_ref=project_ref,
            title=title,
            updated_at=updated_at,
        )

    async def list_sessions(self, request: ChatSessionListRequest) -> ChatSessionListPage:
        after = decode_session_cursor(request.cursor) if request.cursor else None
        records = await self._repository.list_sessions(
            runtime_namespace(request.execution_identity),
            project_ref=request.project_ref,
            after=after,
            limit=request.limit + 1,
        )
        page = records[: request.limit]
        has_more = len(records) > request.limit
        return ChatSessionListPage(
            sessions=tuple(ChatSessionView.model_validate(record.model_dump()) for record in page),
            next_cursor=encode_session_cursor(page[-1].updated_at, page[-1].session_id)
            if has_more and page
            else None,
        )

    async def history(self, request: ChatQueryRequest) -> ChatHistoryPage:
        namespace = runtime_namespace(request.execution_identity)
        records = await self._repository.history(
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
        records = await self._repository.replay(
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
            watermark=await self._repository.watermark(namespace, request.session_id),
        )



__all__ = ["ChatService"]
