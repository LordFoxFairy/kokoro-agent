"""Tenant lineage and timestamp mapping for durable Chat facts."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from kokoro_agent.application.chat.dto import ChatQueryRequest
from kokoro_agent.application.chat.service import ChatService
from kokoro_agent.domain.chat.models import (
    ChatEventDraft,
    ChatMessageDraft,
    ChatProjection,
)
from kokoro_agent.domain.chat.projection import project_chat_fact
from kokoro_agent.domain.run.scope import runtime_namespace
from kokoro_agent.infrastructure.chat_mappers import chat_event_from_row
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, MessageCompletedPayload
from support.chat import FakeChatRepository


def _identity(tenant: str = "tenant-a", subject: str = "subject") -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref=tenant,
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref=subject),
        identity_assertion_ref="assertion",
    )


def _instant(milliseconds: int) -> datetime:
    seconds, remainder = divmod(milliseconds, 1000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(
        microsecond=remainder * 1000
    )


def test_chat_domain_facts_carry_tenant_and_utc_aware_database_time() -> None:
    instant = _instant(1_234)
    event = ChatEventDraft(
        tenant_id="tenant-a",
        namespace="ga:scope-a",
        session_id="session-a",
        run_id="run-a",
        source_index=0,
        event_type="run.started",
        payload_json='{"status":"running"}',
        created_at=instant,
    )

    assert event.tenant_id == "tenant-a"
    assert event.created_at == instant
    assert event.created_at.tzinfo is UTC
    assert event.created_at.microsecond == 234_000

    with pytest.raises(ValidationError):
        ChatMessageDraft(
            tenant_id="tenant-a",
            namespace="ga:scope-a",
            session_id="session-a",
            run_id="run-a",
            chat_message_id="message-a",
            role="user",
            content="hello",
            status="completed",
            created_at=datetime(2026, 9, 4, 12, 0, 0),
            updated_at=instant,
        )


def test_chat_projection_maps_one_tenant_lineage_to_event_and_message() -> None:
    projection = project_chat_fact(
        tenant_id="tenant-a",
        namespace="ga:scope-a",
        session_id="session-a",
        run_id="run-a",
        source_index=1,
        created_at=_instant(2_000),
        payload=MessageCompletedPayload(segment_id="segment-a", content="answer"),
    )

    assert projection is not None
    assert projection.event.tenant_id == "tenant-a"
    assert projection.event.namespace == "ga:scope-a"
    assert projection.message is not None
    assert projection.message.tenant_id == "tenant-a"
    assert projection.message.created_at == _instant(2_000)


def test_chat_row_mapper_rejects_unknown_event_type_before_domain_mapping() -> None:
    instant = _instant(3_000)
    with pytest.raises(TypeError, match="event_type"):
        chat_event_from_row(
            {
                "chat_event_id": "event-a",
                "tenant_id": "tenant-a",
                "namespace": "ga:scope-a",
                "session_id": "session-a",
                "run_id": "run-a",
                "source_index": 0,
                "chat_message_id": None,
                "event_type": "private.execution",
                "payload_json": "{}",
                "created_at": instant,
                "seq": 1,
            }
        )


async def test_chat_query_keeps_epoch_millisecond_wire_shape_and_isolates_tenant() -> None:
    store = FakeChatRepository()
    owner = _identity("tenant-a")
    other = _identity("tenant-b")
    namespace = runtime_namespace(owner)
    await store.append(
        ChatProjection(
            event=ChatEventDraft(
                tenant_id=owner.tenant_ref,
                namespace=namespace,
                session_id="shared-session",
                run_id="run-a",
                source_index=0,
                chat_message_id="message-a",
                event_type="run.started",
                payload_json='{"status":"running"}',
                created_at=_instant(4_321),
            ),
            message=ChatMessageDraft(
                tenant_id=owner.tenant_ref,
                namespace=namespace,
                session_id="shared-session",
                run_id="run-a",
                chat_message_id="message-a",
                role="user",
                content="private",
                status="completed",
                created_at=_instant(4_321),
                updated_at=_instant(4_321),
            ),
        )
    )

    service = ChatService(store)
    owner_history = await service.history(
        ChatQueryRequest(execution_identity=owner, session_id="shared-session")
    )
    other_history = await service.history(
        ChatQueryRequest(execution_identity=other, session_id="shared-session")
    )

    assert owner_history.messages[0].created_at == 4_321
    assert owner_history.messages[0].updated_at == 4_321
    assert "tenant_id" not in owner_history.messages[0].model_dump()
    assert other_history.messages == ()


async def test_chat_repository_scope_isolates_tenants_with_the_same_namespace() -> None:
    store = FakeChatRepository()
    instant = _instant(5_000)
    for tenant, content in (("tenant-a", "private-a"), ("tenant-b", "private-b")):
        await store.save_message(
            ChatMessageDraft(
                tenant_id=tenant,
                namespace="shared-namespace",
                session_id="shared-session",
                run_id=f"run-{tenant}",
                chat_message_id=f"message-{tenant}",
                role="user",
                content=content,
                status="completed",
                created_at=instant,
                updated_at=instant,
            )
        )

    tenant_a_messages = await store.history("tenant-a", "shared-namespace", "shared-session")
    tenant_b_messages = await store.history("tenant-b", "shared-namespace", "shared-session")

    assert [message.content for message in tenant_a_messages] == ["private-a"]
    assert [message.content for message in tenant_b_messages] == ["private-b"]
