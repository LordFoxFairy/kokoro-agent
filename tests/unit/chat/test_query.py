"""GA chat query derives isolation from ExecutionIdentity."""

from kokoro_agent.chat.models import ChatEventDraft, ChatMessageDraft, ChatProjection
from kokoro_agent.chat.query import ChatQueryRequest, ChatQuery
from kokoro_agent.contract import ExecutionIdentity, IdentityRef
from kokoro_agent.execution.scope import runtime_namespace
from support.chat import FakeChatStore


def _identity(subject: str) -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="project", opaque_ref=subject),
        identity_assertion_ref="assertion",
    )


async def test_history_and_replay_are_identity_scoped_without_caller_namespace() -> None:
    store = FakeChatStore()
    owner = _identity("owner")
    other = _identity("other")
    namespace = runtime_namespace(owner)
    await store.save_message(
        ChatMessageDraft(
            chat_message_id="message-1",
            namespace=namespace,
            session_id="same-session",
            run_id="run-1",
            role="user",
            content="private",
            status="completed",
            created_at=1,
            updated_at=1,
        )
    )
    await store.append(
        ChatProjection(
            event=ChatEventDraft(
                namespace=namespace,
                session_id="same-session",
                run_id="run-1",
                source_index=0,
                event_type="run.started",
                payload_json='{"status":"running"}',
                created_at=1,
            )
        )
    )
    query = ChatQuery(store)

    owner_history = await query.history(
        ChatQueryRequest(execution_identity=owner, session_id="same-session")
    )
    owner_replay = await query.replay(
        ChatQueryRequest(execution_identity=owner, session_id="same-session")
    )
    other_history = await query.history(
        ChatQueryRequest(execution_identity=other, session_id="same-session")
    )
    other_replay = await query.replay(
        ChatQueryRequest(execution_identity=other, session_id="same-session")
    )

    assert [message.content for message in owner_history.messages] == ["private"]
    assert [event.event_type for event in owner_replay.events] == ["run.started"]
    assert owner_replay.watermark == 1
    assert other_history.messages == ()
    assert other_replay.events == ()
    assert other_replay.watermark == 0
    assert "namespace" not in owner_history.messages[0].model_dump()
    assert "namespace" not in owner_replay.events[0].model_dump()
