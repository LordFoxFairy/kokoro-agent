"""GA chat query derives isolation from ExecutionIdentity."""

from kokoro_agent.chat.models import ChatEventDraft, ChatMessageDraft, ChatProjection
from kokoro_agent.chat.query import ChatQuery, ChatQueryRequest, ChatSessionListRequest
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


async def test_session_list_is_cursor_paged_and_identity_scoped() -> None:
    store = FakeChatStore()
    owner = _identity("owner")
    other = _identity("other")
    query = ChatQuery(store)
    await query.ensure_session(owner, "session-a", project_ref="project", title="A", updated_at=30)
    await query.ensure_session(owner, "session-b", project_ref="project", title="B", updated_at=20)
    await query.ensure_session(owner, "session-c", project_ref="other-project", title="C", updated_at=10)

    first = await query.list_sessions(
        ChatSessionListRequest(execution_identity=owner, project_ref="project", limit=1)
    )
    second = await query.list_sessions(
        ChatSessionListRequest(
            execution_identity=owner,
            project_ref="project",
            cursor=first.next_cursor,
            limit=1,
        )
    )
    isolated = await query.list_sessions(ChatSessionListRequest(execution_identity=other))

    assert [session.session_id for session in first.sessions] == ["session-a"]
    assert first.next_cursor is not None
    assert [session.session_id for session in second.sessions] == ["session-b"]
    assert second.next_cursor is None
    assert isolated.sessions == ()
