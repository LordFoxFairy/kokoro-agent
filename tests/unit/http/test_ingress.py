"""Agent business ingress: durable admission, control scope, evidence and chat projections."""

from __future__ import annotations

import pytest
from pydantic import JsonValue

from kokoro_agent.chat.models import ChatEventDraft, ChatProjection
from kokoro_agent.chat.query import ChatQuery, ChatQueryRequest
from kokoro_agent.contract import (
    ExecutionIdentity,
    IdentityRef,
    RunInput,
    RunRequest,
)
from kokoro_agent.http.ingress import AgentIngress, IngressError
from kokoro_agent.execution.scope import runtime_namespace
from support.chat import FakeChatStore
from support.fakes import FakeBus, FakeLedger


def identity(subject: str = "subject") -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref=subject),
        identity_assertion_ref="assertion",
    )


def launch_body(run_id: str = "run-1") -> dict[str, object]:
    return {
        "request_id": f"request-{run_id}",
        "run_id": run_id,
        "session_id": "session-1",
        "feature_key": "chat",
        "execution_identity": identity().model_dump(mode="json"),
        "message_id": f"message-{run_id}",
        "content": "hello",
    }


@pytest.mark.asyncio
async def test_launch_durably_admits_before_publishing_worker_envelope() -> None:
    bus = FakeBus()
    ledger = FakeLedger()
    ingress = AgentIngress(bus=bus, ledger=ledger, chat_query=ChatQuery(FakeChatStore()))

    receipt = await ingress.launch(launch_body())

    assert receipt.run_id == "run-1"
    assert receipt.replayed is False
    assert bus.published[0][0] == "kokoro:runs:requests"
    assert bus.published[0][1]["kind"] == "run.request"
    assert bus.published[0][1]["input"] == {"message_id": "message-run-1", "content": "hello"}
    assert ledger.dispatches["run-1"] == "pending"


@pytest.mark.asyncio
async def test_launch_rejects_run_id_reuse_with_different_immutable_envelope() -> None:
    bus = FakeBus()
    ledger = FakeLedger()
    ingress = AgentIngress(bus=bus, ledger=ledger, chat_query=ChatQuery(FakeChatStore()))
    await ingress.launch(launch_body())

    changed = launch_body()
    changed["content"] = "changed"
    with pytest.raises(IngressError, match="different fence") as error:
        await ingress.launch(changed)
    assert error.value.status == 409


@pytest.mark.asyncio
async def test_control_requires_the_run_session_and_publishes_to_isolated_stream() -> None:
    bus = FakeBus()
    ledger = FakeLedger()
    request = RunRequest(
        kind="run.request",
        request_id="request-run-1",
        run_id="run-1",
        session_id="session-1",
        feature_key="chat",
        execution_identity=identity(),
        input=RunInput(message_id="message-run-1", content="hello"),
    )
    ledger.requests[request.run_id] = request
    ingress = AgentIngress(bus=bus, ledger=ledger, chat_query=ChatQuery(FakeChatStore()))

    with pytest.raises(IngressError) as error:
        await ingress.control("run-1", {"kind": "run.cancel", "session_id": "other", "decision_id": "d-1"})
    assert error.value.status == 403

    accepted = await ingress.control(
        "run-1", {"kind": "run.cancel", "session_id": "session-1", "decision_id": "d-1"}
    )
    assert accepted["accepted"] is True
    assert bus.published[-1][0] == "kokoro:run:run-1:control"


@pytest.mark.asyncio
async def test_evidence_filters_by_index_and_chat_query_remains_identity_scoped() -> None:
    bus = FakeBus()
    ledger = FakeLedger()
    request = RunRequest(
        kind="run.request",
        request_id="request-run-1",
        run_id="run-1",
        session_id="session-1",
        feature_key="chat",
        execution_identity=identity(),
        input=RunInput(message_id="message-run-1", content="hello"),
    )
    ledger.requests[request.run_id] = request
    started: dict[str, JsonValue] = {
        "kind": "run.started", "run_id": "run-1", "index": 0, "timestamp": 1, "payload": {}
    }
    completed: dict[str, JsonValue] = {
        "kind": "run.completed",
        "run_id": "run-1",
        "index": 1,
        "timestamp": 2,
        "payload": {"status": "completed", "token_usage": None},
    }
    bus.published.extend(
        [
            ("kokoro:run:run-1:events", started, 100),
            ("kokoro:run:run-1:events", completed, 100),
        ]
    )
    chat = FakeChatStore()
    namespace = runtime_namespace(identity())
    await chat.append(
        ChatProjection(
            event=ChatEventDraft(
                namespace=namespace,
                session_id="session-1",
                run_id="run-1",
                source_index=0,
                event_type="run.started",
                payload_json='{"status":"running"}',
                created_at=1,
            )
        )
    )
    ingress = AgentIngress(bus=bus, ledger=ledger, chat_query=ChatQuery(chat))

    evidence = await ingress.evidence("run-1", after_seq=0, limit=1)
    assert evidence["next_seq"] == 1
    assert evidence["terminal"] is True
    page = await ingress.replay(
        ChatQueryRequest(execution_identity=identity(), session_id="session-1")
    )
    assert page.watermark == 1
    assert len(page.events) == 1
