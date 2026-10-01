"""Real PostgreSQL crash-window facts for one Artifact delivery tool call."""

from __future__ import annotations

from support.fakes import finish_run
from kokoro_agent.domain.run.models import CancelTerminalAuthority, RunTerminalOutcome

import json
import asyncio
import pytest
from typing import Any

from support.fakes import FakeBus
from kokoro_agent.domain.run.repository import RunRepository, LeaseFence
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.infrastructure.postgres_chat_repository import PostgresChatRepository
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.tools.deliver import DeliverResult
from kokoro_agent.protocol import (
    DeliveryCreatedPayload,
    ExecutionIdentity,
    IdentityRef,
    RunInput,
    RunRequest,
    RunControlReceiptPayload,
    RunCompletedPayload,
)


async def _cancel_run(
    repository: RunRepository,
    run_id: str,
    owner: str,
    command_id: str,
    snapshot: tuple[tuple[str, str, str], ...],
    receipt_json: str,
    terminal_json: str,
) -> LeaseFence | None:
    receipt = RunControlReceiptPayload.model_validate_json(receipt_json)
    assert receipt.command_id == command_id
    result = await repository.finalize_terminal(
        run_id,
        CancelTerminalAuthority(owner=owner, command_id=command_id),
        RunTerminalOutcome(
            payload=RunCompletedPayload.model_validate_json(terminal_json), usage=None
        ),
        snapshot,
    )
    return result.lease if result.status == "committed" else None


def _request() -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id="delivery-outbox-run",
        session_id="conversation-1",
        feature_key="chat",
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant-1",
            actor=IdentityRef(kind="user", opaque_ref="user-1"),
            subject=IdentityRef(kind="user", opaque_ref="user-1"),
            identity_assertion_ref="assertion-1",
        ),
        input=RunInput(message_id="message-1", content="Make a report"),
    )


def _payload() -> DeliveryCreatedPayload:
    return DeliveryCreatedPayload(
        tool_call_id="tool-1",
        artifact_id="artifact-1",
        asset_id="asset-1",
        artifact_kind="document",
        path="/report.pdf",
        title="Report",
        mime="application/pdf",
        size=6,
        content_hash="a" * 64,
    )


async def test_intent_is_frozen_before_owner_effect_and_event_replays_exact_frame(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
) -> None:
    run = _request()
    lease = await run_repository.try_claim(run, "worker-1")
    assert lease is not None
    assert await run_repository.journal_tool_started(
        run.run_id, lease, "tool-1", "deliver"
    )
    frozen = {
        "version": 1,
        "run_id": run.run_id,
        "session_id": run.session_id,
        "tenant_ref": "tenant-1",
        "subject_ref": "user-1",
        "tool_call_id": "tool-1",
        "path": "/report.pdf",
        "title": "Report",
        "mime_type": "application/pdf",
        "size_bytes": 6,
        "content_sha256": "a" * 64,
    }
    assert await run_repository.journal_delivery_intent(
        run.run_id, lease, "tool-1", json.dumps(frozen)
    )
    assert not await run_repository.journal_delivery_intent(
        run.run_id, lease, "tool-1", json.dumps({**frozen, "session_id": "other"})
    )
    progressed = {**frozen, "upload_id": "upload-1"}
    assert await run_repository.journal_delivery_intent(
        run.run_id, lease, "tool-1", json.dumps(progressed)
    )
    assert not await run_repository.journal_delivery_intent(
        run.run_id, lease, "tool-1", json.dumps(frozen)
    )
    journal = await run_repository.get_tool_journal(run.run_id, "tool-1")
    assert journal is not None
    assert json.loads(journal.result) == progressed

    bus = FakeBus()
    emitter = await RunEmitter.attach(
        bus,
        run.run_id,
        outbox=run_repository,
        lease=lease,
        chat_repository=chat_repository,
        tenant_id=run.execution_identity.tenant_ref,
        namespace=RunScope.of(run).namespace,
        session_id=run.session_id,
    )
    await emitter.emit(_payload())
    first = await run_repository.list_unpublished_outbox()
    # Live publish marks the row published; the frame is still durable and
    # duplicate staging must reuse its persisted identity and timestamp.
    assert first == []
    original = bus.run_events(run.run_id)[0]
    before = await run_repository.next_event_index(run.run_id)
    await emitter.emit(_payload())
    assert await run_repository.next_event_index(run.run_id) == before
    events = bus.run_events(run.run_id)
    assert len(events) == 1
    assert isinstance(events[0].payload, DeliveryCreatedPayload)
    assert events[0].payload.artifact_kind == "document"
    assert (
        events[0].event_id,
        events[0].durable_seq,
        events[0].index,
        events[0].timestamp,
    ) == (
        original.event_id,
        original.durable_seq,
        original.index,
        original.timestamp,
    )


async def test_concurrent_same_tool_stages_one_event_and_sends_once(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
) -> None:
    run = _request()
    lease = await run_repository.try_claim(run, "worker-1")
    assert lease is not None
    bus = FakeBus()
    first, second = await asyncio.gather(
        RunEmitter.attach(
            bus,
            run.run_id,
            outbox=run_repository,
            lease=lease,
            chat_repository=chat_repository,
            tenant_id=run.execution_identity.tenant_ref,
            namespace=RunScope.of(run).namespace,
            session_id=run.session_id,
        ),
        RunEmitter.attach(
            bus,
            run.run_id,
            outbox=run_repository,
            lease=lease,
            chat_repository=chat_repository,
            tenant_id=run.execution_identity.tenant_ref,
            namespace=RunScope.of(run).namespace,
            session_id=run.session_id,
        ),
    )
    await asyncio.gather(first.emit(_payload()), second.emit(_payload()))
    assert len(bus.run_events(run.run_id)) == 1
    assert bus.run_events(run.run_id)[0].kind == "delivery.created"
    assert await run_repository.next_event_index(run.run_id) == 1


async def test_cancel_terminal_command_and_outbox_survive_crash_before_publish(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
) -> None:
    from kokoro_agent.protocol.events import ChatInteractionState
    from kokoro_agent.domain.chat.models import chat_event_id

    run = _request()
    lease = await run_repository.try_claim(run, "worker-1")
    assert lease is not None
    body = '{"kind":"run.cancel","run_id":"delivery-outbox-run"}'
    await run_repository.admit_control(run.run_id, "cancel-1", "digest-1", body)
    assert await run_repository.record_control_delivery(
        run.run_id, "cancel-1", "digest-1", None, body
    )
    receipt = RunControlReceiptPayload(command_id="cancel-1", control_status="applied")
    terminal = RunCompletedPayload(status="cancelled", token_usage=None)
    won = await _cancel_run(
        run_repository,
        run.run_id,
        "cancel-worker",
        "cancel-1",
        (),
        receipt.model_dump_json(exclude_none=True),
        terminal.model_dump_json(exclude_none=True),
    )
    assert won is not None
    assert await run_repository.is_terminal(run.run_id)
    queued = [
        row
        for row in await run_repository.list_unpublished_outbox()
        if row.run_id == run.run_id
    ]
    assert [row.kind for row in queued] == ["run.control.receipt", "run.completed"]
    assert [row.durable_seq for row in queued] == [1, 2]
    assert [row.index for row in queued] == [0, 2]
    facts = await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    assert [fact.event_type for fact in facts] == ["interaction.state", "run.completed"]
    assert [fact.seq for fact in facts] == [1, 2]
    assert [fact.source_index for fact in facts] == [1, 2]
    assert ChatInteractionState.model_validate_json(facts[0].payload_json).model_dump(
        mode="json"
    ) == {
        "interaction_revision": 1,
        "pause_revision": 0,
        "pause_ref": None,
        "phase": "terminal",
        "groups": [],
        "action_result": None,
    }
    for fact in facts:
        assert (fact.tenant_id, fact.namespace, fact.session_id, fact.run_id) == (
            run.execution_identity.tenant_ref,
            RunScope.of(run).namespace,
            run.session_id,
            run.run_id,
        )
        assert fact.chat_event_id == chat_event_id(
            RunScope.of(run).namespace, run.run_id, fact.source_index
        )
    assert json.loads(queued[0].payload_json) == receipt.model_dump(
        mode="json", exclude_none=True
    )
    assert (
        json.loads(facts[1].payload_json)
        == json.loads(queued[1].payload_json)
        == terminal.model_dump(mode="json", exclude_none=True)
    )
    assert sorted([row.index for row in queued] + [facts[0].source_index]) == [0, 1, 2]
    await run_repository.verify_terminal_frame(queued[1])
    assert (
        await _cancel_run(
            run_repository,
            run.run_id,
            "another-worker",
            "cancel-1",
            (),
            receipt.model_dump_json(exclude_none=True),
            terminal.model_dump_json(exclude_none=True),
        )
        is None
    )
    assert len(await run_repository.list_unpublished_outbox()) == 2

    assert await run_repository.list_unpublished_outbox() == queued
    assert (
        await chat_repository.replay(
            run.execution_identity.tenant_ref,
            RunScope.of(run).namespace,
            run.session_id,
        )
        == facts
    )


async def test_cancel_snapshot_rejects_new_started_and_new_success(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
) -> None:
    run = _request()
    lease = await run_repository.try_claim(run, "worker-1")
    assert lease is not None
    body = '{"kind":"run.cancel","run_id":"delivery-outbox-run"}'
    await run_repository.admit_control(run.run_id, "cancel-1", "digest-1", body)
    assert await run_repository.record_control_delivery(
        run.run_id, "cancel-1", "digest-1", None, body
    )
    receipt_json = RunControlReceiptPayload(
        command_id="cancel-1", control_status="applied"
    ).model_dump_json(exclude_none=True)
    terminal_json = RunCompletedPayload(
        status="cancelled", token_usage=None
    ).model_dump_json(exclude_none=True)
    before_tool: tuple[tuple[str, str, str], ...] = ()
    assert await run_repository.journal_tool_started(
        run.run_id, lease, "tool-1", "deliver"
    )
    assert (
        await _cancel_run(
            run_repository,
            run.run_id,
            "cancel-worker",
            "cancel-1",
            before_tool,
            receipt_json,
            terminal_json,
        )
        is None
    )
    assert not await run_repository.is_terminal(run.run_id)
    started = tuple(await run_repository.list_delivery_journal(run.run_id))
    assert started[0][1] == "started"
    assert (
        await _cancel_run(
            run_repository,
            run.run_id,
            "cancel-worker",
            "cancel-1",
            started,
            receipt_json,
            terminal_json,
        )
        is None
    )
    result = DeliverResult(
        status="delivered",
        artifact_id="artifact-1",
        asset_id="asset-1",
        artifact_kind="document",
        path="/report.pdf",
        title="Report",
        mime="application/pdf",
        size=6,
        content_hash="a" * 64,
        note="",
    )
    assert await run_repository.journal_tool_finished(
        run.run_id, lease, "tool-1", result.model_dump_json(), False
    )
    assert (
        await _cancel_run(
            run_repository,
            run.run_id,
            "cancel-worker",
            "cancel-1",
            started,
            receipt_json,
            terminal_json,
        )
        is None
    )
    assert not await run_repository.is_terminal(run.run_id)
    # A succeeded journal cannot be terminal-fenced until the stable work event
    # is durable. The caller's preflight also confirms its Chat projection.
    success = tuple(await run_repository.list_delivery_journal(run.run_id))
    assert (
        await _cancel_run(
            run_repository,
            run.run_id,
            "cancel-worker",
            "cancel-1",
            success,
            receipt_json,
            terminal_json,
        )
        is None
    )
    emitter = await RunEmitter.attach(
        FakeBus(),
        run.run_id,
        outbox=run_repository,
        lease=lease,
        chat_repository=chat_repository,
        tenant_id=run.execution_identity.tenant_ref,
        namespace=RunScope.of(run).namespace,
        session_id=run.session_id,
    )
    await emitter.ensure_delivery_events()
    assert (
        await _cancel_run(
            run_repository,
            run.run_id,
            "cancel-worker",
            "cancel-1",
            success,
            receipt_json,
            terminal_json,
        )
        is not None
    )


async def test_two_cancel_workers_or_natural_terminal_have_one_winner(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
) -> None:
    run = _request()
    lease = await run_repository.try_claim(run, "worker-1")
    assert lease is not None
    receipt_json = RunControlReceiptPayload(
        command_id="cancel-a", control_status="applied"
    ).model_dump_json(exclude_none=True)
    terminal_json = RunCompletedPayload(
        status="cancelled", token_usage=None
    ).model_dump_json(exclude_none=True)
    for command in ("cancel-a", "cancel-b"):
        body = f'{{"kind":"run.cancel","command_id":"{command}"}}'
        await run_repository.admit_control(run.run_id, command, command, body)
        assert await run_repository.record_control_delivery(
            run.run_id, command, command, None, body
        )
    first, second = await asyncio.gather(
        _cancel_run(
            run_repository,
            run.run_id,
            "worker-a",
            "cancel-a",
            (),
            receipt_json,
            terminal_json,
        ),
        _cancel_run(
            run_repository,
            run.run_id,
            "worker-b",
            "cancel-b",
            (),
            RunControlReceiptPayload(
                command_id="cancel-b", control_status="applied"
            ).model_dump_json(exclude_none=True),
            terminal_json,
        ),
    )
    assert (first is None) != (second is None)
    queued = [
        row
        for row in await run_repository.list_unpublished_outbox()
        if row.run_id == run.run_id
    ]
    assert len(queued) == 2
    natural = run.model_copy(update={"run_id": "natural-terminal-run"})
    natural_lease = await run_repository.try_claim(natural, "worker-natural")
    assert natural_lease is not None
    body = '{"kind":"run.cancel","command_id":"cancel-natural"}'
    await run_repository.admit_control(natural.run_id, "cancel-natural", "digest", body)
    assert await run_repository.record_control_delivery(
        natural.run_id, "cancel-natural", "digest", None, body
    )
    assert await run_repository.add_usage(natural.run_id, natural_lease, 5, 7) == (5, 7)
    assert await finish_run(run_repository, natural.run_id, natural_lease)
    original_natural = [
        row
        for row in await run_repository.list_unpublished_outbox()
        if row.run_id == natural.run_id
    ]
    original_chat = await chat_repository.replay(
        natural.execution_identity.tenant_ref,
        RunScope.of(natural).namespace,
        natural.session_id,
    )
    assert len(original_natural) == 1 and original_natural[0].kind == "run.completed"
    assert (
        await _cancel_run(
            run_repository,
            natural.run_id,
            "worker-cancel",
            "cancel-natural",
            (),
            RunControlReceiptPayload(
                command_id="cancel-natural", control_status="applied"
            ).model_dump_json(exclude_none=True),
            terminal_json,
        )
        is None
    )
    assert [
        row
        for row in await run_repository.list_unpublished_outbox()
        if row.run_id == natural.run_id
    ] == original_natural
    assert await run_repository.add_usage(natural.run_id, natural_lease, 5, 7) == (5, 7)
    assert (
        await chat_repository.replay(
            natural.execution_identity.tenant_ref,
            RunScope.of(natural).namespace,
            natural.session_id,
        )
        == original_chat
    )


async def test_completed_journal_rebuilds_delivery_before_terminal_fence(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
) -> None:
    run = _request()
    lease = await run_repository.try_claim(run, "worker-1")
    assert lease is not None
    assert await run_repository.journal_tool_started(
        run.run_id, lease, "tool-1", "deliver"
    )
    result = DeliverResult(
        status="delivered",
        artifact_id="artifact-1",
        asset_id="asset-1",
        artifact_kind="document",
        path="/report.pdf",
        title="Report",
        mime="application/pdf",
        size=6,
        content_hash="a" * 64,
        note="",
    )
    assert await run_repository.journal_tool_finished(
        run.run_id, lease, "tool-1", result.model_dump_json(), False
    )
    bus = FakeBus()
    emitter = await RunEmitter.attach(
        bus,
        run.run_id,
        outbox=run_repository,
        lease=lease,
        chat_repository=chat_repository,
        tenant_id=run.execution_identity.tenant_ref,
        namespace=RunScope.of(run).namespace,
        session_id=run.session_id,
    )
    await emitter.ensure_delivery_events()
    before_terminal = bus.run_events(run.run_id)
    assert len(before_terminal) == 1 and before_terminal[0].kind == "delivery.created"
    assert await finish_run(run_repository, run.run_id, lease)
    assert await run_repository.is_terminal(run.run_id)
    await emitter.emit(_payload())
    assert len(bus.run_events(run.run_id)) == 1


async def test_terminal_replay_usage_seal_and_chat_sequence_are_immutable(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
) -> None:
    import pytest
    from kokoro_agent.domain.run.models import (
        ExecutionTerminalAuthority,
        RunUsageSegment,
    )
    from kokoro_agent.domain.run.repository import UsageIdentityConflict

    from kokoro_agent.protocol.events import ChatInteractionState
    from kokoro_agent.domain.chat.models import chat_event_id

    run = _request()
    lease = await run_repository.try_claim(run, "worker")
    assert lease is not None
    authority = ExecutionTerminalAuthority(lease=lease)
    outcome = RunTerminalOutcome(
        payload=RunCompletedPayload(status="completed", token_usage=None),
        usage=RunUsageSegment(input_tokens=5, output_tokens=7),
    )
    first = await run_repository.finalize_terminal(run.run_id, authority, outcome, ())
    assert first.status == "committed"
    replay = await run_repository.finalize_terminal(run.run_id, authority, outcome, ())
    assert replay.status == "replayed"
    assert replay.retained_frames == first.retained_frames
    await run_repository.verify_terminal_frame(first.retained_frames[0])
    events = await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    assert [event.event_type for event in events] == [
        "interaction.state",
        "run.completed",
    ]
    assert [event.seq for event in events] == [1, 2]
    assert [event.source_index for event in events] == [0, 1]
    assert ChatInteractionState.model_validate_json(events[0].payload_json).model_dump(
        mode="json"
    ) == {
        "interaction_revision": 1,
        "pause_revision": 0,
        "pause_ref": None,
        "phase": "terminal",
        "groups": [],
        "action_result": None,
    }
    for fact in events:
        assert (fact.tenant_id, fact.namespace, fact.session_id, fact.run_id) == (
            run.execution_identity.tenant_ref,
            RunScope.of(run).namespace,
            run.session_id,
            run.run_id,
        )
        assert fact.chat_event_id == chat_event_id(
            RunScope.of(run).namespace, run.run_id, fact.source_index
        )
    assert [
        (frame.kind, frame.durable_seq, frame.index) for frame in first.retained_frames
    ] == [("run.completed", 1, 1)]
    assert (
        json.loads(events[1].payload_json)
        == json.loads(first.retained_frames[0].payload_json)
        == {
            "status": "completed",
            "token_usage": {"input_tokens": 5, "output_tokens": 7},
        }
    )
    assert sorted(
        [frame.index for frame in first.retained_frames] + [events[0].source_index]
    ) == [0, 1]
    assert await run_repository.add_usage(run.run_id, lease, 5, 7) == (5, 7)
    with pytest.raises(UsageIdentityConflict):
        await run_repository.add_usage(run.run_id, lease, 6, 7)
    assert (
        await chat_repository.replay(
            run.execution_identity.tenant_ref,
            RunScope.of(run).namespace,
            run.session_id,
        )
        == events
    )


async def test_terminal_without_usage_rejects_a_new_segment(
    run_repository: RunRepository,
) -> None:
    run = _request()
    lease = await run_repository.try_claim(run, "worker")
    assert lease is not None
    assert await finish_run(run_repository, run.run_id, lease)
    assert await run_repository.add_usage(run.run_id, lease, 5, 7) is None


async def test_terminal_repairs_retained_started_chat_before_terminal_sequence(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
) -> None:
    from kokoro_agent.protocol.events import ChatInteractionState
    from kokoro_agent.domain.chat.models import chat_event_id

    run = _request()
    lease = await run_repository.try_claim(run, "worker")
    assert lease is not None
    staged = await run_repository.stage_critical_frame(
        run.run_id, lease, "run.started", 1700000000000, "{}", terminal=False
    )
    assert staged is not None
    assert await finish_run(run_repository, run.run_id, lease)
    facts = await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    assert [fact.event_type for fact in facts] == [
        "run.started",
        "interaction.state",
        "run.completed",
    ]
    assert [fact.seq for fact in facts] == [1, 2, 3]
    assert [fact.source_index for fact in facts] == [0, 1, 2]
    assert facts[0].seq < facts[1].seq < facts[2].seq
    assert ChatInteractionState.model_validate_json(facts[1].payload_json).model_dump(
        mode="json"
    ) == {
        "interaction_revision": 1,
        "pause_revision": 0,
        "pause_ref": None,
        "phase": "terminal",
        "groups": [],
        "action_result": None,
    }
    for fact in facts:
        assert (fact.tenant_id, fact.namespace, fact.session_id, fact.run_id) == (
            run.execution_identity.tenant_ref,
            RunScope.of(run).namespace,
            run.session_id,
            run.run_id,
        )
        assert fact.chat_event_id == chat_event_id(
            RunScope.of(run).namespace, run.run_id, fact.source_index
        )
    queued = [
        frame
        for frame in await run_repository.list_unpublished_outbox()
        if frame.run_id == run.run_id
    ]
    assert [(frame.kind, frame.durable_seq, frame.index) for frame in queued] == [
        ("run.started", 1, 0),
        ("run.completed", 2, 2),
    ]
    assert (
        queued[0].durable_seq,
        queued[0].event_id,
        queued[0].index,
        queued[0].timestamp,
    ) == (staged.durable_seq, staged.event_id, staged.index, staged.timestamp)
    assert facts[0].created_at.timestamp() * 1000 == staged.timestamp
    assert json.loads(queued[0].payload_json) == {}
    assert json.loads(facts[0].payload_json) == {"status": "running"}
    assert json.loads(facts[2].payload_json) == json.loads(queued[1].payload_json)
    assert sorted([frame.index for frame in queued] + [facts[1].source_index]) == [
        0,
        1,
        2,
    ]
    await run_repository.verify_terminal_frame(queued[1])


@pytest.mark.parametrize("cancel", [False, True])
async def test_delivery_ack_gc_preserves_mapping_until_terminal_then_rescans(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    monkeypatch: pytest.MonkeyPatch,
    cancel: bool,
) -> None:
    from psycopg import sql
    from kokoro_agent.infrastructure.postgres import connect_pg
    from kokoro_agent.infrastructure.schema import (
        RUN_OUTBOX_TABLE,
        RUN_RECEIPTS_TABLE,
        RUN_RECEIPT_MANIFESTS_TABLE,
    )
    from kokoro_agent.protocol import RunStartedPayload, RunCancel
    from kokoro_agent.domain.run.models import ExecutionTerminalAuthority
    import kokoro_agent.infrastructure.postgres_run_events as event_module
    import kokoro_agent.infrastructure.postgres_run_context as context_module
    import kokoro_agent.infrastructure.postgres_run_leases as lease_module
    from kokoro_agent.infrastructure.schema import RUN_CLAIMS_TABLE

    from kokoro_agent.protocol.events import ChatInteractionState
    from kokoro_agent.domain.chat.models import chat_event_id

    run = _request()
    lease = await run_repository.try_claim(run, "worker")
    assert lease is not None
    assert await run_repository.journal_tool_started(
        run.run_id, lease, "tool-1", "deliver"
    )
    result = DeliverResult(
        status="delivered",
        artifact_id="artifact-1",
        asset_id="asset-1",
        artifact_kind="document",
        path="/report.pdf",
        title="Report",
        mime="application/pdf",
        size=6,
        content_hash="a" * 64,
        note="",
    )
    assert await run_repository.journal_tool_finished(
        run.run_id, lease, "tool-1", result.model_dump_json(), False
    )
    bus = FakeBus()
    emitter = await RunEmitter.attach(
        bus,
        run.run_id,
        outbox=run_repository,
        lease=lease,
        chat_repository=chat_repository,
        tenant_id=run.execution_identity.tenant_ref,
        namespace=RunScope.of(run).namespace,
        session_id=run.session_id,
    )
    await emitter.emit(RunStartedPayload())
    await emitter.ensure_delivery_events()
    original = bus.run_events(run.run_id)
    assert [event.kind for event in original] == ["run.started", "delivery.created"]
    initial_index = await run_repository.next_event_index(run.run_id)
    async with connect_pg(run_chat_database_url) as conn:
        async with conn.transaction():
            for event in original:
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {} (run_id,durable_seq,event_id,status) VALUES (%s,%s,%s,'persisted')"
                    ).format(sql.Identifier(run_chat_schema, RUN_RECEIPTS_TABLE)),
                    (run.run_id, event.durable_seq, event.event_id),
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {} (run_id,persisted_seq,projected_seq,consumed_seq) VALUES (%s,2,2,0)"
                ).format(sql.Identifier(run_chat_schema, RUN_RECEIPT_MANIFESTS_TABLE)),
                (run.run_id,),
            )
    assert (await run_repository.reconcile_receipts(run.run_id)).consumed_through == 2
    async with connect_pg(run_chat_database_url) as conn:
        cursor = await conn.execute(
            sql.SQL(
                "SELECT kind,index_value,event_id,durable_seq FROM {} WHERE run_id=%s ORDER BY durable_seq"
            ).format(sql.Identifier(run_chat_schema, RUN_OUTBOX_TABLE)),
            (run.run_id,),
        )
        retained = await cursor.fetchall()
    assert [
        (row["kind"], row["index_value"], row["event_id"], row["durable_seq"])
        for row in retained
    ] == [
        (
            "delivery.created",
            original[1].index,
            original[1].event_id,
            original[1].durable_seq,
        )
    ]
    # Both independent operations reach the real Run row lock before its holder releases it.
    gc_waiting, effect_waiting = asyncio.Event(), asyncio.Event()
    original_sql = event_module.execute_sql

    async def gc_sql(cur: Any, statement: str, params: Any = None) -> None:
        if "FOR UPDATE" in statement and RUN_CLAIMS_TABLE in statement:
            gc_waiting.set()
        await original_sql(cur, statement, params)

    async def effect_sql(cur: Any, statement: str, params: Any = None) -> None:
        if "FOR UPDATE" in statement and RUN_CLAIMS_TABLE in statement:
            effect_waiting.set()
        await original_sql(cur, statement, params)

    monkeypatch.setattr(event_module, "execute_sql", gc_sql)
    monkeypatch.setattr(context_module, "execute_sql", effect_sql)
    monkeypatch.setattr(lease_module, "execute_sql", effect_sql)
    async with connect_pg(run_chat_database_url) as blocker:
        async with blocker.transaction():
            await blocker.execute(
                sql.SQL("SELECT 1 FROM {} WHERE run_id=%s FOR UPDATE").format(
                    sql.Identifier(run_chat_schema, RUN_CLAIMS_TABLE)
                ),
                (run.run_id,),
            )
            gc_task = asyncio.create_task(run_repository.reconcile_receipts(run.run_id))
            ensure_task = asyncio.create_task(emitter.ensure_delivery_events())
            await asyncio.wait_for(
                asyncio.gather(gc_waiting.wait(), effect_waiting.wait()), timeout=5
            )
            assert not gc_task.done() and not ensure_task.done()
        await asyncio.wait_for(asyncio.gather(gc_task, ensure_task), timeout=5)
    assert await run_repository.next_event_index(run.run_id) == initial_index
    assert len(bus.run_events(run.run_id)) == 2
    facts = await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    assert len([fact for fact in facts if fact.event_type == "delivery"]) == 1
    authority = ExecutionTerminalAuthority(lease=lease)
    terminal_payload = RunCompletedPayload(status="completed", token_usage=None)
    if cancel:
        command = RunCancel(
            kind="run.cancel",
            run_id=run.run_id,
            session_id=run.session_id,
            command_id="cancel-gc",
        )
        body = command.model_dump_json(exclude_none=True)
        await run_repository.admit_control(
            run.run_id, command.command_id, "cancel-gc-digest", body
        )
        assert await run_repository.record_control_delivery(
            run.run_id, command.command_id, "cancel-gc-digest", None, body
        )
        authority = CancelTerminalAuthority(
            owner="cancel-worker", command_id=command.command_id
        )
        terminal_payload = RunCompletedPayload(status="cancelled", token_usage=None)
    outcome = RunTerminalOutcome(payload=terminal_payload, usage=None)
    snapshot = tuple(await run_repository.list_delivery_journal(run.run_id))
    gc_waiting.clear()
    effect_waiting.clear()
    async with connect_pg(run_chat_database_url) as blocker:
        async with blocker.transaction():
            await blocker.execute(
                sql.SQL("SELECT 1 FROM {} WHERE run_id=%s FOR UPDATE").format(
                    sql.Identifier(run_chat_schema, RUN_CLAIMS_TABLE)
                ),
                (run.run_id,),
            )
            gc_task = asyncio.create_task(run_repository.reconcile_receipts(run.run_id))
            terminal_task = asyncio.create_task(
                run_repository.finalize_terminal(
                    run.run_id, authority, outcome, snapshot
                )
            )
            await asyncio.wait_for(
                asyncio.gather(gc_waiting.wait(), effect_waiting.wait()), timeout=5
            )
            assert not gc_task.done() and not terminal_task.done()
        _, terminal_result = await asyncio.wait_for(
            asyncio.gather(gc_task, terminal_task), timeout=5
        )
    assert terminal_result.status == "committed"
    terminal_facts = await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    assert terminal_facts[:2] == facts
    assert [fact.event_type for fact in terminal_facts] == [
        "run.started",
        "delivery",
        "interaction.state",
        "run.completed",
    ]
    assert [fact.seq for fact in terminal_facts] == [1, 2, 3, 4]
    assert [fact.source_index for fact in terminal_facts] == (
        [0, 1, 3, 4] if cancel else [0, 1, 2, 3]
    )
    assert ChatInteractionState.model_validate_json(
        terminal_facts[2].payload_json
    ).model_dump(mode="json") == {
        "interaction_revision": 1,
        "pause_revision": 0,
        "pause_ref": None,
        "phase": "terminal",
        "groups": [],
        "action_result": None,
    }
    for fact in terminal_facts:
        assert (fact.tenant_id, fact.namespace, fact.session_id, fact.run_id) == (
            run.execution_identity.tenant_ref,
            RunScope.of(run).namespace,
            run.session_id,
            run.run_id,
        )
        assert fact.chat_event_id == chat_event_id(
            RunScope.of(run).namespace, run.run_id, fact.source_index
        )
    frames = terminal_result.retained_frames
    assert [(frame.kind, frame.durable_seq, frame.index) for frame in frames] == (
        [("run.control.receipt", 3, 2), ("run.completed", 4, 4)]
        if cancel
        else [("run.completed", 3, 3)]
    )
    if cancel:
        assert RunControlReceiptPayload.model_validate_json(
            frames[0].payload_json
        ) == RunControlReceiptPayload(command_id="cancel-gc", control_status="applied")
    assert (
        json.loads(terminal_facts[-1].payload_json)
        == json.loads(frames[-1].payload_json)
        == terminal_payload.model_dump(mode="json", exclude_none=True)
    )
    assert terminal_facts[-1].source_index == frames[-1].index
    assert sorted(
        [event.index for event in original]
        + [frame.index for frame in frames]
        + [terminal_facts[2].source_index]
    ) == list(range(5 if cancel else 4))
    await run_repository.verify_terminal_frame(frames[-1])
    # No new receipt: the unchanged consumed watermark must still collect the retained delivery.
    await run_repository.reconcile_receipts(run.run_id)
    async with connect_pg(run_chat_database_url) as conn:
        cursor = await conn.execute(
            sql.SQL("SELECT kind FROM {} WHERE run_id=%s ORDER BY durable_seq").format(
                sql.Identifier(run_chat_schema, RUN_OUTBOX_TABLE)
            ),
            (run.run_id,),
        )
        assert [row["kind"] for row in await cursor.fetchall()] == (
            ["run.control.receipt", "run.completed"] if cancel else ["run.completed"]
        )
    # Consume terminal/control facts and verify replay never resurrects any collected frame.
    async with connect_pg(run_chat_database_url) as conn:
        async with conn.transaction():
            for frame in terminal_result.retained_frames:
                await run_repository.mark_critical_published(
                    run.run_id, frame.durable_seq
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {} (run_id,durable_seq,event_id,status) VALUES (%s,%s,%s,'persisted')"
                    ).format(sql.Identifier(run_chat_schema, RUN_RECEIPTS_TABLE)),
                    (run.run_id, frame.durable_seq, frame.event_id),
                )
    await run_repository.reconcile_receipts(run.run_id)
    assert await run_repository.list_unpublished_outbox() == []
    replay = await run_repository.finalize_terminal(
        run.run_id, authority, outcome, snapshot
    )
    assert replay.status == "replayed" and replay.retained_frames == ()
    async with connect_pg(run_chat_database_url) as conn:
        cursor = await conn.execute(
            sql.SQL("SELECT COUNT(*) AS n FROM {} WHERE run_id=%s").format(
                sql.Identifier(run_chat_schema, RUN_OUTBOX_TABLE)
            ),
            (run.run_id,),
        )
        row = await cursor.fetchone()
        assert row is not None and row["n"] == 0
    facts = await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    assert [fact.event_type for fact in facts] == [
        "run.started",
        "delivery",
        "interaction.state",
        "run.completed",
    ]

    assert facts == terminal_facts


async def test_usage_segment_rejects_database_expiry_despite_stale_application_clock(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    import time
    from psycopg import sql
    from kokoro_agent.infrastructure.postgres import connect_pg
    from kokoro_agent.infrastructure.postgres_run_repository import (
        PostgresRunRepository,
    )
    from kokoro_agent.infrastructure.schema import (
        RUN_CLAIMS_TABLE,
        RUN_USAGE_SEGMENTS_TABLE,
    )

    clock = [int(time.time() * 1000)]
    repository = PostgresRunRepository(
        run_chat_database_url,
        ttl_ms=30_000,
        schema=run_chat_schema,
        clock=lambda: clock[0],
    )
    run = _request()
    lease = await repository.try_claim(run, "worker")
    assert lease is not None
    clock[0] -= 10_000
    async with connect_pg(run_chat_database_url) as conn:
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=%s"
            ).format(sql.Identifier(run_chat_schema, RUN_CLAIMS_TABLE)),
            (run.run_id,),
        )
        await conn.commit()
    assert await repository.add_usage(run.run_id, lease, 5, 7) is None
    async with connect_pg(run_chat_database_url) as conn:
        cursor = await conn.execute(
            sql.SQL("SELECT COUNT(*) AS n FROM {} WHERE run_id=%s").format(
                sql.Identifier(run_chat_schema, RUN_USAGE_SEGMENTS_TABLE)
            ),
            (run.run_id,),
        )
        row = await cursor.fetchone()
        assert row is not None and row["n"] == 0
