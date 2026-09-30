"""Real PostgreSQL crash-window facts for one Artifact delivery tool call."""

from __future__ import annotations

import json
import asyncio

from support.fakes import FakeBus
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.execution.events import RunEmitter
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
        bus, run.run_id, outbox=run_repository, lease=lease
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
) -> None:
    run = _request()
    lease = await run_repository.try_claim(run, "worker-1")
    assert lease is not None
    bus = FakeBus()
    first, second = await asyncio.gather(
        RunEmitter.attach(bus, run.run_id, outbox=run_repository, lease=lease),
        RunEmitter.attach(bus, run.run_id, outbox=run_repository, lease=lease),
    )
    await asyncio.gather(first.emit(_payload()), second.emit(_payload()))
    assert len(bus.run_events(run.run_id)) == 1
    assert bus.run_events(run.run_id)[0].kind == "delivery.created"
    assert await run_repository.next_event_index(run.run_id) == 1


async def test_cancel_terminal_command_and_outbox_survive_crash_before_publish(
    run_repository: RunRepository,
) -> None:
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
    won = await run_repository.cancel_with_delivery_barrier(
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
    assert [row.index for row in queued] == [0, 1]
    assert (
        await run_repository.cancel_with_delivery_barrier(
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


async def test_cancel_snapshot_rejects_new_started_and_new_success(
    run_repository: RunRepository,
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
        await run_repository.cancel_with_delivery_barrier(
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
        await run_repository.cancel_with_delivery_barrier(
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
        await run_repository.cancel_with_delivery_barrier(
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
        await run_repository.cancel_with_delivery_barrier(
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
        FakeBus(), run.run_id, outbox=run_repository, lease=lease
    )
    await emitter.ensure_delivery_events()
    assert (
        await run_repository.cancel_with_delivery_barrier(
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
        run_repository.cancel_with_delivery_barrier(
            run.run_id, "worker-a", "cancel-a", (), receipt_json, terminal_json
        ),
        run_repository.cancel_with_delivery_barrier(
            run.run_id, "worker-b", "cancel-b", (), receipt_json, terminal_json
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
    assert await run_repository.try_mark_terminal(natural.run_id, natural_lease)
    assert (
        await run_repository.cancel_with_delivery_barrier(
            natural.run_id,
            "worker-cancel",
            "cancel-natural",
            (),
            receipt_json,
            terminal_json,
        )
        is None
    )
    assert not [
        row
        for row in await run_repository.list_unpublished_outbox()
        if row.run_id == natural.run_id
    ]


async def test_completed_journal_rebuilds_delivery_before_terminal_fence(
    run_repository: RunRepository,
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
        bus, run.run_id, outbox=run_repository, lease=lease
    )
    await emitter.ensure_delivery_events()
    before_terminal = bus.run_events(run.run_id)
    assert len(before_terminal) == 1 and before_terminal[0].kind == "delivery.created"
    assert await run_repository.try_mark_terminal(run.run_id, lease)
    assert await run_repository.is_terminal(run.run_id)
    await emitter.emit(_payload())
    assert len(bus.run_events(run.run_id)) == 1
