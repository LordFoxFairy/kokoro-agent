from __future__ import annotations

import hashlib

import pytest

from kokoro_agent.contract import (
    MessageCompleted,
    MessageCompletedPayload,
    MessageDelta,
    MessageDeltaPayload,
    RunCompleted,
    RunCompletedPayload,
    RunFailed,
    RunFailedPayload,
    RunStarted,
    RunStartedPayload,
    ToolInvoked,
    ToolInvokedPayload,
    ToolReturned,
    ToolReturnedPayload,
    agent_event_adapter,
)
from kokoro_agent.presentation.candidate import AgentAguiEventCandidate
from kokoro_agent.presentation.profile import (
    ClosedRunErrorEvent,
    ClosedToolPreviewActivity,
)
from kokoro_agent.presentation.runtime import (
    AgentPresentationService,
    PresentationAdmissionReceipt,
    PresentationAcknowledgeCommand,
    PresentationAcknowledgeState,
    PresentationCandidateRecord,
    PresentationQuarantineCommand,
    PresentationProjectionState,
    presentation_acknowledgement_digest,
    agent_thread_ref,
    plan_presentation_batch,
)
from kokoro_agent.execution.events import DurableOutputCommitError, RunEmitter
from kokoro_agent.storage.owner_event import (
    OwnerEventCommitResult,
    OwnerEventFenceLost,
)
from tests.fakes import FakeBus, FakeLedger


THREAD = "agent.thread:" + "a" * 64


def started(index: int = 0) -> RunStarted:
    return RunStarted(
        kind="run.started",
        run_id="run.1",
        index=index,
        timestamp=1_000 + index,
        payload=RunStartedPayload(),
    )


def delta(index: int, value: str = "hello") -> MessageDelta:
    return MessageDelta(
        kind="message.delta",
        run_id="run.1",
        index=index,
        timestamp=1_000 + index,
        payload=MessageDeltaPayload(segment_id="message.1", delta=value),
    )


def completed(index: int, value: str = "hello") -> MessageCompleted:
    return MessageCompleted(
        kind="message.completed",
        run_id="run.1",
        index=index,
        timestamp=1_000 + index,
        payload=MessageCompletedPayload(segment_id="message.1", content=value),
    )


def test_first_delta_atomically_plans_message_start_and_content() -> None:
    run = plan_presentation_batch(started(), PresentationProjectionState(), THREAD)
    batch = plan_presentation_batch(delta(1), run.next_state, THREAD)

    assert [candidate.event.type for candidate in batch.candidates] == [
        "TEXT_MESSAGE_START",
        "TEXT_MESSAGE_CONTENT",
    ]
    assert [candidate.source.source_ordinal for candidate in batch.candidates] == [
        "1",
        "2",
    ]
    assert len({candidate.source.source_event_ref for candidate in batch.candidates}) == 2
    assert all(
        candidate.source.route.internal_message_ref == "agent.message:" + hashlib.sha256(
            b"kokoro-agent-message-v1\0run.1\0message.1"
        ).hexdigest()
        for candidate in batch.candidates
    )
    assert batch.next_state.next_ordinal == 3


def test_message_completion_ends_open_message_without_replaying_snapshot() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD).next_state
    state = plan_presentation_batch(delta(1), state, THREAD).next_state

    batch = plan_presentation_batch(completed(2), state, THREAD)

    assert [candidate.event.type for candidate in batch.candidates] == [
        "TEXT_MESSAGE_END"
    ]
    assert batch.next_state.messages[0].state == "closed"


def test_completion_without_prior_delta_commits_whole_text_segment() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD).next_state

    batch = plan_presentation_batch(completed(1, "complete answer"), state, THREAD)

    assert [candidate.event.type for candidate in batch.candidates] == [
        "TEXT_MESSAGE_START",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_END",
    ]


def test_terminal_closes_every_open_message_before_finishing_run() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD).next_state
    state = plan_presentation_batch(delta(1), state, THREAD).next_state
    terminal = RunCompleted(
        kind="run.completed",
        run_id="run.1",
        index=2,
        timestamp=1_002,
        payload=RunCompletedPayload(status="completed"),
    )

    batch = plan_presentation_batch(terminal, state, THREAD)

    assert [candidate.event.type for candidate in batch.candidates] == [
        "TEXT_MESSAGE_END",
        "RUN_FINISHED",
    ]
    assert batch.next_state.run_state == "finished"
    assert batch.next_state.messages[0].state == "closed"


def test_failure_closes_open_message_and_emits_safe_error() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD).next_state
    state = plan_presentation_batch(delta(1), state, THREAD).next_state
    terminal = RunFailed(
        kind="run.failed",
        run_id="run.1",
        index=2,
        timestamp=1_002,
        payload=RunFailedPayload(
            code="internal_error",
            error_kind="RuntimeError",
            message="safe failure",
        ),
    )

    batch = plan_presentation_batch(terminal, state, THREAD)

    assert [candidate.event.type for candidate in batch.candidates] == [
        "TEXT_MESSAGE_END",
        "RUN_ERROR",
    ]
    terminal_event = batch.candidates[-1].event
    assert isinstance(terminal_event, ClosedRunErrorEvent)
    assert terminal_event.message == "The agent run failed."
    assert batch.next_state.run_state == "failed"


def test_tool_projection_is_redacted_activity_and_never_contains_raw_args() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD).next_state
    event = ToolInvoked(
        kind="tool.invoked",
        run_id="run.1",
        index=1,
        timestamp=1_001,
        payload=ToolInvokedPayload(
            segment_id="message.1",
            tool_id="tool.1",
            name="web_fetch",
            args={"authorization": "Bearer secret", "url": "https://private.invalid"},
        ),
    )

    batch = plan_presentation_batch(event, state, THREAD)
    rendered = [
        candidate.model_dump_json(by_alias=True, exclude_none=True)
        for candidate in batch.candidates
    ]

    assert [candidate.event.type for candidate in batch.candidates] == [
        "ACTIVITY_SNAPSHOT",
    ]
    activity = batch.candidates[-1].event
    assert isinstance(activity, ClosedToolPreviewActivity)
    assert activity.activity_type == "kokoro.tool-preview.v1"
    assert activity.message_id.startswith("agent.activity:")
    assert activity.message_id != "agent.message:" + hashlib.sha256(
        b"kokoro-agent-message-v1\0run.1\0message.1"
    ).hexdigest()
    assert all("Bearer secret" not in value for value in rendered)
    assert all("private.invalid" not in value for value in rendered)


def test_activity_identity_is_stable_for_owner_and_does_not_open_text_message() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD).next_state
    invoked = ToolInvoked(
        kind="tool.invoked",
        run_id="run.1",
        index=1,
        timestamp=1_001,
        payload=ToolInvokedPayload(
            segment_id="message.1",
            tool_id="tool.1",
            name="web_fetch",
            args={},
        ),
    )
    first = plan_presentation_batch(invoked, state, THREAD)
    returned = ToolReturned(
        kind="tool.returned",
        run_id="run.1",
        index=2,
        timestamp=1_002,
        payload=ToolReturnedPayload(
            segment_id="message.1",
            tool_id="tool.1",
            name="web_fetch",
            result="secret result",
            is_error=False,
        ),
    )
    second = plan_presentation_batch(returned, first.next_state, THREAD)

    assert [candidate.event.type for candidate in first.candidates] == ["ACTIVITY_SNAPSHOT"]
    assert [candidate.event.type for candidate in second.candidates] == ["ACTIVITY_SNAPSHOT"]
    first_activity = first.candidates[0].event
    second_activity = second.candidates[0].event
    assert isinstance(first_activity, ClosedToolPreviewActivity)
    assert isinstance(second_activity, ClosedToolPreviewActivity)
    assert first_activity.message_id == second_activity.message_id
    assert first.next_state.messages == ()
    assert second.next_state.messages == ()


def test_run_error_candidate_never_contains_raw_exception_message() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD).next_state
    terminal = RunFailed(
        kind="run.failed",
        run_id="run.1",
        index=1,
        timestamp=1_001,
        payload=RunFailedPayload(
            code="internal_error",
            error_kind="ProviderError",
            message="Authorization Bearer sk-secret at mongodb://private.internal",
        ),
    )

    batch = plan_presentation_batch(terminal, state, THREAD)
    rendered = batch.candidates[-1].model_dump_json(by_alias=True, exclude_none=True)

    assert "sk-secret" not in rendered
    assert "private.internal" not in rendered
    assert isinstance(batch.candidates[-1].event, ClosedRunErrorEvent)
    assert batch.candidates[-1].event.message == "The agent run failed."


def test_resume_reuses_state_and_rejects_message_reopen_or_post_terminal() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD).next_state
    state = plan_presentation_batch(completed(1), state, THREAD).next_state

    with pytest.raises(ValueError, match="PRESENTATION_MESSAGE_CLOSED"):
        plan_presentation_batch(delta(2), state, THREAD)

    terminal = RunCompleted(
        kind="run.completed",
        run_id="run.1",
        index=2,
        timestamp=1_002,
        payload=RunCompletedPayload(status="completed"),
    )
    state = plan_presentation_batch(terminal, state, THREAD).next_state
    with pytest.raises(ValueError, match="PRESENTATION_RUN_TERMINAL"):
        plan_presentation_batch(delta(3), state, THREAD)


def test_agent_thread_ref_is_domain_separated_and_not_raw_session_identity() -> None:
    first = agent_thread_ref("opaque.namespace", "session-thread-raw")
    second = agent_thread_ref("opaque.namespace", "session-thread-raw")

    assert first == second
    assert first.startswith("agent.thread:")
    assert "session-thread-raw" not in first
    assert agent_thread_ref("other.namespace", "session-thread-raw") != first


class _OwnerEventStore(FakeLedger):
    def __init__(self, *, head: int = 0, fail_once: bool = False) -> None:
        super().__init__()
        self.head = head
        self.fail_once = fail_once
        self.calls: list[dict[str, object]] = []

    async def owner_event_head(self, run_id: str) -> int:
        assert run_id == "run.1"
        return self.head

    async def commit_owner_event(self, **command: object) -> OwnerEventCommitResult:
        self.calls.append(command)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("transaction aborted")
        raw_index = command["expected_index"]
        assert isinstance(raw_index, int)
        index = raw_index
        payload = command["payload"]
        event = agent_event_adapter.validate_python({
            "kind": command["kind"],
            "run_id": "run.1",
            "index": index,
            "timestamp": 2_000 + index,
            "payload": payload,
        })
        self.head = index + 1
        return OwnerEventCommitResult(status="committed", event=event)


class _FenceLostStore(_OwnerEventStore):
    def __init__(self, drift: str) -> None:
        super().__init__(head=4)
        self.drift = drift

    async def commit_owner_event(self, **command: object) -> OwnerEventCommitResult:
        self.calls.append(command)
        return OwnerEventCommitResult(status="fence_lost")


@pytest.mark.asyncio
async def test_run_emitter_uses_single_owner_uow_and_durable_head_not_redis_history() -> None:
    store = _OwnerEventStore(head=7)
    bus = FakeBus()
    emitter = await RunEmitter.attach(
        bus,
        "run.1",
        outbox=store,
        lease_owner_ref="worker.1",
        agent_thread_ref=THREAD,
    )

    await emitter.emit(MessageDeltaPayload(segment_id="message.1", delta="hello"))

    assert store.calls == [{
        "run_id": "run.1",
        "expected_index": 7,
        "kind": "message.delta",
        "payload": MessageDeltaPayload(segment_id="message.1", delta="hello"),
        "lease_owner_ref": "worker.1",
        "agent_thread_ref": THREAD,
    }]
    assert bus.run_events("run.1")[0].index == 7


@pytest.mark.asyncio
async def test_owner_uow_abort_keeps_index_and_retry_identity_slot_stable() -> None:
    store = _OwnerEventStore(fail_once=True)
    bus = FakeBus()
    emitter = await RunEmitter.attach(
        bus,
        "run.1",
        outbox=store,
        lease_owner_ref="worker.1",
        agent_thread_ref=THREAD,
    )
    payload = MessageDeltaPayload(segment_id="message.1", delta="hello")

    with pytest.raises(DurableOutputCommitError, match="OWNER_EVENT_COMMIT_FAILED"):
        await emitter.emit(payload)
    await emitter.emit(payload)

    assert [call["expected_index"] for call in store.calls] == [0, 0]
    assert bus.run_events("run.1")[0].index == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["lease", "producer_instance", "producer_generation"])
async def test_fence_lost_is_typed_and_fail_fast_before_live_publish(drift: str) -> None:
    store = _FenceLostStore(drift)
    bus = FakeBus()
    emitter = await RunEmitter.attach(
        bus,
        "run.1",
        outbox=store,
        lease_owner_ref="stale.worker",
        agent_thread_ref=THREAD,
    )

    with pytest.raises(OwnerEventFenceLost, match="OWNER_EVENT_FENCE_LOST"):
        await emitter.emit(MessageDeltaPayload(segment_id="message.1", delta="blocked"))

    assert bus.published == []
    assert len(store.calls) == 1


class _Reader:
    def __init__(self, records: tuple[PresentationCandidateRecord, ...]) -> None:
        self.records = records

    async def presentation_head(self, run_id: str) -> int:
        assert run_id == "run.1"
        return len(self.records)

    async def pull_presentation_candidates(
        self, run_id: str, after_presentation_seq: int, through_presentation_seq: int, limit: int
    ) -> tuple[PresentationCandidateRecord, ...]:
        assert run_id == "run.1"
        return tuple(
            record
            for record in self.records
            if after_presentation_seq < record.presentation_seq <= through_presentation_seq
        )[:limit]


@pytest.mark.asyncio
async def test_pull_service_freezes_snapshot_head_across_pages() -> None:
    state = PresentationProjectionState()
    batches: list[AgentAguiEventCandidate] = []
    for event in (started(), delta(1), completed(2)):
        batch = plan_presentation_batch(event, state, THREAD)
        state = batch.next_state
        batches.extend(batch.candidates)
    records = tuple(
        PresentationCandidateRecord.from_candidate(
            run_id="run.1",
            presentation_seq=index,
            candidate=candidate,
            producer_instance_ref="agent.pod.1",
            producer_generation=7,
        )
        for index, candidate in enumerate(batches, start=1)
    )
    service = AgentPresentationService(_Reader(records))

    first = await service.pull_candidate_batches(
        run_id="run.1", after_presentation_seq=0, page_size=2
    )
    second = await service.pull_candidate_batches(
        run_id="run.1",
        after_presentation_seq=first.next_after_presentation_seq or 0,
        page_size=2,
        snapshot_through_presentation_seq=first.snapshot_through_presentation_seq,
    )

    assert first.snapshot_through_presentation_seq == 4
    assert first.has_more is True
    assert second.snapshot_through_presentation_seq == 4
    assert second.has_more is False
    assert [record.presentation_seq for record in first.records + second.records] == [1, 2, 3, 4]


class _DeliveryStore(_Reader):
    def __init__(self, records: tuple[PresentationCandidateRecord, ...]) -> None:
        super().__init__(records)
        self.state = PresentationAcknowledgeState(
            run_id="run.1", acknowledged_through_presentation_seq=0, revision=0
        )
        self.commands: list[PresentationAcknowledgeCommand] = []
        self.quarantines: list[PresentationQuarantineCommand] = []

    async def acknowledge_presentation_admissions(
        self, command: PresentationAcknowledgeCommand
    ) -> PresentationAcknowledgeState:
        self.commands.append(command)
        self.state = PresentationAcknowledgeState(
            run_id=command.run_id,
            acknowledged_through_presentation_seq=command.receipts[-1].presentation_seq,
            revision=self.state.revision + 1,
        )
        return self.state

    async def get_presentation_delivery_state(
        self, run_id: str
    ) -> PresentationAcknowledgeState:
        assert run_id == "run.1"
        return self.state

    async def quarantine_presentation_admission(
        self, command: PresentationQuarantineCommand
    ) -> PresentationAcknowledgeState:
        self.quarantines.append(command)
        self.state = self.state.model_copy(update={
            "revision": self.state.revision + 1,
            "quarantined_presentation_seq": command.presentation_seq,
            "quarantine_reason": command.reason,
        })
        return self.state


@pytest.mark.asyncio
async def test_admission_ack_is_contiguous_digest_bound_and_cas_guarded() -> None:
    state = plan_presentation_batch(started(), PresentationProjectionState(), THREAD)
    record = PresentationCandidateRecord.from_candidate(
        run_id="run.1",
        presentation_seq=1,
        candidate=state.candidates[0],
        producer_instance_ref="agent.pod.1",
        producer_generation=7,
    )
    store = _DeliveryStore((record,))
    service = AgentPresentationService(store)
    receipt = PresentationAdmissionReceipt(
        presentation_seq=1,
        presentation_ref=record.presentation_ref,
        candidate_ref=state.candidates[0].candidate_ref,
        session_receipt_ref="session.agui.receipt.1",
        session_effect_digest="sha256:" + "1" * 64,
    )
    digest = presentation_acknowledgement_digest(
        run_id="run.1",
        acknowledgement_ref="ack.1",
        expected_acknowledged_through_presentation_seq=0,
        receipts=(receipt,),
    )

    result = await service.acknowledge_candidate_admissions(
        run_id="run.1",
        acknowledgement_ref="ack.1",
        expected_acknowledged_through_presentation_seq=0,
        receipts=(receipt,),
        request_effect_digest=digest,
    )

    assert result.acknowledged_through_presentation_seq == 1
    assert store.commands[0].request_effect_digest == digest


@pytest.mark.asyncio
async def test_admission_ack_cannot_cross_gap_or_encode_permanent_rejection() -> None:
    service = AgentPresentationService(_DeliveryStore(()))
    skipped = PresentationAdmissionReceipt(
        presentation_seq=2,
        presentation_ref="agent.presentation:sha256:" + "2" * 64,
        candidate_ref="agui_candidate:sha256:" + "3" * 64,
        session_receipt_ref="session.agui.receipt.2",
        session_effect_digest="sha256:" + "4" * 64,
    )

    with pytest.raises(ValueError, match="PRESENTATION_ACK_SEQUENCE_INVALID"):
        await service.acknowledge_candidate_admissions(
            run_id="run.1",
            acknowledgement_ref="ack.gap",
            expected_acknowledged_through_presentation_seq=0,
            receipts=(skipped,),
            request_effect_digest="sha256:" + "0" * 64,
        )

    with pytest.raises(ValueError, match="PRESENTATION_ACK_DIGEST_INVALID"):
        await service.acknowledge_candidate_admissions(
            run_id="run.1",
            acknowledgement_ref="ack.bad-digest",
            expected_acknowledged_through_presentation_seq=1,
            receipts=(skipped,),
            request_effect_digest="sha256:" + "0" * 64,
        )


@pytest.mark.asyncio
async def test_permanent_rejection_is_typed_quarantine_and_does_not_advance_ack() -> None:
    store = _DeliveryStore(())
    service = AgentPresentationService(store)

    state = await service.quarantine_candidate_admission(
        run_id="run.1",
        rejection_ref="reject.1",
        expected_acknowledged_through_presentation_seq=0,
        presentation_seq=1,
        presentation_ref="agent.presentation:sha256:" + "2" * 64,
        candidate_ref="agui_candidate:sha256:" + "3" * 64,
        reason="SESSION_ADMISSION_PERMANENT_REJECT",
        session_effect_digest="sha256:" + "4" * 64,
    )

    assert state.acknowledged_through_presentation_seq == 0
    assert state.quarantined_presentation_seq == 1
    assert store.quarantines[0].presentation_seq == 1
