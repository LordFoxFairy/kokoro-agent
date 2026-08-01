"""Production Agent facts to durable official AG-UI candidate batches.

This module owns presentation planning and the child-side pull/ack application ports.  It never
owns browser identities, Session cursors or SSE frames; those remain Session authority.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Literal, Protocol, runtime_checkable

from ag_ui.core import (
    ActivitySnapshotEvent,
    BaseEvent,
    RunErrorEvent,
    RunFinishedEvent,
    RunFinishedSuccessOutcome,
    RunStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator

from kokoro_agent.contract import (
    AgentEvent,
    MessageCompleted,
    MessageDelta,
    PlanProposed,
    RunCompleted,
    RunFailed,
    RunStarted,
    SubagentFinished,
    SubagentStarted,
    SubagentToolInvoked,
    SubagentToolReturned,
    ToolAwaitingApproval,
    ToolInvoked,
    ToolReturned,
)
from kokoro_agent.presentation.adapter import build_agui_candidate
from kokoro_agent.presentation.candidate import (
    AgentAguiCandidateRoute,
    AgentAguiCandidateSource,
    AgentAguiEventCandidate,
    canonical_recorded_at,
)

MAX_PRESENTATION_PAGE_SIZE = 256


class _FrozenModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class PresentationMessageState(_FrozenModel):
    internal_message_ref: str = Field(min_length=1, max_length=128)
    source_segment_ref: str = Field(min_length=1, max_length=256)
    state: Literal["open", "closed"]
    opened_ordinal: int = Field(ge=0)
    text_seen: bool = False


class PresentationProjectionState(_FrozenModel):
    internal_run_ref: str | None = None
    internal_thread_ref: str | None = None
    run_state: Literal["new", "running", "finished", "failed"] = "new"
    next_ordinal: int = Field(default=0, ge=0)
    messages: tuple[PresentationMessageState, ...] = ()


class PresentationCandidateBatch(_FrozenModel):
    source_event_ref: str = Field(min_length=1, max_length=128)
    source_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidates: tuple[AgentAguiEventCandidate, ...]
    next_state: PresentationProjectionState


class PresentationCandidateRecord(_FrozenModel):
    presentation_ref: str = Field(
        pattern=r"^agent\.presentation:sha256:[0-9a-f]{64}$"
    )
    run_id: str = Field(min_length=1, max_length=128)
    presentation_seq: int = Field(gt=0)
    candidate_envelope_json: bytes = Field(min_length=1, max_length=128 * 1024)
    envelope_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    recorded_at_ms: int = Field(ge=0)
    producer_instance_ref: str = Field(min_length=1, max_length=256)
    producer_generation: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_envelope(self) -> PresentationCandidateRecord:
        if hashlib.sha256(self.candidate_envelope_json).hexdigest() != self.envelope_sha256:
            raise ValueError("presentation envelope digest mismatch")
        AgentAguiEventCandidate.model_validate_json(self.candidate_envelope_json)
        expected = _presentation_ref(
            self.run_id, self.presentation_seq, self.envelope_sha256
        )
        if self.presentation_ref != expected:
            raise ValueError("presentation record identity mismatch")
        return self

    @classmethod
    def from_candidate(
        cls,
        *,
        run_id: str,
        presentation_seq: int,
        candidate: AgentAguiEventCandidate,
        producer_instance_ref: str,
        producer_generation: int,
    ) -> PresentationCandidateRecord:
        envelope = candidate.model_dump_json(
            by_alias=True, exclude_none=True
        ).encode("utf-8")
        digest = hashlib.sha256(envelope).hexdigest()
        return cls(
            presentation_ref=_presentation_ref(run_id, presentation_seq, digest),
            run_id=run_id,
            presentation_seq=presentation_seq,
            candidate_envelope_json=envelope,
            envelope_sha256=digest,
            recorded_at_ms=candidate.event.timestamp,
            producer_instance_ref=producer_instance_ref,
            producer_generation=producer_generation,
        )


class PresentationCandidateReader(Protocol):
    async def presentation_head(self, run_id: str) -> int: ...

    async def pull_presentation_candidates(
        self,
        run_id: str,
        after_presentation_seq: int,
        through_presentation_seq: int,
        limit: int,
    ) -> Sequence[PresentationCandidateRecord]: ...


class PresentationCandidateWriter(Protocol):
    async def append_presentation_event(
        self, event: AgentEvent, *, agent_thread_ref: str
    ) -> tuple[PresentationCandidateRecord, ...] | None: ...


class PresentationAdmissionReceipt(_FrozenModel):
    presentation_seq: int = Field(gt=0)
    presentation_ref: str = Field(
        pattern=r"^agent\.presentation:sha256:[0-9a-f]{64}$"
    )
    candidate_ref: str = Field(
        pattern=r"^agui_candidate:sha256:[0-9a-f]{64}$"
    )
    session_receipt_ref: str = Field(min_length=1, max_length=256)
    session_effect_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class PresentationAcknowledgeCommand(_FrozenModel):
    run_id: str = Field(min_length=1, max_length=128)
    acknowledgement_ref: str = Field(min_length=1, max_length=256)
    expected_acknowledged_through_presentation_seq: int = Field(ge=0)
    receipts: tuple[PresentationAdmissionReceipt, ...] = Field(
        min_length=1, max_length=MAX_PRESENTATION_PAGE_SIZE
    )
    request_effect_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_command(self) -> PresentationAcknowledgeCommand:
        expected = self.expected_acknowledged_through_presentation_seq + 1
        if any(
            receipt.presentation_seq != expected + offset
            for offset, receipt in enumerate(self.receipts)
        ):
            raise ValueError("presentation acknowledgement must be contiguous")
        if len({receipt.presentation_ref for receipt in self.receipts}) != len(
            self.receipts
        ) or len({receipt.session_receipt_ref for receipt in self.receipts}) != len(
            self.receipts
        ):
            raise ValueError("presentation acknowledgement receipts must be unique")
        if presentation_acknowledgement_digest(
            run_id=self.run_id,
            acknowledgement_ref=self.acknowledgement_ref,
            expected_acknowledged_through_presentation_seq=(
                self.expected_acknowledged_through_presentation_seq
            ),
            receipts=self.receipts,
        ) != self.request_effect_digest:
            raise ValueError("presentation acknowledgement digest mismatch")
        return self


class PresentationQuarantineCommand(_FrozenModel):
    run_id: str = Field(min_length=1, max_length=128)
    rejection_ref: str = Field(min_length=1, max_length=256)
    expected_acknowledged_through_presentation_seq: int = Field(ge=0)
    presentation_seq: int = Field(gt=0)
    presentation_ref: str = Field(
        pattern=r"^agent\.presentation:sha256:[0-9a-f]{64}$"
    )
    candidate_ref: str = Field(
        pattern=r"^agui_candidate:sha256:[0-9a-f]{64}$"
    )
    reason: str = Field(min_length=1, max_length=128)
    session_effect_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_sequence(self) -> PresentationQuarantineCommand:
        if self.presentation_seq != (
            self.expected_acknowledged_through_presentation_seq + 1
        ):
            raise ValueError("presentation quarantine must stop at the first gap")
        return self


class PresentationAcknowledgeState(_FrozenModel):
    run_id: str = Field(min_length=1, max_length=128)
    acknowledged_through_presentation_seq: int = Field(ge=0)
    revision: int = Field(ge=0)
    quarantined_presentation_seq: int | None = Field(default=None, gt=0)
    quarantine_reason: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def validate_quarantine(self) -> PresentationAcknowledgeState:
        if (self.quarantined_presentation_seq is None) != (
            self.quarantine_reason is None
        ):
            raise ValueError("presentation quarantine shape invalid")
        if (
            self.quarantined_presentation_seq is not None
            and self.quarantined_presentation_seq
            <= self.acknowledged_through_presentation_seq
        ):
            raise ValueError("presentation quarantine is behind acknowledgement")
        return self


@runtime_checkable
class PresentationAdmissionReceiptStore(Protocol):
    async def acknowledge_presentation_admissions(
        self, command: PresentationAcknowledgeCommand
    ) -> PresentationAcknowledgeState: ...

    async def quarantine_presentation_admission(
        self, command: PresentationQuarantineCommand
    ) -> PresentationAcknowledgeState: ...

    async def get_presentation_delivery_state(
        self, run_id: str
    ) -> PresentationAcknowledgeState: ...


class PresentationCandidatePage(_FrozenModel):
    snapshot_through_presentation_seq: int = Field(ge=0)
    records: tuple[PresentationCandidateRecord, ...]
    next_after_presentation_seq: int | None = Field(default=None, ge=0)
    has_more: bool


class AgentPresentationService:
    """Child-owned application shape for the future Root Connect provider."""

    def __init__(self, reader: PresentationCandidateReader) -> None:
        self._reader = reader

    async def pull_candidate_batches(
        self,
        *,
        run_id: str,
        after_presentation_seq: int,
        page_size: int,
        snapshot_through_presentation_seq: int | None = None,
    ) -> PresentationCandidatePage:
        if (
            not run_id
            or len(run_id) > 128
            or run_id.strip() != run_id
            or after_presentation_seq < 0
            or page_size < 1
            or page_size > MAX_PRESENTATION_PAGE_SIZE
        ):
            raise ValueError("PRESENTATION_CURSOR_INVALID")
        head = (
            await self._reader.presentation_head(run_id)
            if snapshot_through_presentation_seq is None
            else snapshot_through_presentation_seq
        )
        if head < after_presentation_seq:
            raise ValueError("PRESENTATION_SNAPSHOT_INVALID")
        records = tuple(
            await self._reader.pull_presentation_candidates(
                run_id, after_presentation_seq, head, page_size + 1
            )
        )
        page = records[:page_size]
        return PresentationCandidatePage(
            snapshot_through_presentation_seq=head,
            records=page,
            next_after_presentation_seq=(
                page[-1].presentation_seq if page else None
            ),
            has_more=len(records) > page_size,
        )

    async def acknowledge_candidate_admissions(
        self,
        *,
        run_id: str,
        acknowledgement_ref: str,
        expected_acknowledged_through_presentation_seq: int,
        receipts: tuple[PresentationAdmissionReceipt, ...],
        request_effect_digest: str,
    ) -> PresentationAcknowledgeState:
        if not isinstance(self._reader, PresentationAdmissionReceiptStore):
            raise RuntimeError("PRESENTATION_ACK_PORT_UNAVAILABLE")
        expected = expected_acknowledged_through_presentation_seq + 1
        if not receipts or any(
            receipt.presentation_seq != expected + offset
            for offset, receipt in enumerate(receipts)
        ):
            raise ValueError("PRESENTATION_ACK_SEQUENCE_INVALID")
        actual_digest = presentation_acknowledgement_digest(
            run_id=run_id,
            acknowledgement_ref=acknowledgement_ref,
            expected_acknowledged_through_presentation_seq=(
                expected_acknowledged_through_presentation_seq
            ),
            receipts=receipts,
        )
        if actual_digest != request_effect_digest:
            raise ValueError("PRESENTATION_ACK_DIGEST_INVALID")
        return await self._reader.acknowledge_presentation_admissions(
            PresentationAcknowledgeCommand(
                run_id=run_id,
                acknowledgement_ref=acknowledgement_ref,
                expected_acknowledged_through_presentation_seq=(
                    expected_acknowledged_through_presentation_seq
                ),
                receipts=receipts,
                request_effect_digest=request_effect_digest,
            )
        )

    async def quarantine_candidate_admission(
        self,
        *,
        run_id: str,
        rejection_ref: str,
        expected_acknowledged_through_presentation_seq: int,
        presentation_seq: int,
        presentation_ref: str,
        candidate_ref: str,
        reason: str,
        session_effect_digest: str,
    ) -> PresentationAcknowledgeState:
        if not isinstance(self._reader, PresentationAdmissionReceiptStore):
            raise RuntimeError("PRESENTATION_ACK_PORT_UNAVAILABLE")
        if presentation_seq != expected_acknowledged_through_presentation_seq + 1:
            raise ValueError("PRESENTATION_QUARANTINE_SEQUENCE_INVALID")
        return await self._reader.quarantine_presentation_admission(
            PresentationQuarantineCommand(
                run_id=run_id,
                rejection_ref=rejection_ref,
                expected_acknowledged_through_presentation_seq=(
                    expected_acknowledged_through_presentation_seq
                ),
                presentation_seq=presentation_seq,
                presentation_ref=presentation_ref,
                candidate_ref=candidate_ref,
                reason=reason,
                session_effect_digest=session_effect_digest,
            )
        )

    async def get_delivery_status(
        self, *, run_id: str
    ) -> PresentationAcknowledgeState:
        if not isinstance(self._reader, PresentationAdmissionReceiptStore):
            raise RuntimeError("PRESENTATION_ACK_PORT_UNAVAILABLE")
        return await self._reader.get_presentation_delivery_state(run_id)


def presentation_acknowledgement_digest(
    *,
    run_id: str,
    acknowledgement_ref: str,
    expected_acknowledged_through_presentation_seq: int,
    receipts: tuple[PresentationAdmissionReceipt, ...],
) -> str:
    payload = {
        "acknowledgementRef": acknowledgement_ref,
        "expectedAcknowledgedThroughPresentationSeq": (
            expected_acknowledged_through_presentation_seq
        ),
        "receipts": [
            receipt.model_dump(mode="json", exclude_none=True) for receipt in receipts
        ],
        "runId": run_id,
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    digest = hashlib.sha256(b"kokoro-presentation-ack-v1\0" + canonical).hexdigest()
    return f"sha256:{digest}"


def agent_thread_ref(namespace: str, thread_id: str) -> str:
    if not namespace or not thread_id:
        raise ValueError("PRESENTATION_THREAD_SCOPE_INVALID")
    material = f"kokoro-agent-thread-v1\0{namespace}\0{thread_id}".encode()
    return f"agent.thread:{hashlib.sha256(material).hexdigest()}"


def _message_ref(run_id: str, segment_id: str) -> str:
    material = f"kokoro-agent-message-v1\0{run_id}\0{segment_id}".encode()
    return f"agent.message:{hashlib.sha256(material).hexdigest()}"


def _activity_message_ref(run_id: str, activity_type: str, owner_ref: str) -> str:
    material = f"kokoro-agent-activity-v1\0{run_id}\0{activity_type}\0{owner_ref}".encode()
    return f"agent.activity:{hashlib.sha256(material).hexdigest()}"


def _safe_ref(value: str, *, domain: str) -> str:
    if (
        1 <= len(value) <= 128
        and value[0].isalnum()
        and all(char.isalnum() or char in "._:-" for char in value)
    ):
        return value
    return f"agent.{domain}:{hashlib.sha256(value.encode()).hexdigest()}"


def _clip(value: str, maximum: int = 16_384) -> str:
    if len(value) <= maximum:
        return value
    return value[: maximum - 1] + "…"


def _source_event_ref(event: AgentEvent) -> str:
    stable = event.event_id or f"index:{event.index}"
    material = f"v1\0{event.run_id}\0{event.kind}\0{stable}".encode()
    return f"agent.source:{hashlib.sha256(material).hexdigest()}"


def _candidate_source_ref(source_event_ref: str, member_ordinal: int) -> str:
    material = f"v1\0{source_event_ref}\0{member_ordinal}".encode()
    return f"agent.presentation.source:{hashlib.sha256(material).hexdigest()}"


def _presentation_ref(run_id: str, sequence: int, digest: str) -> str:
    material = f"v1\0{run_id}\0{sequence}\0{digest}".encode()
    return f"agent.presentation:sha256:{hashlib.sha256(material).hexdigest()}"


def _event_payload_digest(event: AgentEvent) -> str:
    encoded = json.dumps(
        event.model_dump(mode="json", exclude_none=True),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _message_state(
    states: dict[str, PresentationMessageState], run_id: str, segment_ref: str
) -> PresentationMessageState | None:
    return states.get(_message_ref(run_id, segment_ref))


def _activity(
    *, message_id: str, timestamp: int, activity_type: str, content: dict[str, object]
) -> ActivitySnapshotEvent:
    return ActivitySnapshotEvent(
        message_id=message_id,
        activity_type=activity_type,
        content=content,
        replace=True,
        timestamp=timestamp,
    )


def _events_for_source(
    event: AgentEvent,
    state: PresentationProjectionState,
) -> tuple[tuple[tuple[BaseEvent, str | None], ...], PresentationProjectionState]:
    if state.run_state in {"finished", "failed"}:
        raise ValueError("PRESENTATION_RUN_TERMINAL")
    presentation_scoped = isinstance(
        event,
        (
            MessageDelta,
            MessageCompleted,
            ToolInvoked,
            ToolReturned,
            ToolAwaitingApproval,
            PlanProposed,
            SubagentStarted,
            SubagentFinished,
            SubagentToolInvoked,
            SubagentToolReturned,
        ),
    )
    if state.run_state == "new" and presentation_scoped:
        raise ValueError("PRESENTATION_RUN_START_REQUIRED")
    if state.run_state == "running" and isinstance(event, RunStarted):
        raise ValueError("PRESENTATION_RUN_ALREADY_STARTED")

    states = {message.internal_message_ref: message for message in state.messages}
    planned: list[tuple[BaseEvent, str | None]] = []
    next_run_state = state.run_state
    thread_ref = state.internal_thread_ref
    if thread_ref is None:
        raise ValueError("PRESENTATION_THREAD_SCOPE_INVALID")

    if isinstance(event, RunStarted):
        planned.append(
            (
                RunStartedEvent(
                    thread_id=thread_ref,
                    run_id=event.run_id,
                    timestamp=event.timestamp,
                ),
                None,
            )
        )
        next_run_state = "running"
    elif isinstance(event, MessageDelta | MessageCompleted):
        segment_ref = event.payload.segment_id
        message_ref = _message_ref(event.run_id, segment_ref)
        message = _message_state(states, event.run_id, segment_ref)
        if message is not None and message.state == "closed":
            raise ValueError("PRESENTATION_MESSAGE_CLOSED")
        if message is None:
            planned.append(
                (
                    TextMessageStartEvent(
                        message_id=message_ref,
                        role="assistant",
                        timestamp=event.timestamp,
                    ),
                    message_ref,
                )
            )
            message = PresentationMessageState(
                internal_message_ref=message_ref,
                source_segment_ref=segment_ref,
                state="open",
                opened_ordinal=state.next_ordinal,
            )
        text = event.payload.delta if isinstance(event, MessageDelta) else event.payload.content
        if text and (isinstance(event, MessageDelta) or not message.text_seen):
            for offset in range(0, len(text), 16_384):
                planned.append(
                    (
                        TextMessageContentEvent(
                            message_id=message_ref,
                            delta=text[offset : offset + 16_384],
                            timestamp=event.timestamp,
                        ),
                        message_ref,
                    )
                )
            message = message.model_copy(update={"text_seen": True})
        if isinstance(event, MessageCompleted):
            planned.append(
                (
                    TextMessageEndEvent(
                        message_id=message_ref, timestamp=event.timestamp
                    ),
                    message_ref,
                )
            )
            message = message.model_copy(update={"state": "closed"})
        states[message_ref] = message
    elif isinstance(event, ToolInvoked | SubagentToolInvoked):
        message_ref = _activity_message_ref(
            event.run_id, "kokoro.tool-preview.v1", event.payload.tool_id
        )
        planned.append(
            (
                _activity(
                    message_id=message_ref,
                    timestamp=event.timestamp,
                    activity_type="kokoro.tool-preview.v1",
                    content={
                        "toolCallRef": _safe_ref(event.payload.tool_id, domain="tool"),
                        "label": _clip(event.payload.name, 1_024),
                        "status": "running",
                    },
                ),
                message_ref,
            )
        )
    elif isinstance(event, ToolReturned | SubagentToolReturned):
        message_ref = _activity_message_ref(
            event.run_id, "kokoro.tool-preview.v1", event.payload.tool_id
        )
        planned.append(
            (
                _activity(
                    message_id=message_ref,
                    timestamp=event.timestamp,
                    activity_type="kokoro.tool-preview.v1",
                    content={
                        "toolCallRef": _safe_ref(event.payload.tool_id, domain="tool"),
                        "label": _clip(event.payload.name, 1_024),
                        "status": "failed" if event.payload.is_error else "completed",
                        "isError": event.payload.is_error,
                        **({"truncated": True} if event.payload.truncated else {}),
                    },
                ),
                message_ref,
            )
        )
    elif isinstance(event, ToolAwaitingApproval):
        message_ref = _activity_message_ref(
            event.run_id, "kokoro.hitl.v1", event.payload.tool_id
        )
        planned.append(
            (
                _activity(
                    message_id=message_ref,
                    timestamp=event.timestamp,
                    activity_type="kokoro.hitl.v1",
                    content={
                        "ownerRef": _safe_ref(event.payload.tool_id, domain="hitl"),
                        "expectedVersion": 1,
                        "kind": (
                            "approval"
                            if event.payload.kind == "tool_approval"
                            else "interaction"
                        ),
                        "title": _clip(event.payload.name, 1_024),
                        "description": _clip(event.payload.description),
                        "allowedActions": list(dict.fromkeys(event.payload.allowed_decisions)),
                        "status": "pending",
                    },
                ),
                message_ref,
            )
        )
    elif isinstance(event, PlanProposed):
        message_ref = _activity_message_ref(
            event.run_id, "kokoro.plan.v1", event.payload.owner_ref
        )
        planned.append(
            (
                _activity(
                    message_id=message_ref,
                    timestamp=event.timestamp,
                    activity_type="kokoro.plan.v1",
                    content={
                        "planRef": _safe_ref(event.payload.owner_ref, domain="plan"),
                        "summary": _clip(event.payload.proposal.summary),
                        "status": "proposed",
                        "steps": [
                            {
                                "stepRef": _safe_ref(step.step_ref, domain="plan.step"),
                                "label": _clip(step.label, 1_024),
                                "status": step.status.replace("_", "-"),
                            }
                            for step in event.payload.proposal.steps[:256]
                        ],
                    },
                ),
                message_ref,
            )
        )
    elif isinstance(event, SubagentStarted | SubagentFinished):
        message_ref = _activity_message_ref(
            event.run_id, "kokoro.subagent.v1", event.payload.subagent_id
        )
        planned.append(
            (
                _activity(
                    message_id=message_ref,
                    timestamp=event.timestamp,
                    activity_type="kokoro.subagent.v1",
                    content={
                        "subagentRef": _safe_ref(
                            event.payload.subagent_id, domain="subagent"
                        ),
                        "status": (
                            "failed"
                            if isinstance(event, SubagentFinished) and event.payload.failed
                            else "completed"
                            if isinstance(event, SubagentFinished)
                            else "running"
                        ),
                    },
                ),
                message_ref,
            )
        )
    elif isinstance(event, RunCompleted | RunFailed):
        if state.run_state == "new":
            planned.append(
                (
                    RunStartedEvent(
                        thread_id=thread_ref,
                        run_id=event.run_id,
                        timestamp=event.timestamp,
                    ),
                    None,
                )
            )
        for message in sorted(states.values(), key=lambda item: item.opened_ordinal):
            if message.state == "open":
                planned.append(
                    (
                        TextMessageEndEvent(
                            message_id=message.internal_message_ref,
                            timestamp=event.timestamp,
                        ),
                        message.internal_message_ref,
                    )
                )
                states[message.internal_message_ref] = message.model_copy(
                    update={"state": "closed"}
                )
        if isinstance(event, RunCompleted) and event.payload.status == "completed":
            planned.append(
                (
                    RunFinishedEvent(
                        thread_id=thread_ref,
                        run_id=event.run_id,
                        timestamp=event.timestamp,
                        outcome=RunFinishedSuccessOutcome(),
                    ),
                    None,
                )
            )
            next_run_state = "finished"
        else:
            message = (
                "The agent run failed."
                if isinstance(event, RunFailed)
                else "Run cancelled."
            )
            code = (
                event.payload.code if isinstance(event, RunFailed) else "run_cancelled"
            )
            planned.append(
                (
                    RunErrorEvent(
                        message=_clip(message), code=code, timestamp=event.timestamp
                    ),
                    None,
                )
            )
            next_run_state = "failed"

    next_state = PresentationProjectionState(
        internal_run_ref=state.internal_run_ref,
        internal_thread_ref=state.internal_thread_ref,
        run_state=next_run_state,
        next_ordinal=state.next_ordinal + len(planned),
        messages=tuple(sorted(states.values(), key=lambda item: item.opened_ordinal)),
    )
    return tuple(planned), next_state


def plan_presentation_batch(
    event: AgentEvent,
    state: PresentationProjectionState,
    agent_thread_ref: str,
) -> PresentationCandidateBatch:
    """Plan one complete source batch; persistence must commit the batch and state together."""

    if not agent_thread_ref.startswith("agent.thread:"):
        raise ValueError("PRESENTATION_THREAD_SCOPE_INVALID")
    if state.run_state == "new":
        state = state.model_copy(
            update={
                "internal_run_ref": event.run_id,
                "internal_thread_ref": agent_thread_ref,
            }
        )
    elif (
        state.internal_run_ref != event.run_id
        or state.internal_thread_ref != agent_thread_ref
    ):
        raise ValueError("PRESENTATION_SCOPE_CONFLICT")

    source_event_ref = _source_event_ref(event)
    planned, next_state = _events_for_source(event, state)
    candidates: list[AgentAguiEventCandidate] = []
    for member, (official, message_ref) in enumerate(planned):
        source = AgentAguiCandidateSource(
            source_event_ref=_candidate_source_ref(source_event_ref, member),
            source_ordinal=str(state.next_ordinal + member),
            recorded_at=canonical_recorded_at(event.timestamp),
            route=AgentAguiCandidateRoute(
                internal_run_ref=event.run_id,
                internal_thread_ref=agent_thread_ref,
                **(
                    {}
                    if message_ref is None
                    else {"internal_message_ref": message_ref}
                ),
            ),
        )
        candidates.append(build_agui_candidate(official, source=source))
    return PresentationCandidateBatch(
        source_event_ref=source_event_ref,
        source_payload_sha256=_event_payload_digest(event),
        candidates=tuple(candidates),
        next_state=next_state,
    )


__all__ = [
    "AgentPresentationService",
    "MAX_PRESENTATION_PAGE_SIZE",
    "PresentationAcknowledgeCommand",
    "PresentationAcknowledgeState",
    "PresentationAdmissionReceipt",
    "PresentationAdmissionReceiptStore",
    "PresentationCandidateBatch",
    "PresentationCandidatePage",
    "PresentationCandidateReader",
    "PresentationCandidateRecord",
    "PresentationCandidateWriter",
    "PresentationMessageState",
    "PresentationProjectionState",
    "PresentationQuarantineCommand",
    "agent_thread_ref",
    "plan_presentation_batch",
    "presentation_acknowledgement_digest",
]
