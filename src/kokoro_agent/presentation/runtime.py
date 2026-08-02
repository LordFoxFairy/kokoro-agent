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
    recorded_at_milliseconds,
)

MAX_PRESENTATION_PAGE_SIZE = 256
MAX_UINT64_DECIMAL = "18446744073709551615"
HITL_OWNER_REF_PREFIX = "agent.hitl-owner:sha256:"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class PresentationMessageState(_FrozenModel):
    internal_message_ref: str = Field(min_length=1, max_length=128)
    source_segment_ref: str = Field(min_length=1, max_length=256)
    state: Literal["open", "closed"]
    opened_ordinal: int = Field(ge=0)
    text_seen: bool = False


class PresentationOwnerState(_FrozenModel):
    owner_key: str = Field(pattern=r"^agent\.presentation-owner:sha256:[0-9a-f]{64}$")
    activity_type: str = Field(min_length=1, max_length=128)
    message_ref: str = Field(min_length=1, max_length=128)
    identity_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    semantic_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    owner_version: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    updated_at: str = Field(
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"\.[0-9]{3}Z$"
        )
    )
    terminal_state: str | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_version(self) -> PresentationOwnerState:
        if (
            len(self.owner_version) > len(MAX_UINT64_DECIMAL)
            or (
                len(self.owner_version) == len(MAX_UINT64_DECIMAL)
                and self.owner_version > MAX_UINT64_DECIMAL
            )
        ):
            raise ValueError("ownerVersion exceeds uint64")
        recorded_at_milliseconds(self.updated_at)
        return self


class PresentationDecisionGroupState(_FrozenModel):
    group_key: str = Field(pattern=r"^agent\.decision-group-key:sha256:[0-9a-f]{64}$")
    decision_group_ref: str = Field(min_length=1, max_length=128)
    control_ref: str = Field(min_length=1, max_length=128)
    required_owner_refs: tuple[str, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_group(self) -> PresentationDecisionGroupState:
        if len(self.required_owner_refs) != len(set(self.required_owner_refs)):
            raise ValueError("required owner refs must be unique")
        return self


class PresentationProjectionState(_FrozenModel):
    internal_run_ref: str | None = None
    internal_thread_ref: str | None = None
    run_state: Literal["new", "running", "finished", "failed"] = "new"
    next_ordinal: int = Field(default=0, ge=0)
    messages: tuple[PresentationMessageState, ...] = ()
    owners: tuple[PresentationOwnerState, ...] = ()
    decision_groups: tuple[PresentationDecisionGroupState, ...] = ()

    @model_validator(mode="after")
    def validate_durable_identities(self) -> PresentationProjectionState:
        if (
            self.messages or self.owners or self.decision_groups
        ) and self.internal_run_ref is None:
            raise ValueError("PRESENTATION_STATE_RUN_REF_REQUIRED")

        message_refs = tuple(message.internal_message_ref for message in self.messages)
        if len(message_refs) != len(set(message_refs)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_INTERNAL_MESSAGE_REF")
        segment_refs = tuple(message.source_segment_ref for message in self.messages)
        if len(segment_refs) != len(set(segment_refs)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_SOURCE_SEGMENT_REF")
        if self.internal_run_ref is not None and any(
            message.internal_message_ref
            != _message_ref(self.internal_run_ref, message.source_segment_ref)
            for message in self.messages
        ):
            raise ValueError("PRESENTATION_STATE_MESSAGE_PLACEMENT_INVALID")

        owner_keys = tuple(owner.owner_key for owner in self.owners)
        if len(owner_keys) != len(set(owner_keys)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_OWNER_KEY")
        owner_message_refs = tuple(owner.message_ref for owner in self.owners)
        if len(owner_message_refs) != len(set(owner_message_refs)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_OWNER_MESSAGE_REF")
        if set(message_refs).intersection(owner_message_refs):
            raise ValueError("PRESENTATION_STATE_MESSAGE_REF_CONFLICT")
        if self.internal_run_ref is not None and any(
            owner.message_ref
            != _activity_message_ref(
                self.internal_run_ref, owner.activity_type, owner.owner_key
            )
            for owner in self.owners
        ):
            raise ValueError("PRESENTATION_STATE_OWNER_PLACEMENT_INVALID")

        group_keys = tuple(group.group_key for group in self.decision_groups)
        if len(group_keys) != len(set(group_keys)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_DECISION_GROUP_KEY")
        decision_group_refs = tuple(
            group.decision_group_ref for group in self.decision_groups
        )
        control_refs = tuple(group.control_ref for group in self.decision_groups)
        if len(decision_group_refs) != len(set(decision_group_refs)) or len(
            control_refs
        ) != len(set(control_refs)):
            raise ValueError("PRESENTATION_STATE_DECISION_GROUP_REFERENCE_CONFLICT")
        required_owner_refs = tuple(
            owner_ref
            for group in self.decision_groups
            for owner_ref in group.required_owner_refs
        )
        if any(
            not owner_ref.startswith(HITL_OWNER_REF_PREFIX)
            or len(owner_ref) != len(HITL_OWNER_REF_PREFIX) + 64
            or any(
                character not in "0123456789abcdef"
                for character in owner_ref[len(HITL_OWNER_REF_PREFIX) :]
            )
            for owner_ref in required_owner_refs
        ):
            raise ValueError("PRESENTATION_STATE_DECISION_OWNER_REF_INVALID")
        if len(required_owner_refs) != len(set(required_owner_refs)):
            raise ValueError("PRESENTATION_STATE_DECISION_OWNER_REF_CONFLICT")
        if any(
            group.decision_group_ref != _private_ref("decision-group", group.group_key)
            or group.control_ref != _private_ref("control-proposal", group.group_key)
            for group in self.decision_groups
        ):
            raise ValueError("PRESENTATION_STATE_DECISION_GROUP_PLACEMENT_INVALID")
        valid_hitl_owner_identities = {
            _fingerprint(
                "kokoro-agent-presentation-owner-identity-v1",
                {
                    "activityType": "kokoro.hitl.v1",
                    "ownerRef": owner_ref,
                    "decisionGroupRef": group.decision_group_ref,
                    "requiredOwnerRefs": group.required_owner_refs,
                    "controlRef": group.control_ref,
                },
            )
            for group in self.decision_groups
            for owner_ref in group.required_owner_refs
        }
        hitl_owner_identities = tuple(
            owner.identity_fingerprint
            for owner in self.owners
            if owner.activity_type == "kokoro.hitl.v1"
        )
        if len(hitl_owner_identities) != len(set(hitl_owner_identities)):
            raise ValueError("PRESENTATION_STATE_DUPLICATE_HITL_OWNER_MEMBERSHIP")
        if any(
            identity not in valid_hitl_owner_identities
            for identity in hitl_owner_identities
        ):
            raise ValueError("PRESENTATION_STATE_HITL_OWNER_MEMBERSHIP_INVALID")
        return self


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


def _private_ref(domain: str, *parts: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"kokoro-agent-{domain}-v1\0".encode())
    for part in parts:
        encoded = part.encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return f"agent.{domain}:sha256:{digest.hexdigest()}"


def _fingerprint(domain: str, value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    digest = hashlib.sha256(domain.encode() + b"\0" + encoded).hexdigest()
    return f"sha256:{digest}"


def _increment_uint64_decimal(value: str) -> str:
    if value == MAX_UINT64_DECIMAL:
        raise ValueError("PRESENTATION_OWNER_VERSION_OVERFLOW")
    digits = list(value)
    carry = 1
    for index in range(len(digits) - 1, -1, -1):
        next_digit = ord(digits[index]) - ord("0") + carry
        digits[index] = chr(ord("0") + next_digit % 10)
        carry = next_digit // 10
        if carry == 0:
            break
    if carry:
        digits.insert(0, "1")
    return "".join(digits)


def _owner_terminal_state(activity_type: str, content: dict[str, object]) -> str | None:
    status = content.get("status")
    if activity_type == "kokoro.safe-summary.v1":
        return None if status == "streaming" else str(status)
    if activity_type in {
        "kokoro.tool-preview.v1",
        "kokoro.plan.v1",
        "kokoro.subagent.v1",
    }:
        return str(status) if status in {"completed", "failed", "canceled"} else None
    if activity_type in {"kokoro.notice.v1", "kokoro.error.v1"}:
        return "terminal"
    return None


def _plan_owner_activity(
    *,
    run_id: str,
    timestamp: int,
    activity_type: str,
    raw_owner_key: str,
    owner_identity: dict[str, object],
    semantic_content: dict[str, object],
    owners: dict[str, PresentationOwnerState],
) -> tuple[ActivitySnapshotEvent | None, str]:
    owner_key = _private_ref("presentation-owner", activity_type, raw_owner_key)
    message_ref = _activity_message_ref(run_id, activity_type, owner_key)
    identity_fingerprint = _fingerprint(
        "kokoro-agent-presentation-owner-identity-v1",
        {"activityType": activity_type, **owner_identity},
    )
    semantic_fingerprint = _fingerprint(
        "kokoro-agent-presentation-owner-semantic-v1",
        {"activityType": activity_type, **semantic_content},
    )
    updated_at = canonical_recorded_at(timestamp)
    current = owners.get(owner_key)
    if current is None:
        owner_version = "1"
    else:
        if current.activity_type != activity_type or current.identity_fingerprint != identity_fingerprint:
            raise ValueError("PRESENTATION_OWNER_IDENTITY_CONFLICT")
        if current.message_ref != message_ref:
            raise ValueError("PRESENTATION_OWNER_PLACEMENT_CONFLICT")
        if updated_at < current.updated_at:
            raise ValueError("PRESENTATION_OWNER_TIME_REGRESSION")
        if current.semantic_fingerprint == semantic_fingerprint:
            return None, message_ref
        if current.terminal_state is not None:
            raise ValueError("PRESENTATION_OWNER_TERMINAL")
        owner_version = _increment_uint64_decimal(current.owner_version)
    terminal_state = _owner_terminal_state(activity_type, semantic_content)
    owners[owner_key] = PresentationOwnerState(
        owner_key=owner_key,
        activity_type=activity_type,
        message_ref=message_ref,
        identity_fingerprint=identity_fingerprint,
        semantic_fingerprint=semantic_fingerprint,
        owner_version=owner_version,
        updated_at=updated_at,
        terminal_state=terminal_state,
    )
    return (
        _activity(
            message_id=message_ref,
            timestamp=timestamp,
            activity_type=activity_type,
            content={
                **semantic_content,
                "ownerVersion": owner_version,
                "updatedAt": updated_at,
            },
        ),
        message_ref,
    )


def _hitl_group(
    event: ToolAwaitingApproval,
    groups: dict[str, PresentationDecisionGroupState],
) -> tuple[PresentationDecisionGroupState, str]:
    pending_tool_ids = tuple(event.payload.pending_tool_ids)
    if (
        not pending_tool_ids
        or len(pending_tool_ids) != len(set(pending_tool_ids))
        or event.payload.tool_id not in pending_tool_ids
    ):
        raise ValueError("PRESENTATION_HITL_GROUP_INVALID")
    group_key = _private_ref(
        "decision-group-key",
        event.run_id,
        event.payload.segment_id,
        *pending_tool_ids,
    )
    decision_group_ref = _private_ref("decision-group", group_key)
    control_ref = _private_ref("control-proposal", group_key)
    owner_refs = tuple(
        _private_ref("hitl-owner", group_key, tool_id) for tool_id in pending_tool_ids
    )
    expected = PresentationDecisionGroupState(
        group_key=group_key,
        decision_group_ref=decision_group_ref,
        control_ref=control_ref,
        required_owner_refs=owner_refs,
    )
    current = groups.get(group_key)
    if current is not None and current != expected:
        raise ValueError("PRESENTATION_HITL_GROUP_CONFLICT")
    groups[group_key] = expected
    return expected, owner_refs[pending_tool_ids.index(event.payload.tool_id)]


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


def _presentation_actions(actions: Sequence[str]) -> list[str]:
    projected = ("respond" if action == "submit" else action for action in actions)
    return list(dict.fromkeys(projected))


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
    owners = {owner.owner_key: owner for owner in state.owners}
    groups = {group.group_key: group for group in state.decision_groups}
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
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.tool-preview.v1",
            raw_owner_key=event.payload.tool_id,
            owner_identity={"toolCallRef": _safe_ref(event.payload.tool_id, domain="tool")},
            semantic_content={
                "toolCallRef": _safe_ref(event.payload.tool_id, domain="tool"),
                "label": _clip(event.payload.name, 1_024),
                "status": "running",
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, ToolReturned | SubagentToolReturned):
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.tool-preview.v1",
            raw_owner_key=event.payload.tool_id,
            owner_identity={"toolCallRef": _safe_ref(event.payload.tool_id, domain="tool")},
            semantic_content={
                "toolCallRef": _safe_ref(event.payload.tool_id, domain="tool"),
                "label": _clip(event.payload.name, 1_024),
                "status": "failed" if event.payload.is_error else "completed",
                "isError": event.payload.is_error,
                **({"truncated": True} if event.payload.truncated else {}),
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, ToolAwaitingApproval):
        group, owner_ref = _hitl_group(event, groups)
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.hitl.v1",
            raw_owner_key=f"{group.group_key}\0{event.payload.tool_id}",
            owner_identity={
                "ownerRef": owner_ref,
                "decisionGroupRef": group.decision_group_ref,
                "requiredOwnerRefs": group.required_owner_refs,
                "controlRef": group.control_ref,
            },
            semantic_content={
                "ownerRef": owner_ref,
                "decisionGroupRef": group.decision_group_ref,
                "requiredOwnerRefs": list(group.required_owner_refs),
                "controlRef": group.control_ref,
                "kind": (
                    "approval"
                    if event.payload.kind == "tool_approval"
                    else "interaction"
                ),
                "title": _clip(event.payload.name, 1_024),
                "description": _clip(event.payload.description),
                "allowedActions": _presentation_actions(event.payload.allowed_decisions),
                "status": "pending",
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, PlanProposed):
        plan_ref = _safe_ref(event.payload.owner_ref, domain="plan")
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.plan.v1",
            raw_owner_key=event.payload.owner_ref,
            owner_identity={"planRef": plan_ref},
            semantic_content={
                "planRef": plan_ref,
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
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, SubagentStarted | SubagentFinished):
        subagent_ref = _safe_ref(event.payload.subagent_id, domain="subagent")
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.subagent.v1",
            raw_owner_key=event.payload.subagent_id,
            owner_identity={"subagentRef": subagent_ref},
            semantic_content={
                "subagentRef": subagent_ref,
                "status": (
                    "failed"
                    if isinstance(event, SubagentFinished) and event.payload.failed
                    else "completed"
                    if isinstance(event, SubagentFinished)
                    else "running"
                ),
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
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
        owners=tuple(sorted(owners.values(), key=lambda item: item.owner_key)),
        decision_groups=tuple(sorted(groups.values(), key=lambda item: item.group_key)),
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
    "PresentationOwnerState",
    "PresentationCandidateWriter",
    "PresentationMessageState",
    "PresentationProjectionState",
    "PresentationQuarantineCommand",
    "agent_thread_ref",
    "plan_presentation_batch",
    "presentation_acknowledgement_digest",
]
