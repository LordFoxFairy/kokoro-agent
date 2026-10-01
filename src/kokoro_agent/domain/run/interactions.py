"""Pure, immutable full-set HITL rules; no persistence or native-evidence inference.

Callers supply already-authorized pause facts and exact private decision bytes.
A returned transition still needs the owner transaction/fence before execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum

from kokoro_agent.domain.run.models import LeaseFence
from typing import Literal

DecisionKind = Literal["approve", "edit", "reject", "respond", "submit"]
_DECISIONS = ("approve", "edit", "reject", "respond", "submit")


class Phase(StrEnum):
    ACTIVE = "active"
    WAITING = "waiting"
    RESUMING = "resuming"
    TERMINAL = "terminal"


class IntentStatus(StrEnum):
    ACCEPTED = "accepted"
    DISPATCH_STARTED = "dispatch_started"
    NATIVE_OBSERVED = "native_observed"
    UNKNOWN = "unknown"
    RECONCILED = "reconciled"
    TERMINAL = "terminal"


class InteractionConflict(ValueError):
    """Internal rule violation, not a published transport error code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _text(value: object) -> None:
    if type(value) is not str or not value:
        raise ValueError("expected nonempty identity")


def _revision(value: object, *, minimum: int) -> None:
    if type(value) is not int or value < minimum:
        raise ValueError("invalid revision")


def _tuple(
    value: tuple[object, ...], item_type: type[object], *, nonempty: bool = True
) -> None:
    if type(value) is not tuple:
        raise ValueError("expected immutable tuple")
    items = value
    if (nonempty and not items) or any(
        not isinstance(item, item_type) for item in items
    ):
        raise ValueError("invalid collection")


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidationIssue:
    instance_path: tuple[str | int, ...] = ()
    code: Literal["json_schema_invalid"] = "json_schema_invalid"

    def __post_init__(self) -> None:
        if self.code != "json_schema_invalid" or type(self.instance_path) is not tuple:
            raise ValueError("invalid validation issue")
        if any(
            type(part) not in (str, int) or (type(part) is int and part < 0)
            for part in self.instance_path
        ):
            raise ValueError("invalid instance path")


@dataclass(frozen=True, slots=True, kw_only=True)
class PendingItem:
    item_id: str
    request_id: str
    allowed_decisions: tuple[DecisionKind, ...]
    validation: ValidationIssue | None = None

    def __post_init__(self) -> None:
        _text(self.item_id)
        _text(self.request_id)
        _tuple(self.allowed_decisions, str)
        if len(set(self.allowed_decisions)) != len(self.allowed_decisions) or any(
            kind not in _DECISIONS for kind in self.allowed_decisions
        ):
            raise ValueError("invalid allowed decisions")
        if self.validation is not None and type(self.validation) is not ValidationIssue:
            raise ValueError("invalid validation issue")


@dataclass(frozen=True, slots=True, kw_only=True)
class PendingGroup:
    """Opaque occurrence group; native locator mapping belongs to the later adapter."""

    group_id: str
    items: tuple[PendingItem, ...]

    def __post_init__(self) -> None:
        _text(self.group_id)
        _tuple(self.items, PendingItem)
        if len({item.item_id for item in self.items}) != len(self.items):
            raise ValueError("duplicate pending item")


@dataclass(frozen=True, slots=True, kw_only=True)
class Decision:
    item_id: str
    kind: DecisionKind
    payload: bytes = field(default=b"", repr=False)

    def __post_init__(self) -> None:
        _text(self.item_id)
        if self.kind not in _DECISIONS or type(self.payload) is not bytes:
            raise ValueError("invalid decision")


@dataclass(frozen=True, slots=True, kw_only=True)
class Submission:
    command_id: str
    pause_revision: int
    pause_ref: str
    decisions: tuple[Decision, ...]

    def __post_init__(self) -> None:
        _text(self.command_id)
        _text(self.pause_ref)
        _revision(self.pause_revision, minimum=1)
        _tuple(self.decisions, Decision, nonempty=False)


@dataclass(frozen=True, slots=True, kw_only=True)
class DecisionGroup:
    group_id: str
    decisions: tuple[Decision, ...]

    def __post_init__(self) -> None:
        _text(self.group_id)
        _tuple(self.decisions, Decision)
        if len({decision.item_id for decision in self.decisions}) != len(
            self.decisions
        ):
            raise ValueError("duplicate decision")


def _decision_map(decisions: tuple[Decision, ...]) -> dict[str, Decision]:
    result = {decision.item_id: decision for decision in decisions}
    if len(result) != len(decisions):
        raise InteractionConflict("duplicate_decision")
    return result


@dataclass(frozen=True, slots=True, kw_only=True)
class ResumeIntent:
    command_id: str
    pause_revision: int
    pause_ref: str
    groups: tuple[DecisionGroup, ...]
    status: IntentStatus = IntentStatus.ACCEPTED
    attempt_id: str | None = None

    def __post_init__(self) -> None:
        _text(self.command_id)
        _text(self.pause_ref)
        _revision(self.pause_revision, minimum=1)
        _tuple(self.groups, DecisionGroup)
        if type(self.status) is not IntentStatus:
            raise ValueError("invalid intent status")
        if len({group.group_id for group in self.groups}) != len(self.groups):
            raise ValueError("duplicate decision group")
        _decision_map(
            tuple(decision for group in self.groups for decision in group.decisions)
        )
        if self.attempt_id is not None:
            _text(self.attempt_id)
        if self.status is IntentStatus.ACCEPTED and self.attempt_id is not None:
            raise ValueError("accepted intent already dispatched")
        if (
            self.status
            in (
                IntentStatus.DISPATCH_STARTED,
                IntentStatus.NATIVE_OBSERVED,
                IntentStatus.UNKNOWN,
                IntentStatus.RECONCILED,
            )
            and self.attempt_id is None
        ):
            raise ValueError("missing dispatch attempt")

    @property
    def can_dispatch(self) -> bool:
        """Pure eligibility only; never permission to skip durable start/fencing."""
        return self.status is IntentStatus.ACCEPTED

    def verify_replay(self, submission: Submission) -> None:
        if (submission.command_id, submission.pause_revision, submission.pause_ref) != (
            self.command_id,
            self.pause_revision,
            self.pause_ref,
        ):
            raise InteractionConflict("command_conflict")
        try:
            incoming = _decision_map(submission.decisions)
        except InteractionConflict:
            raise InteractionConflict("command_conflict") from None
        expected = {
            decision.item_id: decision
            for group in self.groups
            for decision in group.decisions
        }
        if incoming != expected:
            raise InteractionConflict("command_conflict")


@dataclass(frozen=True, slots=True, kw_only=True)
class InteractionState:
    interaction_revision: int = 0
    pause_revision: int = 0
    pause_ref: str | None = None
    phase: Phase = Phase.ACTIVE
    groups: tuple[PendingGroup, ...] = ()
    intent: ResumeIntent | None = None

    def __post_init__(self) -> None:
        _revision(self.interaction_revision, minimum=0)
        _revision(self.pause_revision, minimum=0)
        if (
            type(self.phase) is not Phase
            or self.pause_revision > self.interaction_revision
        ):
            raise ValueError("invalid interaction state")
        _tuple(self.groups, PendingGroup, nonempty=False)
        if self.pause_ref is not None:
            _text(self.pause_ref)
        if (self.pause_revision == 0) != (self.pause_ref is None):
            raise ValueError("pause identity mismatch")
        if bool(self.groups) != (self.phase in (Phase.WAITING, Phase.RESUMING)):
            raise ValueError("phase and collection disagree")
        if self.groups and self.pause_ref is None:
            raise ValueError("missing pause identity")
        group_ids = tuple(group.group_id for group in self.groups)
        item_ids = tuple(item.item_id for group in self.groups for item in group.items)
        if len(set(group_ids)) != len(group_ids) or len(set(item_ids)) != len(item_ids):
            raise ValueError("duplicate pending identity")
        if self.intent is not None and type(self.intent) is not ResumeIntent:
            raise ValueError("invalid intent")
        if self.intent is not None:
            if self.intent.pause_revision > self.pause_revision:
                raise ValueError("intent references future pause")
            if (
                self.intent.pause_revision == self.pause_revision
                and self.intent.pause_ref != self.pause_ref
            ):
                raise ValueError("intent pause identity mismatch")
        if self.phase is Phase.RESUMING:
            if self.interaction_revision <= self.pause_revision:
                raise ValueError("resuming requires accepted revision")
            if self.intent is None or self.intent.status not in (
                IntentStatus.ACCEPTED,
                IntentStatus.DISPATCH_STARTED,
                IntentStatus.NATIVE_OBSERVED,
                IntentStatus.UNKNOWN,
            ):
                raise ValueError("resuming requires unresolved intent")
            if (self.intent.pause_revision, self.intent.pause_ref) != (
                self.pause_revision,
                self.pause_ref,
            ):
                raise ValueError("intent and pause disagree")
            expected = tuple(
                (group.group_id, tuple(item.item_id for item in group.items))
                for group in self.groups
            )
            actual = tuple(
                (group.group_id, tuple(d.item_id for d in group.decisions))
                for group in self.intent.groups
            )
            if expected != actual:
                raise ValueError("intent and pending collection disagree")
            for group, decided in zip(self.groups, self.intent.groups, strict=True):
                if any(
                    decision.kind not in item.allowed_decisions
                    for item, decision in zip(
                        group.items, decided.decisions, strict=True
                    )
                ):
                    raise ValueError("intent decision not allowed")
        elif self.intent is not None:
            expected_status = (
                IntentStatus.TERMINAL
                if self.phase is Phase.TERMINAL
                else IntentStatus.RECONCILED
            )
            if self.intent.status is not expected_status:
                raise ValueError("intent and phase disagree")
            if (
                self.phase is Phase.WAITING
                and self.intent.pause_revision >= self.pause_revision
            ):
                raise ValueError("reconciled intent must precede new pause")

    def pause(
        self, *, pause_ref: str, groups: tuple[PendingGroup, ...]
    ) -> InteractionState:
        """Record an already-proven pause; never infer one from activity or ACK."""
        if self.phase is Phase.TERMINAL:
            return self
        if self.phase is Phase.WAITING:
            if (pause_ref, groups) == (self.pause_ref, self.groups):
                return self
            raise InteractionConflict("pause_conflict")
        intent = self.intent
        if self.phase is Phase.RESUMING:
            if (
                intent is None
                or intent.status
                not in (
                    IntentStatus.DISPATCH_STARTED,
                    IntentStatus.NATIVE_OBSERVED,
                    IntentStatus.UNKNOWN,
                )
                or pause_ref == self.pause_ref
            ):
                raise InteractionConflict("pause_conflict")
            intent = replace(intent, status=IntentStatus.RECONCILED)
        return InteractionState(
            interaction_revision=self.interaction_revision + 1,
            pause_revision=self.pause_revision + 1,
            pause_ref=pause_ref,
            phase=Phase.WAITING,
            groups=groups,
            intent=intent,
        )

    def accept(self, submission: Submission) -> InteractionState:
        if self.intent is not None and self.intent.command_id == submission.command_id:
            self.intent.verify_replay(submission)
            return self
        if self.phase is not Phase.WAITING:
            raise InteractionConflict("not_waiting")
        if (submission.pause_revision, submission.pause_ref) != (
            self.pause_revision,
            self.pause_ref,
        ):
            raise InteractionConflict("stale_pause")
        decisions = _decision_map(submission.decisions)
        expected = {item.item_id for group in self.groups for item in group.items}
        if decisions.keys() != expected:
            raise InteractionConflict("incomplete_collection")
        normalized: list[DecisionGroup] = []
        for group in self.groups:
            ordered = tuple(decisions[item.item_id] for item in group.items)
            if any(
                decision.kind not in item.allowed_decisions
                for item, decision in zip(group.items, ordered, strict=True)
            ):
                raise InteractionConflict("decision_not_allowed")
            normalized.append(DecisionGroup(group_id=group.group_id, decisions=ordered))
        intent = ResumeIntent(
            command_id=submission.command_id,
            pause_revision=submission.pause_revision,
            pause_ref=submission.pause_ref,
            groups=tuple(normalized),
        )
        return replace(
            self,
            phase=Phase.RESUMING,
            interaction_revision=self.interaction_revision + 1,
            intent=intent,
        )

    def start(self, *, command_id: str, attempt_id: str) -> InteractionState:
        _text(attempt_id)
        intent = self._unresolved(command_id)
        if (
            intent.status is IntentStatus.DISPATCH_STARTED
            and intent.attempt_id == attempt_id
        ):
            return self
        if not intent.can_dispatch:
            raise InteractionConflict("dispatch_forbidden")
        return replace(
            self,
            intent=replace(
                intent, status=IntentStatus.DISPATCH_STARTED, attempt_id=attempt_id
            ),
        )

    def unknown(self, *, command_id: str) -> InteractionState:
        if self.phase is Phase.TERMINAL:
            return self
        intent = self._unresolved(command_id)
        if intent.status is IntentStatus.UNKNOWN:
            return self
        if intent.status is not IntentStatus.DISPATCH_STARTED:
            raise InteractionConflict("not_dispatched")
        return replace(self, intent=replace(intent, status=IntentStatus.UNKNOWN))

    def terminal(self) -> InteractionState:
        if self.phase is Phase.TERMINAL:
            return self
        intent = (
            replace(self.intent, status=IntentStatus.TERMINAL)
            if self.intent is not None
            else None
        )
        return replace(
            self,
            phase=Phase.TERMINAL,
            groups=(),
            intent=intent,
            interaction_revision=self.interaction_revision + 1,
        )

    def _unresolved(self, command_id: str) -> ResumeIntent:
        if (
            self.phase is not Phase.RESUMING
            or self.intent is None
            or self.intent.command_id != command_id
        ):
            raise InteractionConflict("intent_conflict")
        return self.intent


@dataclass(frozen=True, slots=True, kw_only=True)
class DurablePauseSnapshot:
    """Complete immutable owner pause input; native provenance is checked upstream."""

    pause_ref: str
    canonical_bytes: bytes = field(repr=False)
    digest: str

    def __post_init__(self) -> None:
        _text(self.pause_ref)
        if (
            type(self.canonical_bytes) is not bytes
            or not 1 <= len(self.canonical_bytes) <= 8388608
        ):
            raise ValueError("invalid pause bytes")
        _text(self.digest)


@dataclass(frozen=True, slots=True, kw_only=True)
class ResumeDispatchPlan:
    attempt_id: str
    canonical_bytes: bytes = field(repr=False)

    def __post_init__(self) -> None:
        _text(self.attempt_id)
        if (
            type(self.canonical_bytes) is not bytes
            or not 1 <= len(self.canonical_bytes) <= 8388608
        ):
            raise ValueError("invalid dispatch plan bytes")


@dataclass(frozen=True, slots=True, kw_only=True)
class InteractionSnapshot:
    state: InteractionState
    pause: DurablePauseSnapshot | None
    source_index: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class InteractionCommitted:
    snapshot: InteractionSnapshot


@dataclass(frozen=True, slots=True, kw_only=True)
class ReplayedResume:
    snapshot: InteractionSnapshot
    original_intent: ResumeIntent | None
    accepted_source_index: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class StartedResume:
    snapshot: InteractionSnapshot
    attempt_id: str


class InteractionRunMissing(RuntimeError):
    """The scoped Run no longer exists; never recreate a child row."""


class InteractionAuthorityLost(RuntimeError):
    """This writer has lost its exact lease authority."""


class InteractionCorrupt(ValueError):
    """Stored interaction evidence failed strict decoding; no payload in message."""


@dataclass(frozen=True, slots=True, kw_only=True)
class AcceptedResume:
    snapshot: InteractionSnapshot
    lease: LeaseFence


@dataclass(frozen=True, slots=True, kw_only=True)
class ObservationTarget:
    generation: int
    command_id: str | None
    attempt_id: str | None

    def __post_init__(self) -> None:
        _revision(self.generation, minimum=1)
        if (self.command_id is None) != (self.attempt_id is None):
            raise ValueError("incomplete observation target")
        if self.command_id is not None:
            _text(self.command_id)
            _text(self.attempt_id)


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckpointObservation:
    target: ObservationTarget
    kind: Literal["pause", "read", "resume", "probe"]
    canonical_bytes: bytes = field(repr=False)
    digest: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ObservationStored:
    digest: str
    disposition: Literal["current", "audit"]
    inserted: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class ConsumedPauseEvidence:
    command_id: str
    attempt_id: str
    attempt_generation: int
    pause_revision: int
    pause_ref: str
    collection_digest: str
    observation_digests: tuple[str, ...]
    disposition: Literal["active", "waiting"]
    next_pause: DurablePauseSnapshot | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class UnknownResumeEvidence:
    command_id: str
    attempt_id: str
    attempt_generation: int
    pause_revision: int
    pause_ref: str
    collection_digest: str
    observation_digests: tuple[str, ...]
    reason: Literal["incomplete_native_evidence"] = "incomplete_native_evidence"


@dataclass(frozen=True, slots=True, kw_only=True)
class ResumeReadContext:
    snapshot: InteractionSnapshot
    original_pause: DurablePauseSnapshot
    intent: ResumeIntent
    dispatch_plan: ResumeDispatchPlan | None
    attempt_generation: int | None
    observation_digest: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class Quiescence:
    worker_boot_id: str
    invocation_id: str
    kind: Literal["local_drained"] = "local_drained"


@dataclass(frozen=True, slots=True, kw_only=True)
class QuiescentProbe:
    read_id: str
    observation: CheckpointObservation
    progress_digest: str
    quiescence: Quiescence


@dataclass(frozen=True, slots=True, kw_only=True)
class ReconcileProbeResult:
    count: int
    exhausted: bool
    snapshot: InteractionSnapshot


@dataclass(frozen=True, slots=True, kw_only=True)
class InteractionRecoveryTarget:
    run_id: str
    command_id: str
