"""Immutable, framework-independent ADR-014 V2 candidates and fences."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import StrEnum

from kokoro_agent.interaction.domain.identities import InteractionIdentityFactory


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_REF_BYTES = 256
_MAX_RUN_ID_BYTES = 128
_MAX_STABLE_TASK_PATH_BYTES = 1024
_MAX_CANONICAL_EVIDENCE_BYTES = 64 * 1024


class InteractionInvariantError(ValueError):
    """A candidate cannot be an immutable ADR-014 fact."""


class InteractionKind(StrEnum):
    APPROVAL = "approval"
    QUESTION = "question"
    STRUCTURED_INPUT = "structured_input"
    RESULT_REVIEW = "result_review"
    PLAN = "plan"


class RevisionState(StrEnum):
    PENDING = "pending"
    RESUME_PERSISTED = "resume_persisted"
    APPLYING = "applying"
    APPLIED = "applied"
    OUTCOME_UNKNOWN = "outcome_unknown"
    RESOLVED = "resolved"
    SUPERSEDED_BY_REVISION = "superseded_by_revision"
    CANCELED = "canceled"
    CLOSED_BY_TERMINAL = "closed_by_terminal"


def _nonempty(value: str, name: str, *, maximum_bytes: int = _MAX_REF_BYTES) -> None:
    if not value:
        raise InteractionInvariantError(f"{name} must be non-empty")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise InteractionInvariantError(f"{name} must be valid Unicode") from exc
    if len(encoded) > maximum_bytes:
        raise InteractionInvariantError(f"{name} exceeds {maximum_bytes} UTF-8 bytes")


def _sha(value: str, name: str) -> None:
    if _SHA256.fullmatch(value) is None:
        raise InteractionInvariantError(f"{name} must be lowercase sha256")


def _pair(left: str | None, right: str | None, name: str) -> None:
    if (left is None) != (right is None):
        raise InteractionInvariantError(f"{name} must be an atomic pair")
    if left is not None:
        _nonempty(left, f"{name}_ref")
    if right is not None:
        _sha(right, f"{name}_sha256")


@dataclass(frozen=True, slots=True)
class RunWriteFence:
    run_id: str
    lease_owner_ref: str
    producer_instance_ref: str
    producer_generation: int
    lease_valid_at_ms: int
    checkpoint_ref: str
    checkpoint_sha256: str
    checkpoint_generation: int

    def __post_init__(self) -> None:
        _nonempty(self.run_id, "run_id", maximum_bytes=_MAX_RUN_ID_BYTES)
        for name in ("lease_owner_ref", "producer_instance_ref", "checkpoint_ref"):
            _nonempty(getattr(self, name), name)
        _sha(self.checkpoint_sha256, "checkpoint_sha256")
        if self.producer_generation < 1 or self.checkpoint_generation < 1:
            raise InteractionInvariantError("run fence generations must be positive")
        if self.lease_valid_at_ms < 0:
            raise InteractionInvariantError("lease_valid_at_ms must be nonnegative")


@dataclass(frozen=True, slots=True)
class OriginDescriptor:
    run_id: str
    stable_task_path: str
    origin_tool_call_ref: str
    invocation_elicitation_cursor: int
    interaction_kind: InteractionKind
    base_descriptor_sha256: str
    base_schema_sha256: str
    continuation_ref: str | None = None
    continuation_sha256: str | None = None
    effect_idempotency_ref: str | None = None
    effect_idempotency_sha256: str | None = None

    def __post_init__(self) -> None:
        _nonempty(self.run_id, "run_id", maximum_bytes=_MAX_RUN_ID_BYTES)
        _nonempty(
            self.stable_task_path,
            "stable_task_path",
            maximum_bytes=_MAX_STABLE_TASK_PATH_BYTES,
        )
        _nonempty(self.origin_tool_call_ref, "origin_tool_call_ref")
        if self.invocation_elicitation_cursor < 1:
            raise InteractionInvariantError("elicitation cursor must start at one")
        _sha(self.base_descriptor_sha256, "base_descriptor_sha256")
        _sha(self.base_schema_sha256, "base_schema_sha256")
        _pair(self.continuation_ref, self.continuation_sha256, "continuation")
        _pair(
            self.effect_idempotency_ref,
            self.effect_idempotency_sha256,
            "effect_idempotency",
        )

    def to_candidate(self) -> OriginCandidate:
        factory = InteractionIdentityFactory()
        application_request = factory.application_request(
            run_id=self.run_id,
            stable_task_path=self.stable_task_path,
            origin_tool_call_ref=self.origin_tool_call_ref,
            interaction_kind=self.interaction_kind.value,
            elicitation_ordinal=self.invocation_elicitation_cursor,
        )
        owner = factory.interaction_owner(
            run_id=self.run_id,
            stable_task_path=self.stable_task_path,
            origin_tool_call_ref=self.origin_tool_call_ref,
            interaction_kind=self.interaction_kind.value,
            application_request_ref=application_request.value,
        )
        return OriginCandidate(
            run_id=self.run_id,
            stable_task_path=self.stable_task_path,
            origin_tool_call_ref=self.origin_tool_call_ref,
            elicitation_ordinal=self.invocation_elicitation_cursor,
            interaction_kind=self.interaction_kind,
            application_request_ref=application_request.value,
            interaction_owner_ref=owner.value,
            origin_key_digest=hashlib.sha256(owner.canonical_json.encode()).hexdigest(),
            base_descriptor_sha256=self.base_descriptor_sha256,
            base_schema_sha256=self.base_schema_sha256,
            continuation_ref=self.continuation_ref,
            continuation_sha256=self.continuation_sha256,
            effect_idempotency_ref=self.effect_idempotency_ref,
            effect_idempotency_sha256=self.effect_idempotency_sha256,
        )


@dataclass(frozen=True, slots=True)
class OriginCandidate:
    run_id: str
    stable_task_path: str
    origin_tool_call_ref: str
    elicitation_ordinal: int
    interaction_kind: InteractionKind
    application_request_ref: str
    interaction_owner_ref: str
    origin_key_digest: str
    base_descriptor_sha256: str
    base_schema_sha256: str
    continuation_ref: str | None
    continuation_sha256: str | None
    effect_idempotency_ref: str | None
    effect_idempotency_sha256: str | None

    def __post_init__(self) -> None:
        if self.elicitation_ordinal < 1:
            raise InteractionInvariantError("elicitation ordinal must be positive")
        _nonempty(self.run_id, "run_id", maximum_bytes=_MAX_RUN_ID_BYTES)
        _nonempty(
            self.stable_task_path,
            "stable_task_path",
            maximum_bytes=_MAX_STABLE_TASK_PATH_BYTES,
        )
        for name in (
            "origin_tool_call_ref",
            "application_request_ref",
            "interaction_owner_ref",
        ):
            _nonempty(getattr(self, name), name)
        for name in (
            "origin_key_digest",
            "base_descriptor_sha256",
            "base_schema_sha256",
        ):
            _sha(getattr(self, name), name)
        _pair(self.continuation_ref, self.continuation_sha256, "continuation")
        _pair(
            self.effect_idempotency_ref,
            self.effect_idempotency_sha256,
            "effect_idempotency",
        )


@dataclass(frozen=True, slots=True)
class OwnerRevisionRef:
    interaction_owner_ref: str
    owner_revision: int

    def __post_init__(self) -> None:
        _nonempty(self.interaction_owner_ref, "interaction_owner_ref")
        if self.owner_revision < 1:
            raise InteractionInvariantError("owner_revision must be positive")


@dataclass(frozen=True, slots=True)
class OwnerRevisionCandidate:
    interaction_owner_ref: str
    origin_key_digest: str
    owner_revision: int
    projection_event_ref: str
    predecessor_projection_event_ref: str | None
    predecessor_evidence_sha256: str | None
    member_evidence_sha256: str
    canonical_member_evidence: bytes
    projection_payload_sha256: str
    application_request_ref: str
    interaction_kind: InteractionKind
    group_member_ordinal: int
    required_owner_revision_refs: tuple[OwnerRevisionRef, ...]
    state: RevisionState

    def __post_init__(self) -> None:
        if self.owner_revision < 1 or self.group_member_ordinal < 1:
            raise InteractionInvariantError(
                "owner revision and member ordinal must be positive"
            )
        for name in (
            "interaction_owner_ref",
            "projection_event_ref",
            "application_request_ref",
        ):
            _nonempty(getattr(self, name), name)
        for name in (
            "origin_key_digest",
            "member_evidence_sha256",
            "projection_payload_sha256",
        ):
            _sha(getattr(self, name), name)
        if not self.canonical_member_evidence:
            raise InteractionInvariantError(
                "canonical member evidence must be non-empty"
            )
        if len(self.canonical_member_evidence) > _MAX_CANONICAL_EVIDENCE_BYTES:
            raise InteractionInvariantError("canonical member evidence exceeds 64 KiB")
        if (
            hashlib.sha256(self.canonical_member_evidence).hexdigest()
            != self.member_evidence_sha256
        ):
            raise InteractionInvariantError(
                "member evidence digest does not match canonical bytes"
            )
        _pair(
            self.predecessor_projection_event_ref,
            self.predecessor_evidence_sha256,
            "member predecessor",
        )
        if (
            self.owner_revision == 1
            and self.predecessor_projection_event_ref is not None
        ):
            raise InteractionInvariantError("revision one cannot have a predecessor")
        if self.owner_revision > 1 and self.predecessor_projection_event_ref is None:
            raise InteractionInvariantError(
                "successor revision requires exact predecessor"
            )
        if self.state is not RevisionState.PENDING:
            raise InteractionInvariantError("new owner revision must start pending")


@dataclass(frozen=True, slots=True)
class GroupRevisionCandidate:
    run_id: str
    decision_group_ref: str
    decision_group_revision: int
    group_projection_ref: str
    predecessor_group_projection_ref: str | None
    predecessor_group_evidence_sha256: str | None
    group_evidence_sha256: str
    canonical_group_evidence: bytes
    pending_frame_digest: str
    member_vector_sha256: str
    members: tuple[OwnerRevisionCandidate, ...]
    successor_proof_ref: str | None
    successor_proof_sha256: str | None

    def __post_init__(self) -> None:
        if self.decision_group_revision < 1:
            raise InteractionInvariantError("group revision must be positive")
        _nonempty(self.run_id, "run_id", maximum_bytes=_MAX_RUN_ID_BYTES)
        for name in ("decision_group_ref", "group_projection_ref"):
            _nonempty(getattr(self, name), name)
        for name in (
            "group_evidence_sha256",
            "pending_frame_digest",
            "member_vector_sha256",
        ):
            _sha(getattr(self, name), name)
        if not self.canonical_group_evidence:
            raise InteractionInvariantError(
                "canonical group evidence must be non-empty"
            )
        if len(self.canonical_group_evidence) > _MAX_CANONICAL_EVIDENCE_BYTES:
            raise InteractionInvariantError("canonical group evidence exceeds 64 KiB")
        if (
            sum(len(member.canonical_member_evidence) for member in self.members)
            > _MAX_CANONICAL_EVIDENCE_BYTES
        ):
            raise InteractionInvariantError(
                "whole-frame member evidence exceeds 64 KiB"
            )
        if (
            hashlib.sha256(self.canonical_group_evidence).hexdigest()
            != self.group_evidence_sha256
        ):
            raise InteractionInvariantError(
                "group evidence digest does not match canonical bytes"
            )
        _pair(
            self.predecessor_group_projection_ref,
            self.predecessor_group_evidence_sha256,
            "group predecessor",
        )
        _pair(self.successor_proof_ref, self.successor_proof_sha256, "successor proof")
        if self.decision_group_revision == 1:
            if (
                self.predecessor_group_projection_ref is not None
                or self.successor_proof_ref is not None
            ):
                raise InteractionInvariantError(
                    "initial group cannot have predecessor/successor proof"
                )
        elif (
            self.predecessor_group_projection_ref is None
            or self.successor_proof_ref is None
        ):
            raise InteractionInvariantError(
                "successor group requires predecessor and proof pairs"
            )
        if not self.members or len(self.members) > 64:
            raise InteractionInvariantError(
                "whole frame must have one through 64 members"
            )
        ordinals = tuple(member.group_member_ordinal for member in self.members)
        if ordinals != tuple(range(1, len(self.members) + 1)):
            raise InteractionInvariantError(
                "group member ordinals must be contiguous and ordered"
            )
        refs = tuple(
            OwnerRevisionRef(member.interaction_owner_ref, member.owner_revision)
            for member in self.members
        )
        if len({ref.interaction_owner_ref for ref in refs}) != len(refs):
            raise InteractionInvariantError("group owner refs must be unique")
        for member in self.members:
            if member.required_owner_revision_refs != refs:
                raise InteractionInvariantError(
                    "every member must bind the complete required owner vector"
                )
            expected = InteractionIdentityFactory().projection_event(
                run_id=self.run_id,
                interaction_owner_ref=member.interaction_owner_ref,
                owner_revision=member.owner_revision,
            )
            if member.projection_event_ref != expected.value:
                raise InteractionInvariantError(
                    "member projection identity does not match Root contract"
                )
        expected_group = InteractionIdentityFactory().group_projection(
            run_id=self.run_id,
            decision_group_ref=self.decision_group_ref,
            decision_group_revision=self.decision_group_revision,
        )
        if self.group_projection_ref != expected_group.value:
            raise InteractionInvariantError(
                "group projection identity does not match Root contract"
            )


@dataclass(frozen=True, slots=True)
class PublishedFrame:
    group_projection_ref: str
    evidence_ref: str
    durable_seq: int
    event_id: str
    created: bool

    def __post_init__(self) -> None:
        if self.durable_seq < 1:
            raise InteractionInvariantError("durable sequence must be positive")
        for name in ("group_projection_ref", "evidence_ref", "event_id"):
            _nonempty(getattr(self, name), name)
