from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from kokoro_agent.interaction.domain.models import (
    GroupRevisionCandidate,
    InteractionInvariantError,
    InteractionKind,
    OriginDescriptor,
    OwnerRevisionCandidate,
    OwnerRevisionRef,
    RevisionState,
    RunWriteFence,
)
from kokoro_agent.interaction.domain.identities import InteractionIdentityFactory
from kokoro_agent.interaction.activation import (
    INTERACTION_V2_ACTIVATION_BLOCKERS,
    INTERACTION_V2_RUNTIME_ACTIVATION,
)


SHA_A = "a" * 64
SHA_B = "b" * 64


def _member(
    *, ordinal: int, owner_ref: str, owner_revision: int = 1
) -> OwnerRevisionCandidate:
    canonical = f"member:{owner_ref}:{owner_revision}".encode()
    predecessor_revision = owner_revision - 1
    projection = InteractionIdentityFactory().projection_event(
        run_id="run-1",
        interaction_owner_ref=owner_ref,
        owner_revision=owner_revision,
    )
    return OwnerRevisionCandidate(
        interaction_owner_ref=owner_ref,
        origin_key_digest=hashlib.sha256(owner_ref.encode()).hexdigest(),
        owner_revision=owner_revision,
        projection_event_ref=projection.value,
        predecessor_projection_event_ref=(
            None
            if predecessor_revision == 0
            else InteractionIdentityFactory()
            .projection_event(
                run_id="run-1",
                interaction_owner_ref=owner_ref,
                owner_revision=predecessor_revision,
            )
            .value
        ),
        predecessor_evidence_sha256=None if predecessor_revision == 0 else SHA_A,
        member_evidence_sha256=hashlib.sha256(canonical).hexdigest(),
        canonical_member_evidence=canonical,
        projection_payload_sha256=SHA_B,
        application_request_ref=f"areq_{owner_ref}",
        interaction_kind=InteractionKind.APPROVAL,
        group_member_ordinal=ordinal,
        required_owner_revision_refs=(
            OwnerRevisionRef("iown_one", owner_revision),
            OwnerRevisionRef("iown_two", owner_revision),
        ),
        state=RevisionState.PENDING,
    )


def _frame(*, revision: int = 1) -> GroupRevisionCandidate:
    canonical = f"group:{revision}".encode()
    group_projection = InteractionIdentityFactory().group_projection(
        run_id="run-1",
        decision_group_ref="igrp-1",
        decision_group_revision=revision,
    )
    predecessor_group = (
        None
        if revision == 1
        else InteractionIdentityFactory()
        .group_projection(
            run_id="run-1",
            decision_group_ref="igrp-1",
            decision_group_revision=revision - 1,
        )
        .value
    )
    return GroupRevisionCandidate(
        run_id="run-1",
        decision_group_ref="igrp-1",
        decision_group_revision=revision,
        group_projection_ref=group_projection.value,
        predecessor_group_projection_ref=predecessor_group,
        predecessor_group_evidence_sha256=None if revision == 1 else SHA_A,
        group_evidence_sha256=hashlib.sha256(canonical).hexdigest(),
        canonical_group_evidence=canonical,
        pending_frame_digest=SHA_A,
        member_vector_sha256=SHA_B,
        members=(
            _member(ordinal=1, owner_ref="iown_one", owner_revision=revision),
            _member(ordinal=2, owner_ref="iown_two", owner_revision=revision),
        ),
        successor_proof_ref=None if revision == 1 else "proof-1",
        successor_proof_sha256=None if revision == 1 else SHA_B,
    )


def test_origin_descriptor_keeps_private_task_and_effect_fences() -> None:
    descriptor = OriginDescriptor(
        run_id="run-1",
        stable_task_path="root/research",
        origin_tool_call_ref="tool-1",
        invocation_elicitation_cursor=1,
        interaction_kind=InteractionKind.APPROVAL,
        base_descriptor_sha256=SHA_A,
        base_schema_sha256=SHA_B,
        continuation_ref="continuation-1",
        continuation_sha256=SHA_A,
        effect_idempotency_ref="effect-1",
        effect_idempotency_sha256=SHA_B,
    )
    candidate = descriptor.to_candidate()
    assert candidate.elicitation_ordinal == 1
    assert candidate.application_request_ref.startswith("areq_")
    assert candidate.interaction_owner_ref.startswith("iown_")
    assert len(candidate.origin_key_digest) == 64


def test_origin_descriptor_rejects_partial_effect_or_continuation_pair() -> None:
    with pytest.raises(InteractionInvariantError):
        OriginDescriptor(
            run_id="run-1",
            stable_task_path="root",
            origin_tool_call_ref="tool-1",
            invocation_elicitation_cursor=1,
            interaction_kind=InteractionKind.QUESTION,
            base_descriptor_sha256=SHA_A,
            base_schema_sha256=SHA_B,
            continuation_ref="orphan",
        )


def test_whole_frame_requires_contiguous_complete_member_vector() -> None:
    first = _member(ordinal=1, owner_ref="iown_one")
    skipped = _member(ordinal=3, owner_ref="iown_two")
    with pytest.raises(InteractionInvariantError, match="contiguous"):
        replace(_frame(), members=(first, skipped))


def test_whole_frame_rejects_partial_required_owner_vector() -> None:
    first = _member(ordinal=1, owner_ref="iown_one")
    partial = replace(
        _member(ordinal=2, owner_ref="iown_two"),
        required_owner_revision_refs=(OwnerRevisionRef("iown_two", 1),),
    )
    with pytest.raises(InteractionInvariantError, match="required owner vector"):
        replace(_frame(), members=(first, partial))


def test_successor_requires_exact_predecessor_and_proof_pairs() -> None:
    with pytest.raises(InteractionInvariantError, match="predecessor"):
        replace(_frame(revision=2), predecessor_group_evidence_sha256=None)


def test_run_fence_requires_positive_generation_and_checkpoint_digest() -> None:
    with pytest.raises(InteractionInvariantError):
        RunWriteFence(
            run_id="run-1",
            lease_owner_ref="worker-1",
            producer_instance_ref="worker-1",
            producer_generation=0,
            lease_valid_at_ms=100,
            checkpoint_ref="checkpoint-1",
            checkpoint_sha256=SHA_A,
            checkpoint_generation=1,
        )


def test_private_refs_and_canonical_frame_bytes_are_bounded() -> None:
    with pytest.raises(InteractionInvariantError, match="UTF-8 bytes"):
        replace(
            OriginDescriptor(
                run_id="run-1",
                stable_task_path="root",
                origin_tool_call_ref="tool-1",
                invocation_elicitation_cursor=1,
                interaction_kind=InteractionKind.APPROVAL,
                base_descriptor_sha256=SHA_A,
                base_schema_sha256=SHA_B,
            ),
            origin_tool_call_ref="x" * 257,
        )
    oversized = b"x" * (64 * 1024 + 1)
    with pytest.raises(InteractionInvariantError, match="64 KiB"):
        replace(
            _frame(),
            canonical_group_evidence=oversized,
            group_evidence_sha256=hashlib.sha256(oversized).hexdigest(),
        )


def test_runtime_activation_is_explicit_no_go_until_canonical_helpers_exist() -> None:
    assert INTERACTION_V2_RUNTIME_ACTIVATION == "no_go"
    assert any(
        "member_vector_sha256" in blocker
        for blocker in INTERACTION_V2_ACTIVATION_BLOCKERS
    )
    assert any(
        "pending_frame_digest" in blocker
        for blocker in INTERACTION_V2_ACTIVATION_BLOCKERS
    )
    assert any(
        "projection_payload_sha256" in blocker
        for blocker in INTERACTION_V2_ACTIVATION_BLOCKERS
    )
    assert any(
        "member evidence digest" in blocker
        for blocker in INTERACTION_V2_ACTIVATION_BLOCKERS
    )
