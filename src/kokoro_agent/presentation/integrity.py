"""Canonical integrity primitives for the Root AgentPresentation V1 contract."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass

from google.protobuf.message import Message

from kokoro.agent.presentation.v1 import agent_presentation_pb2 as wire
from kokoro.common.v2 import command_envelope_pb2 as common_wire

ZERO_SHA256 = "sha256:" + "0" * 64
ACK_EFFECT_DOMAIN = "kokoro.agent.presentation.ack.v1"
QUARANTINE_EFFECT_DOMAIN = "kokoro.agent.presentation.quarantine.v1"
CONTRACT_VERSION = "agent-presentation@v1"
AUDIENCE = "agent.presentation.v1"


@dataclass(frozen=True, slots=True)
class PresentationCommandTrust:
    workload_identity_ref: str
    environment: str
    region: str
    security_epochs: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        axes = [axis for axis, _value in self.security_epochs]
        if (
            not self.workload_identity_ref
            or not self.environment
            or not self.region
            or len(axes) != len(set(axes))
            or any(not axis or value < 0 for axis, value in self.security_epochs)
        ):
            raise ValueError("PRESENTATION_COMMAND_TRUST_INVALID")


def _typed_digest(message: Message) -> str:
    material = (
        message.DESCRIPTOR.full_name.encode()
        + b"\0"
        + message.SerializeToString(deterministic=True)
    )
    return "sha256:" + hashlib.sha256(material).hexdigest()


def producer_fence(
    producer_instance_ref: str, producer_generation: int
) -> wire.PresentationProducerFence:
    payload = wire.PresentationProducerFenceDigestPayload(
        producer_instance_ref=producer_instance_ref,
        producer_generation=producer_generation,
    )
    return wire.PresentationProducerFence(
        producer_instance_ref=producer_instance_ref,
        producer_generation=producer_generation,
        producer_fence_digest=_typed_digest(payload),
    )


def record_chain_genesis_digest(
    run_id: str, producer: wire.PresentationProducerFence
) -> str:
    return _typed_digest(
        wire.PresentationRecordChainGenesisDigestPayload(
            run_id=run_id,
            producer=producer,
        )
    )


def candidate_record_digest(
    run_id: str, record: wire.PresentationCandidateRecord
) -> str:
    return _typed_digest(
        wire.PresentationCandidateRecordDigestPayload(
            run_id=run_id,
            presentation_ref=record.presentation_ref,
            previous_presentation_seq=record.previous_presentation_seq,
            presentation_seq=record.presentation_seq,
            envelope_digest=record.envelope_digest,
            candidate_ref=record.candidate_ref,
            candidate_digest=record.candidate_digest,
            recorded_at=record.recorded_at,
            producer=record.producer,
            previous_record_digest=record.previous_record_digest,
        )
    )


def snapshot_head_digest(
    run_id: str,
    producer: wire.PresentationProducerFence,
    snapshot_through_presentation_seq: int,
    snapshot_head_record_digest: str | None,
) -> str:
    payload = wire.PresentationSnapshotHeadDigestPayload(
        run_id=run_id,
        producer=producer,
        snapshot_through_presentation_seq=snapshot_through_presentation_seq,
    )
    if snapshot_head_record_digest is not None:
        payload.snapshot_head_record_digest = snapshot_head_record_digest
    return _typed_digest(payload)


def delivery_status_digest(status: wire.PresentationDeliveryStatus) -> str:
    payload = wire.PresentationDeliveryStatusDigestPayload(
        run_id=status.run_id,
        producer=status.producer,
        acknowledged_through_presentation_seq=(
            status.acknowledged_through_presentation_seq
        ),
        status_revision=status.status_revision,
        updated_at=status.updated_at,
    )
    for field in (
        "quarantine",
        "last_command",
        "terminal_seal",
    ):
        if status.HasField(field):
            getattr(payload, field).CopyFrom(getattr(status, field))
    if status.HasField("acknowledged_head_record_digest"):
        payload.acknowledged_head_record_digest = (
            status.acknowledged_head_record_digest
        )
    return _typed_digest(payload)


def effect_digest(message: Message, domain: str) -> str:
    zeroed = type(message)()
    zeroed.CopyFrom(message)
    setattr(zeroed, "effect_digest", ZERO_SHA256)
    digest = hashlib.sha256()
    for value in (
        domain.encode(),
        message.DESCRIPTOR.full_name.encode(),
        zeroed.SerializeToString(deterministic=True),
    ):
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return "sha256:" + digest.hexdigest()


def request_digest(
    *,
    operation: str,
    effect: Message,
    target_refs: Iterable[str],
    trust: PresentationCommandTrust,
) -> str:
    refs = tuple(sorted(target_refs))
    if len(refs) != len(set(refs)):
        raise ValueError("PRESENTATION_COMMAND_TARGET_DUPLICATE")
    epochs = tuple(sorted(trust.security_epochs))
    trust_wire = common_wire.CanonicalCommandTrustAxesV2(
        workload_identity_ref=trust.workload_identity_ref,
        audience=AUDIENCE,
        environment=trust.environment,
        region=trust.region,
        security_epochs=[
            common_wire.CanonicalSecurityEpochV2(axis=axis, value=value)
            for axis, value in epochs
        ],
    )
    envelope = common_wire.CanonicalCommandEnvelopeV2(
        contract_version=CONTRACT_VERSION,
        operation=operation,
        trust=trust_wire,
        target_refs=refs,
        effect=common_wire.CanonicalTypedProtobufV2(
            type_name=effect.DESCRIPTOR.full_name,
            known_field_protobuf=effect.SerializeToString(deterministic=True),
        ),
    )
    return _typed_digest(envelope).removeprefix("sha256:")


def reject_unknown_fields(message: Message) -> None:
    original = message.SerializeToString(deterministic=True)
    known = type(message)()
    known.CopyFrom(message)
    known.DiscardUnknownFields()
    if original != known.SerializeToString(deterministic=True):
        raise ValueError("PRESENTATION_UNKNOWN_FIELDS")


__all__ = [
    "ACK_EFFECT_DOMAIN",
    "AUDIENCE",
    "CONTRACT_VERSION",
    "PresentationCommandTrust",
    "QUARANTINE_EFFECT_DOMAIN",
    "ZERO_SHA256",
    "candidate_record_digest",
    "delivery_status_digest",
    "effect_digest",
    "producer_fence",
    "record_chain_genesis_digest",
    "reject_unknown_fields",
    "request_digest",
    "snapshot_head_digest",
]
