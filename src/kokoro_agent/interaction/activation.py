"""Explicit ADR-014 V2 activation decision for this foundation cut."""

from __future__ import annotations


INTERACTION_V2_RUNTIME_ACTIVATION = "no_go"
INTERACTION_V2_ACTIVATION_BLOCKERS: tuple[str, ...] = (
    "Root canonical helper for member_vector_sha256 is not frozen/generated",
    "Root canonical helper for pending_frame_digest is not frozen/generated",
    "Root canonical helper for projection_payload_sha256 is not frozen/generated",
    "Root canonical helper for member evidence digest is not frozen/generated",
    "Root-equivalent CEL/protovalidate runtime is not wired to generated Pydantic mirrors",
    "Mongo ADR-014 unique-index migration and replica-set readiness evidence are absent",
    "Existing durable sequence/evidence/outbox V2 committer is not implemented",
    "Private topology, accept-resume, and resume-receipt recovery are incomplete",
)
