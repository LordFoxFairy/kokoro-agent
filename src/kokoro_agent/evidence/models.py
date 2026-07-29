"""Strict run-local evidence records derived from Agent's durable event authority."""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from google.protobuf.message import DecodeError
from pydantic import BaseModel, ConfigDict, Field, model_validator

from kokoro.agent.execution.v1 import agent_execution_evidence_pb2 as wire
from kokoro_agent.contract import (
    PlanProposedPayload,
    RunCompletedPayload,
    RunFailedPayload,
    RunOwnerCompletedPayload,
    RunStartedPayload,
    ToolAwaitingApprovalPayload,
)

MAX_CANONICAL_PAYLOAD_BYTES = 64 * 1024
EvidenceKind = Literal[
    "run.started",
    "action_owner",
    "plan_owner",
    "run.owner.completed",
    "run.completed",
    "run.failed",
]

_EVIDENCE_KIND_BY_EVENT: dict[str, EvidenceKind] = {
    "run.started": "run.started",
    "tool.awaiting_approval": "action_owner",
    "plan.proposed": "plan_owner",
    "run.owner.completed": "run.owner.completed",
    "run.completed": "run.completed",
    "run.failed": "run.failed",
}
_ONEOF_BY_KIND: dict[EvidenceKind, str] = {
    "run.started": "run_started",
    "action_owner": "action_owner",
    "plan_owner": "plan_owner",
    "run.owner.completed": "run_owner_completed",
    "run.completed": "run_completed",
    "run.failed": "run_failed",
}


class EvidencePayloadTooLarge(ValueError):
    """The typed canonical evidence envelope exceeded its public wire cap."""


class DurableExecutionEvidence(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    evidence_ref: str = Field(min_length=1, max_length=256)
    evidence_version: Literal[1] = 1
    run_id: str = Field(min_length=1, max_length=128)
    durable_seq: int = Field(gt=0)
    event_id: str = Field(min_length=1, max_length=256)
    kind: EvidenceKind
    canonical_payload: bytes = Field(max_length=MAX_CANONICAL_PAYLOAD_BYTES)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    recorded_at_ms: int = Field(ge=0)
    producer_instance_ref: str = Field(min_length=1, max_length=256)
    producer_generation: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_canonical_payload(self) -> DurableExecutionEvidence:
        actual = hashlib.sha256(self.canonical_payload).hexdigest()
        if actual != self.payload_sha256:
            raise ValueError("payload sha256 does not match canonical payload")
        try:
            payload = wire.DurableExecutionCanonicalPayloadV1.FromString(
                self.canonical_payload
            )
        except DecodeError as error:
            raise ValueError("canonical payload is not V1 protobuf") from error
        if payload.WhichOneof("payload") != _ONEOF_BY_KIND[self.kind]:
            raise ValueError("canonical payload kind does not match evidence kind")
        return self


def evidence_kind_for_event(event_kind: str) -> EvidenceKind | None:
    return _EVIDENCE_KIND_BY_EVENT.get(event_kind)


def _canonical_event_json(payload: BaseModel) -> bytes:
    return json.dumps(
        payload.model_dump(mode="json", exclude_none=True),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _typed_payload(event_kind: str, payload_json: str) -> bytes:
    try:
        raw = json.loads(payload_json)
    except (TypeError, ValueError) as error:
        raise ValueError("EVIDENCE_PAYLOAD_INVALID") from error
    try:
        if event_kind == "run.started":
            RunStartedPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                run_started=wire.RunStartedEvidenceV1()
            )
        elif event_kind == "tool.awaiting_approval":
            owner = ToolAwaitingApprovalPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                action_owner=wire.ActionOwnerEvidenceV1(
                    owner_ref=owner.tool_id,
                    owner_version=1,
                    segment_id=owner.segment_id,
                    action_name=owner.name,
                    awaiting_kind=owner.kind,
                    action_payload_sha256=hashlib.sha256(
                        _canonical_event_json(owner)
                    ).hexdigest(),
                )
            )
        elif event_kind == "plan.proposed":
            owner = PlanProposedPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                plan_owner=wire.PlanOwnerEvidenceV1(
                    owner_ref=owner.owner_ref,
                    owner_version=owner.owner_version,
                    segment_id=owner.segment_id,
                    proposal_payload_sha256=hashlib.sha256(
                        _canonical_event_json(owner)
                    ).hexdigest(),
                )
            )
        elif event_kind == "run.owner.completed":
            owner = RunOwnerCompletedPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                run_owner_completed=wire.RunOwnerCompletedEvidenceV1(
                    execution_context_anchor=owner.execution_context_anchor,
                    execution_context_digest=owner.execution_context_digest,
                    owner_revision=owner.owner_revision,
                )
            )
        elif event_kind == "run.completed":
            completed = RunCompletedPayload.model_validate(raw)
            status = (
                wire.RUN_COMPLETED_EVIDENCE_STATUS_COMPLETED
                if completed.status == "completed"
                else wire.RUN_COMPLETED_EVIDENCE_STATUS_CANCELLED
            )
            result = wire.RunCompletedEvidenceV1(status=status)
            if completed.token_usage is not None:
                result.token_usage.CopyFrom(
                    wire.TokenUsageEvidenceV1(
                        input_tokens=completed.token_usage.input_tokens,
                        output_tokens=completed.token_usage.output_tokens,
                    )
                )
            payload = wire.DurableExecutionCanonicalPayloadV1(run_completed=result)
        elif event_kind == "run.failed":
            failed = RunFailedPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                run_failed=wire.RunFailedEvidenceV1(
                    code=failed.code,
                    error_kind=failed.error_kind,
                    message=failed.message,
                )
            )
        else:
            raise ValueError("EVIDENCE_KIND_UNSUPPORTED")
    except (TypeError, ValueError) as error:
        if str(error) in {"EVIDENCE_KIND_UNSUPPORTED", "EVIDENCE_PAYLOAD_INVALID"}:
            raise
        raise ValueError("EVIDENCE_PAYLOAD_INVALID") from error
    encoded = payload.SerializeToString(deterministic=True)
    if len(encoded) > MAX_CANONICAL_PAYLOAD_BYTES:
        raise EvidencePayloadTooLarge("EVIDENCE_PAYLOAD_TOO_LARGE")
    return encoded


def make_durable_execution_evidence(
    *,
    run_id: str,
    durable_seq: int,
    event_id: str,
    event_kind: str,
    payload_json: str,
    recorded_at_ms: int,
    producer_instance_ref: str,
    producer_generation: int,
) -> DurableExecutionEvidence:
    kind = evidence_kind_for_event(event_kind)
    if kind is None:
        raise ValueError("EVIDENCE_KIND_UNSUPPORTED")
    canonical_payload = _typed_payload(event_kind, payload_json)
    identity = f"v1\0{run_id}\0{event_id}".encode()
    return DurableExecutionEvidence(
        evidence_ref=f"aee_{hashlib.sha256(identity).hexdigest()}",
        run_id=run_id,
        durable_seq=durable_seq,
        event_id=event_id,
        kind=kind,
        canonical_payload=canonical_payload,
        payload_sha256=hashlib.sha256(canonical_payload).hexdigest(),
        recorded_at_ms=recorded_at_ms,
        producer_instance_ref=producer_instance_ref,
        producer_generation=producer_generation,
    )
