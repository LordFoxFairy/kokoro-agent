"""Pure validation of strict model-call evidence against frozen attempt facts."""

from __future__ import annotations

from datetime import UTC, datetime

import rfc8785
from pydantic import BaseModel, ConfigDict

from kokoro_agent.model.call_models import (
    ModelCallIdentityConflict,
    ModelCallInputInvalid,
)
from kokoro_agent.model.usage_evidence import parse_evidence
from kokoro_agent.protocol.model_usage_generated import ModelUsageEvidence

_MAX_INT64 = 9_223_372_036_854_775_807


class ValidatedModelCallEvidence(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    evidence: ModelUsageEvidence
    event_id: str
    revision: int
    digest: str
    observed_at: datetime
    outcome: str
    usage_state: str
    totals_state: str
    classification_state: str


def validate_append_evidence(
    raw: bytes,
    *,
    attempt_binding_bytes: bytes,
    planned_bytes: bytes,
    authorization_bytes: bytes,
    prepared_at: datetime,
    dispatch_started_at: datetime | None,
    database_now: datetime,
) -> ValidatedModelCallEvidence:
    try:
        evidence = parse_evidence(raw)
    except ValueError as error:
        raise ModelCallInputInvalid("model-call evidence is invalid") from error
    if rfc8785.dumps(evidence.model_dump(mode="json")) != raw:
        raise ModelCallInputInvalid("model-call evidence is not canonical")
    revision = int(evidence.revision)
    if not 1 <= revision <= _MAX_INT64 or not evidence.execution.seal:
        raise ModelCallInputInvalid("model-call evidence revision or seal is invalid")
    identity = evidence.identity.model_dump(mode="json", exclude={"event_id"})
    if rfc8785.dumps(identity) != attempt_binding_bytes:
        raise ModelCallIdentityConflict("model-call evidence identity conflicts")
    if rfc8785.dumps(evidence.planned.model_dump(mode="json")) != planned_bytes:
        raise ModelCallIdentityConflict("model-call evidence planned binding conflicts")
    if (
        rfc8785.dumps(evidence.authorization_observation.model_dump(mode="json"))
        != authorization_bytes
    ):
        raise ModelCallIdentityConflict("model-call evidence authorization conflicts")
    evidence_prepared = _utc_instant(evidence.execution.prepared_at)
    evidence_dispatch = (
        None
        if evidence.execution.dispatch_started_at is None
        else _utc_instant(evidence.execution.dispatch_started_at)
    )
    observed_at = _utc_instant(evidence.observed_at)
    if evidence_prepared != prepared_at or evidence_dispatch != dispatch_started_at:
        raise ModelCallIdentityConflict("model-call evidence time binding conflicts")
    if observed_at < prepared_at or observed_at > database_now:
        raise ModelCallInputInvalid("model-call evidence observed time is invalid")
    if dispatch_started_at is not None and observed_at < dispatch_started_at:
        raise ModelCallInputInvalid("model-call evidence predates dispatch")
    usage = evidence.usage
    totals_state = "unknown"
    classification_state = "unknown"
    if usage.state == "normalized":
        totals_state = usage.value.totals_state
        classification_state = usage.value.classification_state
    return ValidatedModelCallEvidence(
        evidence=evidence,
        event_id=evidence.identity.event_id,
        revision=revision,
        digest=evidence.digest,
        observed_at=observed_at,
        outcome=evidence.execution.outcome,
        usage_state=usage.state,
        totals_state=totals_state,
        classification_state=classification_state,
    )


def _utc_instant(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ModelCallInputInvalid("model-call evidence time is invalid") from error
    if parsed.tzinfo is None:
        raise ModelCallInputInvalid("model-call evidence time lacks timezone")
    return parsed.astimezone(UTC)


__all__ = ["ValidatedModelCallEvidence", "validate_append_evidence"]
