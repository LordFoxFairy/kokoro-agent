"""Pure validation of strict model-call evidence against frozen attempt facts."""

from __future__ import annotations

from datetime import UTC, datetime

import rfc8785
from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter
import hashlib
from collections.abc import Mapping
from uuid import uuid4

from kokoro_agent.model.call_models import (
    AttemptBinding,
    ModelCallIdentityConflict,
    ModelCallInputInvalid,
)
from kokoro_agent.model.usage_evidence import parse_evidence
from kokoro_agent.protocol.model_usage_generated import (
    AuthorizationGranted,
    AuthorizationNotRequested,
    AuthorizationRequested,
    ModelUsageEvidence,
    PlannedBinding,
)

_MAX_INT64 = 9_223_372_036_854_775_807
_EVIDENCE_DOMAIN = b"kokoro-agent:model-usage-evidence:v1\n"
_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


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


def build_terminal_closure_evidence(
    attempt: Mapping[str, object], database_now: datetime
) -> tuple[bytes, ValidatedModelCallEvidence]:
    binding_bytes = _bytes(attempt["attempt_binding_bytes"])
    planned_bytes = _bytes(attempt["planned_bytes"])
    authorization_bytes = _bytes(attempt["authorization_bytes"])
    _validate_frozen_attempt(attempt, binding_bytes, planned_bytes, authorization_bytes)
    status = _text(attempt["status"])
    authorization_state = _text(attempt["authorization_state"])
    expected_authorization = {
        "prepared": "not_requested",
        "authorization_pending": "requested_unknown",
        "authorized": "granted",
        "dispatch_started": "granted",
    }
    if expected_authorization.get(status) != authorization_state:
        raise ModelCallIdentityConflict("model-call closure state conflicts")
    dispatch_started_at = _optional_instant(attempt["dispatch_started_at"])
    if (status == "dispatch_started") != (dispatch_started_at is not None):
        raise ModelCallIdentityConflict("model-call closure dispatch conflicts")
    prepared_at = _instant(attempt["prepared_at"])
    observed_at = _millisecond(database_now)
    identity = _json_object(binding_bytes)
    identity["event_id"] = str(uuid4())
    outcome = "unknown" if status == "dispatch_started" else "not_dispatched"
    usage_reason = (
        "incomplete_response" if status == "dispatch_started" else "missing_usage"
    )
    document: dict[str, JsonValue] = {
        "artifact_version": "1.0.0",
        "identity": identity,
        "planned": _json_object(planned_bytes),
        "actual": {"state": "unknown", "reason": "not_observed"},
        "authorization_observation": _json_object(authorization_bytes),
        "execution": {
            "outcome": outcome,
            "seal": True,
            "prepared_at": _utc_text(prepared_at),
            "dispatch_started_at": (
                None if dispatch_started_at is None else _utc_text(dispatch_started_at)
            ),
        },
        "usage": {
            "state": "unresolved",
            "reason": usage_reason,
            "raw_usage_digest": None,
        },
        "revision": "1",
        "predecessor": None,
        "observed_at": _utc_text(observed_at),
    }
    digest = hashlib.sha256(_EVIDENCE_DOMAIN + rfc8785.dumps(document)).hexdigest()
    document["digest"] = digest
    raw = rfc8785.dumps(document)
    artifact = validate_append_evidence(
        raw,
        attempt_binding_bytes=binding_bytes,
        planned_bytes=planned_bytes,
        authorization_bytes=authorization_bytes,
        prepared_at=prepared_at,
        dispatch_started_at=dispatch_started_at,
        database_now=observed_at,
    )
    return raw, artifact


def validate_frozen_attempt(attempt: Mapping[str, object]) -> None:
    _validate_frozen_attempt(
        attempt,
        _bytes(attempt["attempt_binding_bytes"]),
        _bytes(attempt["planned_bytes"]),
        _bytes(attempt["authorization_bytes"]),
    )


def _validate_frozen_attempt(
    attempt: Mapping[str, object],
    binding_bytes: bytes,
    planned_bytes: bytes,
    authorization_bytes: bytes,
) -> None:
    for raw, digest in (
        (binding_bytes, attempt["attempt_binding_digest"]),
        (planned_bytes, attempt["planned_digest"]),
        (authorization_bytes, attempt["authorization_digest"]),
    ):
        if not isinstance(digest, str) or hashlib.sha256(raw).hexdigest() != digest:
            raise ModelCallIdentityConflict("model-call frozen digest conflicts")
        if rfc8785.dumps(_json_object(raw)) != raw:
            raise ModelCallIdentityConflict(
                "model-call frozen artifact is not canonical"
            )
    try:
        binding = AttemptBinding.model_validate_json(binding_bytes, strict=True)
        PlannedBinding.model_validate_json(planned_bytes, strict=True)
    except ValueError as error:
        raise ModelCallIdentityConflict(
            "model-call frozen artifact is invalid"
        ) from error
    authorization_state = _text(attempt["authorization_state"])
    try:
        if authorization_state == "not_requested":
            authorization = AuthorizationNotRequested.model_validate_json(
                authorization_bytes, strict=True
            )
        elif authorization_state in {"requested_unknown", "denied"}:
            authorization = AuthorizationRequested.model_validate_json(
                authorization_bytes, strict=True
            )
        elif authorization_state == "granted":
            authorization = AuthorizationGranted.model_validate_json(
                authorization_bytes, strict=True
            )
        else:
            raise ValueError("unknown authorization state")
    except ValueError as error:
        raise ModelCallIdentityConflict(
            "model-call frozen authorization is invalid"
        ) from error
    if authorization.state != authorization_state:
        raise ModelCallIdentityConflict(
            "model-call frozen authorization state conflicts"
        )
    expected = (
        _text(attempt["tenant_id"]),
        _text(attempt["run_id"]),
        str(attempt["call_id"]),
        str(attempt["attempt_id"]),
        str(_integer(attempt["attempt_ordinal"])),
        _text(attempt["lease_owner"]),
        str(_integer(attempt["lease_generation"])),
    )
    actual = (
        binding.tenant_ref,
        binding.run_id,
        str(binding.call_id),
        str(binding.attempt_id),
        binding.attempt_ordinal,
        binding.lease.owner,
        binding.lease.generation,
    )
    if actual != expected:
        raise ModelCallIdentityConflict("model-call frozen identity conflicts")


def _json_object(raw: bytes) -> dict[str, JsonValue]:
    try:
        value = _JSON_OBJECT.validate_json(raw, strict=True)
    except ValueError as error:
        raise ModelCallIdentityConflict(
            "model-call frozen artifact is invalid"
        ) from error
    return value


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ModelCallIdentityConflict("model-call frozen bytes are invalid")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ModelCallIdentityConflict("model-call frozen text is invalid")
    return value


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ModelCallIdentityConflict("model-call frozen integer is invalid")
    return int(value)


def _instant(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ModelCallIdentityConflict("model-call frozen time is invalid")
    return value.astimezone(UTC)


def _optional_instant(value: object) -> datetime | None:
    return None if value is None else _instant(value)


def _millisecond(value: datetime) -> datetime:
    current = _instant(value)
    return current.replace(microsecond=(current.microsecond // 1000) * 1000)


def _utc_text(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _utc_instant(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ModelCallInputInvalid("model-call evidence time is invalid") from error
    if parsed.tzinfo is None:
        raise ModelCallInputInvalid("model-call evidence time lacks timezone")
    return parsed.astimezone(UTC)


__all__ = [
    "ValidatedModelCallEvidence",
    "build_terminal_closure_evidence",
    "validate_append_evidence",
    "validate_frozen_attempt",
]
