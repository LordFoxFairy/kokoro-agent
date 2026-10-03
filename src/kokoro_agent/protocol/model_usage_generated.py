"""Generated from contract/usage/v1/schema.json. Do not edit."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
import re
from typing import Annotated, Literal, TypeAlias

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

SOURCE_SHA256 = "eb3e226f2c5ede407b61558c0375a373a8a7985f333b3d62a6c2b148469043d0"

MAX_JSON_BYTES = 65536
MAX_JSON_DEPTH = 16


def _utf8_limit(limit: int) -> Callable[[str], str]:
    def validate(value: str) -> str:
        if len(value.encode("utf-8", errors="strict")) > limit:
            raise ValueError("usage string exceeds its UTF-8 bound")
        return value

    return validate


def _decimal_limit(value: str) -> str:
    if int(value) > 9223372036854775807:
        raise ValueError("usage decimal exceeds int63")
    return value


def _calendar(value: str) -> str:
    if not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z", value
    ):
        raise ValueError("usage instant must be UTC milliseconds")
    datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    return value


class UsageWireModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


Opaque: TypeAlias = Annotated[
    str,
    Field(pattern="^[^\\x00-\\x1f\\x7f]+$", min_length=1),
    AfterValidator(_utf8_limit(512)),
]


Uuid: TypeAlias = Annotated[
    str, Field(pattern="^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
]


Digest: TypeAlias = Annotated[str, Field(pattern="^[0-9a-f]{64}$")]


Count: TypeAlias = Annotated[
    str,
    Field(pattern="^(0|[1-9][0-9]*)$", max_length=19),
    AfterValidator(_decimal_limit),
]


Ordinal: TypeAlias = Annotated[
    str, Field(pattern="^[1-9][0-9]*$", max_length=19), AfterValidator(_decimal_limit)
]


UtcInstant: TypeAlias = Annotated[
    str,
    Field(
        pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$"
    ),
    AfterValidator(_calendar),
]


class IdentityRef(UsageWireModel):
    kind: Literal["user", "project", "service"]
    opaque_ref: Opaque


class Lease(UsageWireModel):
    owner: Opaque
    generation: Ordinal


class EvidenceIdentity(UsageWireModel):
    event_id: Uuid
    tenant_ref: Opaque
    actor: IdentityRef
    subject: IdentityRef
    identity_assertion_ref: Opaque
    run_id: Opaque
    session_id: Opaque
    call_id: Uuid
    attempt_id: Uuid
    attempt_ordinal: Ordinal
    lease: Lease
    input_digest: Digest
    peer_path: Annotated[
        list[Annotated[str, Field(min_length=1), AfterValidator(_utf8_limit(128))]],
        Field(max_length=32),
    ]


class PlannedBinding(UsageWireModel):
    model: Opaque
    provider: Opaque
    revision_id: Opaque
    provider_model_name: Opaque
    gateway_model_name: Opaque
    feature_key: Opaque
    revision: Ordinal
    generation: Ordinal
    tenant_generation: Ordinal
    digest: Digest
    label: Opaque | None


class UnknownAttribution(UsageWireModel):
    state: Literal["unknown"]
    reason: Literal["not_observed", "unverified_binding", "incomplete_attempt_coverage"]


class VerifiedAttribution(UsageWireModel):
    state: Literal["verified"]
    provider_ref: Opaque
    model_ref: Opaque
    evidence_ref: Opaque
    evidence_digest: Digest


ActualAttribution: TypeAlias = UnknownAttribution | VerifiedAttribution


class AuthorizationNotRequested(UsageWireModel):
    state: Literal["not_requested"]


class AuthorizationRequested(UsageWireModel):
    state: Literal["requested_unknown", "denied"]
    request_ref: Opaque


class AuthorizationGranted(UsageWireModel):
    state: Literal["granted"]
    request_ref: Opaque
    admission_ref: Opaque


AuthorizationObservation: TypeAlias = (
    AuthorizationNotRequested | AuthorizationRequested | AuthorizationGranted
)


class ExecutionObservation(UsageWireModel):
    outcome: Literal["not_dispatched", "completed", "failed", "cancelled", "unknown"]
    seal: bool
    prepared_at: UtcInstant
    dispatch_started_at: UtcInstant | None


class ReportedCount(UsageWireModel):
    state: Literal["reported"]
    count: Count


class UnreportedCount(UsageWireModel):
    state: Literal["unreported"]


class InapplicableCount(UsageWireModel):
    state: Literal["not_applicable"]


CountObservation: TypeAlias = ReportedCount | UnreportedCount | InapplicableCount


class SupportedProfile(UsageWireModel):
    state: Literal["supported"]


class UnsupportedProfile(UsageWireModel):
    state: Literal["unsupported"]
    reason: Literal[
        "unsupported_usage_category",
        "unsupported_service_tier",
        "unverified_source_semantics",
    ]


ProfileAssessment: TypeAlias = SupportedProfile | UnsupportedProfile


class TokenUsageV1(UsageWireModel):
    profile_id: Literal["token_usage_v1"]
    unit: Literal["token"]
    source_protocol: Literal[
        "openai_chat_completions", "ollama_openai_chat_completions"
    ]
    aggregation_mode: Literal["final_cumulative"]
    input_total: CountObservation
    output_total: CountObservation
    total: CountObservation
    input_cache_read: CountObservation
    input_cache_write: CountObservation
    output_reasoning: CountObservation
    totals_state: Literal["known_zero", "known_nonzero", "unknown"]
    classification_state: Literal["complete", "partial", "unknown"]
    profile_assessment: ProfileAssessment


class NormalizedUsage(UsageWireModel):
    state: Literal["normalized"]
    value: TokenUsageV1
    raw_usage_digest: Digest | None


class UnresolvedUsage(UsageWireModel):
    state: Literal["unresolved"]
    reason: Literal[
        "missing_usage", "invalid_usage", "incomplete_response", "unsupported_source"
    ]
    raw_usage_digest: Digest | None


UsageObservation: TypeAlias = NormalizedUsage | UnresolvedUsage


class Predecessor(UsageWireModel):
    event_id: Uuid
    digest: Digest


class ModelUsageEvidence(UsageWireModel):
    artifact_version: Literal["1.0.0"]
    identity: EvidenceIdentity
    planned: PlannedBinding
    actual: ActualAttribution
    authorization_observation: AuthorizationObservation
    execution: ExecutionObservation
    usage: UsageObservation
    revision: Ordinal
    predecessor: Predecessor | None
    observed_at: UtcInstant
    digest: Digest
