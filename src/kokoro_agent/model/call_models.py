"""Immutable values and errors for one durable model-call preparation."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, Protocol

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.model.summary_result import (
    SummaryCallBinding,
    SummaryReplayReceipt,
    SummarySealInput,
)
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_v2_generated import (
    ActualAttribution,
    Digest,
    IdentityRef,
    Lease,
    Opaque,
    Ordinal,
    PlannedBinding,
    UsageObservation,
    Uuid,
)


def _peer_path(value: tuple[str, ...]) -> tuple[str, ...]:
    if not 0 < len(value) <= 32:
        raise ValueError("peer path length is invalid")
    if any(not item or len(item.encode("utf-8")) > 128 for item in value):
        raise ValueError("peer path member is invalid")
    return value


class AttemptBinding(BaseModel):
    """Evidence identity projection before a real evidence event exists."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

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
    peer_path: Annotated[tuple[str, ...], AfterValidator(_peer_path)]


class PrepareModelCall(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    request: RunRequest
    lease: LeaseFence
    call_id: Uuid
    attempt_id: Uuid
    planned: PlannedBinding
    peer_path: tuple[str, ...]
    recipe: StaticRecipeBinding
    final_request_binding_version: Annotated[int, Field(strict=True, ge=1, le=1)]
    final_request_digest: Digest


class ReadSummaryResult(BaseModel):
    """Current execution authority for a read of an original summary attempt."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    request: RunRequest
    lease: LeaseFence
    planned: PlannedBinding
    recipe: StaticRecipeBinding
    binding: SummaryCallBinding


class PreparedModelCall(BaseModel):
    """Immutable creation receipt; it is not the attempt's current-state view."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str
    run_id: str
    call_id: Uuid
    attempt_id: Uuid
    attempt_ordinal: int = Field(ge=1)
    lease: LeaseFence
    attempt_binding_bytes: bytes
    attempt_binding_digest: Digest
    planned_bytes: bytes
    planned_digest: Digest
    final_request_binding_version: Annotated[int, Field(strict=True, ge=1, le=1)]
    final_request_digest: Digest
    prepared_at_ms: int = Field(ge=0)
    initial_authorization_state: str = "not_requested"
    initial_status: str = "prepared"
    replayed: bool


class PreparedModelCallSnapshot(BaseModel):
    """Current read-only admission facts, never a dispatch permit or history receipt."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    binding_version: Literal["2.0.0"] = "2.0.0"
    attempt_id: Uuid
    status: Literal["prepared", "authorization_pending", "authorized"]
    attempt_binding_bytes: bytes = Field(min_length=1, max_length=65_536)
    attempt_binding_digest: Digest
    planned_bytes: bytes = Field(min_length=1, max_length=65_536)
    planned_digest: Digest
    authorization_bytes: bytes = Field(min_length=1, max_length=65_536)
    authorization_digest: Digest
    final_request_binding_version: Annotated[int, Field(strict=True, ge=1, le=1)]
    final_request_digest: Digest
    prepared_at: datetime
    observed_at: datetime
    lease_expires_at: datetime


class BeginModelCallAuthorization(BaseModel):
    """Freeze the opaque Billing operation reference for one prepared attempt."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    attempt_id: Uuid
    lease: LeaseFence
    attempt_binding_digest: Digest
    planned_digest: Digest
    request_ref: Opaque


class AuthorizationPendingReceipt(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str
    run_id: str
    attempt_id: Uuid
    request_ref: Opaque
    authorization_bytes: bytes
    authorization_digest: Digest
    newly_requested: bool


class GrantModelCallAuthorization(BaseModel):
    """Record a validated owner grant as the Agent usage observation."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    attempt_id: Uuid
    lease: LeaseFence
    attempt_binding_digest: Digest
    planned_digest: Digest
    expected_authorization_digest: Digest
    request_ref: Opaque
    admission_ref: Opaque


class ModelCallAuthorizationReceipt(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str
    run_id: str
    attempt_id: Uuid
    request_ref: Opaque
    admission_ref: Opaque
    authorization_bytes: bytes
    authorization_digest: Digest
    newly_recorded: bool


class StartModelCallDispatch(BaseModel):
    """Request the single durable transition that precedes provider I/O."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    attempt_id: Uuid
    lease: LeaseFence
    attempt_binding_digest: Digest
    planned_digest: Digest
    authorization_digest: Digest
    request_ref: Opaque
    admission_ref: Opaque


class ModelCallDispatchPermit(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str
    run_id: str
    attempt_id: Uuid
    authorization_digest: Digest
    dispatch_started_at_ms: int = Field(ge=0)
    newly_started: bool


class AppendModelCallEvidence(BaseModel):
    """Append one complete strict observation under trusted attempt authority."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    attempt_id: Uuid
    lease: LeaseFence
    attempt_binding_digest: Digest
    planned_digest: Digest
    authorization_digest: Digest
    evidence_bytes: bytes = Field(min_length=1, max_length=65_536)


class SealModelCallObservation(BaseModel):
    """Trusted source projection; the owner supplies immutable bindings and DB time."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    tenant_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    attempt_id: Uuid
    lease: LeaseFence
    attempt_binding_digest: Digest
    planned_digest: Digest
    authorization_digest: Digest
    outcome: Literal["completed", "failed", "cancelled", "unknown"]
    actual: ActualAttribution
    usage: UsageObservation
    summary: SummarySealInput | None = Field(default=None, repr=False)
    provider_response_id: (
        Annotated[str, Field(min_length=1, max_length=256, pattern=r"^[!-~]+$")] | None
    ) = Field(default=None, repr=False)


class ModelCallEvidenceReceipt(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    tenant_id: str
    run_id: str
    attempt_id: Uuid
    event_id: Uuid
    revision: int = Field(ge=1)
    evidence_digest: Digest
    observed_at_ms: int = Field(ge=0)
    replayed: bool


class FinalModelCallEvidence(BaseModel):
    """Exact immutable sealed event, including history, published to the Billing reader."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    artifact_version: Literal["1.0.0", "2.0.0"]
    tenant_id: str
    attempt_id: Uuid
    event_id: Uuid
    revision: int = Field(ge=1)
    evidence_bytes: bytes = Field(min_length=1, max_length=65_536)
    evidence_digest: Digest
    observed_at: datetime


class ModelCallAuthorityLost(RuntimeError):
    pass


class ModelCallIdentityConflict(RuntimeError):
    pass


class ModelCallOrdinalExhausted(RuntimeError):
    pass


class ModelCallInputInvalid(ValueError):
    pass


class ModelCallEvidenceInvariantFailed(RuntimeError):
    pass


class ModelCallAuthorizationUnavailable(RuntimeError):
    """No published Billing admission consumer can authorize this attempt."""


class ModelCallSettlementPending(ModelCallAuthorizationUnavailable):
    """The exact sealed event has an authenticated pending financial ACK. Never retry dispatch."""


class ModelCallRepository(Protocol):
    async def read_summary_result(
        self, command: ReadSummaryResult
    ) -> SummaryReplayReceipt | None: ...

    async def prepare(self, command: PrepareModelCall) -> PreparedModelCall: ...

    async def begin_authorization(
        self, command: BeginModelCallAuthorization
    ) -> AuthorizationPendingReceipt: ...

    async def grant_authorization(
        self, command: GrantModelCallAuthorization
    ) -> ModelCallAuthorizationReceipt: ...

    async def start_dispatch(
        self, command: StartModelCallDispatch
    ) -> ModelCallDispatchPermit: ...

    async def append_evidence(
        self, command: AppendModelCallEvidence
    ) -> ModelCallEvidenceReceipt: ...

    async def seal_observation(
        self, command: SealModelCallObservation
    ) -> ModelCallEvidenceReceipt: ...

    async def read_final_evidence(
        self, tenant_id: str, attempt_id: str, event_id: str
    ) -> FinalModelCallEvidence | None: ...


__all__ = [
    "AttemptBinding",
    "AppendModelCallEvidence",
    "AuthorizationPendingReceipt",
    "BeginModelCallAuthorization",
    "GrantModelCallAuthorization",
    "FinalModelCallEvidence",
    "ModelCallAuthorityLost",
    "ModelCallIdentityConflict",
    "ModelCallInputInvalid",
    "ModelCallOrdinalExhausted",
    "ModelCallAuthorizationReceipt",
    "ModelCallAuthorizationUnavailable",
    "ModelCallSettlementPending",
    "ModelCallDispatchPermit",
    "ModelCallEvidenceReceipt",
    "ModelCallEvidenceInvariantFailed",
    "ModelCallRepository",
    "PrepareModelCall",
    "PreparedModelCall",
    "StartModelCallDispatch",
    "SealModelCallObservation",
]
