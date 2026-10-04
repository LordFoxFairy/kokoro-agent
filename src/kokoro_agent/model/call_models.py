"""Immutable values and errors for one durable model-call preparation."""

from __future__ import annotations

from typing import Annotated, Protocol

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_generated import (
    Digest,
    IdentityRef,
    Lease,
    Opaque,
    Ordinal,
    PlannedBinding,
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
    prepared_at_ms: int = Field(ge=0)
    initial_authorization_state: str = "not_requested"
    initial_status: str = "prepared"
    replayed: bool


class ModelCallAuthorityLost(RuntimeError):
    pass


class ModelCallIdentityConflict(RuntimeError):
    pass


class ModelCallOrdinalExhausted(RuntimeError):
    pass


class ModelCallInputInvalid(ValueError):
    pass


class ModelCallRepository(Protocol):
    async def prepare(self, command: PrepareModelCall) -> PreparedModelCall: ...


__all__ = [
    "AttemptBinding",
    "ModelCallAuthorityLost",
    "ModelCallIdentityConflict",
    "ModelCallInputInvalid",
    "ModelCallOrdinalExhausted",
    "ModelCallRepository",
    "PrepareModelCall",
    "PreparedModelCall",
]
