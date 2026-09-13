"""Run-scoped, fail-closed execution-proof issuance policy."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import re
import secrets
from typing import Protocol

from kokoro_agent.domain.run.models import LeaseFence, LeasedRun
from kokoro_agent.execution.execution_proof_profile import (
    MAX_SAFE_INTEGER,
    ExecutionProofInput,
    encode_base64url,
)
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunRequest


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MAX_CLOCK_SKEW = timedelta(seconds=5)
_MIN_ACTUAL_LIFETIME = timedelta(seconds=1)
_MAX_TTL_SECONDS = 60
_OPERATION = re.compile(r"^[a-z][a-z0-9_.]{0,127}$")
_BINDING = re.compile(r"^[0-9a-f]{64}$")
_ERROR_MESSAGE = "execution proof is unavailable"


class ExecutionProofUnavailableError(RuntimeError):
    """Current lease or safe signing inputs could not be established."""


@dataclass(frozen=True, slots=True, kw_only=True)
class CurrentLeaseObservation:
    """The current lease facts returned by one database statement."""

    database_now: datetime
    lease_expires_at: datetime

    def __post_init__(self) -> None:
        try:
            database_now = _utc_instant(self.database_now)
            lease_expires_at = _utc_instant(self.lease_expires_at)
            valid = lease_expires_at > database_now
        except Exception:
            valid = False
            database_now = _EPOCH
            lease_expires_at = _EPOCH
        if not valid:
            raise ExecutionProofUnavailableError(_ERROR_MESSAGE) from None
        object.__setattr__(self, "database_now", database_now)
        object.__setattr__(self, "lease_expires_at", lease_expires_at)


class ExecutionProofLeaseReadPort(Protocol):
    """Proof-only port for a statement-time current lease observation."""

    async def observe_current_lease(
        self, *, run_id: str, fence: LeaseFence
    ) -> CurrentLeaseObservation | None: ...


class _ExecutionProofSignerPort(Protocol):
    def issue_execution_proof(self, issue: ExecutionProofInput) -> str: ...


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class _RunSnapshot:
    tenant_ref: str
    actor: IdentityRef
    subject: IdentityRef
    run_id: str
    execution_session_id: str
    fence: LeaseFence


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class ExecutionProofSupplier:
    """Issue fresh proofs for one immutable RunRequest/LeaseFence pair."""

    _snapshot: _RunSnapshot
    _lease_reader: ExecutionProofLeaseReadPort
    _signer: _ExecutionProofSignerPort
    _clock: Callable[[], datetime]
    _nonce_provider: Callable[[int], bytes]

    def __repr__(self) -> str:
        return "ExecutionProofSupplier(<redacted>)"

    async def issue(self, operation: str, request_binding_sha256: str) -> str:
        """Read current lease and sign exactly once, without caching or retry."""

        failed = False
        proof: str | None = None
        try:
            if not _issue_values_are_valid(operation, request_binding_sha256):
                raise ValueError
            snapshot = self._snapshot
            observation = await self._lease_reader.observe_current_lease(
                run_id=snapshot.run_id,
                fence=snapshot.fence,
            )
            if type(observation) is not CurrentLeaseObservation:
                raise ValueError
            observation = CurrentLeaseObservation(
                database_now=observation.database_now,
                lease_expires_at=observation.lease_expires_at,
            )
            app_now = _utc_instant(self._clock())
            if abs(app_now - observation.database_now) > _MAX_CLOCK_SKEW:
                raise ValueError
            iat = _epoch_floor(app_now)
            lease_expiry = _epoch_floor(observation.lease_expires_at)
            exp = min(iat + _MAX_TTL_SECONDS, lease_expiry)
            if not 0 <= iat <= MAX_SAFE_INTEGER or not iat < exp <= MAX_SAFE_INTEGER:
                raise ValueError
            exp_instant = _EPOCH + timedelta(seconds=exp)
            if (
                exp_instant - max(app_now, observation.database_now)
                < _MIN_ACTUAL_LIFETIME
            ):
                raise ValueError
            nonce = self._nonce_provider(16)
            if type(nonce) is not bytes or len(nonce) != 16:
                raise ValueError
            proof_input = ExecutionProofInput(
                tenant_ref=snapshot.tenant_ref,
                actor=snapshot.actor,
                subject=snapshot.subject,
                run_id=snapshot.run_id,
                execution_session_id=snapshot.execution_session_id,
                lease_generation=snapshot.fence.generation,
                operation=operation,
                request_binding_sha256=request_binding_sha256,
                iat=iat,
                exp=exp,
                jti=encode_base64url(nonce),
            )
            candidate = self._signer.issue_execution_proof(proof_input)
            if type(candidate) is not str or not candidate:
                raise ValueError
            proof = candidate
        except Exception:
            failed = True
        if failed or proof is None:
            raise ExecutionProofUnavailableError(_ERROR_MESSAGE) from None
        return proof


def create_execution_proof_supplier(
    *,
    leased_run: LeasedRun,
    lease_reader: ExecutionProofLeaseReadPort,
    signer: _ExecutionProofSignerPort,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    nonce_provider: Callable[[int], bytes] = secrets.token_bytes,
) -> ExecutionProofSupplier:
    """Atomically snapshot one exact leased run into a run-scoped supplier."""

    failed = False
    snapshot: _RunSnapshot | None = None
    try:
        snapshot = _snapshot_leased_run(leased_run)
        if not callable(getattr(lease_reader, "observe_current_lease", None)):
            raise ValueError
        if not callable(getattr(signer, "issue_execution_proof", None)):
            raise ValueError
        if not callable(clock) or not callable(nonce_provider):
            raise ValueError
    except Exception:
        failed = True
    if failed or snapshot is None:
        raise ExecutionProofUnavailableError(_ERROR_MESSAGE) from None
    return ExecutionProofSupplier(
        _snapshot=snapshot,
        _lease_reader=lease_reader,
        _signer=signer,
        _clock=clock,
        _nonce_provider=nonce_provider,
    )


def _snapshot_leased_run(value: object) -> _RunSnapshot:
    if type(value) is not LeasedRun:
        raise ValueError
    request = value.request
    fence = value.lease
    if type(request) is not RunRequest or type(fence) is not LeaseFence:
        raise ValueError
    identity = request.execution_identity
    if type(identity) is not ExecutionIdentity:
        raise ValueError
    actor = _identity_snapshot(identity.actor)
    subject = _identity_snapshot(identity.subject)
    if not all(
        _non_empty_exact_string(item)
        for item in (
            request.run_id,
            request.session_id,
            identity.tenant_ref,
            identity.identity_assertion_ref,
            fence.owner,
        )
    ):
        raise ValueError
    generation = fence.generation
    if type(generation) is not int or not 1 <= generation <= MAX_SAFE_INTEGER:
        raise ValueError
    return _RunSnapshot(
        tenant_ref=identity.tenant_ref,
        actor=actor,
        subject=subject,
        run_id=request.run_id,
        execution_session_id=request.session_id,
        fence=LeaseFence(owner=fence.owner, generation=generation),
    )


def _identity_snapshot(value: object) -> IdentityRef:
    if type(value) is not IdentityRef:
        raise ValueError
    if type(value.kind) is not str or value.kind not in {"user", "project", "service"}:
        raise ValueError
    if not _non_empty_exact_string(value.opaque_ref):
        raise ValueError
    return IdentityRef(kind=value.kind, opaque_ref=value.opaque_ref)


def _issue_values_are_valid(operation: object, binding: object) -> bool:
    return (
        type(operation) is str
        and _OPERATION.fullmatch(operation) is not None
        and type(binding) is str
        and _BINDING.fullmatch(binding) is not None
    )


def _non_empty_exact_string(value: object) -> bool:
    return type(value) is str and bool(value)


def _utc_instant(value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise ValueError
    offset = value.utcoffset()
    if offset is None:
        raise ValueError
    normalized = value.astimezone(UTC)
    if type(normalized) is not datetime or normalized.tzinfo is None:
        raise ValueError
    return normalized


def _epoch_floor(value: datetime) -> int:
    delta = value - _EPOCH
    return delta.days * 86_400 + delta.seconds


__all__ = [
    "CurrentLeaseObservation",
    "ExecutionProofLeaseReadPort",
    "ExecutionProofSupplier",
    "ExecutionProofUnavailableError",
    "create_execution_proof_supplier",
]
