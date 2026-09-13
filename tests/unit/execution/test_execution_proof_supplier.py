"""Unit contract for the run-scoped execution-proof supplier."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta, timezone
import traceback
from typing import Any

import pytest

from kokoro_agent.domain.run.models import LeaseFence, LeasedRun
from kokoro_agent.execution.execution_proof_profile import ExecutionProofInput
from kokoro_agent.execution.execution_proof_supplier import (
    CurrentLeaseObservation,
    ExecutionProofLeaseReadPort,
    ExecutionProofSupplier,
    ExecutionProofUnavailableError,
    create_execution_proof_supplier,
)
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
BINDING = "ab" * 32


def _leased_run(
    *,
    run_id: str = "run-1",
    session_id: str = "session-1",
    tenant_ref: str = "tenant-1",
    owner: str = "owner-1",
    generation: int = 7,
    assertion: str = "ASSERTION_SENTINEL",
) -> LeasedRun:
    return LeasedRun(
        request=RunRequest(
            kind="run.request",
            run_id=run_id,
            session_id=session_id,
            feature_key="music",
            execution_identity=ExecutionIdentity(
                tenant_ref=tenant_ref,
                actor=IdentityRef(kind="user", opaque_ref=f"actor-{run_id}"),
                subject=IdentityRef(kind="project", opaque_ref=f"subject-{run_id}"),
                identity_assertion_ref=assertion,
            ),
            input=RunInput(message_id="message-1", content="hello"),
        ),
        lease=LeaseFence(owner=owner, generation=generation),
    )


class _LeaseReader(ExecutionProofLeaseReadPort):
    def __init__(
        self,
        observations: Sequence[CurrentLeaseObservation | None | BaseException],
    ) -> None:
        self._observations = iter(observations)
        self.calls: list[tuple[str, LeaseFence]] = []

    async def observe_current_lease(
        self, *, run_id: str, fence: LeaseFence
    ) -> CurrentLeaseObservation | None:
        self.calls.append((run_id, fence))
        value = next(self._observations)
        if isinstance(value, BaseException):
            raise value
        return value


class _Signer:
    def __init__(self, result: str | BaseException = "PROOF") -> None:
        self.result = result
        self.calls: list[ExecutionProofInput] = []

    def issue_execution_proof(self, issue: ExecutionProofInput) -> str:
        self.calls.append(issue)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class _NonceSequence:
    def __init__(self, values: list[object]) -> None:
        self._values: Any = iter(values)
        self.calls = 0

    def __call__(self, size: int) -> bytes:
        assert size == 16
        self.calls += 1
        value = next(self._values)
        if isinstance(value, BaseException):
            raise value
        return value


def _observation(
    now: datetime = EPOCH + timedelta(seconds=100),
    expiry: datetime = EPOCH + timedelta(seconds=161),
) -> CurrentLeaseObservation:
    return CurrentLeaseObservation(database_now=now, lease_expires_at=expiry)


def _supplier(
    *,
    leased_run: Any | None = None,
    observations: Sequence[CurrentLeaseObservation | None | BaseException]
    | None = None,
    signer: _Signer | None = None,
    clock: Callable[[], datetime] | None = None,
    nonce: _NonceSequence | None = None,
) -> tuple[ExecutionProofSupplier, _LeaseReader, _Signer, _NonceSequence]:
    reader = _LeaseReader(observations or [_observation()])
    actual_signer = signer or _Signer()
    actual_nonce = nonce or _NonceSequence([b"\x01" * 16])
    supplier = create_execution_proof_supplier(
        leased_run=_leased_run() if leased_run is None else leased_run,
        lease_reader=reader,
        signer=actual_signer,
        clock=clock or (lambda: EPOCH + timedelta(seconds=100)),
        nonce_provider=actual_nonce,
    )
    return supplier, reader, actual_signer, actual_nonce


def test_current_lease_observation_is_frozen_and_normalizes_offsets() -> None:
    eastern = timezone(timedelta(hours=-5))
    observation = CurrentLeaseObservation(
        database_now=datetime(1970, 1, 1, 0, 1, 40, tzinfo=UTC),
        lease_expires_at=datetime(1969, 12, 31, 19, 2, 41, tzinfo=eastern),
    )

    assert observation.database_now == EPOCH + timedelta(seconds=100)
    assert observation.lease_expires_at == EPOCH + timedelta(seconds=161)
    with pytest.raises((AttributeError, TypeError)):
        setattr(observation, "database_now", EPOCH)


@pytest.mark.parametrize(
    ("database_now", "lease_expires_at"),
    [
        ("bad", EPOCH + timedelta(seconds=2)),
        (EPOCH, "bad"),
        (datetime(2026, 1, 1), EPOCH + timedelta(seconds=2)),
        (EPOCH, datetime(2026, 1, 1)),
        (EPOCH + timedelta(seconds=2), EPOCH + timedelta(seconds=2)),
    ],
)
def test_current_lease_observation_rejects_invalid_time_shape(
    database_now: Any, lease_expires_at: Any
) -> None:
    with pytest.raises(ExecutionProofUnavailableError):
        CurrentLeaseObservation(
            database_now=database_now,
            lease_expires_at=lease_expires_at,
        )


@pytest.mark.asyncio
async def test_issue_reads_then_clocks_then_nonces_then_signs_exact_snapshot() -> None:
    events: list[str] = []
    leased = _leased_run()

    class Reader(_LeaseReader):
        async def observe_current_lease(
            self, *, run_id: str, fence: LeaseFence
        ) -> CurrentLeaseObservation | None:
            events.append("read")
            return await super().observe_current_lease(run_id=run_id, fence=fence)

    class Signer(_Signer):
        def issue_execution_proof(self, issue: ExecutionProofInput) -> str:
            events.append("sign")
            return super().issue_execution_proof(issue)

    reader = Reader([_observation()])
    signer = Signer()

    def clock() -> datetime:
        events.append("clock")
        return EPOCH + timedelta(seconds=100, microseconds=999_999)

    def nonce(size: int) -> bytes:
        events.append("nonce")
        assert size == 16
        return bytes(range(16))

    supplier = create_execution_proof_supplier(
        leased_run=leased,
        lease_reader=reader,
        signer=signer,
        clock=clock,
        nonce_provider=nonce,
    )
    object.__setattr__(leased.request.execution_identity, "tenant_ref", "MUTATED")
    object.__setattr__(leased.request.execution_identity.actor, "opaque_ref", "MUTATED")
    object.__setattr__(leased.request, "session_id", "MUTATED")
    object.__setattr__(leased.lease, "owner", "MUTATED")

    assert await supplier.issue("skills.resolve", BINDING) == "PROOF"
    assert events == ["read", "clock", "nonce", "sign"]
    assert reader.calls == [("run-1", LeaseFence(owner="owner-1", generation=7))]
    issued = signer.calls[0]
    assert issued.tenant_ref == "tenant-1"
    assert issued.run_id == "run-1"
    assert issued.execution_session_id == "session-1"
    assert issued.actor.opaque_ref == "actor-run-1"
    assert issued.lease_generation == 7
    assert issued.iat == 100
    assert issued.exp == 160
    assert (
        issued.jti == base64.urlsafe_b64encode(bytes(range(16))).rstrip(b"=").decode()
    )


@pytest.mark.parametrize(
    "value",
    [object(), {"request": _leased_run().request, "lease": _leased_run().lease}],
)
def test_factory_rejects_duck_typed_pair_before_dependencies(value: Any) -> None:
    reader = _LeaseReader([_observation()])
    signer = _Signer()
    nonce = _NonceSequence([b"\x01" * 16])

    with pytest.raises(ExecutionProofUnavailableError) as caught:
        create_execution_proof_supplier(
            leased_run=value,
            lease_reader=reader,
            signer=signer,
            clock=lambda: EPOCH,
            nonce_provider=nonce,
        )

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert reader.calls == []
    assert signer.calls == []
    assert nonce.calls == 0


def test_factory_rejects_subclassed_pair_request_fence_and_identity() -> None:
    class PairSubclass(LeasedRun):
        pass

    class RequestSubclass(RunRequest):
        pass

    class FenceSubclass(LeaseFence):
        pass

    class IdentitySubclass(ExecutionIdentity):
        pass

    base = _leased_run()
    values = [
        PairSubclass(request=base.request, lease=base.lease),
        LeasedRun(
            request=RequestSubclass.model_validate(base.request.model_dump()),
            lease=base.lease,
        ),
        LeasedRun(
            request=base.request,
            lease=FenceSubclass(owner="owner", generation=1),
        ),
        LeasedRun(
            request=base.request.model_copy(
                update={
                    "execution_identity": IdentitySubclass.model_validate(
                        base.request.execution_identity.model_dump()
                    )
                }
            ),
            lease=base.lease,
        ),
    ]

    for value in values:
        with pytest.raises(ExecutionProofUnavailableError):
            _supplier(leased_run=value)


def test_run_scoped_supplier_dependencies_are_frozen_after_factory() -> None:
    supplier, _, _, _ = _supplier()

    with pytest.raises((AttributeError, TypeError)):
        setattr(supplier, "_clock", lambda: EPOCH + timedelta(seconds=999))


@pytest.mark.parametrize("generation", [True, 1.0, 0, 9_007_199_254_740_992])
def test_factory_rejects_non_safe_generation(generation: object) -> None:
    pair = _leased_run()
    object.__setattr__(pair.lease, "generation", generation)

    with pytest.raises(ExecutionProofUnavailableError):
        _supplier(leased_run=pair)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "binding"),
    [
        ("", BINDING),
        ("Skills.Resolve", BINDING),
        ("skills.resolve", "A" * 64),
    ],
)
async def test_invalid_issue_input_never_reads_or_signs(
    operation: Any, binding: Any
) -> None:
    supplier, reader, signer, nonce = _supplier()

    with pytest.raises(ExecutionProofUnavailableError):
        await supplier.issue(operation, binding)

    assert reader.calls == []
    assert signer.calls == []
    assert nonce.calls == 0


@pytest.mark.asyncio
async def test_string_subclass_issue_input_never_reads_or_signs() -> None:
    class StringSubclass(str):
        pass

    supplier, reader, signer, nonce = _supplier()
    with pytest.raises(ExecutionProofUnavailableError):
        await supplier.issue(StringSubclass("skills.resolve"), BINDING)
    assert reader.calls == []
    assert signer.calls == []
    assert nonce.calls == 0


@pytest.mark.asyncio
async def test_none_or_reader_failure_never_clocks_nonces_or_signs() -> None:
    for result in (None, RuntimeError("READER_SENTINEL")):
        clock_calls = 0

        def clock() -> datetime:
            nonlocal clock_calls
            clock_calls += 1
            return EPOCH

        supplier, reader, signer, nonce = _supplier(observations=[result], clock=clock)
        with pytest.raises(ExecutionProofUnavailableError) as caught:
            await supplier.issue("skills.resolve", BINDING)
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None
        assert reader.calls
        assert clock_calls == 0
        assert nonce.calls == 0
        assert signer.calls == []


@pytest.mark.asyncio
async def test_mutated_observation_is_rebuilt_and_rejected_before_clock() -> None:
    observation = _observation()
    object.__setattr__(observation, "lease_expires_at", datetime(2026, 1, 1))
    clock_calls = 0

    def clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        return EPOCH

    supplier, _, signer, nonce = _supplier(observations=[observation], clock=clock)

    with pytest.raises(ExecutionProofUnavailableError):
        await supplier.issue("skills.resolve", BINDING)

    assert clock_calls == 0
    assert signer.calls == []
    assert nonce.calls == 0


@pytest.mark.asyncio
async def test_external_cancel_is_propagated_by_identity() -> None:
    cancelled = asyncio.CancelledError("CANCEL_SENTINEL")
    supplier, _, signer, nonce = _supplier(observations=[cancelled])

    with pytest.raises(asyncio.CancelledError) as caught:
        await supplier.issue("skills.resolve", BINDING)

    assert caught.value is cancelled
    assert signer.calls == []
    assert nonce.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("app_offset", "accepted"),
    [
        (timedelta(seconds=-5), True),
        (timedelta(seconds=5), True),
        (timedelta(seconds=-5, microseconds=-1), False),
        (timedelta(seconds=5, microseconds=1), False),
    ],
)
async def test_exact_clock_skew_boundary(app_offset: timedelta, accepted: bool) -> None:
    database_now = EPOCH + timedelta(seconds=100)
    supplier, _, signer, nonce = _supplier(
        observations=[_observation(database_now, EPOCH + timedelta(seconds=200))],
        clock=lambda: database_now + app_offset,
    )

    if accepted:
        assert await supplier.issue("skills.resolve", BINDING) == "PROOF"
        assert len(signer.calls) == nonce.calls == 1
    else:
        with pytest.raises(ExecutionProofUnavailableError):
            await supplier.issue("skills.resolve", BINDING)
        assert signer.calls == []
        assert nonce.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("remaining", "accepted"),
    [
        (timedelta(microseconds=1_000), False),
        (timedelta(microseconds=999_000), False),
        (timedelta(seconds=1), True),
    ],
)
async def test_exact_actual_remaining_lifetime_boundary(
    remaining: timedelta, accepted: bool
) -> None:
    app = EPOCH + timedelta(seconds=100)
    expiry = app + remaining
    supplier, _, signer, nonce = _supplier(
        observations=[_observation(app, expiry)], clock=lambda: app
    )

    if accepted:
        assert await supplier.issue("skills.resolve", BINDING) == "PROOF"
        assert signer.calls[0].exp == 101
    else:
        with pytest.raises(ExecutionProofUnavailableError):
            await supplier.issue("skills.resolve", BINDING)
        assert signer.calls == []
        assert nonce.calls == 0


@pytest.mark.asyncio
async def test_year_3000_exact_epoch_floor_avoids_float_false_accept() -> None:
    app = datetime(3000, 1, 1, tzinfo=UTC) + timedelta(microseconds=1)
    expiry = app + timedelta(seconds=1)
    supplier, _, signer, nonce = _supplier(
        observations=[_observation(app, expiry)], clock=lambda: app
    )

    with pytest.raises(ExecutionProofUnavailableError):
        await supplier.issue("skills.resolve", BINDING)

    assert signer.calls == []
    assert nonce.calls == 0


@pytest.mark.asyncio
async def test_year_3000_last_microsecond_uses_exact_integer_epoch_floor() -> None:
    app = datetime(3000, 1, 1, 0, 0, 0, 999_999, tzinfo=UTC)
    expiry = app + timedelta(seconds=61)
    supplier, _, signer, _ = _supplier(
        observations=[_observation(app, expiry)], clock=lambda: app
    )

    await supplier.issue("skills.resolve", BINDING)

    delta = app - EPOCH
    exact_floor = delta.days * 86_400 + delta.seconds
    assert int(app.timestamp()) == exact_floor + 1  # explicit float mutant
    assert signer.calls[0].iat == exact_floor
    assert signer.calls[0].exp == exact_floor + 60


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("remaining", "boundary"),
    [
        (timedelta(seconds=60), "exact-lease-and-ttl"),
        (timedelta(seconds=60, microseconds=1), "lease-floor-before-ttl"),
        (timedelta(seconds=61), "ttl-clips-longer-lease"),
    ],
)
async def test_exact_sixty_second_ttl_clipping_matrix(
    remaining: timedelta, boundary: str
) -> None:
    del boundary
    app = EPOCH + timedelta(seconds=100)
    supplier, _, signer, _ = _supplier(
        observations=[_observation(app, app + remaining)], clock=lambda: app
    )

    await supplier.issue("skills.resolve", BINDING)

    assert signer.calls[0].iat == 100
    assert signer.calls[0].exp == 160


@pytest.mark.asyncio
async def test_epoch_floor_ttl_and_offset_equivalence() -> None:
    offset = timezone(timedelta(hours=5, minutes=30))
    app = (EPOCH + timedelta(seconds=100, microseconds=999_999)).astimezone(offset)
    supplier, _, signer, _ = _supplier(
        observations=[
            _observation(
                EPOCH + timedelta(seconds=100),
                EPOCH + timedelta(seconds=200, microseconds=999_999),
            )
        ],
        clock=lambda: app,
    )

    await supplier.issue("skills.resolve", BINDING)

    assert signer.calls[0].iat == 100
    assert signer.calls[0].exp == 160


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "clock_value",
    [
        "bad",
        datetime(2026, 1, 1),
        EPOCH - timedelta(microseconds=1),
        datetime.max.replace(tzinfo=UTC),
    ],
)
async def test_bad_application_clock_fails_closed(clock_value: Any) -> None:
    supplier, _, signer, nonce = _supplier(
        observations=[_observation(EPOCH, datetime.max.replace(tzinfo=UTC))],
        clock=lambda: clock_value,
    )

    with pytest.raises(ExecutionProofUnavailableError):
        await supplier.issue("skills.resolve", BINDING)

    assert signer.calls == []
    assert nonce.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "nonce_value",
    [
        b"short",
        bytearray(b"\x00" * 16),
        type("BytesSubclass", (bytes,), {})(b"\x00" * 16),
    ],
)
async def test_nonce_must_be_exact_16_byte_bytes(nonce_value: object) -> None:
    nonce = _NonceSequence([nonce_value])
    supplier, _, signer, _ = _supplier(nonce=nonce)

    with pytest.raises(ExecutionProofUnavailableError):
        await supplier.issue("skills.resolve", BINDING)

    assert nonce.calls == 1
    assert signer.calls == []


@pytest.mark.asyncio
async def test_every_issue_freshly_reads_nonces_and_signs_without_cache() -> None:
    observations = [_observation(), _observation()]
    nonce = _NonceSequence([b"\x01" * 16, b"\x02" * 16])
    supplier, reader, signer, _ = _supplier(observations=observations, nonce=nonce)

    first, second = await asyncio.gather(
        supplier.issue("skills.resolve", BINDING),
        supplier.issue("skills.resolve", BINDING),
    )

    assert (first, second) == ("PROOF", "PROOF")
    assert len(reader.calls) == len(signer.calls) == nonce.calls == 2
    assert signer.calls[0].jti != signer.calls[1].jti


@pytest.mark.asyncio
async def test_default_nonce_provider_mints_fresh_canonical_128_bit_jti() -> None:
    reader = _LeaseReader([_observation(), _observation()])
    signer = _Signer()
    supplier = create_execution_proof_supplier(
        leased_run=_leased_run(),
        lease_reader=reader,
        signer=signer,
        clock=lambda: EPOCH + timedelta(seconds=100),
    )

    await supplier.issue("skills.resolve", BINDING)
    await supplier.issue("skills.resolve", BINDING)

    assert len(signer.calls) == 2
    assert signer.calls[0].jti != signer.calls[1].jti
    for issue in signer.calls:
        encoded = issue.jti.encode("ascii")
        assert len(encoded) == 22
        assert len(base64.urlsafe_b64decode(encoded + b"==")) == 16


@pytest.mark.asyncio
async def test_multiple_suppliers_do_not_cross_run_identity_or_fence() -> None:
    suppliers: list[ExecutionProofSupplier] = []
    signers: list[_Signer] = []
    readers: list[_LeaseReader] = []
    for index in range(4):
        signer = _Signer()
        supplier, reader, _, _ = _supplier(
            leased_run=_leased_run(
                run_id=f"run-{index}",
                session_id=f"session-{index}",
                tenant_ref=f"tenant-{index}",
                owner=f"owner-{index}",
                generation=index + 1,
            ),
            signer=signer,
            nonce=_NonceSequence([bytes([index]) * 16]),
        )
        suppliers.append(supplier)
        signers.append(signer)
        readers.append(reader)

    await asyncio.gather(
        *(supplier.issue("skills.resolve", BINDING) for supplier in suppliers)
    )

    for index, (signer, reader) in enumerate(zip(signers, readers, strict=True)):
        issue = signer.calls[0]
        assert (issue.tenant_ref, issue.run_id, issue.execution_session_id) == (
            f"tenant-{index}",
            f"run-{index}",
            f"session-{index}",
        )
        assert issue.lease_generation == index + 1
        assert reader.calls[0] == (
            f"run-{index}",
            LeaseFence(owner=f"owner-{index}", generation=index + 1),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [RuntimeError("SIGNER_SENTINEL"), RuntimeError("NONCE_SENTINEL")],
)
async def test_failures_have_no_sensitive_chain_traceback_repr_or_log(
    failure: RuntimeError, caplog: pytest.LogCaptureFixture
) -> None:
    signer = _Signer(failure) if "SIGNER" in str(failure) else _Signer()
    nonce = (
        _NonceSequence([failure])
        if "NONCE" in str(failure)
        else _NonceSequence([b"\x01" * 16])
    )
    pair = _leased_run(
        run_id="RUN_SENTINEL",
        tenant_ref="TENANT_SENTINEL",
        owner="OWNER_SENTINEL",
        assertion="ASSERTION_SENTINEL",
    )
    supplier, _, _, _ = _supplier(leased_run=pair, signer=signer, nonce=nonce)
    operation = "operation.sentinel"
    binding = "de" * 32

    with pytest.raises(ExecutionProofUnavailableError) as caught:
        await supplier.issue(operation, binding)

    rendered = "\n".join(
        (
            repr(supplier),
            repr(caught.value),
            "".join(traceback.format_exception(caught.value)),
            caplog.text,
        )
    )
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    for secret in (
        "SIGNER_SENTINEL",
        "NONCE_SENTINEL",
        "RUN_SENTINEL",
        "TENANT_SENTINEL",
        "OWNER_SENTINEL",
        "ASSERTION_SENTINEL",
        operation,
        binding,
    ):
        assert secret not in rendered
