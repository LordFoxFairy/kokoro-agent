"""Budget consumer recovery against real Agent transactions; HTTP has separate gates."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
import hashlib
import json
import os
from typing import Literal
from uuid import uuid4

import pytest
from pydantic import SecretStr

from kokoro_agent.domain.run.models import LeaseFence, LeasedRun, StaticRecipeBinding
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.schema import apply_agent_schema
from kokoro_agent.infrastructure.postgres_model_call_repository import (
    PostgresModelCallRepository,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.model.call_models import (
    PrepareModelCall,
    PreparedModelCall,
    ModelCallAuthorityLost,
    ModelCallAuthorizationUnavailable,
)
from kokoro_agent.model.budget import (
    ModelBudgetAuthorizer,
    ModelBudgetCommand,
    ModelBudgetGrant,
)
from kokoro_agent.model.call_artifacts import (
    encode_final_request_digest,
    encode_planned_artifact,
)
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding


@dataclass(frozen=True)
class Prepared:
    repo: PostgresModelCallRepository
    prepared: PreparedModelCall
    leased: LeasedRun
    url: str
    schema: str

    async def row(self):
        async with connect_pg(self.url) as c, c.cursor() as cur:
            await execute_sql(
                cur,
                f"SELECT xmin::text,status,authorization_state,authorization_bytes,authorization_digest,dispatch_started_at FROM {qualified(self.schema, 'kokoro_agent_model_call_attempt')} WHERE attempt_id=%s",
                (self.prepared.attempt_id,),
            )
            row = await fetch_one(cur)
            assert row
            return row


@pytest.fixture
async def prepared() -> AsyncIterator[Prepared]:
    url = os.environ["KOKORO_AGENT_DATABASE_URL"]
    schema = "budget_" + uuid4().hex
    user = IdentityRef(kind="user", opaque_ref="user-1")
    request = RunRequest(
        kind="run.request",
        run_id="run-1",
        session_id="conversation-1",
        feature_key="chat",
        project_ref=None,
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant-1",
            actor=user,
            subject=user,
            identity_assertion_ref="bff:original",
        ),
        input=RunInput(message_id="message-1", content="budget regression"),
    )
    lease = LeaseFence(owner="worker-1", generation=7)
    raw = json.dumps(
        {
            "domain_tag": "kokoro-agent:assembly-recipe:1",
            "feature": {
                "key": "chat",
                "entry_agent": "assistant",
                "handoffs": [],
                "agents": [{"key": "assistant", "subagents": []}],
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    recipe = StaticRecipeBinding(
        canonical_bytes=raw, fingerprint=hashlib.sha256(raw).hexdigest()
    )
    planned = PlannedBinding(
        binding_version="2.0.0",
        model="model-1",
        provider="provider-1",
        revision_id="11111111-1111-4111-8111-111111111111",
        provider_model_name="actual-model",
        request_model="request-model",
        feature_key="chat",
        revision="1",
        generation="1",
        tenant_generation="1",
        digest="a" * 64,
        label=None,
    )
    try:
        async with connect_pg(url) as c:
            await apply_agent_schema(c, schema, require_blank=True)
            async with c.cursor() as cur:
                await execute_sql(
                    cur,
                    f"INSERT INTO {qualified(schema, 'kokoro_agent_run')} (run_id,tenant_id,request_json,assembly_recipe_bytes,assembly_recipe_fingerprint,owner,lease_generation,lease_expires_at) VALUES (%s,%s,%s,%s,%s,%s,7,clock_timestamp()+interval '5 minutes')",
                    (
                        request.run_id,
                        request.execution_identity.tenant_ref,
                        request.model_dump_json(),
                        recipe.canonical_bytes,
                        recipe.fingerprint,
                        lease.owner,
                    ),
                )
        repo = PostgresModelCallRepository(url, schema)
        attempt = await repo.prepare(
            PrepareModelCall(
                request=request,
                lease=lease,
                call_id=str(uuid4()),
                attempt_id=str(uuid4()),
                planned=planned,
                peer_path=("assistant",),
                recipe=recipe,
                final_request_binding_version=1,
                final_request_digest=encode_final_request_digest(
                    endpoint="https://fixture.invalid/chat/completions",
                    planned_digest=encode_planned_artifact(planned)[1],
                    raw=b"{}",
                ),
            )
        )
        yield Prepared(
            repo, attempt, LeasedRun(request=request, lease=lease), url, schema
        )
    finally:
        async with connect_pg(url) as c, c.cursor() as cur:
            await execute_sql(cur, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


class Proofs:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    async def issue(self, operation: str, request_binding_sha256: str) -> str:
        self.calls.append((operation, request_binding_sha256))
        return "proof"


class Budget:
    def __init__(
        self,
        effect: Callable[[], Awaitable[None]] | None = None,
        *,
        admission_ref: str = "22222222-2222-4222-8222-222222222222",
    ):
        self.calls: list[tuple[ModelBudgetCommand, str]] = []
        self.effect = effect
        self.admission_ref = admission_ref
        self.recover_calls: list[tuple[ModelBudgetCommand, str]] = []
        self.recover_found = True
        self.recover_effect: Callable[[], Awaitable[None]] | None = None

    async def reserve(
        self,
        *,
        tenant_id: str,
        command: ModelBudgetCommand,
        proof: SecretStr,
        key: str,
        request_id: str,
    ) -> ModelBudgetGrant:
        assert tenant_id == "tenant-1" and proof.get_secret_value() == "proof"
        self.calls.append((command, key))
        if self.effect:
            await self.effect()
        return ModelBudgetGrant(admission_ref=self.admission_ref)

    async def recover(
        self, *, tenant_id: str, command: ModelBudgetCommand, key: str, request_id: str
    ) -> ModelBudgetGrant | None:
        assert tenant_id == "tenant-1"
        self.recover_calls.append((command, key))
        if self.recover_effect:
            await self.recover_effect()
        return (
            ModelBudgetGrant(admission_ref=self.admission_ref)
            if self.recover_found
            else None
        )


def authorizer(p: Prepared, b: Budget, proofs: Proofs) -> ModelBudgetAuthorizer:
    return ModelBudgetAuthorizer(
        repository=p.repo, budget=b, proofs=proofs, leased_run=p.leased
    )


async def test_pending_unknown_recovers_same_request_then_granted_skips_http(
    prepared: Prepared,
) -> None:
    async def fail():
        raise TimeoutError("unknown ACK")

    b = Budget(fail)
    proofs = Proofs()
    a = authorizer(prepared, b, proofs)
    with pytest.raises(TimeoutError):
        await a.authorize(prepared.prepared)
    pending = await prepared.row()
    assert pending["status"] == "authorization_pending"
    b.effect = None
    grant = await a.authorize(prepared.prepared)
    assert grant.newly_recorded
    assert b.calls == b.recover_calls
    before = await prepared.row()
    assert not (await a.authorize(prepared.prepared)).newly_recorded
    assert len(b.calls) == len(proofs.calls) == len(b.recover_calls) == 1
    assert await prepared.row() == before
    assert before["dispatch_started_at"] is None


@pytest.mark.parametrize("retire", ["expiry", "generation", "terminal"])
async def test_held_ack_after_authority_loss_stays_pending(
    prepared: Prepared, retire: str
) -> None:
    async def lose():
        clause = {
            "expiry": "lease_expires_at=clock_timestamp()-interval '1 second'",
            "generation": "lease_generation=8",
            "terminal": "terminal=TRUE,interaction_phase='terminal'",
        }[retire]
        async with connect_pg(prepared.url) as c, c.cursor() as cur:
            await execute_sql(
                cur,
                f"UPDATE {qualified(prepared.schema, 'kokoro_agent_run')} SET {clause} WHERE run_id=%s",
                ("run-1",),
            )

    b = Budget(lose)
    with pytest.raises(ModelCallAuthorityLost):
        await authorizer(prepared, b, Proofs()).authorize(prepared.prepared)
    row = await prepared.row()
    assert (
        row["status"] == "authorization_pending" and row["dispatch_started_at"] is None
    )


async def test_concurrent_authorization_never_sends_a_second_reserve(
    prepared: Prepared,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait_for_owner():
        entered.set()
        await asyncio.wait_for(release.wait(), 5)

    b, proofs = Budget(wait_for_owner), Proofs()
    b.recover_found = False
    a = authorizer(prepared, b, proofs)
    first = asyncio.create_task(a.authorize(prepared.prepared))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        with pytest.raises(ModelCallAuthorizationUnavailable):
            await a.authorize(prepared.prepared)
        assert len(b.calls) == len(proofs.calls) == len(b.recover_calls) == 1
        assert (await prepared.row())["authorization_state"] == "requested_unknown"
    finally:
        release.set()
        await first
    assert not (await a.authorize(prepared.prepared)).newly_recorded
    assert len(b.calls) == 1
    assert (await prepared.row())["dispatch_started_at"] is None


async def test_recover_miss_preserves_pending_and_does_not_reserve_again(
    prepared: Prepared,
) -> None:
    async def unknown():
        raise TimeoutError("unknown original acknowledgement")

    b, proofs = Budget(unknown), Proofs()
    a = authorizer(prepared, b, proofs)
    with pytest.raises(TimeoutError):
        await a.authorize(prepared.prepared)
    before = await prepared.row()
    b.recover_found = False
    with pytest.raises(ModelCallAuthorizationUnavailable):
        await a.authorize(prepared.prepared)
    assert await prepared.row() == before
    assert len(b.calls) == len(proofs.calls) == len(b.recover_calls) == 1


async def test_cancel_with_late_held_ack_never_grants(prepared: Prepared) -> None:
    entered = asyncio.Event()

    async def late():
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    b = Budget(late)
    a = authorizer(prepared, b, Proofs())
    task = asyncio.create_task(a.authorize(prepared.prepared))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    row = await prepared.row()
    assert (
        row["status"] == "authorization_pending"
        and row["authorization_state"] == "requested_unknown"
    )
    assert row["dispatch_started_at"] is None


async def test_cross_run_input_is_rejected_before_pending_or_http(
    prepared: Prepared,
) -> None:
    b = Budget()
    proofs = Proofs()
    before = await prepared.row()
    from kokoro_agent.model.call_models import ModelCallInputInvalid

    with pytest.raises(ModelCallInputInvalid):
        await authorizer(prepared, b, proofs).authorize(
            prepared.prepared.model_copy(update={"run_id": "foreign"})
        )
    assert await prepared.row() == before
    assert not b.calls and not proofs.calls


async def test_execution_consumer_grants_exactly_one_dispatch_per_attempt(
    prepared: Prepared,
) -> None:
    from kokoro_agent.model.execution import ModelCallExecutor
    from kokoro_agent.model.call_models import ModelCallAuthorizationUnavailable

    class NoSettlement:
        async def reconcile(self, tenant_id: str, attempt_id: str) -> None:
            raise AssertionError("no settlement before provider result")

    b, proofs = Budget(), Proofs()
    execution = ModelCallExecutor(
        repository=prepared.repo,
        authorizer=authorizer(prepared, b, proofs),
        financial=NoSettlement(),
    )
    call = await execution.authorize(prepared.prepared)
    assert call.dispatch.newly_started and len(b.calls) == 1
    before = await prepared.row()
    with pytest.raises(ModelCallAuthorizationUnavailable):
        await execution.authorize(prepared.prepared)
    assert await prepared.row() == before and len(b.calls) == 1


async def test_observed_call_uses_owner_clock_and_replays_one_durable_financial_ack(
    prepared: Prepared,
) -> None:
    from kokoro_agent.clients.system import ResolvedModel, ModelConnection
    from kokoro_agent.model.execution import ModelCallExecutor
    from kokoro_agent.model.usage_observation import (
        PhysicalUsageObservation,
        ObservedCount,
    )
    from kokoro_agent.model.financial_reconciliation import ModelFinancialReconciler
    from kokoro_agent.infrastructure.postgres_model_financial_repository import (
        PostgresModelFinancialRepository,
    )
    from kokoro_agent.model.settlement import (
        ModelSettlementCommand,
        ModelSettlementReceipt,
    )
    from dataclasses import asdict
    from types import MappingProxyType

    class FinancialOwner:
        async def recover(
            self,
            *,
            tenant_id: str,
            command: ModelBudgetCommand,
            key: str,
            request_id: str,
        ) -> ModelBudgetGrant | None:
            raise AssertionError("granted execution must not recover missing funds")

        async def settle(
            self,
            *,
            tenant_id: str,
            admission_ref: str,
            command: ModelSettlementCommand,
            key: str,
            request_id: str,
        ) -> ModelSettlementReceipt:
            return ModelSettlementReceipt(
                admission_ref=admission_ref,
                **asdict(command),
                status="captured",
                reason=None,
                captured_credits="1",
                released_credits="9",
                usage_event_ref="33333333-3333-4333-8333-333333333333",
                settlement_ref="44444444-4444-4444-8444-444444444444",
                updated_at="2026-10-23T00:00:00.000Z",
            )

    financial = ModelFinancialReconciler(
        repository=PostgresModelFinancialRepository(prepared.url, prepared.schema),
        client=FinancialOwner(),
    )
    execution = ModelCallExecutor(
        repository=prepared.repo,
        authorizer=authorizer(prepared, Budget(), Proofs()),
        financial=financial,
    )
    route = ResolvedModel(
        model_id="model-1",
        provider_id="provider-1",
        revision_id="11111111-1111-4111-8111-111111111111",
        revision=1,
        digest="a" * 64,
        generation="1",
        tenant_generation="1",
        provider_model_name="actual-model",
        request_model="request-model",
        feature_key="chat",
        label_key="default",
        context_window=128,
        max_output_tokens=16,
        connection=ModelConnection(
            protocol="openai_chat_completions",
            connection_type="direct",
            base_url="https://fixture.invalid",
            secret_handle_ref="fixture-handle",
        ),
    )
    observed = PhysicalUsageObservation(
        status="complete",
        reason=None,
        compression_integrity="validated",
        usage_presence="object",
        counts=MappingProxyType(
            {
                k: ObservedCount("reported", v)
                if v is not None
                else ObservedCount("missing")
                for k, v in {
                    "prompt_tokens": 17,
                    "completion_tokens": 3,
                    "total_tokens": 20,
                    "cached_tokens": None,
                    "reasoning_tokens": None,
                }.items()
            }
        ),
        projection_digest="a" * 64,
        reported_model="actual-model",
        provider_response_id="gen-durable-source",
    )
    call = await execution.authorize(prepared.prepared)
    result = await execution.seal(
        call=call, route=route, observation=observed, outcome="completed"
    )
    assert result.status == "captured" and result.captured_credits == "1"
    async with connect_pg(prepared.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT provider_response_id FROM {qualified(prepared.schema, 'kokoro_agent_model_call_attempt')} WHERE attempt_id=%s",
            (prepared.prepared.attempt_id,),
        )
        assert await fetch_one(cur) == {"provider_response_id": "gen-durable-source"}

    async def facts():
        async with connect_pg(prepared.url) as c, c.cursor() as cur:
            await execute_sql(
                cur,
                f"SELECT row_to_json(t)::text,xmin::text FROM {qualified(prepared.schema, 'kokoro_agent_model_usage_evidence')} t",
            )
            return await fetch_one(cur)

    before = await facts()
    assert (
        await execution.seal(
            call=call, route=route, observation=observed, outcome="completed"
        )
        == result
    )
    assert await facts() == before

    # A later logical call may dispatch after a complete, exact captured ACK.
    next_attempt = await next_prepared(prepared)
    next_execution = ModelCallExecutor(
        repository=prepared.repo,
        authorizer=authorizer(prepared, Budget(admission_ref=str(uuid4())), Proofs()),
        financial=financial,
    )
    assert (await next_execution.authorize(next_attempt)).dispatch.newly_started


async def next_prepared(prepared: Prepared) -> PreparedModelCall:
    p = prepared.prepared
    async with connect_pg(prepared.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT assembly_recipe_bytes,assembly_recipe_fingerprint FROM {qualified(prepared.schema, 'kokoro_agent_run')} WHERE run_id=%s",
            (p.run_id,),
        )
        row = await fetch_one(cur)
        assert row is not None
    raw = row["assembly_recipe_bytes"]
    assert isinstance(raw, bytes)
    return await prepared.repo.prepare(
        PrepareModelCall(
            request=prepared.leased.request,
            lease=prepared.leased.lease,
            call_id=str(uuid4()),
            attempt_id=str(uuid4()),
            planned=PlannedBinding.model_validate_json(p.planned_bytes, strict=True),
            peer_path=("assistant",),
            recipe=StaticRecipeBinding(
                canonical_bytes=raw, fingerprint=str(row["assembly_recipe_fingerprint"])
            ),
            final_request_binding_version=p.final_request_binding_version,
            final_request_digest=p.final_request_digest,
        )
    )


@pytest.mark.parametrize("ack_state", ["unacked", "pending", "corrupt"])
async def test_prior_unacked_observation_stops_new_dispatch(
    prepared: Prepared, ack_state: str
) -> None:
    from kokoro_agent.model.execution import ModelCallExecutor
    from kokoro_agent.model.call_models import (
        SealModelCallObservation,
        ModelCallAuthorizationUnavailable,
    )
    from kokoro_agent.protocol.model_usage_v2_generated import (
        UnknownAttribution,
        UnresolvedUsage,
    )

    class NoSettlement:
        async def reconcile(self, tenant_id: str, attempt_id: str) -> None:
            raise AssertionError("not needed")

    first = ModelCallExecutor(
        repository=prepared.repo,
        authorizer=authorizer(prepared, Budget(), Proofs()),
        financial=NoSettlement(),
    )
    call = await first.authorize(prepared.prepared)
    p = call.prepared
    await prepared.repo.seal_observation(
        SealModelCallObservation(
            tenant_id=p.tenant_id,
            run_id=p.run_id,
            attempt_id=p.attempt_id,
            lease=p.lease,
            attempt_binding_digest=p.attempt_binding_digest,
            planned_digest=p.planned_digest,
            authorization_digest=call.authorization.authorization_digest,
            outcome="unknown",
            actual=UnknownAttribution(state="unknown", reason="not_observed"),
            usage=UnresolvedUsage(
                state="unresolved", reason="incomplete_response", raw_usage_digest=None
            ),
        )
    )
    if ack_state != "unacked":
        from dataclasses import asdict
        from kokoro_agent.infrastructure.postgres_model_financial_repository import (
            PostgresModelFinancialRepository,
        )
        from kokoro_agent.model.settlement import ModelSettlementReceipt

        financial = PostgresModelFinancialRepository(prepared.url, prepared.schema)
        work = await financial.next_work(p.tenant_id, p.attempt_id)
        assert work is not None
        await financial.record_admission(work, call.authorization.admission_ref)
        await financial.record_ack(
            work,
            ModelSettlementReceipt(
                admission_ref=call.authorization.admission_ref,
                **asdict(work.evidence),
                status="pending_reconciliation",
                reason="unknown_usage_or_outcome",
                captured_credits="0",
                released_credits="0",
                usage_event_ref=None,
                settlement_ref=None,
                updated_at="2026-10-23T00:00:00.000Z",
            ),
        )
        if ack_state == "corrupt":
            async with connect_pg(prepared.url) as c, c.cursor() as cur:
                await execute_sql(
                    cur,
                    f"UPDATE {qualified(prepared.schema, 'kokoro_agent_model_usage_evidence')} SET financial_ack_digest=%s WHERE attempt_id=%s",
                    ("f" * 64, p.attempt_id),
                )
    next_attempt = await next_prepared(prepared)
    # A valid pending ACK is still unresolved; a corrupt ACK is never accepted.
    from kokoro_agent.model.financial_receipt import ModelFinancialConflict

    with pytest.raises((ModelCallAuthorizationUnavailable, ModelFinancialConflict)):
        await first.authorize(next_attempt)
    async with connect_pg(prepared.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT dispatch_started_at FROM {qualified(prepared.schema, 'kokoro_agent_model_call_attempt')} WHERE attempt_id=%s",
            (next_attempt.attempt_id,),
        )
        row = await fetch_one(cur)
        assert row is not None and row["dispatch_started_at"] is None


@pytest.mark.parametrize("reject_receipt_write", [False, True])
async def test_source_response_locator_is_atomic_and_immutable(
    prepared: Prepared, reject_receipt_write: bool
) -> None:
    from psycopg.errors import CheckViolation
    from kokoro_agent.model.call_models import (
        SealModelCallObservation,
        StartModelCallDispatch,
        ModelCallIdentityConflict,
    )
    from kokoro_agent.protocol.model_usage_v2_generated import (
        UnknownAttribution,
        UnresolvedUsage,
    )

    p = prepared.prepared
    grant = await authorizer(prepared, Budget(), Proofs()).authorize(p)
    await prepared.repo.start_dispatch(
        StartModelCallDispatch(
            tenant_id=p.tenant_id,
            run_id=p.run_id,
            attempt_id=p.attempt_id,
            lease=p.lease,
            attempt_binding_digest=p.attempt_binding_digest,
            planned_digest=p.planned_digest,
            authorization_digest=grant.authorization_digest,
            request_ref=grant.request_ref,
            admission_ref=grant.admission_ref,
        )
    )
    command = SealModelCallObservation.model_validate(
        {
            "tenant_id": p.tenant_id,
            "run_id": p.run_id,
            "attempt_id": p.attempt_id,
            "lease": p.lease,
            "attempt_binding_digest": p.attempt_binding_digest,
            "planned_digest": p.planned_digest,
            "authorization_digest": grant.authorization_digest,
            "outcome": "completed",
            "actual": UnknownAttribution(state="unknown", reason="unverified_binding"),
            "usage": UnresolvedUsage(
                state="unresolved", reason="missing_usage", raw_usage_digest=None
            ),
            "provider_response_id": "gen-original-receipt",
        }
    )
    table = qualified(prepared.schema, "kokoro_agent_model_call_attempt")
    evidence = qualified(prepared.schema, "kokoro_agent_model_usage_evidence")
    # A locator cannot exist before the physical observation has been sealed.
    async with connect_pg(prepared.url) as c, c.cursor() as cur:
        with pytest.raises(CheckViolation):
            async with c.transaction():
                await execute_sql(
                    cur,
                    f"UPDATE {table} SET provider_response_id=%s WHERE attempt_id=%s",
                    ("gen-too-early", p.attempt_id),
                )
    if reject_receipt_write:
        async with connect_pg(prepared.url) as c, c.cursor() as cur:
            await execute_sql(
                cur,
                f"ALTER TABLE {table} ADD CONSTRAINT qa_receipt_write_failure CHECK (provider_response_id IS NULL)",
            )
        before = await prepared.row()
        with pytest.raises(CheckViolation):
            await prepared.repo.seal_observation(command)
        assert await prepared.row() == before
        async with connect_pg(prepared.url) as c, c.cursor() as cur:
            await execute_sql(cur, f"SELECT count(*) AS count FROM {evidence}")
            assert await fetch_one(cur) == {"count": 0}
        return
    receipt = await prepared.repo.seal_observation(command)
    async with connect_pg(prepared.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT provider_response_id FROM {table} WHERE attempt_id=%s",
            (p.attempt_id,),
        )
        assert await fetch_one(cur) == {"provider_response_id": "gen-original-receipt"}
        for invalid in ("", "x" * 257, "bad\nname", "a b", "编号"):
            with pytest.raises(CheckViolation):
                async with c.transaction():
                    await execute_sql(
                        cur,
                        f"UPDATE {table} SET provider_response_id=%s WHERE attempt_id=%s",
                        (invalid, p.attempt_id),
                    )
        async with c.transaction(force_rollback=True):
            await execute_sql(
                cur,
                f"UPDATE {table} SET provider_response_id=%s WHERE attempt_id=%s",
                ("x" * 256, p.attempt_id),
            )
    # A new repository instance reads the original receipt, including after commit ACK loss.
    reopened = PostgresModelCallRepository(prepared.url, prepared.schema)
    before = await prepared.row()
    replay = await reopened.seal_observation(command)
    assert replay.replayed and replay.event_id == receipt.event_id
    assert await prepared.row() == before
    for replacement in (None, "gen-different"):
        changed = command.model_copy(update={"provider_response_id": replacement})
        with pytest.raises(ModelCallIdentityConflict):
            await reopened.seal_observation(changed)
        assert await prepared.row() == before


@pytest.mark.parametrize(
    "unsupported_reason",
    [
        None,
        "unsupported_usage_category",
        "unsupported_service_tier",
        "unverified_source_semantics",
    ],
)
@pytest.mark.parametrize("zero", [False, True])
async def test_seal_observation_preserves_known_totals_with_profile_assessment(
    prepared: Prepared,
    unsupported_reason: Literal[
        "unsupported_usage_category",
        "unsupported_service_tier",
        "unverified_source_semantics",
    ]
    | None,
    zero: bool,
) -> None:
    from psycopg.errors import CheckViolation
    from kokoro_agent.model.call_models import (
        SealModelCallObservation,
        StartModelCallDispatch,
        ModelCallInputInvalid,
    )
    from kokoro_agent.model.usage_evidence_v2 import parse_evidence
    from kokoro_agent.model.usage_observation import (
        PhysicalUsageObservation,
        ObservedCount,
    )
    from kokoro_agent.model.usage_producer import normalize_observed_usage
    from kokoro_agent.protocol.model_usage_v2_generated import (
        UnknownAttribution,
        UnsupportedProfile,
    )

    p = prepared.prepared
    grant = await authorizer(prepared, Budget(), Proofs()).authorize(p)
    await prepared.repo.start_dispatch(
        StartModelCallDispatch(
            tenant_id=p.tenant_id,
            run_id=p.run_id,
            attempt_id=p.attempt_id,
            lease=p.lease,
            attempt_binding_digest=p.attempt_binding_digest,
            planned_digest=p.planned_digest,
            authorization_digest=grant.authorization_digest,
            request_ref=grant.request_ref,
            admission_ref=grant.admission_ref,
        )
    )
    usage = normalize_observed_usage(
        PhysicalUsageObservation(
            status="complete",
            reason=None,
            compression_integrity="validated",
            usage_presence="object",
            projection_digest="b" * 64,
            counts={
                "prompt_tokens": ObservedCount("reported", 0 if zero else 20),
                "completion_tokens": ObservedCount("reported", 0 if zero else 3),
                "total_tokens": ObservedCount("reported", 0 if zero else 23),
                "cached_tokens": ObservedCount("missing"),
                "reasoning_tokens": ObservedCount("missing"),
            },
        )
    )
    assert usage.state == "normalized"
    if unsupported_reason is not None:
        usage = usage.model_copy(
            update={
                "value": usage.value.model_copy(
                    update={
                        "classification_state": "unknown",
                        "profile_assessment": UnsupportedProfile(
                            state="unsupported", reason=unsupported_reason
                        ),
                    }
                )
            }
        )
    command = SealModelCallObservation(
        tenant_id=p.tenant_id,
        run_id=p.run_id,
        attempt_id=p.attempt_id,
        lease=p.lease,
        attempt_binding_digest=p.attempt_binding_digest,
        planned_digest=p.planned_digest,
        authorization_digest=grant.authorization_digest,
        outcome="completed",
        actual=UnknownAttribution(state="unknown", reason="unverified_binding"),
        usage=usage,
    )
    before = await prepared.row()
    # The normal writer must still reject a classification that contradicts its profile.
    wrong = usage.model_copy(
        update={
            "value": usage.value.model_copy(
                update={
                    "classification_state": "unknown"
                    if unsupported_reason is None
                    else "partial",
                }
            )
        }
    )
    with pytest.raises(ModelCallInputInvalid):
        await prepared.repo.seal_observation(
            command.model_copy(update={"usage": wrong})
        )
    assert await prepared.row() == before

    receipt = await prepared.repo.seal_observation(command)
    stored = await PostgresModelCallRepository(
        prepared.url, prepared.schema
    ).read_final_evidence(
        p.tenant_id,
        p.attempt_id,
        receipt.event_id,
    )
    assert stored is not None
    fact = parse_evidence(stored.evidence_bytes)
    assert fact.execution.outcome == "completed" and fact.actual == command.actual
    assert fact.usage == usage
    assert fact.usage.state == "normalized"
    assert fact.usage.value.totals_state == ("known_zero" if zero else "known_nonzero")
    assert fact.usage.value.input_cache_read.state == "unreported"
    assert fact.usage.value.output_reasoning.state == "unreported"

    # Defense in depth: even bypassing the typed writer cannot persist a false profile.
    evidence_table = qualified(prepared.schema, "kokoro_agent_model_usage_evidence")
    for assessment in (
        None,
        {},
        {"state": "unsupported"},
        {"state": "unsupported", "reason": "invented_reason"},
        {"state": "supported"},
    ):
        document = fact.model_dump(mode="json")
        document["usage"]["value"]["classification_state"] = "unknown"
        if assessment is None:
            del document["usage"]["value"]["profile_assessment"]
        else:
            document["usage"]["value"]["profile_assessment"] = assessment
        async with connect_pg(prepared.url) as c, c.cursor() as cur:
            with pytest.raises(CheckViolation, match="ck_model_evidence_state"):
                async with c.transaction():
                    await execute_sql(
                        cur,
                        f"UPDATE {evidence_table} SET evidence_bytes=%s,classification_state='unknown' WHERE event_id=%s",
                        (json.dumps(document).encode(), receipt.event_id),
                    )
    replay = await prepared.repo.seal_observation(command)
    assert replay.replayed and replay.event_id == receipt.event_id

    # An unsupported assessment cannot be relabeled partial to look priceable.
    document = fact.model_dump(mode="json")
    document["usage"]["value"]["classification_state"] = "partial"
    document["usage"]["value"]["profile_assessment"] = {
        "state": "unsupported",
        "reason": "unsupported_usage_category",
    }
    async with connect_pg(prepared.url) as c, c.cursor() as cur:
        with pytest.raises(CheckViolation, match="ck_model_evidence_state"):
            async with c.transaction():
                await execute_sql(
                    cur,
                    f"UPDATE {evidence_table} SET evidence_bytes=%s,classification_state='partial' WHERE event_id=%s",
                    (json.dumps(document).encode(), receipt.event_id),
                )
    preserved = await prepared.repo.read_final_evidence(
        p.tenant_id, p.attempt_id, receipt.event_id
    )
    assert preserved is not None and preserved.evidence_bytes == stored.evidence_bytes


async def test_confirmed_credit_denial_is_atomically_sealed_and_never_reserved_again(
    prepared: Prepared,
) -> None:
    from kokoro_agent.model.call_models import ModelCallAuthorizationDenied
    from kokoro_agent.model.usage_evidence_v2 import parse_evidence
    from kokoro_agent.infrastructure.postgres_model_financial_repository import (
        PostgresModelFinancialRepository,
    )

    async def deny():
        raise ModelCallAuthorizationDenied("MODEL_BUDGET_INSUFFICIENT_CREDIT")

    b, proofs = Budget(deny), Proofs()
    a = authorizer(prepared, b, proofs)
    with pytest.raises(ModelCallAuthorizationDenied):
        await a.authorize(prepared.prepared)
    row = await prepared.row()
    assert (row["status"], row["authorization_state"], row["dispatch_started_at"]) == (
        "authorization_denied",
        "denied",
        None,
    )
    assert json.loads(row["authorization_bytes"]) == {
        "state": "denied",
        "request_ref": b.calls[0][1],
    }
    async with connect_pg(prepared.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT evidence_bytes FROM {qualified(prepared.schema, 'kokoro_agent_model_usage_evidence')} WHERE attempt_id=%s",
            (prepared.prepared.attempt_id,),
        )
        evidence_row = await fetch_one(cur)
        assert evidence_row
        evidence = parse_evidence(evidence_row["evidence_bytes"])
        assert evidence.authorization_observation.state == "denied"
        assert (
            evidence.execution.outcome == "not_dispatched" and evidence.execution.seal
        )
        assert (
            evidence.usage.state == "unresolved"
        )  # no invented zero usage or money ACK
        await execute_sql(
            cur,
            f"SELECT model_attempt_count,model_dispatched_attempt_count,model_sealed_attempt_count FROM {qualified(prepared.schema, 'kokoro_agent_run')} WHERE run_id=%s",
            ("run-1",),
        )
        assert await fetch_one(cur) == {
            "model_attempt_count": 1,
            "model_dispatched_attempt_count": 0,
            "model_sealed_attempt_count": 1,
        }
    financial = PostgresModelFinancialRepository(prepared.url, prepared.schema)
    assert await financial.list_pending(limit=16) == []
    from kokoro_agent.model.call_models import (
        DenyModelCallAuthorization,
        GrantModelCallAuthorization,
        ModelCallIdentityConflict,
    )
    from kokoro_agent.model.call_artifacts import encode_requested_artifact

    command = DenyModelCallAuthorization(
        tenant_id=prepared.prepared.tenant_id,
        run_id=prepared.prepared.run_id,
        attempt_id=prepared.prepared.attempt_id,
        lease=prepared.prepared.lease,
        attempt_binding_digest=prepared.prepared.attempt_binding_digest,
        planned_digest=prepared.prepared.planned_digest,
        request_ref=b.calls[0][1],
        expected_authorization_digest=encode_requested_artifact(b.calls[0][1])[1],
    )
    replay = await prepared.repo.deny_authorization(command)
    assert replay.replayed and replay.event_id == evidence.identity.event_id
    assert await prepared.row() == row
    with pytest.raises(ModelCallIdentityConflict):
        await prepared.repo.grant_authorization(
            GrantModelCallAuthorization(
                **command.model_dump(), admission_ref=b.admission_ref
            )
        )
    with pytest.raises(ModelCallAuthorizationDenied):
        await a.authorize(prepared.prepared)
    assert await prepared.row() == row
    assert len(b.calls) == len(proofs.calls) == 1 and not b.recover_calls


@pytest.mark.parametrize("retire", ["expiry", "generation", "terminal"])
async def test_credit_denial_after_lease_loss_cannot_rewrite_pending(
    prepared: Prepared, retire: str
) -> None:
    from kokoro_agent.model.call_models import ModelCallAuthorizationDenied

    async def denied_after_retirement():
        clause = {
            "expiry": "lease_expires_at=clock_timestamp()-interval '1 second'",
            "generation": "lease_generation=8",
            "terminal": "terminal=TRUE,interaction_phase='terminal'",
        }[retire]
        async with connect_pg(prepared.url) as c, c.cursor() as cur:
            await execute_sql(
                cur,
                f"UPDATE {qualified(prepared.schema, 'kokoro_agent_run')} SET {clause} WHERE run_id=%s",
                ("run-1",),
            )
        raise ModelCallAuthorizationDenied("MODEL_BUDGET_INSUFFICIENT_CREDIT")

    with pytest.raises(ModelCallAuthorityLost):
        await authorizer(prepared, Budget(denied_after_retirement), Proofs()).authorize(
            prepared.prepared
        )
    row = await prepared.row()
    assert (
        row["status"] == "authorization_pending"
        and row["authorization_state"] == "requested_unknown"
    )
    assert row["dispatch_started_at"] is None


async def test_cancelled_denial_transaction_keeps_pending_and_has_no_evidence(
    prepared: Prepared, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kokoro_agent.model.call_models import ModelCallAuthorizationDenied
    import kokoro_agent.infrastructure.postgres_model_call_evidence as evidence_writer

    async def refuse():
        raise ModelCallAuthorizationDenied("MODEL_BUDGET_INSUFFICIENT_CREDIT")

    async def cancel_after_writes(*_args: object, **_kwargs: object) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(
        evidence_writer, "recompute_run_completeness", cancel_after_writes
    )
    with pytest.raises(asyncio.CancelledError):
        await authorizer(prepared, Budget(refuse), Proofs()).authorize(
            prepared.prepared
        )
    row = await prepared.row()
    assert (row["status"], row["authorization_state"]) == (
        "authorization_pending",
        "requested_unknown",
    )
    async with connect_pg(prepared.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT count(*) AS n FROM {qualified(prepared.schema, 'kokoro_agent_model_usage_evidence')} WHERE attempt_id=%s",
            (prepared.prepared.attempt_id,),
        )
        assert await fetch_one(cur) == {"n": 0}


async def test_cancel_with_late_recovered_ack_never_grants(prepared: Prepared) -> None:
    async def unknown():
        raise TimeoutError("original acknowledgement unknown")

    b, proofs = Budget(unknown), Proofs()
    a = authorizer(prepared, b, proofs)
    with pytest.raises(TimeoutError):
        await a.authorize(prepared.prepared)
    before = await prepared.row()
    entered = asyncio.Event()

    async def late():
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    b.recover_effect = late
    task = asyncio.create_task(a.authorize(prepared.prepared))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await prepared.row() == before
    assert len(b.calls) == len(proofs.calls) == len(b.recover_calls) == 1
