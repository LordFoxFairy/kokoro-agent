"""Budget consumer recovery against real Agent transactions; HTTP has separate gates."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
import hashlib
import json
import os
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
    assert b.calls[0] == b.calls[1]
    before = await prepared.row()
    assert not (await a.authorize(prepared.prepared)).newly_recorded
    assert len(b.calls) == len(proofs.calls) == 2
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


async def test_two_concurrent_same_acks_grant_only_once(prepared: Prepared) -> None:
    entered = 0
    both = asyncio.Event()

    async def barrier():
        nonlocal entered
        entered += 1
        if entered == 2:
            both.set()
        await asyncio.wait_for(both.wait(), 5)

    b = Budget(barrier)
    a = authorizer(prepared, b, Proofs())
    results = await asyncio.gather(
        a.authorize(prepared.prepared), a.authorize(prepared.prepared)
    )
    assert sorted(r.newly_recorded for r in results) == [False, True]
    assert b.calls[0] == b.calls[1]
    assert results[0].admission_ref == results[1].admission_ref
    assert (await prepared.row())["dispatch_started_at"] is None


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
    )
    call = await execution.authorize(prepared.prepared)
    result = await execution.seal(
        call=call, route=route, observation=observed, outcome="completed"
    )
    assert result.status == "captured" and result.captured_credits == "1"

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
