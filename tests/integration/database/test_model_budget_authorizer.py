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
    def __init__(self, effect: Callable[[], Awaitable[None]] | None = None):
        self.calls: list[tuple[ModelBudgetCommand, str]] = []
        self.effect = effect

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
        return ModelBudgetGrant(admission_ref="22222222-2222-4222-8222-222222222222")


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
