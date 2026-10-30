"""Financial delivery recovery on real owner PostgreSQL, with named owner ports."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from dataclasses import dataclass, asdict, replace
from uuid import uuid4

import pytest
import rfc8785
from psycopg.errors import CheckViolation
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one, fetch_all
from kokoro_agent.infrastructure.postgres_model_call_repository import (
    PostgresModelCallRepository,
)
from kokoro_agent.infrastructure.postgres_model_call_evidence import (
    close_model_call_attempts_for_terminal,
)
from kokoro_agent.infrastructure.schema import verify_agent_schema, SchemaNotReadyError
from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.protocol import RunRequest, RunInput, ExecutionIdentity, IdentityRef
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding
from kokoro_agent.model.call_models import (
    PrepareModelCall,
    BeginModelCallAuthorization,
    GrantModelCallAuthorization,
    StartModelCallDispatch,
    AppendModelCallEvidence,
)
from kokoro_agent.model.budget import ModelBudgetCommand, ModelBudgetGrant
from kokoro_agent.model.settlement import ModelSettlementCommand, ModelSettlementReceipt

from kokoro_agent.infrastructure.postgres_model_financial_repository import (
    PostgresModelFinancialRepository,
)
from kokoro_agent.model.financial_reconciliation import (
    ModelFinancialReconciler,
    ModelFinancialWork,
)
from kokoro_agent.model.financial_receipt import ModelFinancialConflict

ADMISSION = "11111111-1111-4111-8111-111111111111"


@dataclass(frozen=True)
class Fixture:
    url: str
    schema: str
    attempt: str
    tenant: str = "tenant-1"
    run: str = "run-1"

    async def facts(self) -> list[list[dict[str, object]]]:
        async with connect_pg(self.url) as c, c.cursor() as cur:
            result: list[list[dict[str, object]]] = []
            for table in [
                "kokoro_agent_run",
                "kokoro_agent_model_call_attempt",
                "kokoro_agent_model_usage_evidence",
            ]:
                await execute_sql(
                    cur,
                    f"SELECT row_to_json(t)::text AS fact,xmin::text AS version FROM {qualified(self.schema, table)} t ORDER BY 1",
                )
                result.append(await fetch_all(cur))
            return result


@pytest.fixture
async def sealed(
    run_chat_database_url: str,
    run_chat_schema: str,
    request: pytest.FixtureRequest,
) -> Fixture:
    u = IdentityRef(kind="user", opaque_ref="user-1")
    run_request = RunRequest(
        kind="run.request",
        run_id="run-1",
        session_id="conversation-1",
        feature_key="chat",
        project_ref=None,
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant-1",
            actor=u,
            subject=u,
            identity_assertion_ref="bff:original",
        ),
        input=RunInput(message_id="message-1", content="non-executing recovery sample"),
    )
    recipe_raw = rfc8785.dumps(
        {
            "domain_tag": "kokoro-agent:assembly-recipe:1",
            "feature": {
                "key": "chat",
                "entry_agent": "assistant",
                "handoffs": [],
                "agents": [{"key": "assistant", "subagents": []}],
            },
        }
    )
    recipe = StaticRecipeBinding(
        canonical_bytes=recipe_raw, fingerprint=hashlib.sha256(recipe_raw).hexdigest()
    )
    lease = LeaseFence(owner="qa-worker", generation=7)
    async with connect_pg(run_chat_database_url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_run')} (run_id,tenant_id,request_json,assembly_recipe_bytes,assembly_recipe_fingerprint,owner,lease_generation,lease_expires_at) VALUES (%s,%s,%s,%s,%s,%s,7,clock_timestamp()+interval '1 minute')",
            (
                run_request.run_id,
                "tenant-1",
                run_request.model_dump_json(),
                recipe_raw,
                recipe.fingerprint,
                lease.owner,
            ),
        )
    repo = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    p = await repo.prepare(
        PrepareModelCall(
            request=run_request,
            lease=lease,
            call_id=str(uuid4()),
            attempt_id=str(uuid4()),
            peer_path=("assistant",),
            recipe=recipe,
            planned=PlannedBinding(
                binding_version="2.0.0",
                model="model-1",
                provider="provider-1",
                revision_id=ADMISSION,
                provider_model_name="raw-model",
                request_model="model-1",
                feature_key="chat",
                revision="1",
                generation="1",
                tenant_generation="1",
                digest="a" * 64,
                label=None,
            ),
            final_request_binding_version=1,
            final_request_digest="f" * 64,
        )
    )
    pending = await repo.begin_authorization(
        BeginModelCallAuthorization(
            tenant_id="tenant-1",
            run_id=run_request.run_id,
            attempt_id=p.attempt_id,
            lease=lease,
            attempt_binding_digest=p.attempt_binding_digest,
            planned_digest=p.planned_digest,
            request_ref="original-budget-key",
        )
    )
    grant_digest: str | None = None
    mode = getattr(request, "param", None)
    if mode in {"granted", "history"}:
        grant = await repo.grant_authorization(
            GrantModelCallAuthorization(
                tenant_id="tenant-1",
                run_id="run-1",
                attempt_id=p.attempt_id,
                lease=lease,
                attempt_binding_digest=p.attempt_binding_digest,
                planned_digest=p.planned_digest,
                expected_authorization_digest=pending.authorization_digest,
                request_ref="original-budget-key",
                admission_ref=ADMISSION,
            )
        )
        grant_digest = grant.authorization_digest
        if mode == "history":
            await repo.start_dispatch(
                StartModelCallDispatch(
                    tenant_id="tenant-1",
                    run_id="run-1",
                    attempt_id=p.attempt_id,
                    lease=lease,
                    attempt_binding_digest=p.attempt_binding_digest,
                    planned_digest=p.planned_digest,
                    authorization_digest=grant.authorization_digest,
                    request_ref="original-budget-key",
                    admission_ref=ADMISSION,
                )
            )
    async with connect_pg(run_chat_database_url) as c:
        async with c.transaction(), c.cursor() as cur:
            await execute_sql(
                cur,
                f"SELECT run_id FROM {qualified(run_chat_schema, 'kokoro_agent_run')} WHERE run_id=%s FOR UPDATE",
                (run_request.run_id,),
            )
            await execute_sql(cur, "SELECT clock_timestamp() AS now")
            now = await fetch_one(cur)
            assert now is not None and isinstance(now["now"], datetime)
            await close_model_call_attempts_for_terminal(
                cur,
                schema=run_chat_schema,
                tenant_id="tenant-1",
                run_id=run_request.run_id,
                database_now=now["now"],
                allow_create=True,
            )
    if mode == "history":
        assert grant_digest is not None
        async with connect_pg(run_chat_database_url) as c, c.cursor() as cur:
            await execute_sql(
                cur,
                f"SELECT evidence_bytes FROM {qualified(run_chat_schema, 'kokoro_agent_model_usage_evidence')} WHERE attempt_id=%s",
                (p.attempt_id,),
            )
            first = await fetch_one(cur)
            assert first is not None
            original = json.loads(bytes(first["evidence_bytes"]))
            await execute_sql(cur, "SELECT clock_timestamp() AS now")
            now = await fetch_one(cur)
            assert now is not None and isinstance(now["now"], datetime)
        successor = original | {
            "identity": original["identity"] | {"event_id": str(uuid4())},
            "revision": "2",
            "predecessor": {
                "event_id": original["identity"]["event_id"],
                "digest": original["digest"],
            },
            "execution": original["execution"] | {"outcome": "completed"},
            "observed_at": now["now"]
            .astimezone(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
        }
        successor.pop("digest")
        successor["digest"] = hashlib.sha256(
            b"kokoro-agent:model-usage-evidence:v2\n" + rfc8785.dumps(successor)
        ).hexdigest()
        await repo.append_evidence(
            AppendModelCallEvidence(
                tenant_id="tenant-1",
                run_id="run-1",
                attempt_id=p.attempt_id,
                lease=lease,
                attempt_binding_digest=p.attempt_binding_digest,
                planned_digest=p.planned_digest,
                authorization_digest=grant_digest,
                evidence_bytes=rfc8785.dumps(successor),
            )
        )
    async with connect_pg(run_chat_database_url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"UPDATE {qualified(run_chat_schema, 'kokoro_agent_run')} SET terminal=true,terminal_at=clock_timestamp(),interaction_phase='terminal',lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=%s",
            (run_request.run_id,),
        )
    return Fixture(run_chat_database_url, run_chat_schema, p.attempt_id)


def repository(f: Fixture) -> PostgresModelFinancialRepository:
    return PostgresModelFinancialRepository(f.url, f.schema)


class Owner:
    def __init__(self, *, miss: bool = False, crash: bool = False):
        self.miss = miss
        self.crash = crash
        self.recoveries: list[tuple[ModelBudgetCommand, str]] = []
        self.notifications: list[tuple[ModelSettlementCommand, str]] = []
        self.receipts: dict[str, ModelSettlementReceipt] = {}

    async def recover(
        self, *, tenant_id: str, command: ModelBudgetCommand, key: str, request_id: str
    ) -> ModelBudgetGrant | None:
        assert tenant_id == "tenant-1" and request_id
        self.recoveries.append((command, key))
        return None if self.miss else ModelBudgetGrant(admission_ref=ADMISSION)

    async def settle(
        self,
        *,
        tenant_id: str,
        admission_ref: str,
        command: ModelSettlementCommand,
        key: str,
        request_id: str,
    ) -> ModelSettlementReceipt:
        assert tenant_id == "tenant-1" and admission_ref == ADMISSION and request_id
        self.notifications.append((command, key))
        value = self.receipts.setdefault(
            key,
            ModelSettlementReceipt(
                admission_ref=admission_ref,
                **asdict(command),
                status="released",
                reason=None,
                captured_credits="0",
                released_credits="10",
                usage_event_ref=None,
                settlement_ref=None,
                updated_at="2026-10-23T00:00:00.000Z",
            ),
        )
        if self.crash:
            self.crash = False
            raise TimeoutError("owner committed; client did not observe ACK")
        return value


def coordinator(
    repo: PostgresModelFinancialRepository, owner: Owner
) -> ModelFinancialReconciler:
    return ModelFinancialReconciler(repository=repo, client=owner)


async def test_current_schema_contains_separate_financial_delivery_metadata(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    async with connect_pg(run_chat_database_url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=%s AND column_name LIKE 'financial_%%' ORDER BY 1,2",
            (run_chat_schema,),
        )
        rows = await fetch_all(cur)
    assert {(r["table_name"], r["column_name"]) for r in rows} == {
        ("kokoro_agent_model_call_attempt", "financial_admission_ref"),
        ("kokoro_agent_model_usage_evidence", "financial_ack_bytes"),
        ("kokoro_agent_model_usage_evidence", "financial_ack_digest"),
        ("kokoro_agent_model_usage_evidence", "financial_acked_at"),
    }


async def test_seal_before_notification_recovers_after_terminal_without_changing_original_evidence(
    sealed: Fixture,
) -> None:
    r = repository(sealed)
    owner = Owner()
    value = await coordinator(r, owner).reconcile(sealed.tenant, sealed.attempt)
    assert value is not None and value.status == "released"
    assert len(owner.recoveries) == len(owner.notifications) == 1
    assert owner.recoveries[0][1] == "original-budget-key"
    before = await sealed.facts()
    same = await coordinator(repository(sealed), Owner()).reconcile(
        sealed.tenant, sealed.attempt
    )
    assert same == value and await sealed.facts() == before
    async with connect_pg(sealed.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT authorization_state,authorization_bytes,dispatch_started_at FROM {qualified(sealed.schema, 'kokoro_agent_model_call_attempt')}",
        )
        row = await fetch_one(cur)
        assert row is not None
        assert (
            row["authorization_state"] == "requested_unknown"
            and row["dispatch_started_at"] is None
        )
        assert json.loads(bytes(row["authorization_bytes"])) == {
            "state": "requested_unknown",
            "request_ref": "original-budget-key",
        }


async def test_owner_commit_without_local_ack_reuses_original_event_and_key(
    sealed: Fixture,
) -> None:
    owner = Owner(crash=True)
    with pytest.raises(TimeoutError):
        await coordinator(repository(sealed), owner).reconcile(
            sealed.tenant, sealed.attempt
        )
    value = await coordinator(repository(sealed), owner).reconcile(
        sealed.tenant, sealed.attempt
    )
    assert value is not None and value.status == "released"
    assert (
        len(owner.recoveries) == 1
        and len(owner.notifications) == 2
        and len(owner.receipts) == 1
    )
    assert owner.notifications[0] == owner.notifications[1]


async def test_exact_recover_miss_preserves_pending_and_never_marks_zero(
    sealed: Fixture,
) -> None:
    owner = Owner(miss=True)
    r = repository(sealed)
    before = await sealed.facts()
    assert await coordinator(r, owner).reconcile(sealed.tenant, sealed.attempt) is None
    assert await sealed.facts() == before and not owner.notifications
    assert len(await r.list_pending(limit=1)) == 1


async def test_foreign_tenant_is_hidden_and_does_not_contact_owner(
    sealed: Fixture,
) -> None:
    owner = Owner()
    before = await sealed.facts()
    assert (
        await coordinator(repository(sealed), owner).reconcile(
            "foreign", sealed.attempt
        )
        is None
    )
    assert (
        not owner.recoveries
        and not owner.notifications
        and await sealed.facts() == before
    )


@pytest.mark.parametrize("sealed", ["granted"], indirect=True)
async def test_known_grant_skips_recovery_but_saves_independent_association(
    sealed: Fixture,
) -> None:
    owner = Owner(miss=True)
    value = await coordinator(repository(sealed), owner).reconcile(
        sealed.tenant, sealed.attempt
    )
    assert value is not None and not owner.recoveries
    assert len(owner.notifications) == 1
    async with connect_pg(sealed.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT financial_admission_ref FROM {qualified(sealed.schema, 'kokoro_agent_model_call_attempt')}",
        )
        row = await fetch_one(cur)
        assert row is not None and str(row["financial_admission_ref"]) == ADMISSION


async def test_local_ack_failure_after_owner_success_replays_same_ack_after_restart(
    sealed: Fixture,
) -> None:
    class BeforeAckCrash(PostgresModelFinancialRepository):
        async def record_ack(
            self, work: ModelFinancialWork, value: ModelSettlementReceipt
        ) -> ModelSettlementReceipt:
            raise RuntimeError("injected crash after owner ACK before local commit")

    owner = Owner()
    with pytest.raises(RuntimeError, match="injected crash"):
        await coordinator(BeforeAckCrash(sealed.url, sealed.schema), owner).reconcile(
            sealed.tenant, sealed.attempt
        )
    result = await coordinator(repository(sealed), owner).reconcile(
        sealed.tenant, sealed.attempt
    )
    assert (
        result is not None
        and len(owner.receipts) == 1
        and len(owner.notifications) == 2
    )
    assert owner.notifications[0] == owner.notifications[1]


async def test_concurrent_same_event_and_local_replay_have_one_durable_ack(
    sealed: Fixture,
) -> None:
    owner = Owner()
    values = await asyncio.gather(
        *(
            coordinator(repository(sealed), owner).reconcile(
                sealed.tenant, sealed.attempt
            )
            for _ in range(3)
        )
    )
    assert (
        values[0] is not None
        and all(v == values[0] for v in values)
        and len(owner.receipts) == 1
    )
    before = await sealed.facts()
    assert (
        await coordinator(repository(sealed), owner).reconcile(
            sealed.tenant, sealed.attempt
        )
        == values[0]
    )
    assert await sealed.facts() == before


async def test_same_ack_identity_with_different_result_or_request_is_rejected_without_writes(
    sealed: Fixture,
) -> None:
    r = repository(sealed)
    work = await r.next_work(sealed.tenant, sealed.attempt)
    assert work is not None
    value = await coordinator(r, Owner()).reconcile(sealed.tenant, sealed.attempt)
    assert value is not None
    before = await sealed.facts()
    with pytest.raises(ModelFinancialConflict):
        await r.record_ack(work, replace(value, released_credits="11"))
    with pytest.raises(ModelFinancialConflict):
        await r.record_admission(
            replace(work, budget=replace(work.budget, planned_digest="0" * 64)),
            ADMISSION,
        )
    with pytest.raises(ModelFinancialConflict):
        await r.record_admission(work, str(uuid4()))
    assert await sealed.facts() == before


async def test_financial_recovery_does_not_require_retained_run_or_active_lease(
    sealed: Fixture,
) -> None:
    async with connect_pg(sealed.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"DELETE FROM {qualified(sealed.schema, 'kokoro_agent_run')} WHERE run_id=%s",
            (sealed.run,),
        )
    value = await coordinator(repository(sealed), Owner()).reconcile(
        sealed.tenant, sealed.attempt
    )
    assert value is not None and value.status == "released"


@pytest.mark.parametrize("damage", ["bytes", "digest", "association"])
async def test_corrupt_durable_ack_is_not_reported_as_complete_or_repaired(
    sealed: Fixture, damage: str
) -> None:
    r = repository(sealed)
    await coordinator(r, Owner()).reconcile(sealed.tenant, sealed.attempt)
    async with connect_pg(sealed.url) as c, c.cursor() as cur:
        if damage == "bytes":
            await execute_sql(
                cur,
                f"UPDATE {qualified(sealed.schema, 'kokoro_agent_model_usage_evidence')} SET financial_ack_bytes=%s",
                (b"{}",),
            )
        elif damage == "digest":
            await execute_sql(
                cur,
                f"UPDATE {qualified(sealed.schema, 'kokoro_agent_model_usage_evidence')} SET financial_ack_digest=%s",
                ("0" * 64,),
            )
        else:
            await execute_sql(
                cur,
                f"UPDATE {qualified(sealed.schema, 'kokoro_agent_model_call_attempt')} SET financial_admission_ref=NULL",
            )
    before = await sealed.facts()
    with pytest.raises(ModelFinancialConflict):
        await r.latest_ack(sealed.tenant, sealed.attempt)
    assert await sealed.facts() == before


async def test_partial_ack_is_rejected_by_canonical_check(sealed: Fixture) -> None:
    with pytest.raises(CheckViolation, match="ck_model_evidence_financial_ack"):
        async with connect_pg(sealed.url) as c, c.cursor() as cur:
            await execute_sql(
                cur,
                f"UPDATE {qualified(sealed.schema, 'kokoro_agent_model_usage_evidence')} SET financial_ack_digest=%s",
                ("a" * 64,),
            )


@pytest.mark.parametrize("damage", ["column", "check", "index"])
async def test_fresh_schema_gate_rejects_financial_delivery_drift(
    run_chat_database_url: str, run_chat_schema: str, damage: str
) -> None:
    async with connect_pg(run_chat_database_url) as c:
        await verify_agent_schema(c, run_chat_schema)
        async with c.cursor() as cur:
            if damage == "column":
                await execute_sql(
                    cur,
                    f"ALTER TABLE {qualified(run_chat_schema, 'kokoro_agent_model_usage_evidence')} DROP COLUMN financial_acked_at CASCADE",
                )
            elif damage == "check":
                await execute_sql(
                    cur,
                    f"ALTER TABLE {qualified(run_chat_schema, 'kokoro_agent_model_usage_evidence')} DROP CONSTRAINT ck_model_evidence_financial_ack",
                )
            else:
                await execute_sql(
                    cur,
                    f"DROP INDEX {qualified(run_chat_schema, 'ix_model_financial_pending')}",
                )
        with pytest.raises(SchemaNotReadyError):
            await verify_agent_schema(c, run_chat_schema)


async def test_local_commit_ack_loss_recovers_without_any_second_owner_request(
    sealed: Fixture,
) -> None:
    class CommittedAckLost(PostgresModelFinancialRepository):
        async def record_ack(
            self, work: ModelFinancialWork, value: ModelSettlementReceipt
        ) -> ModelSettlementReceipt:
            await super().record_ack(work, value)
            raise TimeoutError("injected lost local commit acknowledgement")

    owner = Owner()
    with pytest.raises(TimeoutError, match="lost local commit"):
        await coordinator(CommittedAckLost(sealed.url, sealed.schema), owner).reconcile(
            sealed.tenant, sealed.attempt
        )
    before = await sealed.facts()
    calls = (len(owner.recoveries), len(owner.notifications))
    assert (
        await coordinator(repository(sealed), owner).reconcile(
            sealed.tenant, sealed.attempt
        )
    ) is not None
    assert (
        calls == (len(owner.recoveries), len(owner.notifications))
        and await sealed.facts() == before
    )


async def test_cancel_after_owner_commit_keeps_original_event_for_next_process(
    sealed: Fixture,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowOwner(Owner):
        async def settle(
            self,
            *,
            tenant_id: str,
            admission_ref: str,
            command: ModelSettlementCommand,
            key: str,
            request_id: str,
        ) -> ModelSettlementReceipt:
            result = await super().settle(
                tenant_id=tenant_id,
                admission_ref=admission_ref,
                command=command,
                key=key,
                request_id=request_id,
            )
            entered.set()
            await release.wait()
            return result

    owner = SlowOwner()
    task = asyncio.create_task(
        coordinator(repository(sealed), owner).reconcile(sealed.tenant, sealed.attempt)
    )
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    value = await coordinator(repository(sealed), owner).reconcile(
        sealed.tenant, sealed.attempt
    )
    assert (
        value is not None
        and len(owner.receipts) == 1
        and owner.notifications[0] == owner.notifications[1]
    )


class PendingOwner(Owner):
    """Named financial port: unknown actual usage stays pending, never captured."""

    async def settle(
        self,
        *,
        tenant_id: str,
        admission_ref: str,
        command: ModelSettlementCommand,
        key: str,
        request_id: str,
    ) -> ModelSettlementReceipt:
        value = await super().settle(
            tenant_id=tenant_id,
            admission_ref=admission_ref,
            command=command,
            key=key,
            request_id=request_id,
        )
        pending = replace(
            value,
            status="pending_reconciliation",
            reason="unknown_usage_or_outcome",
            released_credits="0",
        )
        self.receipts[key] = pending
        return pending


async def core_facts(f: Fixture) -> list[list[dict[str, object]]]:
    """All original execution/sealed facts, excluding only new delivery metadata."""
    result: list[list[dict[str, object]]] = []
    async with connect_pg(f.url) as c, c.cursor() as cur:
        for table in [
            "kokoro_agent_run",
            "kokoro_agent_model_call_attempt",
            "kokoro_agent_model_usage_evidence",
        ]:
            await execute_sql(
                cur,
                f"SELECT (to_jsonb(t)-'financial_admission_ref'-'financial_ack_bytes'-'financial_ack_digest'-'financial_acked_at')::text AS fact FROM {qualified(f.schema, table)} t ORDER BY 1",
            )
            result.append(await fetch_all(cur))
    return result


@pytest.mark.parametrize("sealed", ["history"], indirect=True)
async def test_historical_unacked_events_are_delivered_in_order_and_old_pending_ack_stays_immutable(
    sealed: Fixture,
) -> None:
    r, owner = repository(sealed), PendingOwner()
    original = await core_facts(sealed)
    first_work = await r.next_work(sealed.tenant, sealed.attempt)
    assert first_work is not None and first_work.evidence.revision == "1"
    first = await coordinator(r, owner).reconcile(sealed.tenant, sealed.attempt)
    assert first is not None and first.status == "pending_reconciliation"
    second_work = await r.next_work(sealed.tenant, sealed.attempt)
    assert second_work is not None and second_work.evidence.revision == "2"
    assert second_work.evidence.event_id != first_work.evidence.event_id
    second = await coordinator(repository(sealed), owner).reconcile(
        sealed.tenant, sealed.attempt
    )
    assert second is not None and second.status == "pending_reconciliation"
    assert [v.revision for v, _ in owner.notifications] == ["1", "2"]
    assert all(
        key == "agent-model-settle:" + v.event_id for v, key in owner.notifications
    )
    assert not owner.recoveries and not await r.list_pending()
    before = await sealed.facts()
    # Late replay of revision 1 cannot overwrite or inherit revision 2's ACK.
    assert await r.record_ack(first_work, first) == first
    assert await r.latest_ack(sealed.tenant, sealed.attempt) == second
    assert (
        await coordinator(r, owner).reconcile(sealed.tenant, sealed.attempt) == second
    )
    assert len(owner.notifications) == 2 and await sealed.facts() == before
    assert await core_facts(sealed) == original


@pytest.mark.parametrize("sealed", ["history"], indirect=True)
async def test_successor_ack_cannot_skip_unacked_predecessor(sealed: Fixture) -> None:
    r, owner = repository(sealed), PendingOwner()
    work = await r.next_work(sealed.tenant, sealed.attempt)
    assert work is not None
    async with connect_pg(sealed.url) as c, c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT event_id,evidence_digest FROM {qualified(sealed.schema, 'kokoro_agent_model_usage_evidence')} WHERE revision=2",
        )
        row = await fetch_one(cur)
        assert row is not None
    successor = replace(
        work,
        evidence=replace(
            work.evidence,
            event_id=str(row["event_id"]),
            digest=str(row["evidence_digest"]),
            revision="2",
        ),
    )
    await r.record_admission(work, ADMISSION)
    value = await owner.settle(
        tenant_id=sealed.tenant,
        admission_ref=ADMISSION,
        command=successor.evidence,
        key="named-invalid-order-port",
        request_id="qa",
    )
    before = await sealed.facts()
    with pytest.raises(ModelFinancialConflict):
        await r.record_ack(successor, value)
    assert await sealed.facts() == before


@pytest.mark.parametrize("sealed", ["history"], indirect=True)
async def test_terminal_ack_rejects_later_event_before_network_and_preserves_sealed_history(
    sealed: Fixture,
) -> None:
    r = repository(sealed)
    # A deliberately terminal named port tests the journal guard, not real Billing pricing.
    owner = Owner()
    first = await coordinator(r, owner).reconcile(sealed.tenant, sealed.attempt)
    assert first is not None and first.status == "released"
    before = await sealed.facts()
    with pytest.raises(ModelFinancialConflict):
        await coordinator(r, owner).reconcile(sealed.tenant, sealed.attempt)
    assert len(owner.notifications) == 1 and await sealed.facts() == before
    assert await r.list_pending() == [(sealed.tenant, sealed.attempt)]


@pytest.mark.parametrize("termination", ["deadline", "cancel"])
async def test_sql_wait_is_bounded_and_never_holds_external_io(
    sealed: Fixture, termination: str
) -> None:
    owner = Owner()
    before = await sealed.facts()
    async with connect_pg(sealed.url) as c, c.transaction(), c.cursor() as cur:
        await execute_sql(
            cur,
            f"SELECT attempt_id FROM {qualified(sealed.schema, 'kokoro_agent_model_call_attempt')} WHERE attempt_id=%s FOR UPDATE",
            (sealed.attempt,),
        )
        task = asyncio.create_task(
            coordinator(
                PostgresModelFinancialRepository(
                    sealed.url, sealed.schema, timeout_s=0.1
                ),
                owner,
            ).reconcile(sealed.tenant, sealed.attempt)
        )
        if termination == "cancel":
            await asyncio.sleep(0.02)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(TimeoutError):
                await task
    assert (
        not owner.recoveries
        and not owner.notifications
        and await sealed.facts() == before
    )


@pytest.mark.parametrize("sealed", ["history"], indirect=True)
async def test_pending_scan_keyset_boundary_deduplicates_events_and_preserves_rows(
    sealed: Fixture,
) -> None:
    r = repository(sealed)
    before = await sealed.facts()
    key = (sealed.tenant, sealed.attempt)
    assert await r.list_pending(limit=1) == [key]
    assert await r.list_pending(limit=1, after=key) == []
    assert await r.list_pending(
        limit=1, after=(sealed.tenant, "00000000-0000-0000-0000-000000000000")
    ) == [key]
    assert await r.list_pending(limit=1, after=("tenant-0", sealed.attempt)) == [key]
    assert await r.list_pending(limit=1, after=("tenant-z", sealed.attempt)) == []
    assert await sealed.facts() == before
