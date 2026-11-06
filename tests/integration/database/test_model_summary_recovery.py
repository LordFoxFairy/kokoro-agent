"""Real PG summary/usage/token transactions; named financial owner, no model HTTP."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any
from uuid import uuid4

import pytest
import rfc8785
from support.model_http_mechanism import synthetic_final_digest

from kokoro_agent.clients.system import ModelConnection, ResolvedModel
from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_model_call_repository import (
    PostgresModelCallRepository,
)
from kokoro_agent.infrastructure.postgres_model_financial_repository import (
    PostgresModelFinancialRepository,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.model.call_artifacts import encode_planned_artifact
from kokoro_agent.model.call_models import (
    BeginModelCallAuthorization,
    GrantModelCallAuthorization,
    ModelCallAuthorityLost,
    ModelCallAuthorizationUnavailable,
    ModelCallIdentityConflict,
    ModelCallSettlementPending,
    PreparedModelCall,
    PrepareModelCall,
    ReadSummaryResult,
    SealModelCallObservation,
    StartModelCallDispatch,
)
from kokoro_agent.model.execution import AuthorizedModelCall, ModelCallExecutor
from kokoro_agent.model.financial_reconciliation import ModelFinancialReconciler
from kokoro_agent.model.settlement import ModelSettlementCommand, ModelSettlementReceipt
from kokoro_agent.model.summary_result import (
    SummaryCallBinding,
    SummarySealInput,
    decode_summary_result,
)
from kokoro_agent.model.usage_evidence_v2 import parse_evidence
from kokoro_agent.model.usage_observation import ObservedCount, PhysicalUsageObservation
from kokoro_agent.model.usage_producer import (
    actual_from_response,
    normalize_observed_usage,
)
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding


class _NeverAuthorizer:
    async def authorize(self, prepared: PreparedModelCall):
        raise AssertionError("prepared fixture must not authorize on replay")


class _FinancialPeer:
    def __init__(self, repository: PostgresModelCallRepository) -> None:
        self.repository = repository
        self.effects: dict[str, ModelSettlementReceipt] = {}
        self.calls = 0

    async def recover(self, **_kwargs: Any):
        raise AssertionError("summary replay never creates or recovers a new grant")

    async def settle(
        self,
        *,
        tenant_id: str,
        admission_ref: str,
        command: ModelSettlementCommand,
        key: str,
        request_id: str,
    ) -> ModelSettlementReceipt:
        del request_id
        self.calls += 1
        assert key == "agent-model-settle:" + command.event_id
        if key in self.effects:
            return self.effects[key]
        final = await self.repository.read_final_evidence(
            tenant_id, command.attempt_id, command.event_id
        )
        assert final is not None
        value = parse_evidence(final.evidence_bytes)
        known = (
            value.usage.state == "normalized"
            and value.usage.value.totals_state != "unknown"
        )
        receipt = ModelSettlementReceipt(
            admission_ref=admission_ref,
            **asdict(command),
            status="captured" if known else "pending_reconciliation",
            reason=None if known else "unknown_usage_or_outcome",
            captured_credits="0",
            released_credits="0",
            usage_event_ref=str(uuid4()) if known else None,
            settlement_ref=str(uuid4()) if known else None,
            updated_at=datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
        )
        self.effects[key] = receipt
        return receipt


@dataclass
class _Case:
    url: str
    schema: str
    repository: PostgresModelCallRepository
    executor: ModelCallExecutor
    financial: _FinancialPeer
    request: RunRequest
    lease: LeaseFence
    recipe: StaticRecipeBinding
    planned: PlannedBinding
    call: AuthorizedModelCall
    route: ResolvedModel
    summary: SummarySealInput

    def read(self, lease: LeaseFence | None = None) -> ReadSummaryResult:
        return ReadSummaryResult(
            request=self.request,
            lease=lease or self.lease,
            recipe=self.recipe,
            planned=self.planned,
            binding=self.summary.binding,
        )

    def observation(self, *, unknown: bool = False) -> PhysicalUsageObservation:
        counts = {
            "prompt_tokens": ObservedCount("reported", 19),
            "completion_tokens": ObservedCount("reported", 5),
            "total_tokens": ObservedCount("reported", 24),
            "cached_tokens": ObservedCount("missing"),
            "reasoning_tokens": ObservedCount("missing"),
        }
        return PhysicalUsageObservation(
            status="complete",
            reason=None,
            compression_integrity="validated",
            usage_presence="missing" if unknown else "object",
            counts=MappingProxyType(counts),
            projection_digest="a" * 64,
            reported_model=self.route.provider_model_name,
            provider_response_id="summary-fixture-response",
        )

    async def seal(self, *, unknown: bool = False) -> ModelSettlementReceipt:
        return await self.executor.seal(
            call=self.call,
            route=self.route,
            observation=self.observation(unknown=unknown),
            outcome="completed",
            summary=self.summary,
        )

    async def facts(self) -> dict[str, Any]:
        async with connect_pg(self.url) as connection, connection.cursor() as cursor:
            await execute_sql(
                cursor,
                f"SELECT a.*,a.xmin::text AS attempt_xmin,r.token_total,r.xmin::text AS run_xmin,(SELECT count(*) FROM {qualified(self.schema, 'kokoro_agent_model_usage_evidence')} e WHERE e.attempt_id=a.attempt_id) AS evidence_count FROM {qualified(self.schema, 'kokoro_agent_model_call_attempt')} a JOIN {qualified(self.schema, 'kokoro_agent_run')} r ON r.run_id=a.run_id WHERE a.attempt_id=%s",
                (self.call.prepared.attempt_id,),
            )
            row = await fetch_one(cursor)
        assert row is not None
        return row


@pytest.fixture
async def summary_case(run_chat_database_url: str, run_chat_schema: str) -> _Case:
    identity = IdentityRef(kind="user", opaque_ref="actor")
    request = RunRequest(
        kind="run.request",
        run_id=str(uuid4()),
        session_id=str(uuid4()),
        feature_key="chat",
        project_ref=None,
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant-summary",
            actor=identity,
            subject=identity,
            identity_assertion_ref="summary-fixture",
        ),
        input=RunInput(message_id="message", content="controlled summary transaction"),
    )
    recipe_bytes = rfc8785.dumps(
        {
            "domain_tag": "kokoro-agent:assembly-recipe:1",
            "feature": {
                "key": "chat",
                "entry_agent": "general",
                "handoffs": [],
                "agents": [{"key": "general", "subagents": []}],
            },
        }
    )
    recipe = StaticRecipeBinding(
        canonical_bytes=recipe_bytes,
        fingerprint=hashlib.sha256(recipe_bytes).hexdigest(),
    )
    lease = LeaseFence(owner="summary-original-worker", generation=1)
    admission = str(uuid4())
    planned = PlannedBinding(
        binding_version="2.0.0",
        model="summary-model",
        provider="summary-provider",
        revision_id=str(uuid4()),
        provider_model_name="provider-summary",
        request_model="route-summary",
        feature_key="chat",
        revision="1",
        generation="1",
        tenant_generation="1",
        digest="a" * 64,
        label=None,
    )
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            f"INSERT INTO {qualified(run_chat_schema, 'kokoro_agent_run')} (run_id,tenant_id,request_json,owner,lease_generation,lease_expires_at,assembly_recipe_bytes,assembly_recipe_fingerprint) VALUES (%s,%s,%s,%s,1,clock_timestamp()+interval '5 minutes',%s,%s)",
            (
                request.run_id,
                request.execution_identity.tenant_ref,
                request.model_dump_json(),
                lease.owner,
                recipe_bytes,
                recipe.fingerprint,
            ),
        )
    repository = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    prepared = await repository.prepare(
        PrepareModelCall(
            request=request,
            lease=lease,
            call_id=str(uuid4()),
            attempt_id=str(uuid4()),
            planned=planned,
            peer_path=("general",),
            recipe=recipe,
            final_request_binding_version=1,
            final_request_digest=synthetic_final_digest(planned, "summary fixture"),
        )
    )
    pending = await repository.begin_authorization(
        BeginModelCallAuthorization(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            request_ref="summary-fixture-admit",
        )
    )
    grant = await repository.grant_authorization(
        GrantModelCallAuthorization(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            expected_authorization_digest=pending.authorization_digest,
            request_ref=pending.request_ref,
            admission_ref=admission,
        )
    )
    dispatch = await repository.start_dispatch(
        StartModelCallDispatch(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            authorization_digest=grant.authorization_digest,
            request_ref=grant.request_ref,
            admission_ref=admission,
        )
    )
    financial = _FinancialPeer(repository)
    executor = ModelCallExecutor(
        repository=repository,
        authorizer=_NeverAuthorizer(),
        financial=ModelFinancialReconciler(
            repository=PostgresModelFinancialRepository(
                run_chat_database_url, run_chat_schema
            ),
            client=financial,
        ),
    )
    binding = SummaryCallBinding(
        tenant_ref=prepared.tenant_id,
        run_id=prepared.run_id,
        call_id=prepared.call_id,
        attempt_id=prepared.attempt_id,
        peer_path=("general",),
        checkpoint_id="native-checkpoint",
        checkpoint_ns="",
        task_id="native-task",
        recipe_digest=recipe.fingerprint,
        planned_digest=encode_planned_artifact(planned)[1],
        prompt_digest="b" * 64,
    )
    route = ResolvedModel(
        model_id=planned.model,
        provider_id=planned.provider,
        revision_id=planned.revision_id,
        revision=1,
        digest=planned.digest,
        generation="1",
        tenant_generation="1",
        provider_model_name=planned.provider_model_name,
        request_model=planned.request_model,
        feature_key="chat",
        label_key="default",
        context_window=4096,
        max_output_tokens=128,
        connection=ModelConnection(
            protocol="openai_chat_completions",
            connection_type="direct",
            base_url="https://summary.invalid/v1",
            secret_handle_ref="summary-fixture",
        ),
    )
    return _Case(
        run_chat_database_url,
        run_chat_schema,
        repository,
        executor,
        financial,
        request,
        lease,
        recipe,
        planned,
        AuthorizedModelCall(prepared=prepared, authorization=grant, dispatch=dispatch),
        route,
        SummarySealInput(binding=binding, text=" original summary result \n"),
    )


async def test_summary_first_seal_and_exact_replay_count_once(
    summary_case: _Case,
) -> None:
    case = summary_case
    await case.seal()
    original = await case.facts()
    assert original["token_total"] == 24 and original["evidence_count"] == 1
    result = decode_summary_result(
        original["summary_result_bytes"], original["summary_result_digest"]
    )
    assert result.message.content == case.summary.text
    assert result.usage.known_total == 24
    await case.seal()
    assert await case.facts() == original
    replay = await case.executor.replay_summary(case.read())
    assert replay is not None and replay.state == "captured"
    assert replay.token_total == 24 and replay.result == result
    assert len(case.financial.effects) == case.financial.calls == 1


async def test_summary_new_lease_reads_original_binding_without_prepare_or_dispatch(
    summary_case: _Case,
) -> None:
    case = summary_case
    await case.seal()
    original = await case.facts()
    new_lease = LeaseFence(owner="summary-recovery-worker", generation=2)
    async with connect_pg(case.url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            f"UPDATE {qualified(case.schema, 'kokoro_agent_run')} SET owner=%s,lease_generation=2,lease_expires_at=clock_timestamp()+interval '5 minutes' WHERE run_id=%s",
            (new_lease.owner, case.request.run_id),
        )
    before_read = await case.facts()
    with pytest.raises(ModelCallAuthorityLost):
        await case.executor.replay_summary(case.read())
    replay = await case.executor.replay_summary(case.read(new_lease))
    assert replay is not None and replay.result.message.content == case.summary.text
    assert await case.facts() == before_read
    assert before_read["attempt_binding_bytes"] == original["attempt_binding_bytes"]
    assert before_read["lease_generation"] == 1
    assert case.financial.calls == 1


async def test_summary_transaction_rollback_has_no_result_evidence_or_token_increment(
    summary_case: _Case,
) -> None:
    case = summary_case
    async with connect_pg(case.url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            f"CREATE FUNCTION {qualified(case.schema, 'stop_summary_write')}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned summary rollback fixture'; END $$",
        )
        await execute_sql(
            cursor,
            f"CREATE TRIGGER owned_summary_stop BEFORE UPDATE OF summary_result_bytes ON {qualified(case.schema, 'kokoro_agent_model_call_attempt')} FOR EACH ROW EXECUTE FUNCTION {qualified(case.schema, 'stop_summary_write')}()",
        )
    with pytest.raises(Exception, match="owned summary rollback fixture"):
        await case.seal()
    facts = await case.facts()
    assert (
        facts["latest_revision"] == facts["token_total"] == facts["evidence_count"] == 0
    )
    assert facts["summary_result_bytes"] is facts["summary_result_digest"] is None
    assert case.financial.calls == 0


async def test_summary_commit_unknown_reconciles_original_event_then_replays(
    summary_case: _Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = summary_case
    original = case.repository.seal_observation

    async def lost_ack(command: SealModelCallObservation):
        await original(command)
        raise TimeoutError("owned commit receipt lost")

    monkeypatch.setattr(case.repository, "seal_observation", lost_ack)
    with pytest.raises(TimeoutError, match="owned commit receipt lost"):
        await case.seal()
    assert (await case.facts())["token_total"] == 24
    assert case.financial.calls == 0
    replay = await case.executor.replay_summary(case.read())
    assert replay is not None and replay.state == "captured"
    assert case.financial.calls == 1
    facts = await case.facts()
    assert facts["token_total"] == 24 and facts["evidence_count"] == 1


@pytest.mark.parametrize("mutation", ["digest", "prompt", "null"])
async def test_summary_corruption_binding_drift_or_old_null_never_dispatch(
    summary_case: _Case, mutation: str
) -> None:
    case = summary_case
    await case.seal()
    read = case.read()
    if mutation == "prompt":
        read = read.model_copy(
            update={
                "binding": read.binding.model_copy(update={"prompt_digest": "0" * 64})
            }
        )
    else:
        async with connect_pg(case.url) as connection, connection.cursor() as cursor:
            expression = (
                "summary_result_digest=repeat('0',64)"
                if mutation == "digest"
                else "summary_result_bytes=NULL,summary_result_digest=NULL"
            )
            await execute_sql(
                cursor,
                f"UPDATE {qualified(case.schema, 'kokoro_agent_model_call_attempt')} SET {expression} WHERE attempt_id=%s",
                (case.call.prepared.attempt_id,),
            )
    with pytest.raises((ModelCallAuthorizationUnavailable, ModelCallIdentityConflict)):
        await case.executor.replay_summary(read)
    assert case.financial.calls == 1
    assert (await case.facts())["token_total"] == 24


async def test_summary_unknown_first_usage_never_replays_or_counts_as_known_zero(
    summary_case: _Case,
) -> None:
    case = summary_case
    with pytest.raises(ModelCallSettlementPending):
        await case.seal(unknown=True)
    facts = await case.facts()
    value = decode_summary_result(
        facts["summary_result_bytes"], facts["summary_result_digest"]
    )
    assert value.usage.known_total is None
    assert facts["token_total"] == 0
    with pytest.raises(ModelCallSettlementPending):
        await case.executor.replay_summary(case.read())
    assert case.financial.calls == 1


async def test_summary_equal_concurrent_seals_keep_one_result_and_token_increment(
    summary_case: _Case,
) -> None:
    case = summary_case
    observation = case.observation()
    command = SealModelCallObservation(
        tenant_id=case.call.prepared.tenant_id,
        run_id=case.request.run_id,
        attempt_id=case.call.prepared.attempt_id,
        lease=case.lease,
        attempt_binding_digest=case.call.prepared.attempt_binding_digest,
        planned_digest=case.call.prepared.planned_digest,
        authorization_digest=case.call.authorization.authorization_digest,
        outcome="completed",
        actual=actual_from_response(
            route=case.route, prepared=case.call.prepared, observation=observation
        ),
        usage=normalize_observed_usage(observation),
        provider_response_id=observation.provider_response_id,
        summary=case.summary,
    )
    first, second = await asyncio.gather(
        case.repository.seal_observation(command),
        case.repository.seal_observation(command),
    )
    assert first.event_id == second.event_id and {first.replayed, second.replayed} == {
        False,
        True,
    }
    facts = await case.facts()
    assert facts["token_total"] == 24 and facts["evidence_count"] == 1
    changed = command.model_copy(
        update={"summary": case.summary.model_copy(update={"text": "different result"})}
    )
    with pytest.raises(ModelCallIdentityConflict):
        await case.repository.seal_observation(changed)
    assert await case.facts() == facts


@pytest.mark.parametrize("mutation", ["terminal", "expired", "request", "route"])
async def test_summary_replay_requires_current_run_authority_and_original_route(
    summary_case: _Case,
    mutation: str,
) -> None:
    case = summary_case
    await case.seal()
    read = case.read()
    if mutation in {"terminal", "expired"}:
        expression = (
            "terminal=TRUE,interaction_phase='terminal',terminal_at=clock_timestamp()"
            if mutation == "terminal"
            else "lease_expires_at=clock_timestamp()-interval '1 second'"
        )
        async with connect_pg(case.url) as connection, connection.cursor() as cursor:
            await execute_sql(
                cursor,
                f"UPDATE {qualified(case.schema, 'kokoro_agent_run')} SET {expression} WHERE run_id=%s",
                (case.request.run_id,),
            )
    elif mutation == "request":
        read = read.model_copy(
            update={
                "request": case.request.model_copy(
                    update={
                        "input": case.request.input.model_copy(
                            update={"content": "changed request"}
                        )
                    }
                )
            }
        )
    else:
        read = read.model_copy(
            update={"planned": case.planned.model_copy(update={"revision": "2"})}
        )
    with pytest.raises((ModelCallAuthorityLost, ModelCallIdentityConflict)):
        await case.executor.replay_summary(read)
    assert case.financial.calls == 1


async def test_summary_wrong_financial_ack_digest_never_grants_replay(
    summary_case: _Case,
) -> None:
    from kokoro_agent.model.financial_receipt import ModelFinancialConflict

    case = summary_case
    await case.seal()
    async with connect_pg(case.url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            f"UPDATE {qualified(case.schema, 'kokoro_agent_model_usage_evidence')} SET financial_ack_digest=repeat('0',64) WHERE attempt_id=%s",
            (case.call.prepared.attempt_id,),
        )
    with pytest.raises(ModelFinancialConflict):
        await case.executor.replay_summary(case.read())
    assert case.financial.calls == 1


async def test_later_captured_ack_cannot_upgrade_unknown_first_usage_to_free_replay(
    summary_case: _Case,
) -> None:
    from dataclasses import replace

    from kokoro_agent.model.financial_receipt import encode_financial_receipt

    case = summary_case
    with pytest.raises(ModelCallSettlementPending):
        await case.seal(unknown=True)
    original = next(iter(case.financial.effects.values()))
    raw, digest = encode_financial_receipt(
        replace(
            original,
            status="captured",
            reason=None,
            usage_event_ref=str(uuid4()),
            settlement_ref=str(uuid4()),
        )
    )
    async with connect_pg(case.url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            f"UPDATE {qualified(case.schema, 'kokoro_agent_model_usage_evidence')} SET financial_ack_bytes=%s,financial_ack_digest=%s,financial_acked_at=clock_timestamp() WHERE attempt_id=%s",
            (raw, digest, case.call.prepared.attempt_id),
        )
    with pytest.raises(
        ModelCallAuthorizationUnavailable, match="original usage is not replayable"
    ):
        await case.executor.replay_summary(case.read())
    assert (await case.facts())["token_total"] == 0
    assert case.financial.calls == 1


async def test_reported_summary_total_counts_once_while_missing_inclusive_counts_stay_pending(
    summary_case: _Case,
) -> None:
    from dataclasses import replace

    case = summary_case
    counts = {
        "prompt_tokens": ObservedCount("missing"),
        "completion_tokens": ObservedCount("missing"),
        "total_tokens": ObservedCount("reported", 24),
        "cached_tokens": ObservedCount("missing"),
        "reasoning_tokens": ObservedCount("missing"),
    }
    observation = replace(case.observation(), counts=MappingProxyType(counts))
    with pytest.raises(ModelCallSettlementPending):
        await case.executor.seal(
            call=case.call,
            route=case.route,
            observation=observation,
            outcome="completed",
            summary=case.summary,
        )
    facts = await case.facts()
    assert facts["token_total"] == 24
    capsule = decode_summary_result(
        facts["summary_result_bytes"], facts["summary_result_digest"]
    )
    assert capsule.usage.reported_total == 24 and capsule.usage.known_total is None
    with pytest.raises(ModelCallSettlementPending):
        await case.executor.replay_summary(case.read())
    assert (await case.facts())["token_total"] == 24
