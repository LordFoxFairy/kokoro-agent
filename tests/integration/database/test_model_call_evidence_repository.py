"""Real-PostgreSQL gates for the approved A1 schema and prepare repository.

The production prepare seam binds trusted durable Run/recipe facts to the
canonical attempt table and covers immutable replay, ordinal allocation, lease
authority, peer scope, and same-transaction Run completeness. Root's isolated
PostgreSQL run passed all 56 cases here; the full database gate passed 267 cases.
Root remains the only runner and owner of PostgreSQL resources.

Authorization begin/grant and the one-time dispatch permit are implemented by
this slice. Evidence/finalizer and runtime-provider wiring remain absent. Real
lost-COMMIT-ACK recovery and the complete not-dispatched knowledge matrix remain
open rather than represented by mocks, skips, or import failures.
"""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4
import hashlib
import json

import pytest
import rfc8785
from psycopg.errors import CheckViolation, UniqueViolation

from support.model_http_mechanism import synthetic_final_digest

from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.infrastructure.postgres_model_call_repository import (
    PostgresModelCallRepository,
)
from kokoro_agent.model.call_models import (
    AppendModelCallEvidence,
    BeginModelCallAuthorization,
    GrantModelCallAuthorization,
    ModelCallAuthorityLost,
    ModelCallIdentityConflict,
    ModelCallInputInvalid,
    ModelCallOrdinalExhausted,
    PrepareModelCall,
    PreparedModelCall,
    StartModelCallDispatch,
)
from kokoro_agent.model.usage_evidence import parse_evidence
from kokoro_agent.protocol.control import (
    ExecutionIdentity,
    IdentityRef,
    RunInput,
    RunRequest,
)
from kokoro_agent.protocol.model_usage_generated import PlannedBinding
from kokoro_agent.agents.definition import Agent
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.config import AppConfig
from kokoro_agent.execution.runtime_profile_plan import (
    RuntimeAssemblyPolicy,
    prepare_feature,
)
from kokoro_agent.execution.runtime_profile_sources import production_manifest
from kokoro_agent.features.definition import Feature
from kokoro_agent.tools.toolbox import build_toolbox


_TARGET_TABLES = {
    "kokoro_agent_model_call_attempt",
    "kokoro_agent_model_usage_evidence",
}
_TARGET_RUN_COMPLETENESS_COLUMNS = {
    "model_attempt_count",
    "model_dispatched_attempt_count",
    "model_sealed_attempt_count",
    "model_unknown_attempt_count",
    "model_usage_completeness",
}
_CURRENT_RUN_CONTROL_COLUMNS = {
    "run_id",
    "tenant_id",
    "owner",
    "lease_generation",
    "lease_expires_at",
    "terminal",
    "token_total",
    "usage_input_total",
    "usage_output_total",
}
_DIGEST = "a" * 64
_OTHER_DIGEST = "b" * 64
_EVIDENCE_DOMAIN = b"kokoro-agent:model-usage-evidence:v1\n"


def _utc_millis(value: int) -> str:
    return (
        datetime.fromtimestamp(value / 1000, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _strict_completed_evidence(
    prepared: PreparedModelCall,
    authorization_bytes: bytes,
    dispatch_started_at_ms: int,
) -> tuple[str, bytes, str]:
    event_id = str(uuid4())
    observed_at = _utc_millis(dispatch_started_at_ms + 1)
    identity = json.loads(prepared.attempt_binding_bytes)
    identity["event_id"] = event_id
    document = {
        "artifact_version": "1.0.0",
        "identity": identity,
        "planned": json.loads(prepared.planned_bytes),
        "actual": {"state": "unknown", "reason": "not_observed"},
        "authorization_observation": json.loads(authorization_bytes),
        "execution": {
            "outcome": "completed",
            "seal": True,
            "prepared_at": _utc_millis(prepared.prepared_at_ms),
            "dispatch_started_at": _utc_millis(dispatch_started_at_ms),
        },
        "revision": "1",
        "predecessor": None,
        "observed_at": observed_at,
        "usage": {
            "state": "unresolved",
            "reason": "missing_usage",
            "raw_usage_digest": None,
        },
    }
    digest = hashlib.sha256(_EVIDENCE_DOMAIN + rfc8785.dumps(document)).hexdigest()
    document["digest"] = digest
    raw = rfc8785.dumps(document)
    parse_evidence(raw)
    return event_id, raw, digest


def _append_command(
    *, row: Mapping[str, object], lease: LeaseFence, evidence_bytes: bytes
) -> AppendModelCallEvidence:
    return AppendModelCallEvidence(
        tenant_id=_string(row["tenant_id"]),
        run_id=_string(row["run_id"]),
        attempt_id=str(row["attempt_id"]),
        lease=lease,
        attempt_binding_digest=_string(row["attempt_binding_digest"]),
        planned_digest=_string(row["planned_digest"]),
        authorization_digest=_string(row["authorization_digest"]),
        evidence_bytes=evidence_bytes,
    )


async def _append_evidence(
    repository: PostgresModelCallRepository, command: AppendModelCallEvidence
) -> object:
    return await repository.append_evidence(command)


def _strict_evidence(
    row: Mapping[str, object],
    *,
    outcome: str,
    revision: int,
    predecessor_event_id: str | None = None,
    predecessor_digest: str | None = None,
    normalized: bool = False,
    observed_offset_ms: int | None = None,
    seal: bool = True,
    noncanonical: bool = False,
) -> tuple[str, bytes, str]:
    event_id = str(uuid4())
    identity = json.loads(_raw_bytes(row["attempt_binding_bytes"]))
    identity["event_id"] = event_id
    prepared_at = row["prepared_at"]
    dispatch_started_at = row["dispatch_started_at"]
    assert isinstance(prepared_at, datetime)
    assert isinstance(dispatch_started_at, datetime)
    offset_ms = revision if observed_offset_ms is None else observed_offset_ms
    observed_at = dispatch_started_at.timestamp() + 0.001 * offset_ms
    usage: object
    if normalized:
        vectors = json.loads(
            (
                Path(__file__).resolve().parents[3] / "contract/usage/v1/vectors.json"
            ).read_text()
        )
        usage = json.loads(vectors["positive"][0]["json_utf8"])["usage"]
    else:
        usage = {
            "state": "unresolved",
            "reason": "incomplete_response",
            "raw_usage_digest": None,
        }
    document = {
        "artifact_version": "1.0.0",
        "identity": identity,
        "planned": json.loads(_raw_bytes(row["planned_bytes"])),
        "actual": {"state": "unknown", "reason": "not_observed"},
        "authorization_observation": json.loads(_raw_bytes(row["authorization_bytes"])),
        "execution": {
            "outcome": outcome,
            "seal": seal,
            "prepared_at": _utc_millis(int(prepared_at.timestamp() * 1000)),
            "dispatch_started_at": _utc_millis(
                int(dispatch_started_at.timestamp() * 1000)
            ),
        },
        "usage": usage,
        "revision": str(revision),
        "predecessor": (
            None
            if revision == 1
            else {
                "event_id": predecessor_event_id,
                "digest": predecessor_digest,
            }
        ),
        "observed_at": _utc_millis(int(observed_at * 1000)),
    }
    canonical_without_digest = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    digest = hashlib.sha256(_EVIDENCE_DOMAIN + canonical_without_digest).hexdigest()
    document["digest"] = digest
    raw = (
        json.dumps(document, indent=1, ensure_ascii=False).encode()
        if noncanonical
        else json.dumps(
            document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    )
    parse_evidence(raw)
    return event_id, raw, digest


def _raw_bytes(value: object) -> bytes:
    assert isinstance(value, bytes)
    return value


def _string(value: object) -> str:
    assert isinstance(value, str)
    return value


async def _wait_for_owned_run_lock_queue(
    cursor: object, *, blocker_pid: int, schema: str
) -> tuple[int, int]:
    for _ in range(100):
        await asyncio.sleep(0.01)
        await execute_sql(cursor, "SELECT pg_stat_clear_snapshot()")
        await execute_sql(
            cursor,
            """WITH RECURSIVE waiters AS (
                 SELECT pid FROM pg_stat_activity
                 WHERE datname=current_database()
                   AND pid<>pg_backend_pid() AND wait_event_type='Lock'
                   AND query LIKE %s AND position(%s in query)>0
               ), blocker_chain(waiter_pid,pid,path) AS (
                 SELECT pid,pid,ARRAY[pid] FROM waiters
                 UNION ALL
                 SELECT chain.waiter_pid,blocker.pid,chain.path || blocker.pid
                 FROM blocker_chain AS chain
                 CROSS JOIN LATERAL
                   unnest(pg_blocking_pids(chain.pid)) AS blocker(pid)
                 WHERE NOT blocker.pid=ANY(chain.path)
               )
               SELECT min(waiters.pid) AS first_pid,
                      max(waiters.pid) AS second_pid,
                      count(*) AS waiter_count,
                      count(*)=2 AND count(*) FILTER (
                        WHERE EXISTS (
                          SELECT 1 FROM blocker_chain AS chain
                          WHERE chain.waiter_pid=waiters.pid AND chain.pid=%s
                        )
                      )=2 AS owned_blocker
               FROM waiters""",
            ("SELECT owner,lease_generation,%", schema, blocker_pid),
        )
        waiting = await fetch_one(cursor)
        if waiting is not None:
            first_pid = waiting["first_pid"]
            second_pid = waiting["second_pid"]
            if (
                isinstance(first_pid, int)
                and isinstance(second_pid, int)
                and waiting["waiter_count"] == 2
                and waiting["owned_blocker"] is True
            ):
                return first_pid, second_pid
    pytest.fail("both repository connections did not enter the owned lock queue")


def _prepare_values() -> tuple[RunRequest, StaticRecipeBinding, PlannedBinding]:
    request = RunRequest(
        kind="run.request",
        run_id="run-prepare-a1",
        session_id="session-a1",
        feature_key="chat",
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant-a1",
            actor=IdentityRef(kind="user", opaque_ref="actor-a1"),
            subject=IdentityRef(kind="user", opaque_ref="subject-a1"),
            identity_assertion_ref="assertion-a1",
        ),
        input=RunInput(message_id="message-a1", content="hello"),
    )
    recipe_bytes = json.dumps(
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
        canonical_bytes=recipe_bytes,
        fingerprint=hashlib.sha256(recipe_bytes).hexdigest(),
    )
    planned = PlannedBinding(
        model="model-a1",
        provider="provider-a1",
        revision_id="revision-a1",
        provider_model_name="provider-model-a1",
        gateway_model_name="gateway-a1",
        feature_key="chat",
        revision="1",
        generation="1",
        tenant_generation="1",
        digest=_DIGEST,
        label=None,
    )
    return request, recipe, planned


def _real_recipe(
    feature: Feature, *, custom_subagents: str | None = None
) -> StaticRecipeBinding:
    config = AppConfig.from_env({})
    plan = prepare_feature(
        feature,
        runtime_policy=RuntimeAssemblyPolicy.from_settings(
            run_token_budget=17,
            recursion_limit=43,
            model=config.model,
            sandbox=config.sandbox,
        ),
        manifest=production_manifest(),
        toolbox=build_toolbox(fetch_allow_private=False, search=None),
        subagent_catalog=build_subagent_catalog(custom_subagents),
        delivery_available=False,
    )
    return StaticRecipeBinding(
        canonical_bytes=plan.recipe_bytes, fingerprint=plan.fingerprint
    )


async def _insert_leased_prepare_run(
    database_url: str,
    schema: str,
    request: RunRequest,
    recipe: StaticRecipeBinding,
) -> None:
    async with connect_pg(database_url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            """INSERT INTO {} (
                run_id,tenant_id,request_json,assembly_recipe_bytes,
                assembly_recipe_fingerprint,owner,lease_generation,lease_expires_at
            ) VALUES (%s,%s,%s,%s,%s,'worker-a1',1,
                      clock_timestamp() + interval '5 minutes')""".format(
                qualified(schema, "kokoro_agent_run")
            ),
            (
                request.run_id,
                request.execution_identity.tenant_ref,
                request.model_dump_json(),
                recipe.canonical_bytes,
                recipe.fingerprint,
            ),
        )


async def _base_tables(database_url: str, schema: str) -> set[str]:
    async with connect_pg(database_url) as connection, connection.cursor() as cursor:
        await cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (schema,),
        )
        return {str(row["table_name"]) for row in await cursor.fetchall()}


async def _columns(database_url: str, schema: str, table: str) -> set[str]:
    async with connect_pg(database_url) as connection, connection.cursor() as cursor:
        await cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return {str(row["column_name"]) for row in await cursor.fetchall()}


def _missing(required: Collection[str], actual: Collection[str]) -> set[str]:
    return set(required).difference(actual)


async def test_fresh_install_contains_a1_attempt_and_evidence_tables(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    """The fresh installer owns both approved A1 tables."""
    tables = await _base_tables(run_chat_database_url, run_chat_schema)

    assert not _missing(_TARGET_TABLES, tables), (
        "A1 canonical tables are absent from the fresh installed owner schema: "
        f"{sorted(_missing(_TARGET_TABLES, tables))}"
    )


async def test_fresh_install_contains_a1_run_completeness_projection(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    """The fresh Run table exposes the closed A1 completeness projection."""
    columns = await _columns(run_chat_database_url, run_chat_schema, "kokoro_agent_run")

    assert not _missing(_TARGET_RUN_COMPLETENESS_COLUMNS, columns), (
        "A1 Run completeness columns are absent from the fresh canonical schema: "
        f"{sorted(_missing(_TARGET_RUN_COMPLETENESS_COLUMNS, columns))}"
    )


async def test_fresh_install_preserves_existing_run_fence_and_budget_columns(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    """Positive control: REDs ran against the formal current Run table."""
    columns = await _columns(run_chat_database_url, run_chat_schema, "kokoro_agent_run")

    assert not _missing(_CURRENT_RUN_CONTROL_COLUMNS, columns), (
        "fresh installer did not provide the existing Run authority/budget baseline: "
        f"{sorted(_missing(_CURRENT_RUN_CONTROL_COLUMNS, columns))}"
    )


async def test_prepare_freezes_trusted_identity_and_exact_replay(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    request, recipe, planned = _prepare_values()
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    repository = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    command = PrepareModelCall(
        final_request_binding_version=1,
        final_request_digest=synthetic_final_digest(planned, request.input.content),
        request=request,
        lease=LeaseFence(owner="worker-a1", generation=1),
        call_id=str(uuid4()),
        attempt_id=str(uuid4()),
        planned=planned,
        peer_path=("assistant",),
        recipe=recipe,
    )
    created = await repository.prepare(command)
    replayed = await repository.prepare(command)
    assert created.attempt_ordinal == 1
    assert replayed == created.model_copy(update={"replayed": True})
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT model_attempt_count,model_dispatched_attempt_count,
                      model_sealed_attempt_count,model_unknown_attempt_count,
                      model_usage_completeness FROM {} WHERE run_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (request.run_id,),
        )
        assert await cursor.fetchone() == {
            "model_attempt_count": 1,
            "model_dispatched_attempt_count": 0,
            "model_sealed_attempt_count": 0,
            "model_unknown_attempt_count": 0,
            "model_usage_completeness": "unknown",
        }
        await execute_sql(
            cursor,
            """UPDATE {} SET authorization_bytes=%s,authorization_digest=%s,
                      authorization_state='granted',status='authorized'
               WHERE attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (b'{"state":"granted"}', _DIGEST, command.attempt_id),
        )
    advanced_replay = await repository.prepare(command)
    assert advanced_replay == created.model_copy(update={"replayed": True})


async def _prepared_attempt(
    database_url: str, schema: str
) -> tuple[PostgresModelCallRepository, PrepareModelCall, PreparedModelCall]:
    request, recipe, planned = _prepare_values()
    await _insert_leased_prepare_run(database_url, schema, request, recipe)
    repository = PostgresModelCallRepository(database_url, schema)
    command = PrepareModelCall(
        final_request_binding_version=1,
        final_request_digest=synthetic_final_digest(planned, request.input.content),
        request=request,
        lease=LeaseFence(owner="worker-a1", generation=1),
        call_id=str(uuid4()),
        attempt_id=str(uuid4()),
        planned=planned,
        peer_path=("assistant",),
        recipe=recipe,
    )
    return repository, command, await repository.prepare(command)


async def _authorized_attempt(
    database_url: str, schema: str
) -> tuple[PostgresModelCallRepository, StartModelCallDispatch]:
    repository, prepare, prepared = await _prepared_attempt(database_url, schema)
    pending = await repository.begin_authorization(
        BeginModelCallAuthorization(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=prepare.lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            request_ref="billing-operation-a1",
        )
    )
    granted = await repository.grant_authorization(
        GrantModelCallAuthorization(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=prepare.lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            expected_authorization_digest=pending.authorization_digest,
            request_ref=pending.request_ref,
            admission_ref="billing-admission-a1",
        )
    )
    return repository, StartModelCallDispatch(
        tenant_id=prepared.tenant_id,
        run_id=prepared.run_id,
        attempt_id=prepared.attempt_id,
        lease=prepare.lease,
        attempt_binding_digest=prepared.attempt_binding_digest,
        planned_digest=prepared.planned_digest,
        authorization_digest=granted.authorization_digest,
        request_ref=granted.request_ref,
        admission_ref=granted.admission_ref,
    )


async def _dispatched_attempt_row(
    database_url: str, schema: str
) -> tuple[PostgresModelCallRepository, StartModelCallDispatch, Mapping[str, object]]:
    repository, dispatch = await _authorized_attempt(database_url, schema)
    await repository.start_dispatch(dispatch)
    async with connect_pg(database_url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            "SELECT * FROM {} WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s".format(
                qualified(schema, "kokoro_agent_model_call_attempt")
            ),
            (dispatch.tenant_id, dispatch.run_id, dispatch.attempt_id),
        )
        row = await cursor.fetchone()
        assert row is not None
        return repository, dispatch, row


async def test_authorization_begin_freezes_observation_ref_and_exact_replay(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, prepare, prepared = await _prepared_attempt(
        run_chat_database_url, run_chat_schema
    )
    command = BeginModelCallAuthorization(
        tenant_id=prepared.tenant_id,
        run_id=prepared.run_id,
        attempt_id=prepared.attempt_id,
        lease=prepare.lease,
        attempt_binding_digest=prepared.attempt_binding_digest,
        planned_digest=prepared.planned_digest,
        request_ref="billing-operation-a1",
    )

    created = await repository.begin_authorization(command)
    replayed = await repository.begin_authorization(command)

    expected_bytes = (
        b'{"request_ref":"billing-operation-a1","state":"requested_unknown"}'
    )
    assert created.authorization_bytes == expected_bytes
    assert created.authorization_digest == hashlib.sha256(expected_bytes).hexdigest()
    assert created.newly_requested is True
    assert replayed == created.model_copy(update={"newly_requested": False})
    with pytest.raises(ModelCallIdentityConflict):
        await repository.begin_authorization(
            command.model_copy(update={"request_ref": "billing-operation-other"})
        )

    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT status,authorization_state,authorization_bytes,
                      authorization_digest,dispatch_started_at
               FROM {} WHERE attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (prepared.attempt_id,),
        )
        assert await cursor.fetchone() == {
            "status": "authorization_pending",
            "authorization_state": "requested_unknown",
            "authorization_bytes": expected_bytes,
            "authorization_digest": created.authorization_digest,
            "dispatch_started_at": None,
        }


async def test_authorization_grant_freezes_refs_and_replays_immutable_view(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, prepare, prepared = await _prepared_attempt(
        run_chat_database_url, run_chat_schema
    )
    pending = await repository.begin_authorization(
        BeginModelCallAuthorization(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=prepare.lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            request_ref="billing-operation-a1",
        )
    )
    command = GrantModelCallAuthorization(
        tenant_id=prepared.tenant_id,
        run_id=prepared.run_id,
        attempt_id=prepared.attempt_id,
        lease=prepare.lease,
        attempt_binding_digest=prepared.attempt_binding_digest,
        planned_digest=prepared.planned_digest,
        expected_authorization_digest=pending.authorization_digest,
        request_ref=pending.request_ref,
        admission_ref="billing-admission-a1",
    )

    with pytest.raises(ModelCallIdentityConflict):
        await repository.grant_authorization(
            command.model_copy(update={"request_ref": "billing-operation-other"})
        )
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "SELECT status,authorization_bytes,authorization_digest FROM {} WHERE attempt_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (prepared.attempt_id,),
        )
        assert await cursor.fetchone() == {
            "status": "authorization_pending",
            "authorization_bytes": pending.authorization_bytes,
            "authorization_digest": pending.authorization_digest,
        }

    created = await repository.grant_authorization(command)
    replayed = await repository.grant_authorization(command)

    expected_bytes = (
        b'{"admission_ref":"billing-admission-a1",'
        b'"request_ref":"billing-operation-a1","state":"granted"}'
    )
    assert created.authorization_bytes == expected_bytes
    assert created.authorization_digest == hashlib.sha256(expected_bytes).hexdigest()
    assert created.newly_recorded is True
    assert replayed == created.model_copy(update={"newly_recorded": False})
    with pytest.raises(ModelCallIdentityConflict):
        await repository.grant_authorization(
            command.model_copy(update={"admission_ref": "billing-admission-other"})
        )


async def test_advanced_begin_rejects_noncanonical_grant_bytes_with_matching_digest(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, dispatch = await _authorized_attempt(
        run_chat_database_url, run_chat_schema
    )
    semantic_grant = {
        "state": "granted",
        "request_ref": dispatch.request_ref,
        "admission_ref": dispatch.admission_ref,
    }
    corrupt_bytes = json.dumps(semantic_grant, indent=1).encode()
    corrupt_digest = hashlib.sha256(corrupt_bytes).hexdigest()
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """UPDATE {} SET authorization_bytes=%s,authorization_digest=%s
               WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (
                corrupt_bytes,
                corrupt_digest,
                dispatch.tenant_id,
                dispatch.run_id,
                dispatch.attempt_id,
            ),
        )

    begin = BeginModelCallAuthorization(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        attempt_id=dispatch.attempt_id,
        lease=dispatch.lease,
        attempt_binding_digest=dispatch.attempt_binding_digest,
        planned_digest=dispatch.planned_digest,
        request_ref=dispatch.request_ref,
    )
    with pytest.raises(ModelCallIdentityConflict):
        await repository.begin_authorization(begin)

    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT status,authorization_state,authorization_bytes,
                      authorization_digest,dispatch_started_at
               FROM {} WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (dispatch.tenant_id, dispatch.run_id, dispatch.attempt_id),
        )
        assert await cursor.fetchone() == {
            "status": "authorized",
            "authorization_state": "granted",
            "authorization_bytes": corrupt_bytes,
            "authorization_digest": corrupt_digest,
            "dispatch_started_at": None,
        }


@pytest.mark.parametrize(
    ("change", "error"),
    [
        (
            {"lease": LeaseFence(owner="worker-a1", generation=2)},
            ModelCallAuthorityLost,
        ),
        ({"planned_digest": _OTHER_DIGEST}, ModelCallIdentityConflict),
    ],
)
async def test_begin_rejects_stale_authority_or_digest_without_write(
    run_chat_database_url: str,
    run_chat_schema: str,
    change: dict[str, object],
    error: type[Exception],
) -> None:
    repository, prepare, prepared = await _prepared_attempt(
        run_chat_database_url, run_chat_schema
    )
    command = BeginModelCallAuthorization(
        tenant_id=prepared.tenant_id,
        run_id=prepared.run_id,
        attempt_id=prepared.attempt_id,
        lease=prepare.lease,
        attempt_binding_digest=prepared.attempt_binding_digest,
        planned_digest=prepared.planned_digest,
        request_ref="billing-operation-a1",
    )
    with pytest.raises(error):
        await repository.begin_authorization(command.model_copy(update=change))
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "SELECT status,authorization_state FROM {} WHERE attempt_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (prepared.attempt_id,),
        )
        assert await cursor.fetchone() == {
            "status": "prepared",
            "authorization_state": "not_requested",
        }


async def test_begin_timeout_rolls_back_without_authorization_write(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    _, prepare, prepared = await _prepared_attempt(
        run_chat_database_url, run_chat_schema
    )
    repository = PostgresModelCallRepository(
        run_chat_database_url, run_chat_schema, total_timeout_seconds=0.1
    )
    command = BeginModelCallAuthorization(
        tenant_id=prepared.tenant_id,
        run_id=prepared.run_id,
        attempt_id=prepared.attempt_id,
        lease=prepare.lease,
        attempt_binding_digest=prepared.attempt_binding_digest,
        planned_digest=prepared.planned_digest,
        request_ref="billing-operation-a1",
    )
    async with connect_pg(run_chat_database_url) as blocker:
        async with blocker.transaction(), blocker.cursor() as cursor:
            await execute_sql(
                cursor,
                "SELECT run_id FROM {} WHERE tenant_id=%s AND run_id=%s FOR UPDATE".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
                (prepared.tenant_id, prepared.run_id),
            )
            with pytest.raises(TimeoutError):
                await repository.begin_authorization(command)
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "SELECT status,authorization_state FROM {} WHERE attempt_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (prepared.attempt_id,),
        )
        assert await cursor.fetchone() == {
            "status": "prepared",
            "authorization_state": "not_requested",
        }


async def test_dispatch_permit_is_true_only_for_first_transition_and_replay_is_false(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, prepare, prepared = await _prepared_attempt(
        run_chat_database_url, run_chat_schema
    )
    pending = await repository.begin_authorization(
        BeginModelCallAuthorization(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=prepare.lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            request_ref="billing-operation-a1",
        )
    )
    granted = await repository.grant_authorization(
        GrantModelCallAuthorization(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=prepare.lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            expected_authorization_digest=pending.authorization_digest,
            request_ref=pending.request_ref,
            admission_ref="billing-admission-a1",
        )
    )
    command = StartModelCallDispatch(
        tenant_id=prepared.tenant_id,
        run_id=prepared.run_id,
        attempt_id=prepared.attempt_id,
        lease=prepare.lease,
        attempt_binding_digest=prepared.attempt_binding_digest,
        planned_digest=prepared.planned_digest,
        authorization_digest=granted.authorization_digest,
        request_ref=granted.request_ref,
        admission_ref=granted.admission_ref,
    )
    begin = BeginModelCallAuthorization(
        tenant_id=prepared.tenant_id,
        run_id=prepared.run_id,
        attempt_id=prepared.attempt_id,
        lease=prepare.lease,
        attempt_binding_digest=prepared.attempt_binding_digest,
        planned_digest=prepared.planned_digest,
        request_ref=granted.request_ref,
    )

    authorized_begin_replay = await repository.begin_authorization(begin)
    assert authorized_begin_replay.newly_requested is False
    assert authorized_begin_replay.authorization_bytes == granted.authorization_bytes
    assert authorized_begin_replay.authorization_digest == granted.authorization_digest

    created = await repository.start_dispatch(command)
    replayed = await repository.start_dispatch(command)
    dispatch_begin_replay = await repository.begin_authorization(begin)

    assert created.newly_started is True
    assert replayed == created.model_copy(update={"newly_started": False})
    assert replayed.dispatch_started_at_ms == created.dispatch_started_at_ms
    assert dispatch_begin_replay == authorized_begin_replay
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        event_id, evidence_bytes, evidence_digest = _strict_completed_evidence(
            prepared, granted.authorization_bytes, created.dispatch_started_at_ms
        )
        await execute_sql(
            cursor,
            """INSERT INTO {} (event_id,tenant_id,run_id,attempt_id,call_id,
                   revision,evidence_bytes,evidence_digest,execution_outcome,
                   usage_state,totals_state,classification_state,sealed,observed_at)
               VALUES (%s,%s,%s,%s,%s,1,%s,%s,'completed','unresolved',
                       'unknown','unknown',TRUE,clock_timestamp())""".format(
                qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
            ),
            (
                event_id,
                prepared.tenant_id,
                prepared.run_id,
                prepared.attempt_id,
                prepared.call_id,
                evidence_bytes,
                evidence_digest,
            ),
        )
        await execute_sql(
            cursor,
            """UPDATE {} SET status='observed',sealed_at=clock_timestamp(),
                      latest_revision=1,latest_event_id=%s,latest_evidence_digest=%s
               WHERE attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (event_id, evidence_digest, prepared.attempt_id),
        )

    advanced_replay = await repository.start_dispatch(command)
    observed_begin_replay = await repository.begin_authorization(begin)
    assert advanced_replay == created.model_copy(update={"newly_started": False})
    assert observed_begin_replay == authorized_begin_replay

    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT status,authorization_state,dispatch_started_at,sealed_at,
                      latest_revision,latest_event_id,latest_evidence_digest,
                      model_dispatched_attempt_count
               FROM {} AS attempt JOIN {} AS run USING (run_id)
               WHERE attempt.attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt"),
                qualified(run_chat_schema, "kokoro_agent_run"),
            ),
            (prepared.attempt_id,),
        )
        row = await cursor.fetchone()
        assert row is not None
        assert row["status"] == "observed"
        assert row["authorization_state"] == "granted"
        assert row["dispatch_started_at"] is not None
        assert row["sealed_at"] is not None
        assert row["latest_revision"] == 1
        assert str(row["latest_event_id"]) == event_id
        assert row["latest_evidence_digest"] == evidence_digest
        assert row["model_dispatched_attempt_count"] == 1


async def test_dispatch_two_connections_queue_and_only_one_gets_new_authority(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, command = await _authorized_attempt(
        run_chat_database_url, run_chat_schema
    )
    async with connect_pg(run_chat_database_url) as blocker:
        async with blocker.transaction(), blocker.cursor() as cursor:
            await execute_sql(cursor, "SELECT pg_backend_pid() AS pid")
            blocker_row = await cursor.fetchone()
            assert blocker_row is not None
            blocker_pid = blocker_row["pid"]
            await execute_sql(
                cursor,
                "SELECT run_id FROM {} WHERE tenant_id=%s AND run_id=%s FOR UPDATE".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
                (command.tenant_id, command.run_id),
            )
            first = asyncio.create_task(repository.start_dispatch(command))
            second = asyncio.create_task(repository.start_dispatch(command))
            await _wait_for_owned_run_lock_queue(
                cursor,
                blocker_pid=blocker_pid,
                schema=run_chat_schema,
            )
        permits = await asyncio.gather(first, second)

    assert sorted(permit.newly_started for permit in permits) == [False, True]
    assert permits[0].dispatch_started_at_ms == permits[1].dispatch_started_at_ms
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT model_attempt_count,model_dispatched_attempt_count,
                      model_sealed_attempt_count,model_unknown_attempt_count,
                      model_usage_completeness
               FROM {} WHERE tenant_id=%s AND run_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (command.tenant_id, command.run_id),
        )
        assert await cursor.fetchone() == {
            "model_attempt_count": 1,
            "model_dispatched_attempt_count": 1,
            "model_sealed_attempt_count": 0,
            "model_unknown_attempt_count": 0,
            "model_usage_completeness": "unknown",
        }


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ({"tenant_id": "tenant-other"}, ModelCallAuthorityLost),
        (
            {"lease": LeaseFence(owner="worker-a1", generation=2)},
            ModelCallAuthorityLost,
        ),
        ({"attempt_binding_digest": _OTHER_DIGEST}, ModelCallIdentityConflict),
        ({"request_ref": "billing-operation-other"}, ModelCallIdentityConflict),
    ],
)
async def test_dispatch_rejects_untrusted_identity_without_write(
    run_chat_database_url: str,
    run_chat_schema: str,
    change: dict[str, object],
    error: type[Exception],
) -> None:
    repository, command = await _authorized_attempt(
        run_chat_database_url, run_chat_schema
    )
    with pytest.raises(error):
        await repository.start_dispatch(command.model_copy(update=change))
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "SELECT status,dispatch_started_at FROM {} WHERE attempt_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (command.attempt_id,),
        )
        assert await cursor.fetchone() == {
            "status": "authorized",
            "dispatch_started_at": None,
        }


async def test_dispatch_rejects_pending_and_terminal_authority_without_write(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, prepare, prepared = await _prepared_attempt(
        run_chat_database_url, run_chat_schema
    )
    pending = await repository.begin_authorization(
        BeginModelCallAuthorization(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=prepare.lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            request_ref="billing-operation-a1",
        )
    )
    provisional = StartModelCallDispatch(
        tenant_id=prepared.tenant_id,
        run_id=prepared.run_id,
        attempt_id=prepared.attempt_id,
        lease=prepare.lease,
        attempt_binding_digest=prepared.attempt_binding_digest,
        planned_digest=prepared.planned_digest,
        authorization_digest=pending.authorization_digest,
        request_ref=pending.request_ref,
        admission_ref="billing-admission-a1",
    )
    with pytest.raises(ModelCallIdentityConflict):
        await repository.start_dispatch(provisional)
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """UPDATE {} SET terminal=TRUE,terminal_at=clock_timestamp(),
                      interaction_phase='terminal'
               WHERE tenant_id=%s AND run_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (prepared.tenant_id, prepared.run_id),
        )
    with pytest.raises(ModelCallAuthorityLost):
        await repository.begin_authorization(
            BeginModelCallAuthorization(
                tenant_id=prepared.tenant_id,
                run_id=prepared.run_id,
                attempt_id=prepared.attempt_id,
                lease=prepare.lease,
                attempt_binding_digest=prepared.attempt_binding_digest,
                planned_digest=prepared.planned_digest,
                request_ref=pending.request_ref,
            )
        )


async def test_dispatch_timeout_rolls_back_without_starting(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    _, command = await _authorized_attempt(run_chat_database_url, run_chat_schema)
    repository = PostgresModelCallRepository(
        run_chat_database_url, run_chat_schema, total_timeout_seconds=0.1
    )
    async with connect_pg(run_chat_database_url) as blocker:
        async with blocker.transaction(), blocker.cursor() as cursor:
            await execute_sql(
                cursor,
                "SELECT run_id FROM {} WHERE tenant_id=%s AND run_id=%s FOR UPDATE".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
                (command.tenant_id, command.run_id),
            )
            with pytest.raises(TimeoutError):
                await repository.start_dispatch(command)
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "SELECT status,dispatch_started_at FROM {} WHERE attempt_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (command.attempt_id,),
        )
        assert await cursor.fetchone() == {
            "status": "authorized",
            "dispatch_started_at": None,
        }


async def test_evidence_first_observation_updates_head_and_run_atomically(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, dispatch, row = await _dispatched_attempt_row(
        run_chat_database_url, run_chat_schema
    )
    event_id, evidence_bytes, evidence_digest = _strict_evidence(
        row, outcome="completed", revision=1
    )
    receipt = await _append_evidence(
        repository,
        _append_command(row=row, lease=dispatch.lease, evidence_bytes=evidence_bytes),
    )
    assert str(getattr(receipt, "event_id")) == event_id
    assert getattr(receipt, "revision") == 1
    assert getattr(receipt, "evidence_digest") == evidence_digest
    assert getattr(receipt, "replayed") is False

    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT attempt.status,attempt.latest_revision,
                      attempt.latest_event_id,attempt.latest_evidence_digest,
                      run.model_attempt_count,run.model_dispatched_attempt_count,
                      run.model_sealed_attempt_count,run.model_unknown_attempt_count,
                      run.model_usage_completeness
               FROM {} AS attempt JOIN {} AS run USING (run_id)
               WHERE attempt.tenant_id=%s AND attempt.attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt"),
                qualified(run_chat_schema, "kokoro_agent_run"),
            ),
            (dispatch.tenant_id, dispatch.attempt_id),
        )
        assert await cursor.fetchone() == {
            "status": "observed",
            "latest_revision": 1,
            "latest_event_id": UUID(event_id),
            "latest_evidence_digest": evidence_digest,
            "model_attempt_count": 1,
            "model_dispatched_attempt_count": 1,
            "model_sealed_attempt_count": 1,
            "model_unknown_attempt_count": 1,
            "model_usage_completeness": "unknown",
        }


async def test_evidence_exact_replay_and_two_connection_same_revision_are_idempotent(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    _, dispatch, row = await _dispatched_attempt_row(
        run_chat_database_url, run_chat_schema
    )
    _, evidence_bytes, _ = _strict_evidence(row, outcome="completed", revision=1)
    command = _append_command(
        row=row, lease=dispatch.lease, evidence_bytes=evidence_bytes
    )
    repositories = [
        PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
        for _ in range(2)
    ]
    async with connect_pg(run_chat_database_url) as blocker:
        async with blocker.transaction(), blocker.cursor() as cursor:
            await execute_sql(cursor, "SELECT pg_backend_pid() AS pid")
            blocker_row = await fetch_one(cursor)
            assert blocker_row is not None and isinstance(blocker_row["pid"], int)
            await execute_sql(
                cursor,
                "SELECT run_id FROM {} WHERE tenant_id=%s AND run_id=%s FOR UPDATE".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
                (dispatch.tenant_id, dispatch.run_id),
            )
            tasks = [
                asyncio.create_task(_append_evidence(repository, command))
                for repository in repositories
            ]
            waiters = await _wait_for_owned_run_lock_queue(
                cursor,
                blocker_pid=blocker_row["pid"],
                schema=run_chat_schema,
            )
            assert waiters[0] != waiters[1]
        first, second = await asyncio.gather(*tasks)
    replayed = await _append_evidence(repositories[0], command)
    assert sorted([getattr(first, "replayed"), getattr(second, "replayed")]) == [
        False,
        True,
    ]
    assert getattr(replayed, "replayed") is True
    assert getattr(first, "observed_at_ms") == getattr(second, "observed_at_ms")
    assert getattr(second, "observed_at_ms") == getattr(replayed, "observed_at_ms")

    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT count(*) AS evidence_count FROM {}
               WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
            ),
            (dispatch.tenant_id, dispatch.run_id, dispatch.attempt_id),
        )
        count_row = await fetch_one(cursor)
        assert count_row == {"evidence_count": 1}
        await execute_sql(
            cursor,
            """SELECT attempt.latest_revision,attempt.latest_event_id,
                      attempt.latest_evidence_digest,
                      run.model_attempt_count,run.model_dispatched_attempt_count,
                      run.model_sealed_attempt_count,run.model_unknown_attempt_count,
                      run.model_usage_completeness
               FROM {} AS attempt JOIN {} AS run USING (run_id)
               WHERE attempt.tenant_id=%s AND attempt.attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt"),
                qualified(run_chat_schema, "kokoro_agent_run"),
            ),
            (dispatch.tenant_id, dispatch.attempt_id),
        )
        projection = await fetch_one(cursor)
        assert projection is not None
        assert projection["latest_revision"] == 1
        assert str(projection["latest_event_id"]) == str(getattr(first, "event_id"))
        assert projection["latest_evidence_digest"] == getattr(first, "evidence_digest")
        assert tuple(
            projection[name]
            for name in (
                "model_attempt_count",
                "model_dispatched_attempt_count",
                "model_sealed_attempt_count",
                "model_unknown_attempt_count",
                "model_usage_completeness",
            )
        ) == (1, 1, 1, 1, "unknown")

    _, conflict_bytes, _ = _strict_evidence(row, outcome="failed", revision=1)
    with pytest.raises(ModelCallIdentityConflict):
        await _append_evidence(
            repositories[0],
            _append_command(
                row=row, lease=dispatch.lease, evidence_bytes=conflict_bytes
            ),
        )


async def test_terminal_evidence_exact_replay_is_read_only_and_new_facts_are_rejected(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, dispatch, row = await _dispatched_attempt_row(
        run_chat_database_url, run_chat_schema
    )
    event1, raw1, digest1 = _strict_evidence(row, outcome="unknown", revision=1)
    command = _append_command(row=row, lease=dispatch.lease, evidence_bytes=raw1)
    created = await _append_evidence(repository, command)
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """UPDATE {} SET terminal=TRUE,terminal_at=clock_timestamp(),
                      interaction_phase='terminal'
               WHERE tenant_id=%s AND run_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (dispatch.tenant_id, dispatch.run_id),
        )
        await execute_sql(
            cursor,
            """SELECT event_id,revision,evidence_bytes,evidence_digest,
                      observed_at,created_at
               FROM {} WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s
               ORDER BY revision""".format(
                qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
            ),
            (dispatch.tenant_id, dispatch.run_id, dispatch.attempt_id),
        )
        evidence_before = await cursor.fetchall()
        await execute_sql(
            cursor,
            """SELECT status,sealed_at,latest_revision,latest_event_id,
                      latest_evidence_digest,updated_at
               FROM {} WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (dispatch.tenant_id, dispatch.run_id, dispatch.attempt_id),
        )
        attempt_before = await fetch_one(cursor)
        await execute_sql(
            cursor,
            """SELECT model_attempt_count,model_dispatched_attempt_count,
                      model_sealed_attempt_count,model_unknown_attempt_count,
                      model_usage_completeness,updated_at
               FROM {} WHERE tenant_id=%s AND run_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (dispatch.tenant_id, dispatch.run_id),
        )
        run_before = await fetch_one(cursor)

    replayed = await _append_evidence(repository, command)
    assert getattr(replayed, "replayed") is True
    for name in (
        "tenant_id",
        "run_id",
        "attempt_id",
        "event_id",
        "revision",
        "evidence_digest",
        "observed_at_ms",
    ):
        assert getattr(replayed, name) == getattr(created, name)

    _, raw2, _ = _strict_evidence(
        row,
        outcome="completed",
        revision=2,
        predecessor_event_id=event1,
        predecessor_digest=digest1,
        normalized=True,
    )
    with pytest.raises(ModelCallAuthorityLost):
        await _append_evidence(
            repository,
            _append_command(row=row, lease=dispatch.lease, evidence_bytes=raw2),
        )
    _, drift_bytes, _ = _strict_evidence(row, outcome="completed", revision=1)
    with pytest.raises(ModelCallIdentityConflict):
        await _append_evidence(
            repository,
            _append_command(row=row, lease=dispatch.lease, evidence_bytes=drift_bytes),
        )

    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT event_id,revision,evidence_bytes,evidence_digest,
                      observed_at,created_at
               FROM {} WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s
               ORDER BY revision""".format(
                qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
            ),
            (dispatch.tenant_id, dispatch.run_id, dispatch.attempt_id),
        )
        assert await cursor.fetchall() == evidence_before
        await execute_sql(
            cursor,
            """SELECT status,sealed_at,latest_revision,latest_event_id,
                      latest_evidence_digest,updated_at
               FROM {} WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (dispatch.tenant_id, dispatch.run_id, dispatch.attempt_id),
        )
        assert await fetch_one(cursor) == attempt_before
        await execute_sql(
            cursor,
            """SELECT model_attempt_count,model_dispatched_attempt_count,
                      model_sealed_attempt_count,model_unknown_attempt_count,
                      model_usage_completeness,updated_at
               FROM {} WHERE tenant_id=%s AND run_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (dispatch.tenant_id, dispatch.run_id),
        )
        assert await fetch_one(cursor) == run_before


@pytest.mark.parametrize(
    "mutation",
    ["stale_lease", "planned_digest", "terminal", "timeout"],
)
async def test_evidence_authority_failure_rolls_back_all_projection_writes(
    run_chat_database_url: str, run_chat_schema: str, mutation: str
) -> None:
    repository, dispatch, row = await _dispatched_attempt_row(
        run_chat_database_url, run_chat_schema
    )
    _, evidence_bytes, _ = _strict_evidence(row, outcome="completed", revision=1)
    lease = dispatch.lease
    changed_row = dict(row)
    expected_error: type[BaseException] = ModelCallAuthorityLost
    if mutation == "stale_lease":
        lease = LeaseFence(owner=lease.owner, generation=lease.generation + 1)
    elif mutation == "planned_digest":
        changed_row["planned_digest"] = _OTHER_DIGEST
        expected_error = ModelCallIdentityConflict
    elif mutation == "terminal":
        async with (
            connect_pg(run_chat_database_url) as connection,
            connection.cursor() as cursor,
        ):
            await execute_sql(
                cursor,
                """UPDATE {} SET terminal=TRUE,terminal_at=clock_timestamp(),
                          interaction_phase='terminal'
                   WHERE tenant_id=%s AND run_id=%s""".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
                (dispatch.tenant_id, dispatch.run_id),
            )
    else:
        repository = PostgresModelCallRepository(
            run_chat_database_url, run_chat_schema, total_timeout_seconds=0.1
        )

    command = _append_command(
        row=changed_row, lease=lease, evidence_bytes=evidence_bytes
    )
    if mutation == "timeout":
        async with connect_pg(run_chat_database_url) as blocker:
            async with blocker.transaction(), blocker.cursor() as cursor:
                await execute_sql(
                    cursor,
                    "SELECT run_id FROM {} WHERE tenant_id=%s AND run_id=%s FOR UPDATE".format(
                        qualified(run_chat_schema, "kokoro_agent_run")
                    ),
                    (dispatch.tenant_id, dispatch.run_id),
                )
                with pytest.raises(TimeoutError):
                    await _append_evidence(repository, command)
    else:
        with pytest.raises(expected_error):
            await _append_evidence(repository, command)

    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT attempt.latest_revision,attempt.latest_event_id,
                      run.model_sealed_attempt_count,run.model_unknown_attempt_count
               FROM {} AS attempt JOIN {} AS run USING (run_id)
               WHERE attempt.attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt"),
                qualified(run_chat_schema, "kokoro_agent_run"),
            ),
            (dispatch.attempt_id,),
        )
        assert await cursor.fetchone() == {
            "latest_revision": 0,
            "latest_event_id": None,
            "model_sealed_attempt_count": 0,
            "model_unknown_attempt_count": 0,
        }


async def test_evidence_unknown_to_known_successor_preserves_history(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    repository, dispatch, row = await _dispatched_attempt_row(
        run_chat_database_url, run_chat_schema
    )
    event1, raw1, digest1 = _strict_evidence(row, outcome="unknown", revision=1)
    await _append_evidence(
        repository,
        _append_command(row=row, lease=dispatch.lease, evidence_bytes=raw1),
    )
    event2, raw2, digest2 = _strict_evidence(
        row,
        outcome="completed",
        revision=2,
        predecessor_event_id=event1,
        predecessor_digest=digest1,
        normalized=True,
    )
    receipt = await _append_evidence(
        repository,
        _append_command(row=row, lease=dispatch.lease, evidence_bytes=raw2),
    )
    assert str(getattr(receipt, "event_id")) == event2
    assert getattr(receipt, "evidence_digest") == digest2

    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT revision,event_id,evidence_bytes,evidence_digest
               FROM {} WHERE attempt_id=%s ORDER BY revision""".format(
                qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
            ),
            (dispatch.attempt_id,),
        )
        evidence_rows = await cursor.fetchall()
        assert [
            (item["revision"], str(item["event_id"])) for item in evidence_rows
        ] == [
            (1, event1),
            (2, event2),
        ]
        assert evidence_rows[0]["evidence_bytes"] == raw1
        assert evidence_rows[0]["evidence_digest"] == digest1
        await execute_sql(
            cursor,
            """SELECT model_unknown_attempt_count,model_usage_completeness
               FROM {} WHERE tenant_id=%s AND run_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (dispatch.tenant_id, dispatch.run_id),
        )
        assert await cursor.fetchone() == {
            "model_unknown_attempt_count": 0,
            "model_usage_completeness": "partial",
        }


@pytest.mark.parametrize("invalid", ["noncanonical", "future", "unsealed"])
async def test_evidence_rejects_invalid_raw_time_or_seal_without_write(
    run_chat_database_url: str, run_chat_schema: str, invalid: str
) -> None:
    repository, dispatch, row = await _dispatched_attempt_row(
        run_chat_database_url, run_chat_schema
    )
    _, raw, _ = _strict_evidence(
        row,
        outcome="completed",
        revision=1,
        noncanonical=invalid == "noncanonical",
        observed_offset_ms=86_400_000 if invalid == "future" else None,
        seal=invalid != "unsealed",
    )
    with pytest.raises((ModelCallIdentityConflict, ModelCallInputInvalid)):
        await _append_evidence(
            repository,
            _append_command(row=row, lease=dispatch.lease, evidence_bytes=raw),
        )
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "SELECT latest_revision,latest_event_id,sealed_at FROM {} WHERE attempt_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (dispatch.attempt_id,),
        )
        assert await cursor.fetchone() == {
            "latest_revision": 0,
            "latest_event_id": None,
            "sealed_at": None,
        }


async def test_prepare_allocates_contiguous_ordinals(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    request, recipe, planned = _prepare_values()
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    repository = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    call_id = str(uuid4())
    commands = [
        PrepareModelCall(
            final_request_binding_version=1,
            final_request_digest=synthetic_final_digest(planned, request.input.content),
            request=request,
            lease=LeaseFence(owner="worker-a1", generation=1),
            call_id=call_id,
            attempt_id=str(uuid4()),
            planned=planned,
            peer_path=("assistant",),
            recipe=recipe,
        )
        for _ in range(2)
    ]
    receipts = await asyncio.gather(
        *(repository.prepare(command) for command in commands)
    )
    assert {item.attempt_id for item in receipts} == {
        command.attempt_id for command in commands
    }
    assert sorted(item.attempt_ordinal for item in receipts) == [1, 2]
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """SELECT attempt_id,call_id,attempt_ordinal FROM {}
               WHERE run_id=%s AND call_id=%s ORDER BY attempt_ordinal""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (request.run_id, call_id),
        )
        rows = await cursor.fetchall()
        assert [
            (str(row["attempt_id"]), str(row["call_id"]), row["attempt_ordinal"])
            for row in rows
        ] == sorted(
            (
                (item.attempt_id, item.call_id, item.attempt_ordinal)
                for item in receipts
            ),
            key=lambda item: item[2],
        )
        await execute_sql(
            cursor,
            """SELECT model_attempt_count,model_dispatched_attempt_count,
                      model_sealed_attempt_count,model_unknown_attempt_count,
                      model_usage_completeness FROM {} WHERE run_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (request.run_id,),
        )
        run = await cursor.fetchone()
        assert run == {
            "model_attempt_count": 2,
            "model_dispatched_attempt_count": 0,
            "model_sealed_attempt_count": 0,
            "model_unknown_attempt_count": 0,
            "model_usage_completeness": "unknown",
        }


async def test_prepare_rejects_peer_outside_locked_recipe(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    request, recipe, planned = _prepare_values()
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    repository = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    with pytest.raises(ModelCallInputInvalid):
        await repository.prepare(
            PrepareModelCall(
                final_request_binding_version=1,
                final_request_digest=synthetic_final_digest(
                    planned, request.input.content
                ),
                request=request,
                lease=LeaseFence(owner="worker-a1", generation=1),
                call_id=str(uuid4()),
                attempt_id=str(uuid4()),
                planned=planned,
                peer_path=("not-declared",),
                recipe=recipe,
            )
        )


@pytest.mark.parametrize("kind", ["handoff", "subagent"])
async def test_prepare_accepts_scope_from_real_runtime_recipe(
    run_chat_database_url: str, run_chat_schema: str, kind: str
) -> None:
    request, _, planned = _prepare_values()
    if kind == "handoff":
        recipe = _real_recipe(
            Feature(
                key="chat",
                agents=(
                    Agent(key="assistant", prompt="a"),
                    Agent(key="helper", prompt="h"),
                ),
                entry_agent="assistant",
                handoffs=(("assistant", "helper"),),
            )
        )
    else:
        recipe = _real_recipe(
            Feature(
                key="chat",
                agents=(Agent(key="assistant", prompt="a", subagents=("helper",)),),
                entry_agent="assistant",
            ),
            custom_subagents='[{"name":"helper","description":"helper","system_prompt":"help"}]',
        )
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    receipt = await PostgresModelCallRepository(
        run_chat_database_url, run_chat_schema
    ).prepare(
        PrepareModelCall(
            final_request_binding_version=1,
            final_request_digest=synthetic_final_digest(planned, request.input.content),
            request=request,
            lease=LeaseFence(owner="worker-a1", generation=1),
            call_id=str(uuid4()),
            attempt_id=str(uuid4()),
            planned=planned,
            peer_path=("assistant", "helper"),
            recipe=recipe,
        )
    )
    assert receipt.attempt_ordinal == 1


async def test_prepare_same_attempt_across_runs_is_typed_conflict(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    request, recipe, planned = _prepare_values()
    other = request.model_copy(update={"run_id": "run-prepare-b1"})
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, other, recipe
    )
    repository = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    attempt_id = str(uuid4())
    results = await asyncio.gather(
        *[
            repository.prepare(
                PrepareModelCall(
                    final_request_binding_version=1,
                    final_request_digest=synthetic_final_digest(
                        planned, item.input.content
                    ),
                    request=item,
                    lease=LeaseFence(owner="worker-a1", generation=1),
                    call_id=str(uuid4()),
                    attempt_id=attempt_id,
                    planned=planned,
                    peer_path=("assistant",),
                    recipe=recipe,
                )
            )
            for item in (request, other)
        ],
        return_exceptions=True,
    )
    assert sum(not isinstance(item, BaseException) for item in results) == 1
    assert sum(isinstance(item, ModelCallIdentityConflict) for item in results) == 1


@pytest.mark.parametrize(
    "mutation",
    [
        "owner='stale-worker'",
        "lease_generation=2",
        "lease_expires_at=clock_timestamp() - interval '1 second'",
        "terminal=TRUE,terminal_at=clock_timestamp(),interaction_phase='terminal'",
    ],
)
async def test_prepare_stale_or_terminal_authority_writes_nothing(
    run_chat_database_url: str, run_chat_schema: str, mutation: str
) -> None:
    request, recipe, planned = _prepare_values()
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "UPDATE {} SET {} WHERE run_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_run"), mutation
            ),
            (request.run_id,),
        )
    repository = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    with pytest.raises(ModelCallAuthorityLost):
        await repository.prepare(
            PrepareModelCall(
                final_request_binding_version=1,
                final_request_digest=synthetic_final_digest(
                    planned, request.input.content
                ),
                request=request,
                lease=LeaseFence(owner="worker-a1", generation=1),
                call_id=str(uuid4()),
                attempt_id=str(uuid4()),
                planned=planned,
                peer_path=("assistant",),
                recipe=recipe,
            )
        )


async def test_prepare_rejects_exhausted_ordinal_without_projection_write(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    request, recipe, planned = _prepare_values()
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    repository = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    call_id = str(uuid4())
    first = PrepareModelCall(
        final_request_binding_version=1,
        final_request_digest=synthetic_final_digest(planned, request.input.content),
        request=request,
        lease=LeaseFence(owner="worker-a1", generation=1),
        call_id=call_id,
        attempt_id=str(uuid4()),
        planned=planned,
        peer_path=("assistant",),
        recipe=recipe,
    )
    await repository.prepare(first)
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "UPDATE {} SET attempt_ordinal=9223372036854775807 WHERE attempt_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (first.attempt_id,),
        )
    with pytest.raises(ModelCallOrdinalExhausted):
        await repository.prepare(first.model_copy(update={"attempt_id": str(uuid4())}))


async def test_prepare_total_deadline_cancels_run_lock_wait_without_write(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    request, recipe, planned = _prepare_values()
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    command = PrepareModelCall(
        final_request_binding_version=1,
        final_request_digest=synthetic_final_digest(planned, request.input.content),
        request=request,
        lease=LeaseFence(owner="worker-a1", generation=1),
        call_id=str(uuid4()),
        attempt_id=str(uuid4()),
        planned=planned,
        peer_path=("assistant",),
        recipe=recipe,
    )
    repository = PostgresModelCallRepository(
        run_chat_database_url, run_chat_schema, total_timeout_seconds=0.1
    )
    async with connect_pg(run_chat_database_url) as blocker:
        async with blocker.transaction(), blocker.cursor() as cursor:
            await execute_sql(
                cursor,
                "SELECT run_id FROM {} WHERE run_id=%s FOR UPDATE".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
                (request.run_id,),
            )
            with pytest.raises(TimeoutError):
                await repository.prepare(command)
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "SELECT count(*) AS total FROM {} WHERE attempt_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (command.attempt_id,),
        )
        row = await cursor.fetchone()
        assert row is not None
        assert row["total"] == 0


async def test_prepare_recompute_excludes_sealed_not_dispatched_from_unknown(
    run_chat_database_url: str, run_chat_schema: str
) -> None:
    request, recipe, planned = _prepare_values()
    await _insert_leased_prepare_run(
        run_chat_database_url, run_chat_schema, request, recipe
    )
    repository = PostgresModelCallRepository(run_chat_database_url, run_chat_schema)
    first = await repository.prepare(
        PrepareModelCall(
            final_request_binding_version=1,
            final_request_digest=synthetic_final_digest(planned, request.input.content),
            request=request,
            lease=LeaseFence(owner="worker-a1", generation=1),
            call_id=str(uuid4()),
            attempt_id=str(uuid4()),
            planned=planned,
            peer_path=("assistant",),
            recipe=recipe,
        )
    )
    event_id = uuid4()
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            """INSERT INTO {} (event_id,tenant_id,run_id,attempt_id,call_id,
                revision,evidence_bytes,evidence_digest,execution_outcome,usage_state,
                totals_state,classification_state,sealed,observed_at)
                VALUES (%s,%s,%s,%s,%s,1,%s,%s,'not_dispatched','unresolved',
                        'unknown','unknown',TRUE,clock_timestamp())""".format(
                qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
            ),
            (
                event_id,
                first.tenant_id,
                first.run_id,
                first.attempt_id,
                first.call_id,
                b"{}",
                _DIGEST,
            ),
        )
        await execute_sql(
            cursor,
            """UPDATE {} SET status='not_dispatched',sealed_at=clock_timestamp(),
                latest_revision=1,latest_event_id=%s,latest_evidence_digest=%s
                WHERE attempt_id=%s""".format(
                qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
            ),
            (event_id, _DIGEST, first.attempt_id),
        )
    await repository.prepare(
        PrepareModelCall(
            final_request_binding_version=1,
            final_request_digest=synthetic_final_digest(planned, request.input.content),
            request=request,
            lease=LeaseFence(owner="worker-a1", generation=1),
            call_id=str(uuid4()),
            attempt_id=str(uuid4()),
            planned=planned,
            peer_path=("assistant",),
            recipe=recipe,
        )
    )
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        await execute_sql(
            cursor,
            "SELECT model_unknown_attempt_count FROM {} WHERE run_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (request.run_id,),
        )
        row = await cursor.fetchone()
        assert row is not None
        assert row["model_unknown_attempt_count"] == 0


@pytest.mark.parametrize(
    (
        "status",
        "authorization_state",
        "has_dispatch",
        "has_latest",
    ),
    [
        ("prepared", "not_requested", False, False),
        ("authorization_pending", "requested_unknown", False, False),
        ("authorization_denied", "denied", False, True),
        ("authorized", "granted", False, False),
        ("dispatch_started", "granted", True, False),
        ("observed", "granted", True, True),
        ("unknown", "granted", True, True),
        ("not_dispatched", "granted", False, True),
    ],
)
async def test_attempt_table_accepts_each_approved_state_shape(
    run_chat_database_url: str,
    run_chat_schema: str,
    status: str,
    authorization_state: str,
    has_dispatch: bool,
    has_latest: bool,
) -> None:
    prepared_at = datetime.now(UTC)
    fixture_request, _, fixture_planned = _prepare_values()
    final_digest = synthetic_final_digest(
        fixture_planned, fixture_request.input.content
    )
    async with connect_pg(run_chat_database_url) as connection:
        async with connection.cursor() as cursor:
            await execute_sql(
                cursor,
                """INSERT INTO {} (
                    attempt_id,tenant_id,run_id,call_id,attempt_ordinal,
                    lease_owner,lease_generation,attempt_binding_bytes,
                    attempt_binding_digest,planned_bytes,planned_digest,
                    authorization_bytes,authorization_digest,authorization_state,
                    status,prepared_at,dispatch_started_at,sealed_at,
                    latest_revision,latest_event_id,latest_evidence_digest,
                    final_request_binding_version,final_request_digest
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,1,%s)""".format(
                    qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
                ),
                (
                    uuid4(),
                    "tenant-a1",
                    f"run-{uuid4()}",
                    uuid4(),
                    1,
                    "worker-a1",
                    1,
                    b"{}",
                    _DIGEST,
                    b"{}",
                    _DIGEST,
                    b"{}",
                    _DIGEST,
                    authorization_state,
                    status,
                    prepared_at,
                    prepared_at if has_dispatch else None,
                    prepared_at if has_latest else None,
                    1 if has_latest else 0,
                    uuid4() if has_latest else None,
                    _DIGEST if has_latest else None,
                    final_digest,
                ),
            )


@pytest.mark.parametrize(
    ("status", "authorization_state", "has_dispatch", "has_latest"),
    [
        ("prepared", "granted", False, False),
        ("prepared", "not_requested", True, False),
        ("authorization_pending", "requested_unknown", True, False),
        ("authorization_denied", "denied", False, False),
        ("authorized", "granted", False, True),
        ("dispatch_started", "granted", False, False),
        ("observed", "granted", True, False),
        ("unknown", "granted", False, True),
        ("not_dispatched", "denied", False, True),
        ("not-a-state", "granted", False, False),
    ],
)
async def test_attempt_table_rejects_invalid_state_cross_products(
    run_chat_database_url: str,
    run_chat_schema: str,
    status: str,
    authorization_state: str,
    has_dispatch: bool,
    has_latest: bool,
) -> None:
    prepared_at = datetime.now(UTC)
    fixture_request, _, fixture_planned = _prepare_values()
    final_digest = synthetic_final_digest(
        fixture_planned, fixture_request.input.content
    )
    async with connect_pg(run_chat_database_url) as connection:
        with pytest.raises(CheckViolation):
            async with connection.cursor() as cursor:
                await execute_sql(
                    cursor,
                    """INSERT INTO {} (
                        attempt_id,tenant_id,run_id,call_id,attempt_ordinal,
                        lease_owner,lease_generation,attempt_binding_bytes,
                        attempt_binding_digest,planned_bytes,planned_digest,
                        authorization_bytes,authorization_digest,authorization_state,
                        status,prepared_at,dispatch_started_at,sealed_at,
                        latest_revision,latest_event_id,latest_evidence_digest,
                        final_request_binding_version,final_request_digest
                    ) VALUES (%s,%s,%s,%s,1,%s,1,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,1,%s)""".format(
                        qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
                    ),
                    (
                        uuid4(),
                        "tenant-a1",
                        f"run-{uuid4()}",
                        uuid4(),
                        "worker-a1",
                        b"{}",
                        _DIGEST,
                        b"{}",
                        _DIGEST,
                        b"{}",
                        _DIGEST,
                        authorization_state,
                        status,
                        prepared_at,
                        prepared_at if has_dispatch else None,
                        prepared_at if has_latest else None,
                        1 if has_latest else 0,
                        uuid4() if has_latest else None,
                        _DIGEST if has_latest else None,
                        final_digest,
                    ),
                )


async def test_attempt_identity_and_evidence_revision_uniqueness_are_durable(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    attempt_id = uuid4()
    call_id = uuid4()
    event_id = uuid4()
    prepared_at = datetime.now(UTC)
    fixture_request, _, fixture_planned = _prepare_values()
    final_digest = synthetic_final_digest(
        fixture_planned, fixture_request.input.content
    )
    async with connect_pg(run_chat_database_url) as connection:
        async with connection.cursor() as cursor:
            await execute_sql(
                cursor,
                """INSERT INTO {} (
                    attempt_id,tenant_id,run_id,call_id,attempt_ordinal,
                    lease_owner,lease_generation,attempt_binding_bytes,
                    attempt_binding_digest,planned_bytes,planned_digest,
                    authorization_bytes,authorization_digest,authorization_state,
                    status,prepared_at,sealed_at,latest_revision,
                    latest_event_id,latest_evidence_digest,
                    final_request_binding_version,final_request_digest
                ) VALUES (%s,'tenant-a1','run-a1',%s,1,'worker-a1',1,
                    %s,%s,%s,%s,%s,%s,'not_requested','not_dispatched',%s,
                    %s,1,%s,%s,1,%s)""".format(
                    qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
                ),
                (
                    attempt_id,
                    call_id,
                    b"{}",
                    _DIGEST,
                    b"{}",
                    _DIGEST,
                    b"{}",
                    _DIGEST,
                    prepared_at,
                    prepared_at,
                    event_id,
                    _DIGEST,
                    final_digest,
                ),
            )
            await execute_sql(
                cursor,
                """INSERT INTO {} (
                    event_id,tenant_id,run_id,attempt_id,call_id,revision,
                    evidence_bytes,evidence_digest,execution_outcome,usage_state,
                    totals_state,classification_state,sealed,observed_at
                ) VALUES (%s,'tenant-a1','run-a1',%s,%s,1,%s,%s,
                    'not_dispatched','unresolved','unknown','unknown',TRUE,%s)""".format(
                    qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
                ),
                (event_id, attempt_id, call_id, b"{}", _DIGEST, prepared_at),
            )
        with pytest.raises(UniqueViolation):
            async with connection.cursor() as cursor:
                await execute_sql(
                    cursor,
                    """INSERT INTO {} (
                        event_id,tenant_id,run_id,attempt_id,call_id,revision,
                        evidence_bytes,evidence_digest,execution_outcome,usage_state,
                        totals_state,classification_state,sealed,observed_at
                    ) VALUES (%s,'tenant-a1','run-a1',%s,%s,1,%s,%s,
                        'not_dispatched','unresolved','unknown','unknown',TRUE,%s)""".format(
                        qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
                    ),
                    (
                        uuid4(),
                        attempt_id,
                        call_id,
                        b"different",
                        _OTHER_DIGEST,
                        prepared_at,
                    ),
                )


@pytest.mark.parametrize(
    (
        "revision",
        "has_predecessor",
        "evidence_bytes",
        "evidence_digest",
        "outcome",
        "usage_state",
        "totals_state",
        "classification_state",
        "sealed",
    ),
    [
        (
            0,
            False,
            b"{}",
            _DIGEST,
            "not_dispatched",
            "unresolved",
            "unknown",
            "unknown",
            True,
        ),
        (
            2,
            False,
            b"{}",
            _DIGEST,
            "completed",
            "normalized",
            "known_zero",
            "complete",
            True,
        ),
        (
            1,
            True,
            b"{}",
            _DIGEST,
            "not_dispatched",
            "unresolved",
            "unknown",
            "unknown",
            True,
        ),
        (
            1,
            False,
            b"",
            _DIGEST,
            "not_dispatched",
            "unresolved",
            "unknown",
            "unknown",
            True,
        ),
        (
            1,
            False,
            b"{}",
            "not-a-digest",
            "not_dispatched",
            "unresolved",
            "unknown",
            "unknown",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "unknown",
            "unresolved",
            "known_zero",
            "complete",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "completed",
            "unresolved",
            "known_zero",
            "complete",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "not_dispatched",
            "normalized",
            "known_nonzero",
            "partial",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "completed",
            "normalized",
            "unknown",
            "complete",
            True,
        ),
        (
            1,
            False,
            b"{}",
            _DIGEST,
            "completed",
            "normalized",
            "known_zero",
            "complete",
            False,
        ),
    ],
)
async def test_evidence_table_rejects_invalid_revision_bytes_and_knowledge(
    run_chat_database_url: str,
    run_chat_schema: str,
    revision: int,
    has_predecessor: bool,
    evidence_bytes: bytes,
    evidence_digest: str,
    outcome: str,
    usage_state: str,
    totals_state: str,
    classification_state: str,
    sealed: bool,
) -> None:
    async with connect_pg(run_chat_database_url) as connection:
        with pytest.raises(CheckViolation):
            async with connection.cursor() as cursor:
                await execute_sql(
                    cursor,
                    """INSERT INTO {} (
                        event_id,tenant_id,run_id,attempt_id,call_id,revision,
                        predecessor_event_id,predecessor_digest,evidence_bytes,
                        evidence_digest,execution_outcome,usage_state,totals_state,
                        classification_state,sealed,observed_at
                    ) VALUES (%s,'tenant-a1','run-a1',%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s)""".format(
                        qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
                    ),
                    (
                        uuid4(),
                        uuid4(),
                        uuid4(),
                        revision,
                        uuid4() if has_predecessor else None,
                        _OTHER_DIGEST if has_predecessor else None,
                        evidence_bytes,
                        evidence_digest,
                        outcome,
                        usage_state,
                        totals_state,
                        classification_state,
                        sealed,
                        datetime.now(UTC),
                    ),
                )


async def test_normalized_unknown_usage_remains_a_legal_codec_projection(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    async with connect_pg(run_chat_database_url) as connection:
        async with connection.cursor() as cursor:
            await execute_sql(
                cursor,
                """INSERT INTO {} (
                    event_id,tenant_id,run_id,attempt_id,call_id,revision,
                    evidence_bytes,evidence_digest,execution_outcome,usage_state,
                    totals_state,classification_state,sealed,observed_at
                ) VALUES (%s,'tenant-a1','run-a1',%s,%s,1,%s,%s,
                    'completed','normalized','unknown','unknown',TRUE,%s)""".format(
                    qualified(run_chat_schema, "kokoro_agent_model_usage_evidence")
                ),
                (
                    uuid4(),
                    uuid4(),
                    uuid4(),
                    b"{}",
                    _DIGEST,
                    datetime.now(UTC),
                ),
            )


@pytest.mark.parametrize(
    (
        "attempts",
        "dispatched",
        "sealed",
        "unknown",
        "completeness",
        "valid",
    ),
    [
        (0, 0, 0, 0, "none", True),
        (1, 0, 1, 0, "complete", True),
        (2, 1, 2, 0, "partial", True),
        (1, 1, 0, 0, "unknown", True),
        (1, 1, 1, 1, "unknown", True),
        (0, 0, 0, 0, "unknown", False),
        (1, 0, 0, 0, "complete", False),
        (1, 0, 1, 1, "complete", False),
        (1, 2, 1, 0, "partial", False),
        (1, 0, 1, 0, "not-a-state", False),
    ],
)
async def test_run_completeness_count_constraints(
    run_chat_database_url: str,
    run_chat_schema: str,
    attempts: int,
    dispatched: int,
    sealed: int,
    unknown: int,
    completeness: str,
    valid: bool,
) -> None:
    statement = """INSERT INTO {} (
        run_id,tenant_id,model_attempt_count,model_dispatched_attempt_count,
        model_sealed_attempt_count,model_unknown_attempt_count,
        model_usage_completeness
    ) VALUES (%s,'tenant-a1',%s,%s,%s,%s,%s)""".format(
        qualified(run_chat_schema, "kokoro_agent_run")
    )
    values = (f"run-{uuid4()}", attempts, dispatched, sealed, unknown, completeness)
    async with connect_pg(run_chat_database_url) as connection:
        if valid:
            async with connection.cursor() as cursor:
                await execute_sql(cursor, statement, values)
        else:
            with pytest.raises(CheckViolation):
                async with connection.cursor() as cursor:
                    await execute_sql(cursor, statement, values)


async def test_r246_final_request_receipt_exact_replay_and_drift(
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    repository, command, created = await _prepared_attempt(
        run_chat_database_url, run_chat_schema
    )
    assert created.final_request_binding_version == 1
    assert created.final_request_digest == synthetic_final_digest(
        command.planned, command.request.input.content
    )

    async def row() -> dict[str, object]:
        async with (
            connect_pg(run_chat_database_url) as connection,
            connection.cursor() as cursor,
        ):
            await execute_sql(
                cursor,
                f"SELECT * FROM {qualified(run_chat_schema, 'kokoro_agent_model_call_attempt')} WHERE attempt_id=%s",
                (command.attempt_id,),
            )
            value = await fetch_one(cursor)
            assert value is not None
            return dict(value)

    before = await row()
    replayed = await repository.prepare(command)
    assert replayed == created.model_copy(update={"replayed": True})
    changed = command.model_copy(
        update={
            "final_request_digest": synthetic_final_digest(
                command.planned,
                command.request.input.content + " changed-final-transform",
            )
        }
    )
    assert changed.final_request_digest != command.final_request_digest
    with pytest.raises(ModelCallIdentityConflict):
        await repository.prepare(changed)
    assert await row() == before
    outcomes = await asyncio.gather(
        repository.prepare(command), repository.prepare(changed), return_exceptions=True
    )
    assert isinstance(outcomes[0], PreparedModelCall) and outcomes[0].replayed
    assert isinstance(outcomes[1], ModelCallIdentityConflict)
    assert await row() == before


@pytest.mark.parametrize("fault", ["version", "digest"])
async def test_r246_corrupt_final_request_blocks_transition_and_closure(
    run_chat_database_url: str,
    run_chat_schema: str,
    fault: str,
) -> None:
    from kokoro_agent.model.call_evidence import build_terminal_closure_evidence

    repository, command, created = await _prepared_attempt(
        run_chat_database_url, run_chat_schema
    )
    async with (
        connect_pg(run_chat_database_url) as connection,
        connection.cursor() as cursor,
    ):
        table = qualified(run_chat_schema, "kokoro_agent_model_call_attempt")
        await execute_sql(
            cursor,
            f"ALTER TABLE {table} DROP CONSTRAINT ck_model_attempt_final_request",
        )
        column, value = (
            ("final_request_binding_version", 2)
            if fault == "version"
            else ("final_request_digest", "INVALID")
        )
        await execute_sql(
            cursor,
            f"UPDATE {table} SET {column}=%s WHERE attempt_id=%s",
            (value, command.attempt_id),
        )
        await execute_sql(
            cursor,
            f"SELECT *,clock_timestamp() AS db_now FROM {table} WHERE attempt_id=%s",
            (command.attempt_id,),
        )
        row = await fetch_one(cursor)
        assert row is not None
    with pytest.raises(ModelCallIdentityConflict):
        await repository.prepare(command)
    with pytest.raises(ModelCallIdentityConflict):
        await repository.begin_authorization(
            BeginModelCallAuthorization(
                tenant_id=created.tenant_id,
                run_id=created.run_id,
                attempt_id=created.attempt_id,
                lease=created.lease,
                attempt_binding_digest=created.attempt_binding_digest,
                planned_digest=created.planned_digest,
                request_ref="r246-corrupt-qa",
            )
        )
    database_now = row["db_now"]
    assert isinstance(database_now, datetime)
    with pytest.raises(ModelCallIdentityConflict):
        build_terminal_closure_evidence(row, database_now)
