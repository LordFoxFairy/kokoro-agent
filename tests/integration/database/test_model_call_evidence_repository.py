"""Real-PostgreSQL gates for the approved A1 schema and prepare repository.

The production prepare seam binds trusted durable Run/recipe facts to the
canonical attempt table and covers immutable replay, ordinal allocation, lease
authority, peer scope, and same-transaction Run completeness. Root's isolated
PostgreSQL run passed all 56 cases here; the full database gate passed 267 cases.
Root remains the only runner and owner of PostgreSQL resources.

Authorization/dispatch/evidence/finalizer and runtime-provider wiring are not
implemented by this slice. Real lost-COMMIT-ACK recovery, the complete
not-dispatched knowledge matrix, same-identity race-loser side effects, and
comprehensive timeout recovery remain open rather than represented by mocks,
skips, or import failures.
"""

from __future__ import annotations

import asyncio
from collections.abc import Collection
from datetime import UTC, datetime
from uuid import uuid4
import hashlib
import json

import pytest
from psycopg.errors import CheckViolation, UniqueViolation

from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql
from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.infrastructure.postgres_model_call_repository import (
    PostgresModelCallRepository,
)
from kokoro_agent.model.call_models import (
    ModelCallAuthorityLost,
    ModelCallIdentityConflict,
    ModelCallInputInvalid,
    ModelCallOrdinalExhausted,
    PrepareModelCall,
)
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
                    latest_revision,latest_event_id,latest_evidence_digest
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s)""".format(
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
                        latest_revision,latest_event_id,latest_evidence_digest
                    ) VALUES (%s,%s,%s,%s,1,%s,1,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s)""".format(
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
                    latest_event_id,latest_evidence_digest
                ) VALUES (%s,'tenant-a1','run-a1',%s,1,'worker-a1',1,
                    %s,%s,%s,%s,%s,%s,'not_requested','not_dispatched',%s,
                    %s,1,%s,%s)""".format(
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
