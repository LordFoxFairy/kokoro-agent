"""HITL owner transaction REDs on real Repository/Schema, not native proof.

Root supplies an owned PostgreSQL database. Existing fixtures install a unique
owner schema and clean only that schema. No Redis, provider, or service startup.
Missing production capability is an explicit test-body assertion, not an import
or collection failure. Pause input below is a trusted storage fixture, not a
claim that a LangGraph checkpoint was actually observed.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from typing import Any, TypedDict, cast
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from psycopg.errors import RaiseException
from pydantic import JsonValue
from langchain_core.runnables.config import RunnableConfig

from kokoro_agent.domain.run import interactions
from kokoro_agent.domain.run.repository import LeaseFence, RunRepository
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.interfaces.http.ingress import AgentIngress
from kokoro_agent.application.chat.service import ChatService
from kokoro_agent.infrastructure.postgres_chat_repository import PostgresChatRepository
from kokoro_agent.protocol import RunRequest
from support.fakes import FakeBus, request


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _capability(repository: RunRepository) -> Any:
    for name in ("record_pause", "accept_resume"):
        assert callable(getattr(repository, name, None)), (
            f"Real RunRepository.{name} is missing: HITL Run-command-Chat "
            "transaction capability has not been implemented"
        )
    # Tests address the approved future port without importing a missing module.
    return cast(Any, repository)


def _pause_snapshot(run: RunRequest) -> tuple[dict[str, Any], bytes]:
    groups: list[dict[str, Any]] = [
        {
            "group_id": "group-1",
            "items": [
                {
                    "item_id": "item-1",
                    "request_id": "call-1",
                    "kind": "tool_approval",
                    "allowed_decisions": ["approve", "reject"],
                    "display": {
                        "name": "lookup",
                        "description": "Review lookup",
                        "editable": False,
                        "input_schema": {"type": "object", "properties": {}},
                    },
                    "validation": None,
                },
                {
                    "item_id": "item-2",
                    "request_id": "call-2",
                    "kind": "tool_approval",
                    "allowed_decisions": ["approve", "reject"],
                    "display": {
                        "name": "lookup",
                        "description": "Review another lookup",
                        "editable": False,
                        "input_schema": {"type": "object", "properties": {}},
                    },
                    "validation": {
                        "code": "json_schema_invalid",
                        "instance_path": ["query"],
                    },
                },
            ],
        }
    ]
    snapshot: dict[str, Any] = {
        "format": "kokoro-agent:pause-collection:1",
        "groups": groups,
        "locator": {
            "groups": [
                {
                    "group_id": "group-1",
                    "thread_id": RunScope.of(run).scoped_thread_id,
                    "checkpoint_ns": "",
                    "checkpoint_id": "fixture-checkpoint-1",
                    "tasks": [
                        {
                            "task_id": "task-1",
                            "interrupt_id": "interrupt-1",
                            "item_ids": ["item-1", "item-2"],
                        }
                    ],
                }
            ]
        },
    }
    return snapshot, _canonical(snapshot)


def _pause_value(raw: bytes) -> Any:
    constructor = getattr(interactions, "DurablePauseSnapshot", None)
    assert callable(constructor), "Approved DurablePauseSnapshot value is missing"
    return constructor(
        pause_ref="pause-opaque-1",
        canonical_bytes=raw,
        digest=hashlib.sha256(raw).hexdigest(),
    )


async def _launch_claim(
    repository: RunRepository,
    url: str,
    schema: str,
) -> tuple[RunRequest, LeaseFence]:
    """Use the actual launch/dispatch owner path before any Run or pause exists."""
    seed = request(f"hitl-{uuid4().hex}")
    ingress = AgentIngress(
        bus=FakeBus(),
        run_repository=repository,
        chat_service=ChatService(PostgresChatRepository(url, schema)),
    )
    receipt = await ingress.launch(
        {
            "request_id": seed.run_id + "-request",
            "run_id": seed.run_id,
            "session_id": seed.session_id,
            "feature_key": seed.feature_key,
            "selected_skill_source_refs": list(seed.selected_skill_source_refs),
            "message_id": seed.input.message_id,
            "content": seed.input.content,
        },
        execution_identity=seed.execution_identity,
    )
    assert receipt.run_id == seed.run_id and receipt.replayed is False
    run = await repository.get_pending_dispatch(receipt.run_id)
    assert run is not None
    async with connect_pg(url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT request_json,tenant_id,namespace,status FROM {} WHERE run_id=%s".format(
                qualified(schema, "kokoro_agent_run_dispatch")
            ),
            (run.run_id,),
        )
        stored = await fetch_one(cur)
        assert stored is not None
        assert stored["request_json"].encode("utf-8") == run.model_dump_json().encode(
            "utf-8"
        )
        assert stored["tenant_id"] == run.execution_identity.tenant_ref
        assert stored["namespace"] == RunScope.of(run).namespace
        assert stored["status"] == "pending"
    lease = await repository.claim_dispatch(run, "worker-1")
    assert lease is not None
    scoped = await repository.get_request_scoped(
        run.run_id, run.execution_identity.tenant_ref, RunScope.of(run).namespace
    )
    assert scoped is not None and scoped.model_dump_json() == run.model_dump_json()
    return run, lease


async def _admit(
    repository: RunRepository,
    run: RunRequest,
    url: str,
    schema: str,
    *,
    command_id: str = "command-1",
) -> None:
    # Actual public ingress owns command digest/serialization; only notification
    # transport is in-memory. All admission/Run/Chat facts remain real PostgreSQL.
    ingress = AgentIngress(
        bus=FakeBus(),
        run_repository=repository,
        chat_service=ChatService(PostgresChatRepository(url, schema)),
    )
    receipt = await ingress.control(
        run.run_id,
        {
            "kind": "run.resume",
            "session_id": run.session_id,
            "expected_pause_revision": 1,
            "pause_ref": "pause-opaque-1",
            "decisions": [
                {"type": "approve", "item_id": "item-2"},
                {"type": "approve", "item_id": "item-1"},
            ],
        },
        command_id=command_id,
        execution_identity=run.execution_identity,
    )
    assert receipt["status"] == "pending"


async def _facts(url: str, schema: str, run: RunRequest) -> dict[str, Any]:
    """Independent connection reads actual committed owner rows, not return values."""
    async with connect_pg(url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            "SELECT to_jsonb(r) AS row FROM {} r WHERE run_id=%s".format(
                qualified(schema, "kokoro_agent_run")
            ),
            (run.run_id,),
        )
        head = await fetch_one(cursor)
        assert head is not None
        result: dict[str, Any] = {"run": head["row"]}
        for key, table, order in (
            ("commands", "kokoro_agent_run_control_command", "command_id"),
            ("chat", "kokoro_agent_chat_event", "source_index"),
        ):
            await execute_sql(
                cursor,
                "SELECT to_jsonb(t) AS row FROM {} t WHERE run_id=%s ORDER BY {}".format(
                    qualified(schema, table), order
                ),
                (run.run_id,),
            )
            result[key] = [row["row"] for row in await fetch_all(cursor)]
        scope = RunScope.of(run)
        await execute_sql(
            cursor,
            "SELECT to_jsonb(s) AS row FROM {} s WHERE tenant_id=%s AND namespace=%s AND session_id=%s".format(
                qualified(schema, "kokoro_agent_chat_sequence")
            ),
            (run.execution_identity.tenant_ref, scope.namespace, run.session_id),
        )
        result["sequence"] = [row["row"] for row in await fetch_all(cursor)]
        return result


async def _install_write_failure(
    url: str, schema: str, *, stage: str, phase: str
) -> None:
    # Fixed test-owned SQL values only. A real AFTER trigger aborts the actual tx.
    assert (stage, phase) in {
        ("head", "waiting"),
        ("head", "resuming"),
        ("command", "resuming"),
        ("command", "dispatch_started"),
        ("chat", "waiting"),
        ("chat", "resuming"),
        ("chat", "terminal"),
    }
    function = qualified(schema, "fail_hitl_write")
    if stage == "head":
        table = "kokoro_agent_run"
        timing = "AFTER UPDATE OF interaction_phase"
        condition = f"NEW.interaction_phase = '{phase}'"
    elif stage == "command":
        table = "kokoro_agent_run_control_command"
        timing = "AFTER INSERT OR UPDATE"
        status = "dispatch_started" if phase == "dispatch_started" else "accepted"
        condition = f"NEW.resume_intent_status = '{status}'"
    else:
        table = "kokoro_agent_chat_event"
        timing = "AFTER INSERT"
        condition = (
            "NEW.event_type = 'interaction.state' AND "
            f"NEW.payload_json::jsonb->>'phase' = '{phase}'"
        )
    async with connect_pg(url) as connection, connection.cursor() as cursor:
        await execute_sql(
            cursor,
            f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN RAISE EXCEPTION 'injected_hitl_write_failure'; END; $$",
        )
        await execute_sql(
            cursor,
            f"CREATE TRIGGER fail_hitl_write {timing} ON {qualified(schema, table)} "
            f"FOR EACH ROW WHEN ({condition}) EXECUTE FUNCTION {function}()",
        )


async def test_pause_and_accept_commit_one_complete_state_and_command(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    run, lease = await _launch_claim(
        run_repository, run_chat_database_url, run_chat_schema
    )
    assert lease is not None  # Existing real repository/schema path is exercised first.
    capability = _capability(run_repository)
    snapshot, raw = _pause_snapshot(run)
    await capability.record_pause(run, lease, _pause_value(raw))
    waiting = await _facts(run_chat_database_url, run_chat_schema, run)
    assert waiting["run"]["interaction_phase"] == "waiting"
    assert waiting["run"]["lease_expires_at"] is None
    assert waiting["run"]["pending_groups_json"] == snapshot["groups"]
    assert waiting["run"]["pause_snapshot_json"] == snapshot
    assert waiting["run"]["interaction_revision"] == 1
    assert waiting["run"]["pause_revision"] == 1
    assert len(waiting["chat"]) == 1
    pause_source = waiting["chat"][0]
    assert pause_source["event_type"] == "interaction.state"
    assert json.loads(pause_source["payload_json"])["groups"] == snapshot["groups"]
    assert pause_source["source_index"] == waiting["run"]["interaction_source_index"]

    await _admit(run_repository, run, run_chat_database_url, run_chat_schema)
    await capability.accept_resume(run, "command-1", "worker-2")
    accepted = await _facts(run_chat_database_url, run_chat_schema, run)
    head = accepted["run"]
    assert head["interaction_phase"] == "resuming"
    assert head["interaction_revision"] == 2 and head["pause_revision"] == 1
    assert head["owner"] == "worker-2"
    assert head["lease_generation"] == lease.generation + 1
    assert head["lease_expires_at"] is not None
    assert head["pending_groups_json"] == snapshot["groups"]
    assert len(accepted["commands"]) == 1
    command = accepted["commands"][0]
    from kokoro_agent.protocol.control import RunResume, control_request_digest

    persisted_request = RunResume.model_validate_json(command["body"])
    assert persisted_request.model_dump_json() == command["body"]
    assert persisted_request.run_id == run.run_id
    assert persisted_request.command_id == "command-1"
    assert persisted_request.expected_pause_revision == 1
    assert persisted_request.pause_ref == "pause-opaque-1"
    assert command["request_digest"] == control_request_digest(persisted_request)
    assert persisted_request.request_digest == command["request_digest"]
    assert command["resume_intent_status"] == "accepted"
    assert command["resume_pause_snapshot_json"] == snapshot
    assert command["resume_pause_collection_digest"] == hashlib.sha256(raw).hexdigest()
    assert command["resume_attempt_id"] is None and command["resume_started_at"] is None
    # PostgreSQL to_jsonb(BYTEA) returns the actual encoded bytes in hex form.
    stored = command["resume_decisions_bytes"]
    assert stored.startswith("\\x")
    decisions_raw = bytes.fromhex(stored[2:])
    decisions = json.loads(decisions_raw)
    assert [d["item_id"] for d in decisions["groups"][0]["decisions"]] == [
        "item-1",
        "item-2",
    ]
    assert (
        command["resume_decisions_digest"] == hashlib.sha256(decisions_raw).hexdigest()
    )
    admitted_items = {item.item_id: item for item in persisted_request.decisions}
    expected_items: list[dict[str, str]] = []
    for item_id in ("item-1", "item-2"):
        item = admitted_items[item_id]
        payload_bytes = _canonical(
            item.model_dump(mode="json", exclude={"type", "item_id"}, exclude_none=True)
        )
        expected_items.append(
            {
                "item_id": item_id,
                "kind": item.type,
                "payload_b64": base64.b64encode(payload_bytes).decode("ascii"),
            }
        )
    assert decisions == {
        "format": "kokoro-agent:resume-decisions:1",
        "groups": [{"group_id": "group-1", "decisions": expected_items}],
    }
    assert len(accepted["chat"]) == 2
    source = accepted["chat"][1]
    from kokoro_agent.protocol.events import ChatInteractionState

    payload = json.loads(source["payload_json"])
    ChatInteractionState.model_validate_json(source["payload_json"])
    ChatInteractionState.model_validate_json(pause_source["payload_json"])
    assert payload["pause_revision"] == 1 and payload["pause_ref"] == "pause-opaque-1"
    assert source["event_type"] == "interaction.state"
    assert payload["phase"] == "resuming" and payload["interaction_revision"] == 2
    assert payload["groups"] == snapshot["groups"]
    assert command["resume_accepted_revision"] == head["interaction_revision"]
    assert command["resume_accepted_source_index"] == source["source_index"]
    assert head["interaction_source_index"] == source["source_index"]
    assert source["seq"] > pause_source["seq"]
    assert "fixture-checkpoint" not in source["payload_json"]
    assert "payload_b64" not in source["payload_json"]

    await capability.accept_resume(run, "command-1", "worker-3")
    assert await _facts(run_chat_database_url, run_chat_schema, run) == accepted


@pytest.mark.parametrize("stage", ["head", "chat"])
async def test_pause_failure_rolls_back_head_lease_chat_and_sequence(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    stage: str,
) -> None:
    run, lease = await _launch_claim(
        run_repository, run_chat_database_url, run_chat_schema
    )
    assert lease is not None
    capability = _capability(run_repository)
    _, raw = _pause_snapshot(run)
    before = await _facts(run_chat_database_url, run_chat_schema, run)
    await _install_write_failure(
        run_chat_database_url, run_chat_schema, stage=stage, phase="waiting"
    )
    with pytest.raises(RaiseException, match="injected_hitl_write_failure"):
        await capability.record_pause(run, lease, _pause_value(raw))
    assert await _facts(run_chat_database_url, run_chat_schema, run) == before


@pytest.mark.parametrize("stage", ["head", "command", "chat"])
async def test_accept_failure_rolls_back_head_command_chat_and_sequence(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    stage: str,
) -> None:
    run, lease = await _launch_claim(
        run_repository, run_chat_database_url, run_chat_schema
    )
    assert lease is not None
    capability = _capability(run_repository)
    _, raw = _pause_snapshot(run)
    await capability.record_pause(run, lease, _pause_value(raw))
    await _admit(run_repository, run, run_chat_database_url, run_chat_schema)
    before = await _facts(run_chat_database_url, run_chat_schema, run)
    await _install_write_failure(
        run_chat_database_url, run_chat_schema, stage=stage, phase="resuming"
    )
    with pytest.raises(RaiseException, match="injected_hitl_write_failure"):
        await capability.accept_resume(run, "command-1", "worker-2")
    assert await _facts(run_chat_database_url, run_chat_schema, run) == before


def _plan() -> interactions.ResumeDispatchPlan:
    # Native evidence is an opaque trusted fixture here, not a saver observation.
    value = {
        "format": "kokoro-agent:resume-dispatch:1",
        "attempt_id": "attempt-1",
        "groups": [
            {
                "group_id": "group-1",
                "tasks": [
                    {
                        "task_id": "task-1",
                        "interrupt_id": "interrupt-1",
                        "pre_resume_length": 0,
                        "pre_resume_digest": "0" * 64,
                        "expected_append_digest": "1" * 64,
                    }
                ],
            }
        ],
    }
    return interactions.ResumeDispatchPlan(
        attempt_id="attempt-1", canonical_bytes=_canonical(value)
    )


async def _accepted(
    repository: RunRepository, url: str, schema: str
) -> tuple[RunRequest, interactions.AcceptedResume]:
    run, lease = await _launch_claim(repository, url, schema)
    assert lease is not None
    _, raw = _pause_snapshot(run)
    await repository.record_pause(run, lease, _pause_value(raw))
    await _admit(repository, run, url, schema)
    result = await repository.accept_resume(run, "command-1", "worker-2")
    assert isinstance(result, interactions.AcceptedResume)
    return run, result


async def test_start_once_unknown_never_grants_second_dispatch(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    run, accepted = await _accepted(
        run_repository, run_chat_database_url, run_chat_schema
    )
    before = await _facts(run_chat_database_url, run_chat_schema, run)
    first = await run_repository.start_resume(run, accepted.lease, "command-1", _plan())
    assert isinstance(first, interactions.StartedResume)
    committed = await _facts(run_chat_database_url, run_chat_schema, run)
    assert committed["commands"][0]["resume_intent_status"] == "dispatch_started"
    assert (
        committed["commands"][0]["resume_attempt_generation"]
        == accepted.lease.generation
    )
    assert committed["commands"][0]["resume_started_at"] is not None
    assert committed["run"] == before["run"] and committed["chat"] == before["chat"]
    replay = await run_repository.start_resume(
        run, accepted.lease, "command-1", _plan()
    )
    assert isinstance(replay, interactions.ReplayedResume)
    assert await _facts(run_chat_database_url, run_chat_schema, run) == committed
    await run_repository.mark_resume_unknown(
        run, accepted.lease, "command-1", "attempt-1"
    )
    unknown = await _facts(run_chat_database_url, run_chat_schema, run)
    assert unknown["commands"][0]["resume_intent_status"] == "unknown"
    assert unknown["run"] == committed["run"] and unknown["chat"] == committed["chat"]
    with pytest.raises(interactions.InteractionConflict):
        await run_repository.start_resume(run, accepted.lease, "command-1", _plan())
    assert await _facts(run_chat_database_url, run_chat_schema, run) == unknown


@pytest.mark.parametrize("fault", ["generation", "expiry", "plan", "body", "digest"])
async def test_start_or_accept_rejects_lost_authority_and_corrupt_evidence_without_writes(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    fault: str,
) -> None:
    from kokoro_agent.domain.run.models import LeaseFence

    run, accepted = await _accepted(
        run_repository, run_chat_database_url, run_chat_schema
    )
    lease, plan = accepted.lease, _plan()
    if fault == "generation":
        lease = LeaseFence(owner=lease.owner, generation=lease.generation + 1)
    elif fault == "plan":
        plan = interactions.ResumeDispatchPlan(
            attempt_id=plan.attempt_id,
            canonical_bytes=plan.canonical_bytes.replace(b'"task-1"', b'"task-other"'),
        )
    else:
        async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
            if fault == "expiry":
                await execute_sql(
                    cur,
                    "UPDATE {} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=%s".format(
                        qualified(run_chat_schema, "kokoro_agent_run")
                    ),
                    (run.run_id,),
                )
            elif fault == "body":
                await execute_sql(
                    cur,
                    "UPDATE {} SET body=body || ' ' WHERE run_id=%s AND command_id='command-1'".format(
                        qualified(run_chat_schema, "kokoro_agent_run_control_command")
                    ),
                    (run.run_id,),
                )
            else:
                await execute_sql(
                    cur,
                    "UPDATE {} SET request_digest='sha256:wrong' WHERE run_id=%s AND command_id='command-1'".format(
                        qualified(run_chat_schema, "kokoro_agent_run_control_command")
                    ),
                    (run.run_id,),
                )
    before = await _facts(run_chat_database_url, run_chat_schema, run)
    with pytest.raises((interactions.InteractionAuthorityLost, ValueError)):
        if fault in ("body", "digest"):
            await run_repository.accept_resume(run, "command-1", "worker-other")
        else:
            await run_repository.start_resume(run, lease, "command-1", plan)
    assert await _facts(run_chat_database_url, run_chat_schema, run) == before


async def test_validation_repause_keeps_original_intent_snapshot_and_historical_replay(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    run, accepted = await _accepted(
        run_repository, run_chat_database_url, run_chat_schema
    )
    await run_repository.start_resume(run, accepted.lease, "command-1", _plan())
    original = await _facts(run_chat_database_url, run_chat_schema, run)
    value, _ = _pause_snapshot(run)
    value["groups"][0]["items"][0]["validation"] = {
        "code": "json_schema_invalid",
        "instance_path": ["next"],
    }
    raw = _canonical(value)
    new_pause = interactions.DurablePauseSnapshot(
        pause_ref="pause-opaque-2",
        canonical_bytes=raw,
        digest=hashlib.sha256(raw).hexdigest(),
    )
    await run_repository.record_pause(run, accepted.lease, new_pause)
    repaused = await _facts(run_chat_database_url, run_chat_schema, run)
    assert repaused["run"]["pause_revision"] == 2
    assert (
        repaused["commands"][0]["resume_pause_snapshot_json"]
        == original["commands"][0]["resume_pause_snapshot_json"]
    )
    assert repaused["commands"][0]["resume_intent_status"] == "reconciled"
    replay = await run_repository.accept_resume(run, "command-1", "worker-other")
    assert isinstance(replay, interactions.ReplayedResume)
    assert (
        replay.original_intent is not None
        and replay.original_intent.pause_revision == 1
    )
    assert (
        replay.accepted_source_index
        == original["commands"][0]["resume_accepted_source_index"]
    )
    assert await _facts(run_chat_database_url, run_chat_schema, run) == repaused


async def test_terminal_absorbs_interaction_and_purge_blocks_late_recreation(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    from kokoro_agent.domain.run.models import (
        ExecutionTerminalAuthority,
        RunTerminalOutcome,
    )
    from kokoro_agent.protocol import RunCompletedPayload

    run, accepted = await _accepted(
        run_repository, run_chat_database_url, run_chat_schema
    )
    await run_repository.start_resume(run, accepted.lease, "command-1", _plan())
    await run_repository.mark_resume_unknown(
        run, accepted.lease, "command-1", "attempt-1"
    )
    outcome = RunTerminalOutcome(
        payload=RunCompletedPayload(status="completed"), usage=None
    )
    authority = ExecutionTerminalAuthority(lease=accepted.lease)
    result = await run_repository.finalize_terminal(run.run_id, authority, outcome, ())
    assert result.status == "committed"
    terminal = await _facts(run_chat_database_url, run_chat_schema, run)
    assert terminal["run"]["terminal"] is True
    assert terminal["run"]["pending_groups_json"] == []
    assert terminal["run"]["pause_snapshot_json"] is not None
    assert terminal["commands"][0]["resume_intent_status"] == "terminal"
    assert [row["event_type"] for row in terminal["chat"]] == [
        "interaction.state",
        "interaction.state",
        "interaction.state",
        "run.completed",
    ]
    assert json.loads(terminal["chat"][-2]["payload_json"])["phase"] == "terminal"
    assert (
        await run_repository.finalize_terminal(run.run_id, authority, outcome, ())
    ).status == "replayed"
    assert await _facts(run_chat_database_url, run_chat_schema, run) == terminal
    with pytest.raises(interactions.InteractionAuthorityLost):
        await run_repository.mark_resume_unknown(
            run, accepted.lease, "command-1", "attempt-1"
        )
    assert await run_repository.purge_terminal(0) == 1
    assert await run_repository.read_interaction(run) is None
    with pytest.raises(interactions.InteractionRunMissing):
        await run_repository.admit_control(run.run_id, "late", "sha256:late", "{}")
    assert not await run_repository.record_control_delivery(
        run.run_id, "command-1", None, None, "{}"
    )
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT count(*) AS count FROM {} WHERE run_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_run_control_command")
            ),
            (run.run_id,),
        )
        row = await fetch_one(cur)
        assert row is not None and row["count"] == 0


@pytest.mark.parametrize("drift", ["time_precision", "constraint", "index", "column"])
async def test_interaction_schema_catalog_rejects_exact_drift(
    run_chat_database_url: str,
    run_chat_schema: str,
    drift: str,
) -> None:
    from kokoro_agent.infrastructure.schema import (
        SchemaNotReadyError,
        verify_agent_schema,
    )

    async with connect_pg(run_chat_database_url) as conn:
        await verify_agent_schema(conn, run_chat_schema)
        async with conn.cursor() as cur:
            command = qualified(run_chat_schema, "kokoro_agent_run_control_command")
            head = qualified(run_chat_schema, "kokoro_agent_run")
            if drift == "time_precision":
                sql = f"ALTER TABLE {command} ALTER COLUMN resume_started_at TYPE timestamptz(6)"
            elif drift == "constraint":
                sql = f"ALTER TABLE {head} DROP CONSTRAINT ck_run_interaction_revision; ALTER TABLE {head} ADD CONSTRAINT ck_run_interaction_revision CHECK (pause_revision >= -1 AND interaction_revision >= pause_revision)"
            elif drift == "index":
                sql = f"DROP INDEX {qualified(run_chat_schema, 'uq_control_resume_attempt')}"
            else:
                sql = f"ALTER TABLE {head} ALTER COLUMN interaction_revision DROP NOT NULL"
            await execute_sql(cur, sql)
        with pytest.raises(SchemaNotReadyError):
            await verify_agent_schema(conn, run_chat_schema)


async def test_competing_full_set_commands_and_duplicate_starts_have_one_winner(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    run, lease = await _launch_claim(
        run_repository, run_chat_database_url, run_chat_schema
    )
    assert lease is not None
    _, raw = _pause_snapshot(run)
    await run_repository.record_pause(run, lease, _pause_value(raw))
    for command in ("command-1", "command-2"):
        await _admit(
            run_repository,
            run,
            run_chat_database_url,
            run_chat_schema,
            command_id=command,
        )
    results = await asyncio.gather(
        run_repository.accept_resume(run, "command-1", "worker-a"),
        run_repository.accept_resume(run, "command-2", "worker-b"),
        return_exceptions=True,
    )
    winners = [
        result for result in results if isinstance(result, interactions.AcceptedResume)
    ]
    assert len(winners) == 1
    assert (
        sum(isinstance(result, interactions.InteractionConflict) for result in results)
        == 1
    )
    winner = winners[0]
    assert winner.snapshot.state.intent is not None
    command_id = winner.snapshot.state.intent.command_id
    starts = await asyncio.gather(
        run_repository.start_resume(run, winner.lease, command_id, _plan()),
        run_repository.start_resume(run, winner.lease, command_id, _plan()),
    )
    assert sum(isinstance(result, interactions.StartedResume) for result in starts) == 1
    assert (
        sum(isinstance(result, interactions.ReplayedResume) for result in starts) == 1
    )
    rows = await _facts(run_chat_database_url, run_chat_schema, run)
    assert len(rows["chat"]) == 2
    assert sum(row["resume_intent_status"] is not None for row in rows["commands"]) == 1


async def test_start_checks_database_expiry_after_real_run_lock_wait(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    run, accepted = await _accepted(
        run_repository, run_chat_database_url, run_chat_schema
    )
    async with connect_pg(run_chat_database_url) as blocker:
        async with blocker.cursor() as cur:
            await execute_sql(cur, "BEGIN")
            await execute_sql(cur, "SELECT pg_backend_pid() AS pid")
            pid_row = await fetch_one(cur)
            assert pid_row is not None
            blocker_pid = pid_row["pid"]
            await execute_sql(
                cur,
                "SELECT run_id FROM {} WHERE run_id=%s FOR UPDATE".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
                (run.run_id,),
            )
            task = asyncio.create_task(
                run_repository.start_resume(run, accepted.lease, "command-1", _plan())
            )
            try:
                # The fixture owns this DB and exact blocking backend. No generic
                # instance waiter count, sleeps or timeout increase proves arrival.
                async with (
                    asyncio.timeout(5),
                    connect_pg(run_chat_database_url) as observer,
                    observer.cursor() as probe,
                ):
                    while True:
                        await execute_sql(
                            probe,
                            """SELECT pid FROM pg_stat_activity
                            WHERE datname=current_database() AND %s=ANY(pg_blocking_pids(pid))""",
                            (blocker_pid,),
                        )
                        blocked = await fetch_all(probe)
                        if len(blocked) == 1:
                            break
                        assert not task.done(), "start did not wait on its Run lock"
                await execute_sql(
                    cur,
                    "UPDATE {} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=%s".format(
                        qualified(run_chat_schema, "kokoro_agent_run")
                    ),
                    (run.run_id,),
                )
                await blocker.commit()
                before = await _facts(run_chat_database_url, run_chat_schema, run)
                with pytest.raises(interactions.InteractionAuthorityLost):
                    await task
                assert (
                    await _facts(run_chat_database_url, run_chat_schema, run) == before
                )
            finally:
                await blocker.rollback()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_start_write_failure_rolls_back_attempt_without_dispatch_permission(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    run, accepted = await _accepted(
        run_repository, run_chat_database_url, run_chat_schema
    )
    before = await _facts(run_chat_database_url, run_chat_schema, run)
    await _install_write_failure(
        run_chat_database_url,
        run_chat_schema,
        stage="command",
        phase="dispatch_started",
    )
    with pytest.raises(RaiseException, match="injected_hitl_write_failure"):
        await run_repository.start_resume(run, accepted.lease, "command-1", _plan())
    assert await _facts(run_chat_database_url, run_chat_schema, run) == before


async def test_terminal_chat_failure_rolls_back_intent_head_and_original_terminal_outbox(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    from kokoro_agent.domain.run.models import (
        ExecutionTerminalAuthority,
        RunTerminalOutcome,
    )
    from kokoro_agent.protocol import RunCompletedPayload

    run, accepted = await _accepted(
        run_repository, run_chat_database_url, run_chat_schema
    )
    before = await _facts(run_chat_database_url, run_chat_schema, run)
    await _install_write_failure(
        run_chat_database_url, run_chat_schema, stage="chat", phase="terminal"
    )
    with pytest.raises(RaiseException, match="injected_hitl_write_failure"):
        await run_repository.finalize_terminal(
            run.run_id,
            ExecutionTerminalAuthority(lease=accepted.lease),
            RunTerminalOutcome(
                payload=RunCompletedPayload(status="completed"), usage=None
            ),
            (),
        )
    assert await _facts(run_chat_database_url, run_chat_schema, run) == before
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT count(*) AS count FROM {} WHERE run_id=%s".format(
                qualified(run_chat_schema, "kokoro_agent_run_outbox")
            ),
            (run.run_id,),
        )
        row = await fetch_one(cur)
        assert row is not None and row["count"] == 0


async def _wait_for_owned_lock_queue(
    url: str, blocker_pid: int, count: int
) -> set[int]:
    """Exact owned blocker ancestry, including PostgreSQL soft lock-queue edges."""
    async with asyncio.timeout(5), connect_pg(url) as conn, conn.cursor() as cur:
        while True:
            await execute_sql(
                cur,
                """WITH RECURSIVE waiters(pid) AS (
                SELECT %s::integer
                UNION
                SELECT a.pid FROM pg_stat_activity a JOIN waiters w
                  ON w.pid=ANY(pg_blocking_pids(a.pid))
                WHERE a.datname=current_database()
            ) SELECT pid FROM waiters WHERE pid <> %s""",
                (blocker_pid, blocker_pid),
            )
            pids = {int(row["pid"]) for row in await fetch_all(cur)}
            if len(pids) == count:
                return pids


@pytest.mark.parametrize("operation", ["admit", "delivery"])
@pytest.mark.parametrize("first", ["purge", "control"])
async def test_purge_and_control_creation_have_no_orphans_in_both_lock_orders(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    operation: str,
    first: str,
) -> None:
    from kokoro_agent.domain.run.models import (
        ExecutionTerminalAuthority,
        RunTerminalOutcome,
    )
    from kokoro_agent.interfaces.http.ingress import IngressError
    from kokoro_agent.protocol import RunCompletedPayload

    run, accepted = await _accepted(
        run_repository, run_chat_database_url, run_chat_schema
    )
    ingress = AgentIngress(
        bus=FakeBus(),
        run_repository=run_repository,
        chat_service=ChatService(
            PostgresChatRepository(run_chat_database_url, run_chat_schema)
        ),
    )
    cancel_body: dict[str, object] = {
        "kind": "run.cancel",
        "session_id": run.session_id,
    }
    # Delivery updates an already admitted command; it never creates a child.
    await ingress.control(
        run.run_id,
        cancel_body,
        command_id="delivery-cmd",
        execution_identity=run.execution_identity,
    )
    facts = await _facts(run_chat_database_url, run_chat_schema, run)
    delivery = next(
        command
        for command in facts["commands"]
        if command["command_id"] == "delivery-cmd"
    )
    result = await run_repository.finalize_terminal(
        run.run_id,
        ExecutionTerminalAuthority(lease=accepted.lease),
        RunTerminalOutcome(payload=RunCompletedPayload(status="completed"), usage=None),
        (),
    )
    assert result.status == "committed"

    async def control() -> object:
        if operation == "admit":
            return await ingress.control(
                run.run_id,
                cancel_body,
                command_id="late-cmd",
                execution_identity=run.execution_identity,
            )
        return await run_repository.record_control_delivery(
            run.run_id,
            "delivery-cmd",
            delivery["request_digest"],
            None,
            delivery["body"],
        )

    async def purge() -> object:
        return await run_repository.purge_terminal(0)

    tasks: dict[str, asyncio.Task[object]] = {}
    async with connect_pg(run_chat_database_url) as blocker, blocker.cursor() as cur:
        await execute_sql(cur, "BEGIN")
        await execute_sql(cur, "SELECT pg_backend_pid() AS pid")
        pid_row = await fetch_one(cur)
        assert pid_row is not None
        pid = int(pid_row["pid"])
        await execute_sql(
            cur,
            "SELECT run_id FROM {} WHERE run_id=%s FOR UPDATE".format(
                qualified(run_chat_schema, "kokoro_agent_run")
            ),
            (run.run_id,),
        )
        try:
            operations = {"control": control, "purge": purge}
            second = "control" if first == "purge" else "purge"
            tasks[first] = asyncio.create_task(operations[first]())
            first_pids = await _wait_for_owned_lock_queue(run_chat_database_url, pid, 1)
            tasks[second] = asyncio.create_task(operations[second]())
            both = await _wait_for_owned_lock_queue(run_chat_database_url, pid, 2)
            assert first_pids < both
            assert all(not task.done() for task in tasks.values())
            await blocker.commit()
            results = await asyncio.gather(
                tasks["purge"], tasks["control"], return_exceptions=True
            )
            assert results[0] == 1  # exactly one owner purge, never a retry loop
            if operation == "delivery":
                # Terminal or missing Run has no delivery eligibility in either order.
                assert results[1] is False
            elif first == "purge":
                error = results[1]
                assert isinstance(error, IngressError)
                assert error.status == 404 and error.code == "run_not_found"
            else:
                receipt = results[1]
                assert isinstance(receipt, dict) and receipt["status"] == "pending"
        finally:
            await blocker.rollback()
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        for table in (
            "kokoro_agent_run",
            "kokoro_agent_run_control_command",
            "kokoro_agent_run_dispatch",
        ):
            await execute_sql(
                cur,
                "SELECT count(*) AS count FROM {} WHERE run_id=%s".format(
                    qualified(run_chat_schema, table)
                ),
                (run.run_id,),
            )
            row = await fetch_one(cur)
            assert row is not None and row["count"] == 0


def _bridge_capability(repository: RunRepository) -> Any:
    """An explicit body failure, never a collection/import error or fake port."""
    for name in (
        "record_checkpoint_observation",
        "read_checkpoint_observations",
        "reconcile_resume",
        "record_reconcile_probe",
        "reset_reconcile_probe",
        "list_unsettled_interactions",
    ):
        assert callable(getattr(repository, name, None)), (
            f"Approved real RunRepository.{name} is missing: native bridge not implemented"
        )
    return cast(Any, repository)


async def test_bridge_r35_real_schema_has_exact_observation_identity_and_precision(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    """Schema existence must fail by assertion, before querying a missing table."""
    del run_repository  # Fixture installs the actual canonical owner schema.
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT to_regclass(%s)::text AS relation",
            (qualified(run_chat_schema, "kokoro_agent_run_checkpoint_observation"),),
        )
        row = await fetch_one(cur)
        assert row is not None and row["relation"] is not None, (
            "Approved Run checkpoint observation table is missing"
        )
        await execute_sql(
            cur,
            "SELECT column_name,data_type,is_nullable,datetime_precision "
            "FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
            (run_chat_schema, "kokoro_agent_run_checkpoint_observation"),
        )
        columns = {item["column_name"]: item for item in await fetch_all(cur)}
        assert set(columns) == {
            "run_id",
            "observation_digest",
            "generation",
            "command_id",
            "attempt_id",
            "kind",
            "disposition",
            "evidence_bytes",
            "probe_read_id",
            "created_at",
        }
        assert columns["generation"]["data_type"] == "bigint"
        assert columns["evidence_bytes"]["data_type"] == "bytea"
        assert columns["created_at"]["data_type"] == "timestamp with time zone"
        assert columns["created_at"]["datetime_precision"] == 3
        assert {
            key for key, value in columns.items() if value["is_nullable"] == "YES"
        } == {"command_id", "attempt_id", "probe_read_id"}
        await execute_sql(
            cur,
            "SELECT c.conname,c.contype,pg_get_constraintdef(c.oid) AS definition "
            "FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid "
            "JOIN pg_namespace n ON n.oid=t.relnamespace "
            "WHERE n.nspname=%s AND t.relname=%s",
            (run_chat_schema, "kokoro_agent_run_checkpoint_observation"),
        )
        constraints = await fetch_all(cur)
        assert all(item["contype"] != "f" for item in constraints)
        assert {item["conname"] for item in constraints if item["contype"] == "c"} == {
            "ck_checkpoint_observation_identity",
            "ck_checkpoint_observation_target",
            "ck_checkpoint_observation_kind",
            "ck_checkpoint_observation_bytes",
            "ck_checkpoint_observation_probe",
        }
        assert [
            item["definition"] for item in constraints if item["contype"] == "p"
        ] == ["PRIMARY KEY (run_id, observation_digest)"]
        await execute_sql(
            cur,
            "SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=%s AND tablename=%s",
            (run_chat_schema, "kokoro_agent_run_checkpoint_observation"),
        )
        indexes = {item["indexname"]: item["indexdef"] for item in await fetch_all(cur)}
        assert "UNIQUE" in indexes["uq_checkpoint_observation_probe"]
        assert (
            "(run_id, command_id, attempt_id, probe_read_id)"
            in indexes["uq_checkpoint_observation_probe"]
        )
        assert (
            "WHERE (kind = 'probe'::text)" in indexes["uq_checkpoint_observation_probe"]
        )
        assert (
            "(run_id, command_id, attempt_id, generation, observation_digest)"
            in indexes["idx_checkpoint_observation_target"]
        )


def _bridge_module() -> Any:
    from importlib import import_module
    from importlib.util import find_spec

    name = "kokoro_agent.infrastructure.checkpoint_interactions"
    assert find_spec(name) is not None, "Approved native checkpoint bridge is missing"
    module = import_module(name)
    assert callable(getattr(module, "make_interaction_checkpointer", None))
    for target in ("PauseReadTarget", "ResumeReadTarget"):
        assert callable(getattr(module, target, None)), f"Approved {target} is missing"
    return cast(Any, module)


async def _bridge_observation_rows(
    url: str, schema: str, run_id: str
) -> list[dict[str, Any]]:
    async with connect_pg(url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT to_jsonb(o) AS row FROM {} o WHERE run_id=%s ORDER BY observation_digest".format(
                qualified(schema, "kokoro_agent_run_checkpoint_observation")
            ),
            (run_id,),
        )
        return [row["row"] for row in await fetch_all(cur)]


# SDK objects stay in this test fixture. Native scheduling, checkpoint writes,
# policy collection and resume mapping are all delegated to actual implementations.
class _BridgeState(TypedDict, total=False):
    messages: list[Any]
    approval: JsonValue
    review: JsonValue
    answer: JsonValue


class _ObservedBridgeRunnable:
    """Count the real native entry, without making permission or recovery decisions."""

    def __init__(self, graph: Any) -> None:
        self.graph = graph
        self.calls: list[object] = []
        self.identities: list[dict[str, object]] = []

    async def astream_events(
        self,
        payload: object,
        *,
        version: str,
        config: RunnableConfig,
        transformers: Any,
    ) -> Any:
        self.calls.append(payload)
        metadata = config.get("metadata", {})
        self.identities.append(
            {
                key: metadata[key]
                for key in (
                    "kokoro_run_id",
                    "kokoro_generation",
                    "kokoro_command_id",
                    "kokoro_attempt_id",
                )
                if key in metadata
            }
        )
        return await self.graph.astream_events(
            payload, version=version, config=config, transformers=transformers
        )

    async def aget_state(
        self, config: RunnableConfig, *, subgraphs: bool = False
    ) -> Any:
        return await self.graph.aget_state(config, subgraphs=subgraphs)


@asynccontextmanager
async def _bridge_environment(
    repository: RunRepository,
    url: str,
    schema: str,
    *,
    mixed: bool = True,
    observed_writes: bool = True,
    entered: asyncio.Event | None = None,
    release: asyncio.Event | None = None,
) -> AsyncGenerator[dict[str, Any], None]:
    from deepagents._subagent_transformer import SubagentTransformer
    from langchain_core.messages import AIMessage
    from langgraph.graph import END, START, StateGraph
    from langgraph.prebuilt import ToolCallTransformer
    from langgraph.types import interrupt

    from kokoro_agent.agent_factory import AgentHandle
    from kokoro_agent.execution.protocols import require_agent_runnable
    from kokoro_agent.hitl import InputSubmitted, request_human, request_input
    from kokoro_agent.infrastructure.checkpoints import (
        CheckpointSettings,
        make_checkpointer,
    )
    from kokoro_agent.worker.supervisor import RunSupervisor

    capability = _bridge_capability(repository)
    assert callable(getattr(capability, "read_resume_context", None)), (
        "Approved read_resume_context must return the stored plan, not reconstruct it"
    )
    module = _bridge_module()
    run, lease = await _launch_claim(repository, url, schema)
    effects: list[str] = []

    async def input_node(state: _BridgeState) -> _BridgeState:
        del state
        result = request_input(
            request_id="native-input",
            schema={
                "type": "object",
                "properties": {"otp": {"type": "string"}},
                "required": ["otp"],
                "additionalProperties": False,
            },
            context={"name": "input", "args": {}},
        )
        assert isinstance(result, InputSubmitted)
        if entered is not None:
            entered.set()
        if release is not None:
            await release.wait()
        effects.append("input")
        return {"answer": result.value}

    def approval_node(state: _BridgeState) -> _BridgeState:
        del state
        value = interrupt(
            {
                "action_requests": [
                    {"name": "lookup", "args": {}, "description": "Lookup"}
                ],
                "review_configs": [
                    {
                        "action_name": "lookup",
                        "allowed_decisions": ["approve", "edit", "reject"],
                    }
                ],
            }
        )
        effects.append("approval")
        return {"approval": value}

    def review_node(state: _BridgeState) -> _BridgeState:
        del state
        value = request_human(
            kind="review",
            request_id="native-review",
            context={
                "name": "review",
                "args": {},
                "result": "reviewed",
                "is_error": False,
            },
        )
        effects.append("review")
        return {"review": value}

    settings = CheckpointSettings(database_url=url, schema_name=schema)
    async with (
        make_checkpointer(settings) as saver,
        make_checkpointer(settings) as reader,
    ):
        assert saver is not reader
        wrapped = module.make_interaction_checkpointer(
            saver=saver, reader=reader, repository=repository
        )
        assert callable(getattr(wrapped, "read_interaction", None))
        # Official SDK generic stubs are incomplete; boundary matches native proof.
        native_graph: Any = StateGraph
        child = (
            native_graph(_BridgeState)
            .add_node("ask", input_node)
            .add_edge(START, "ask")
            .add_edge("ask", END)
            .compile()
        )
        builder = native_graph(_BridgeState)
        if mixed:
            builder.add_node("child", child).add_node(
                "approval_node", approval_node
            ).add_node("review_node", review_node)
            for node in ("child", "approval_node", "review_node"):
                builder.add_edge(START, node).add_edge(node, END)
        else:
            builder.add_node("input", input_node).add_edge(START, "input").add_edge(
                "input", END
            )
        # Match the pinned create_agent/create_deep_agent v3 composition. A bare
        # StateGraph has no tool_calls/subagents projections and is not an
        # AgentRunStream; these are actual SDK transformers, not empty channels.
        graph = builder.compile(
            checkpointer=wrapped if observed_writes else saver,
            transformers=[ToolCallTransformer, SubagentTransformer],
        )
        observed_runnable = _ObservedBridgeRunnable(graph)
        handle = AgentHandle(
            runnable=require_agent_runnable(observed_runnable),
            tool_descriptions={
                "lookup": "Lookup",
                "input": "Input",
                "review": "Review",
            },
        )
        config: RunnableConfig = {
            "configurable": {"thread_id": RunScope.of(run).scoped_thread_id},
            "metadata": {
                "kokoro_run_id": run.run_id,
                "kokoro_generation": lease.generation,
            },
        }
        await graph.ainvoke(
            {
                "messages": [
                    AIMessage(
                        content="",
                        id="segment",
                        tool_calls=[
                            {"id": "native-tool", "name": "lookup", "args": {}}
                        ],
                    )
                ]
            },
            config,
        )
        native = await graph.aget_state(config, subgraphs=True)
        assert len(native.interrupts) == (3 if mixed else 1)
        assert effects == []
        materialized = await wrapped.read_interaction(
            request=run,
            lease=lease,
            handle=handle,
            target=module.PauseReadTarget(command_id=None, attempt_id=None),
        )
        assert isinstance(materialized.pause, interactions.DurablePauseSnapshot)
        await capability.record_checkpoint_observation(
            run, lease, materialized.observation
        )
        await repository.record_pause(run, lease, materialized.pause)

        async def build(
            request_value: RunRequest, lease_value: LeaseFence
        ) -> AgentHandle:
            assert request_value.model_dump_json() == run.model_dump_json()
            assert lease_value.generation >= lease.generation
            return handle

        # This is the approved production constructor injection, not a test-only
        # scheduler or permission shim. Missing constructor support remains RED.
        def tool_names(_request: RunRequest) -> frozenset[str]:
            return frozenset({"lookup"})

        def trace(_request: RunRequest) -> None:
            return None

        def source(_name: str) -> str:
            return "runtime-custom"

        constructor: Any = RunSupervisor
        supervisor = constructor(
            agent_builder=build,
            run_repository=repository,
            approval_tool_names=tool_names,
            trace_factory=trace,
            source_for=source,
            consumer="worker-bridge",
            interaction_reader=wrapped.read_interaction,
            chat_repository=PostgresChatRepository(url, schema),
        )
        env: dict[str, Any] = {
            "module": module,
            "repository": capability,
            "run": run,
            "lease": lease,
            "wrapped": wrapped,
            "reader": reader,
            "graph": graph,
            "handle": handle,
            "config": config,
            "native": native,
            "pause": materialized,
            "effects": effects,
            "native_calls": observed_runnable.calls,
            "native_identities": observed_runnable.identities,
            "supervisor": supervisor,
            "bus": FakeBus(),
            "url": url,
            "schema": schema,
        }
        try:
            yield env
        finally:
            if release is not None:
                release.set()
            tasks = tuple(supervisor.tasks.values()) + tuple(
                supervisor.control_listeners.values()
            )
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)


async def _bridge_admit(
    env: dict[str, Any],
    *,
    fault: str | None = None,
    command_id: str = "bridge-command",
    input_value: JsonValue = "valid",
) -> Any:
    from kokoro_agent.protocol import RunResume

    repository, run = env["repository"], env["run"]
    snapshot = await repository.read_interaction(run)
    assert snapshot is not None and snapshot.pause is not None
    raw = json.loads(snapshot.pause.canonical_bytes)
    decisions: list[dict[str, Any]] = []
    for group in raw["groups"]:
        for item in group["items"]:
            decision: dict[str, Any] = {"item_id": item["item_id"], "type": "approve"}
            if item["kind"] == "input":
                decision = {
                    "item_id": item["item_id"],
                    "type": "submit",
                    "value": {"otp": input_value},
                }
            decisions.append(decision)
    if fault == "missing":
        decisions.pop()
    elif fault == "duplicate":
        decisions.append(dict(decisions[0]))
    body: dict[str, Any] = {
        "kind": "run.resume",
        "session_id": run.session_id,
        "expected_pause_revision": snapshot.state.pause_revision
        + (1 if fault == "stale" else 0),
        "pause_ref": snapshot.state.pause_ref,
        "decisions": list(reversed(decisions)),
    }
    ingress = AgentIngress(
        bus=env["bus"],
        run_repository=repository,
        chat_service=ChatService(PostgresChatRepository(env["url"], env["schema"])),
    )
    await ingress.control(
        run.run_id,
        body,
        command_id=command_id,
        execution_identity=run.execution_identity,
    )
    facts = await _facts(env["url"], env["schema"], run)
    command = next(c for c in facts["commands"] if c["command_id"] == command_id)
    typed = RunResume.model_validate_json(command["body"])
    assert typed.model_dump_json().encode() == command["body"].encode()
    return typed


async def _bridge_prepare(env: dict[str, Any], command: Any) -> tuple[Any, Any]:
    accepted = await env["repository"].accept_resume(
        env["run"], command.command_id, "worker-bridge"
    )
    assert isinstance(accepted, interactions.AcceptedResume)
    prepared = await env["wrapped"].read_interaction(
        request=env["run"],
        lease=accepted.lease,
        handle=env["handle"],
        target=env["module"].ResumeReadTarget(command_id=command.command_id),
    )
    assert isinstance(prepared.plan, interactions.ResumeDispatchPlan)
    assert isinstance(prepared.command.resume, dict)
    env["accepted"] = accepted
    env["prepared"] = prepared
    return accepted, prepared


async def _bridge_start_and_invoke(env: dict[str, Any], command: Any) -> None:
    accepted, prepared = await _bridge_prepare(env, command)
    started = await env["repository"].start_resume(
        env["run"], accepted.lease, command.command_id, prepared.plan
    )
    assert isinstance(started, interactions.StartedResume)
    independent = await _facts(env["url"], env["schema"], env["run"])
    row = next(
        c for c in independent["commands"] if c["command_id"] == command.command_id
    )
    assert row["resume_attempt_id"] == started.attempt_id
    assert row["resume_intent_status"] == "dispatch_started"
    config = {
        **env["config"],
        "metadata": {
            "kokoro_run_id": env["run"].run_id,
            "kokoro_generation": accepted.lease.generation,
            "kokoro_command_id": command.command_id,
            "kokoro_attempt_id": started.attempt_id,
        },
    }
    await env["graph"].ainvoke(prepared.command, config)


async def test_bridge_r35_mixed_root_child_full_map_and_stored_plan_identity(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    async with _bridge_environment(
        run_repository, run_chat_database_url, run_chat_schema
    ) as env:
        command = await _bridge_admit(env)
        accepted, prepared = await _bridge_prepare(env, command)
        expected_ids = {item.id for item in env["native"].interrupts}
        assert set(prepared.command.resume) == expected_ids
        assert len(prepared.command.resume) == 3
        pause = json.loads(env["pause"].pause.canonical_bytes)
        namespaces = {group["checkpoint_ns"] for group in pause["locator"]["groups"]}
        assert "" in namespaces and any(
            namespace.startswith("child:") for namespace in namespaces
        )
        assert sum(len(group["items"]) for group in pause["groups"]) == 3
        started = await run_repository.start_resume(
            env["run"], accepted.lease, command.command_id, prepared.plan
        )
        assert isinstance(started, interactions.StartedResume)
        context = await env["repository"].read_resume_context(
            env["run"], command.command_id
        )
        assert context.dispatch_plan.canonical_bytes == prepared.plan.canonical_bytes
        assert context.attempt_generation == accepted.lease.generation
        recovery = await env["wrapped"].read_interaction(
            request=env["run"],
            lease=accepted.lease,
            handle=env["handle"],
            target=env["module"].ResumeReadTarget(command_id=command.command_id),
        )
        assert not hasattr(recovery, "command") and not hasattr(recovery, "plan")
        assert env["effects"] == []
        # Real public supervisor must reconcile the already-started intent, never
        # reconstruct a Command from the still-identical pending native snapshot.
        await env["supervisor"].dispatch(env["bus"], command)
        for task in tuple(env["supervisor"].tasks.values()):
            await task
        assert env["effects"] == []
        after = await env["repository"].read_resume_context(
            env["run"], command.command_id
        )
        assert after.dispatch_plan.canonical_bytes == prepared.plan.canonical_bytes
        assert after.intent.attempt_id == started.attempt_id
        assert env["native_calls"] == []


@pytest.mark.parametrize("fault", ["missing", "duplicate", "stale"])
async def test_bridge_r35_invalid_full_set_has_zero_native_effects(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    fault: str,
) -> None:
    from kokoro_agent.interfaces.http.ingress import IngressError

    async with _bridge_environment(
        run_repository, run_chat_database_url, run_chat_schema
    ) as env:
        before = await run_repository.read_interaction(env["run"])
        if fault == "duplicate":
            with pytest.raises(IngressError) as caught:
                await _bridge_admit(env, fault=fault)
            assert caught.value.status == 400
        else:
            command = await _bridge_admit(env, fault=fault)
            await env["supervisor"].dispatch(env["bus"], command)
            for task in tuple(env["supervisor"].tasks.values()):
                await task
        after = await run_repository.read_interaction(env["run"])
        assert before is not None and after is not None
        assert after.state.phase == before.state.phase
        assert after.state.groups == before.state.groups
        assert after.state.pause_revision == before.state.pause_revision
        assert env["effects"] == []
        facts = await _facts(run_chat_database_url, run_chat_schema, env["run"])
        assert all(row["resume_started_at"] is None for row in facts["commands"])
        assert env["native_calls"] == []


@pytest.mark.parametrize(
    "fault", ["start_rollback", "lost_start_ack", "unknown", "duplicate_delivery"]
)
async def test_bridge_r35_real_start_barrier_never_reinvokes_unknown_attempt(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    fault: str,
) -> None:
    async with _bridge_environment(
        run_repository, run_chat_database_url, run_chat_schema
    ) as env:
        command = await _bridge_admit(env)
        if fault == "start_rollback":
            await _install_write_failure(
                run_chat_database_url,
                run_chat_schema,
                stage="command",
                phase="dispatch_started",
            )
        elif fault in {"lost_start_ack", "unknown"}:
            accepted, prepared = await _bridge_prepare(env, command)
            assert isinstance(
                await run_repository.start_resume(
                    env["run"], accepted.lease, command.command_id, prepared.plan
                ),
                interactions.StartedResume,
            )
            # The committed response is deliberately not used to execute native.
            if fault == "unknown":
                await run_repository.mark_resume_unknown(
                    env["run"],
                    accepted.lease,
                    command.command_id,
                    prepared.plan.attempt_id,
                )
        try:
            await env["supervisor"].dispatch(env["bus"], command)
        except RaiseException as error:
            # Either the public supervisor propagates this exact storage fault
            # or its existing failure boundary finalizes it; neither may invoke.
            assert fault == "start_rollback"
            assert "injected_hitl_write_failure" in str(error)
        for task in tuple(env["supervisor"].tasks.values()):
            await task
        if fault == "duplicate_delivery":
            assert sorted(env["effects"]) == ["approval", "input", "review"]
            await env["supervisor"].dispatch(env["bus"], command)
            for task in tuple(env["supervisor"].tasks.values()):
                await task
            assert sorted(env["effects"]) == ["approval", "input", "review"]
        else:
            assert env["effects"] == []
        assert len(env["native_calls"]) == (1 if fault == "duplicate_delivery" else 0)
        facts = await _facts(run_chat_database_url, run_chat_schema, env["run"])
        if fault == "duplicate_delivery":
            command_row = facts["commands"][0]
            assert env["native_identities"] == [
                {
                    "kokoro_run_id": env["run"].run_id,
                    "kokoro_generation": command_row["resume_attempt_generation"],
                    "kokoro_command_id": command.command_id,
                    "kokoro_attempt_id": command_row["resume_attempt_id"],
                }
            ]
        if fault == "start_rollback":
            assert facts["commands"][0]["resume_started_at"] is None
        elif fault in {"lost_start_ack", "unknown"}:
            assert (
                facts["commands"][0]["resume_attempt_id"]
                == env["prepared"].plan.attempt_id
            )


async def _bridge_observe(env: dict[str, Any], command: Any) -> Any:
    result = await env["wrapped"].read_interaction(
        request=env["run"],
        lease=env["accepted"].lease,
        handle=env["handle"],
        target=env["module"].ResumeReadTarget(command_id=command.command_id),
    )
    assert not hasattr(result, "command") and not hasattr(result, "plan")
    assert isinstance(result.observations, tuple)
    return result


async def _bridge_commit_observed(env: dict[str, Any], observed: Any) -> Any:
    for observation in observed.observations:
        await env["repository"].record_checkpoint_observation(
            env["run"], env["accepted"].lease, observation
        )
    return await env["repository"].reconcile_resume(
        env["run"], env["accepted"].lease, observed.evidence
    )


async def test_bridge_r35_native_commit_without_observer_recovers_from_independent_reader(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    # Compile the real graph with the raw official saver solely for this failure
    # boundary: native commits happen, while the post-save observer never runs.
    # The production reader/classifier and real owner repository perform recovery.
    async with _bridge_environment(
        run_repository,
        run_chat_database_url,
        run_chat_schema,
        mixed=False,
        observed_writes=False,
    ) as env:
        command = await _bridge_admit(env)
        before = await _bridge_observation_rows(
            run_chat_database_url, run_chat_schema, env["run"].run_id
        )
        await _bridge_start_and_invoke(env, command)
        assert env["effects"] == ["input"]
        assert (
            await _bridge_observation_rows(
                run_chat_database_url, run_chat_schema, env["run"].run_id
            )
            == before
        )
        observed = await _bridge_observe(env, command)
        assert observed.evidence.disposition == "active"
        committed = await _bridge_commit_observed(env, observed)
        assert committed.snapshot.state.phase.value == "active"
        facts = await _facts(run_chat_database_url, run_chat_schema, env["run"])
        assert facts["commands"][0]["resume_intent_status"] == "reconciled"
        state = json.loads(facts["chat"][-1]["payload_json"])
        assert (
            state["groups"] == []
            and state["action_result"]["kind"] == "native_consumed"
        )
        replay = await _bridge_commit_observed(env, observed)
        assert isinstance(replay, interactions.ReplayedResume)
        assert await _facts(run_chat_database_url, run_chat_schema, env["run"]) == facts
        assert env["effects"] == ["input"]


async def test_bridge_r35_same_id_validation_new_rounds_then_stale_pg_vector_is_unknown(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    from langgraph._internal._constants import RESUME

    async with _bridge_environment(
        run_repository,
        run_chat_database_url,
        run_chat_schema,
        mixed=False,
        observed_writes=False,
    ) as env:
        original_config: RunnableConfig = {
            "configurable": {
                key: value
                for key in ("thread_id", "checkpoint_ns", "checkpoint_id")
                if isinstance(
                    value := env["native"].config.get("configurable", {}).get(key), str
                )
            }
        }
        interrupt_id = env["native"].interrupts[0].id
        received_values: list[Any] = []
        refs = [env["pause"].pause.pause_ref]
        for index, value in enumerate((987654321, 987654321, "valid"), start=1):
            command = await _bridge_admit(
                env, command_id=f"validation-{index}", input_value=value
            )
            await _bridge_start_and_invoke(env, command)
            received_values.append(
                json.loads(json.dumps(env["prepared"].command.resume[interrupt_id]))
            )
            observed = await _bridge_observe(env, command)
            if index < 3:
                assert observed.evidence.disposition == "waiting"
                committed = await _bridge_commit_observed(env, observed)
                assert committed.snapshot.state.pause_revision == index + 1
                refs.append(committed.snapshot.state.pause_ref)
                native = await env["graph"].aget_state(env["config"])
                assert native.interrupts[0].id == interrupt_id
                facts = await _facts(run_chat_database_url, run_chat_schema, env["run"])
                public = json.loads(facts["chat"][-1]["payload_json"])
                assert public["phase"] == "waiting"
                assert public["action_result"] == {
                    "command_id": command.command_id,
                    "pause_revision": index,
                    "kind": "validation_failed",
                }
                item = public["groups"][0]["items"][0]
                assert item["validation"] == {
                    "code": "json_schema_invalid",
                    "instance_path": ["otp"],
                }
                assert "987654321" not in facts["chat"][-1]["payload_json"]
                assert env["effects"] == []
            else:
                unknown_type = getattr(interactions, "UnknownResumeEvidence", None)
                assert isinstance(unknown_type, type), (
                    "Approved UnknownResumeEvidence missing"
                )
                assert isinstance(observed.evidence, unknown_type)
                await _bridge_commit_observed(env, observed)
                async with connect_pg(run_chat_database_url) as conn:
                    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

                    await execute_sql(conn, f'SET search_path TO "{run_chat_schema}"')
                    reader = AsyncPostgresSaver(conn)
                    exact = await reader.aget_tuple(original_config)
                    assert exact is not None
                    vectors = [
                        data
                        for _, channel, data in exact.pending_writes or ()
                        if channel == RESUME
                    ]
                    assert vectors == [received_values[:2]]
                    assert vectors != [received_values]
                facts = await _facts(run_chat_database_url, run_chat_schema, env["run"])
                current = json.loads(facts["chat"][-1]["payload_json"])
                assert current["phase"] == "resuming" and current["groups"]
                assert current["action_result"]["kind"] == "unknown"
                assert env["effects"] == ["input"]
                await env["supervisor"].dispatch(env["bus"], command)
                for task in tuple(env["supervisor"].tasks.values()):
                    await task
                assert env["effects"] == ["input"]
        assert env["native_calls"] == []
        assert len(set(refs)) == 3


async def _bridge_failure_trigger(url: str, schema: str, target: str) -> None:
    tables = {
        "observation": "kokoro_agent_run_checkpoint_observation",
        "chat": "kokoro_agent_chat_event",
    }
    assert target in tables
    function = qualified(schema, "fail_bridge_write")
    async with connect_pg(url) as conn:
        await execute_sql(
            conn,
            f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'bridge write fault'; END $$",
        )
        await execute_sql(
            conn,
            f"CREATE TRIGGER bridge_write_fault AFTER INSERT ON {qualified(schema, tables[target])} FOR EACH ROW EXECUTE FUNCTION {function}()",
        )


@pytest.mark.parametrize("target", ["observation", "chat"])
async def test_bridge_r35_observer_or_source_failure_rolls_back_real_transaction(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    target: str,
) -> None:
    async with _bridge_environment(
        run_repository,
        run_chat_database_url,
        run_chat_schema,
        mixed=False,
        observed_writes=False,
    ) as env:
        command = await _bridge_admit(env)
        await _bridge_start_and_invoke(env, command)
        observed = await _bridge_observe(env, command)
        assert observed.evidence.disposition == "active" and observed.observations
        if target == "chat":
            for observation in observed.observations:
                await env["repository"].record_checkpoint_observation(
                    env["run"], env["accepted"].lease, observation
                )
        before = await _facts(run_chat_database_url, run_chat_schema, env["run"])
        observations_before = await _bridge_observation_rows(
            run_chat_database_url, run_chat_schema, env["run"].run_id
        )
        await _bridge_failure_trigger(run_chat_database_url, run_chat_schema, target)
        with pytest.raises(RaiseException, match="bridge write fault"):
            if target == "observation":
                await env["repository"].record_checkpoint_observation(
                    env["run"], env["accepted"].lease, observed.observations[0]
                )
            else:
                await env["repository"].reconcile_resume(
                    env["run"], env["accepted"].lease, observed.evidence
                )
        assert (
            await _facts(run_chat_database_url, run_chat_schema, env["run"]) == before
        )
        assert (
            await _bridge_observation_rows(
                run_chat_database_url, run_chat_schema, env["run"].run_id
            )
            == observations_before
        )
        assert env["effects"] == ["input"]


async def test_bridge_r35_healthy_native_task_three_heartbeats_do_not_fail_or_count(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    async with _bridge_environment(
        run_repository,
        run_chat_database_url,
        run_chat_schema,
        mixed=False,
        entered=entered,
        release=release,
    ) as env:
        command = await _bridge_admit(env)
        await env["supervisor"].dispatch(env["bus"], command)
        async with asyncio.timeout(5):
            await entered.wait()
        assert any(not task.done() for task in env["supervisor"].tasks.values())
        for _ in range(3):
            await env["supervisor"].heartbeat_once(env["bus"])
            facts = await _facts(run_chat_database_url, run_chat_schema, env["run"])
            assert facts["run"]["terminal"] is False
            assert facts["run"]["interaction_phase"] == "resuming"
            assert facts["commands"][0]["resume_probe_count"] == 0
            assert facts["commands"][0]["resume_intent_status"] == "dispatch_started"
            assert env["effects"] == []
        release.set()
        for task in tuple(env["supervisor"].tasks.values()):
            await task
        assert env["effects"] == ["input"]


@pytest.mark.parametrize("operation", ["terminal", "purge"])
@pytest.mark.parametrize("first", ["observer", "owner"])
async def test_bridge_r35_late_observation_and_terminal_purge_use_run_first_lock_order(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    operation: str,
    first: str,
) -> None:
    from kokoro_agent.domain.run.models import (
        ExecutionTerminalAuthority,
        RunTerminalOutcome,
    )
    from kokoro_agent.protocol import RunCompletedPayload

    async with _bridge_environment(
        run_repository,
        run_chat_database_url,
        run_chat_schema,
        mixed=False,
        observed_writes=False,
    ) as env:
        command = await _bridge_admit(env)
        await _bridge_start_and_invoke(env, command)
        observed = await _bridge_observe(env, command)
        observation = next(
            item for item in observed.observations if item.kind == "resume"
        )
        run, lease = env["run"], env["accepted"].lease

        async def terminal() -> Any:
            return await run_repository.finalize_terminal(
                run.run_id,
                ExecutionTerminalAuthority(lease=lease),
                RunTerminalOutcome(
                    payload=RunCompletedPayload(status="completed"), usage=None
                ),
                (),
            )

        if operation == "purge":
            assert (await terminal()).status == "committed"

        async def owner() -> Any:
            return (
                await terminal()
                if operation == "terminal"
                else await run_repository.purge_terminal(0)
            )

        async def observer() -> Any:
            return await env["repository"].record_checkpoint_observation(
                run, lease, observation
            )

        pending: dict[str, asyncio.Task[Any]] = {}
        async with (
            connect_pg(run_chat_database_url) as blocker,
            blocker.cursor() as cur,
        ):
            await execute_sql(cur, "BEGIN")
            await execute_sql(cur, "SELECT pg_backend_pid() AS pid")
            pid_row = await fetch_one(cur)
            assert pid_row is not None
            pid = int(pid_row["pid"])
            await execute_sql(
                cur,
                "SELECT run_id FROM {} WHERE run_id=%s FOR UPDATE".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
                (run.run_id,),
            )
            try:
                operations = {"owner": owner, "observer": observer}
                second = "owner" if first == "observer" else "observer"
                pending[first] = asyncio.create_task(operations[first]())
                one = await _wait_for_owned_lock_queue(run_chat_database_url, pid, 1)
                pending[second] = asyncio.create_task(operations[second]())
                two = await _wait_for_owned_lock_queue(run_chat_database_url, pid, 2)
                assert one < two and all(not task.done() for task in pending.values())
                await blocker.commit()
                results = await asyncio.gather(
                    pending["owner"], pending["observer"], return_exceptions=True
                )
                if operation == "purge":
                    assert results[0] == 1
                    if first == "owner":
                        assert isinstance(
                            results[1], interactions.InteractionRunMissing
                        )
                    else:
                        assert not isinstance(results[1], BaseException)
                        assert results[1].disposition == "audit"
                    assert await run_repository.read_interaction(run) is None
                    assert (
                        await _bridge_observation_rows(
                            run_chat_database_url, run_chat_schema, run.run_id
                        )
                        == []
                    )
                else:
                    assert (
                        not isinstance(results[0], BaseException)
                        and results[0].status == "committed"
                    )
                    assert not isinstance(results[1], BaseException)
                    assert results[1].disposition == (
                        "audit" if first == "owner" else "current"
                    )
                    facts = await _facts(run_chat_database_url, run_chat_schema, run)
                    assert facts["run"]["terminal"] is True
                    assert facts["run"]["pending_groups_json"] == []
                    assert facts["commands"][0]["resume_intent_status"] == "terminal"
                    assert [row["event_type"] for row in facts["chat"][-2:]] == [
                        "interaction.state",
                        "run.completed",
                    ]
                    assert (
                        json.loads(facts["chat"][-2]["payload_json"])["phase"]
                        == "terminal"
                    )
                assert env["effects"] == ["input"]
            finally:
                await blocker.rollback()
                for task in pending.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*pending.values(), return_exceptions=True)


async def test_first_worker_dispatch_seals_usage_before_durable_initial_pause(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    """No pre-invoke: the actual worker owns first native entry and pause commit."""
    from collections.abc import Sequence
    from typing import Protocol, Self, runtime_checkable

    from deepagents._subagent_transformer import SubagentTransformer
    from langchain_core.messages import AIMessage
    from langchain_core.outputs import ChatGeneration, LLMResult
    from langchain_core.runnables.config import get_async_callback_manager_for_config
    from langgraph.checkpoint.base import BaseCheckpointSaver
    from langgraph.graph import END, START, StateGraph
    from langgraph.prebuilt import ToolCallTransformer
    from langgraph.stream import StreamTransformer

    from kokoro_agent.agent_factory import AgentHandle
    from kokoro_agent.execution.protocols import require_agent_runnable
    from kokoro_agent.hitl import request_input
    from kokoro_agent.infrastructure.checkpoint_interactions import (
        make_interaction_checkpointer,
    )
    from kokoro_agent.infrastructure.checkpoints import (
        CheckpointSettings,
        make_checkpointer,
    )
    from kokoro_agent.protocol import SubagentSource
    from kokoro_agent.worker.supervisor import RunSupervisor

    @runtime_checkable
    class NativeBuilder(Protocol):
        def add_node(self, name: str, action: object) -> Self: ...
        def add_edge(self, start: str, end: str) -> Self: ...
        def compile(
            self,
            *,
            checkpointer: BaseCheckpointSaver[str],
            transformers: Sequence[type[StreamTransformer]],
        ) -> object: ...

    def checked_builder(value: object) -> NativeBuilder:
        assert isinstance(value, NativeBuilder)
        return value

    entries: list[str] = []
    effects: list[str] = []

    async def ask(state: _BridgeState, config: RunnableConfig) -> _BridgeState:
        del state
        entries.append("ask")
        # Deterministic native SDK callback fixture; no model/provider is called.
        # Nonzero totals expose a missing/duplicate usage segment in independent SQL.
        manager = get_async_callback_manager_for_config(config)
        runs = await manager.on_llm_start({"name": "native-usage-fixture"}, ["usage"])
        for run_manager in runs:
            await run_manager.on_llm_end(
                LLMResult(
                    generations=[
                        [
                            ChatGeneration(
                                message=AIMessage(
                                    content="",
                                    response_metadata={
                                        "model_name": "native-usage-fixture"
                                    },
                                    usage_metadata={
                                        "input_tokens": 7,
                                        "output_tokens": 3,
                                        "total_tokens": 10,
                                    },
                                ),
                            )
                        ]
                    ]
                )
            )
        request_input(
            request_id="first-input",
            schema={"type": "string"},
            context={"name": "input", "args": {}},
        )
        effects.append("answered")
        return {"answer": "done"}

    url, schema = run_chat_database_url, run_chat_schema
    seed = request(f"worker-first-pause-{uuid4().hex}")
    bus = FakeBus()
    ingress = AgentIngress(
        bus=bus,
        run_repository=run_repository,
        chat_service=ChatService(PostgresChatRepository(url, schema)),
    )
    await ingress.launch(
        {
            "request_id": seed.run_id + "-request",
            "run_id": seed.run_id,
            "session_id": seed.session_id,
            "feature_key": seed.feature_key,
            "selected_skill_source_refs": list(seed.selected_skill_source_refs),
            "message_id": seed.input.message_id,
            "content": seed.input.content,
        },
        execution_identity=seed.execution_identity,
    )
    request_value = await run_repository.get_pending_dispatch(seed.run_id)
    assert request_value is not None
    settings = CheckpointSettings(database_url=url, schema_name=schema)
    async with (
        make_checkpointer(settings) as saver,
        make_checkpointer(settings) as reader,
    ):
        assert saver is not reader
        bridge = make_interaction_checkpointer(
            saver=saver, reader=reader, repository=run_repository
        )
        builder = checked_builder(StateGraph(_BridgeState))
        graph = require_agent_runnable(
            builder.add_node("ask", ask)
            .add_edge(START, "ask")
            .add_edge("ask", END)
            .compile(
                checkpointer=bridge,
                transformers=[ToolCallTransformer, SubagentTransformer],
            )
        )
        handle = AgentHandle(runnable=graph, tool_descriptions={"input": "Input"})

        async def build(run: RunRequest, lease: LeaseFence) -> AgentHandle:
            assert run.model_dump_json() == request_value.model_dump_json()
            assert lease.owner == "first-worker"
            return handle

        def tools(_run: RunRequest) -> frozenset[str]:
            return frozenset()

        def trace(_run: RunRequest) -> None:
            return None

        def source(_name: str) -> SubagentSource:
            return "runtime-custom"

        supervisor = RunSupervisor(
            agent_builder=build,
            run_repository=run_repository,
            approval_tool_names=tools,
            trace_factory=trace,
            source_for=source,
            consumer="first-worker",
            interaction_reader=bridge.read_interaction,
            chat_repository=PostgresChatRepository(url, schema),
        )
        config: RunnableConfig = {
            "configurable": {"thread_id": RunScope.of(request_value).scoped_thread_id}
        }
        assert await reader.aget_tuple(config) is None
        assert entries == []
        try:
            await supervisor.dispatch(bus, request_value)
            tasks = tuple(supervisor.tasks.values())
            assert len(tasks) == 1
            async with asyncio.timeout(5):
                await asyncio.gather(*tasks)
            assert entries == ["ask"] and effects == []
            native = await reader.aget_tuple(config)
            assert native is not None and native.pending_writes
            facts = await _facts(url, schema, request_value)
            assert facts["run"]["interaction_phase"] == "waiting"
            assert facts["run"]["pause_revision"] == 1
            assert facts["run"]["lease_expires_at"] is None
            assert facts["run"]["terminal"] is False
            assert facts["run"]["usage_input_total"] == 7
            assert facts["run"]["usage_output_total"] == 3
            assert facts["commands"] == []
            snapshot = await run_repository.read_interaction(request_value)
            assert snapshot is not None and snapshot.pause is not None
            assert len(snapshot.state.groups) == 1
            assert not {"run.completed", "run.failed"}.intersection(
                bus.kinds(seed.run_id)
            )
        finally:
            tasks = tuple(supervisor.tasks.values()) + tuple(
                supervisor.control_listeners.values()
            )
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
