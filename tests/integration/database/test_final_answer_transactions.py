"""Same-owner PostgreSQL proof and terminal freeze; isolated UUID schema fixture."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import pytest

from kokoro_agent.domain.chat.models import ChatMessageDraft, assistant_message_id
from kokoro_agent.domain.chat.repositories import ChatIdentityConflict
from kokoro_agent.domain.run.models import (
    ExecutionTerminalAuthority,
    CancelTerminalAuthority,
    RunTerminalOutcome,
    RunUsageSegment,
)
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.infrastructure.postgres_chat_repository import PostgresChatRepository
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.schema import RUN_CLAIMS_TABLE
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.protocol import RunCompletedPayload, RunRequest
from support.fakes import request


async def _save(
    chat: PostgresChatRepository,
    run: RunRequest,
    segment: str,
    *,
    wrong_run: bool = False,
) -> None:
    now = datetime.now(UTC)
    await chat.save_message(
        ChatMessageDraft(
            tenant_id=run.execution_identity.tenant_ref,
            namespace=RunScope.of(run).namespace,
            session_id=run.session_id,
            run_id="other" if wrong_run else run.run_id,
            chat_message_id=assistant_message_id(
                RunScope.of(run).namespace, run.run_id, segment
            ),
            role="assistant",
            content="",
            status="completed",
            created_at=now,
            updated_at=now,
        )
    )


@pytest.mark.parametrize("final", [None, "answer"])
async def test_final_binding_usage_and_exact_replay_are_frozen(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
    final: str | None,
) -> None:
    run = request("final-binding")
    lease = await run_repository.try_claim(run, "worker")
    assert lease is not None
    await _save(chat_repository, run, "answer")
    await _save(chat_repository, run, "different")
    authority = ExecutionTerminalAuthority(lease=lease)
    outcome = RunTerminalOutcome(
        payload=RunCompletedPayload(status="completed", final_segment_id=final),
        usage=RunUsageSegment(input_tokens=5, output_tokens=7),
    )
    first = await run_repository.finalize_terminal(run.run_id, authority, outcome, ())
    assert first.status == "committed"
    repeated = await run_repository.finalize_terminal(
        run.run_id, authority, outcome, ()
    )
    assert (
        repeated.status == "replayed"
        and repeated.retained_frames == first.retained_frames
    )
    facts = await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    terminal = json.loads(
        next(e.payload_json for e in facts if e.event_type == "run.completed")
    )
    assert terminal.get("final_message_id") == (
        assistant_message_id(RunScope.of(run).namespace, run.run_id, final)
        if final
        else None
    )
    frame = json.loads(first.retained_frames[-1].payload_json)
    assert frame.get("final_segment_id") == final
    assert (
        frame["token_usage"]
        == terminal["token_usage"]
        == {"input_tokens": 5, "output_tokens": 7}
    )
    for conflicting in [None, "different"] if final else ["answer"]:
        result = await run_repository.finalize_terminal(
            run.run_id,
            authority,
            RunTerminalOutcome(
                payload=RunCompletedPayload(
                    status="completed", final_segment_id=conflicting
                ),
                usage=outcome.usage,
            ),
            (),
        )
        assert result.status == "lost"
    assert (
        await chat_repository.replay(
            run.execution_identity.tenant_ref,
            RunScope.of(run).namespace,
            run.session_id,
        )
        == facts
    )


@pytest.mark.parametrize("wrong_run", [False, True])
async def test_final_reference_is_rechecked_and_entire_terminal_rolls_back(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    wrong_run: bool,
) -> None:
    run = request("bad-final-binding")
    lease = await run_repository.try_claim(run, "worker")
    assert lease is not None
    if wrong_run:
        await _save(chat_repository, run, "answer", wrong_run=True)
    with pytest.raises(ChatIdentityConflict):
        await run_repository.finalize_terminal(
            run.run_id,
            ExecutionTerminalAuthority(lease=lease),
            RunTerminalOutcome(
                payload=RunCompletedPayload(
                    status="completed", final_segment_id="answer"
                ),
                usage=RunUsageSegment(input_tokens=5, output_tokens=7),
            ),
            (),
        )
    assert not await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    assert not await run_repository.list_unpublished_outbox()
    async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT terminal,usage_input_total,usage_output_total FROM {} WHERE run_id=%s".format(
                qualified(run_chat_schema, RUN_CLAIMS_TABLE)
            ),
            (run.run_id,),
        )
        row = await fetch_one(cur)
        assert row == {
            "terminal": False,
            "usage_input_total": 0,
            "usage_output_total": 0,
        }


@pytest.mark.parametrize("cancel_first", [False, True])
async def test_cancel_and_success_winner_preserves_only_its_final_fact(
    run_repository: RunRepository,
    chat_repository: PostgresChatRepository,
    cancel_first: bool,
) -> None:
    run = request("cancel-final-binding")
    lease = await run_repository.try_claim(run, "worker")
    assert lease is not None
    await _save(chat_repository, run, "answer")
    body = '{"kind":"run.cancel","command_id":"cancel-final"}'
    await run_repository.admit_control(run.run_id, "cancel-final", "digest", body)
    assert await run_repository.record_control_delivery(
        run.run_id, "cancel-final", "digest", None, body
    )
    cancel = (
        CancelTerminalAuthority(owner="canceller", command_id="cancel-final"),
        RunTerminalOutcome(payload=RunCompletedPayload(status="cancelled"), usage=None),
    )
    complete = (
        ExecutionTerminalAuthority(lease=lease),
        RunTerminalOutcome(
            payload=RunCompletedPayload(status="completed", final_segment_id="answer"),
            usage=None,
        ),
    )
    winner, loser = (cancel, complete) if cancel_first else (complete, cancel)
    assert (
        await run_repository.finalize_terminal(run.run_id, *winner, ())
    ).status == "committed"
    assert (
        await run_repository.finalize_terminal(run.run_id, *loser, ())
    ).status == "lost"
    facts = await chat_repository.replay(
        run.execution_identity.tenant_ref, RunScope.of(run).namespace, run.session_id
    )
    terminals = [
        json.loads(e.payload_json) for e in facts if e.event_type == "run.completed"
    ]
    assert len(terminals) == 1
    assert terminals[0]["status"] == ("cancelled" if cancel_first else "completed")
    assert ("final_message_id" in terminals[0]) is not cancel_first
