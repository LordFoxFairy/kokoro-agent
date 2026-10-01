"""Real PG static recipe transactions; isolated owner schema, no Redis/provider."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from psycopg.errors import DivisionByZero

from kokoro_agent.domain.run.models import (
    ExecutionTerminalAuthority,
    LeaseFence,
    RunTerminalOutcome,
    StaticRecipeAuthorityLost,
    StaticRecipeBinding,
    StaticRecipeIncompatible,
)
from kokoro_agent.domain.run.repository import RunRepository
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_repository import PostgresRunRepository
import kokoro_agent.infrastructure.postgres_run_profiles as profiles
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.protocol import RunCompletedPayload
from support.fakes import request


def _binding(value: int = 1) -> StaticRecipeBinding:
    raw = json.dumps(
        {"domain_tag": "kokoro-agent:assembly-recipe:1", "value": value},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return StaticRecipeBinding(
        canonical_bytes=raw, fingerprint=hashlib.sha256(raw).hexdigest()
    )


async def _row(url: str, schema: str, run_id: str) -> dict[str, Any]:
    async with connect_pg(url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "SELECT assembly_recipe_bytes, assembly_recipe_fingerprint, updated_at, terminal, lease_generation FROM {} WHERE run_id = %s".format(
                qualified(schema, "kokoro_agent_run")
            ),
            (run_id,),
        )
        row = await fetch_one(cur)
        assert row is not None
        return row


async def _mutate(
    url: str, schema: str, run_id: str, assignment: str, values: tuple[object, ...] = ()
) -> None:
    # Test-owned fixed SQL only. Never takes untrusted identifiers or expressions.
    async with connect_pg(url) as conn, conn.cursor() as cur:
        await execute_sql(
            cur,
            "UPDATE {} SET {} WHERE run_id = %s".format(
                qualified(schema, "kokoro_agent_run"), assignment
            ),
            (*values, run_id),
        )


def _observe_profile_connections(
    monkeypatch: pytest.MonkeyPatch, participants: set[int]
) -> None:
    original = profiles.connect_pg

    @asynccontextmanager
    async def observed(url: str) -> AsyncGenerator[Any]:
        async with original(url) as connection:
            participants.add(connection.info.backend_pid)
            yield connection

    # Observe the real adapter connections, not replacements for PG transactions.
    monkeypatch.setattr(profiles, "connect_pg", observed)


async def _blocked(url: str, blocker: int, participants: set[int], count: int) -> None:
    async with asyncio.timeout(5):
        while True:
            async with connect_pg(url) as conn, conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """WITH RECURSIVE activity AS MATERIALIZED (
                           SELECT pid, pg_blocking_pids(pid) AS blockers
                           FROM pg_stat_activity WHERE datname = current_database()
                       ), rooted(pid) AS (
                           SELECT pid FROM activity WHERE pid = %s
                           UNION
                           SELECT child.pid FROM activity child
                           JOIN rooted parent ON parent.pid = ANY(child.blockers)
                       )
                       SELECT array_agg(pid ORDER BY pid) FILTER (WHERE pid <> %s) AS waiters
                       FROM rooted""",
                    (blocker, blocker),
                )
                row = await fetch_one(cur)
                waiters: set[int] = (
                    set(row["waiters"] or ()) if row is not None else set()
                )
                # Soft queue edges may be participant2 -> participant1 -> blocker.
                # Only the exact real adapter PIDs count, in this owned database's
                # rooted graph; unrelated instance/database waiters cannot satisfy it.
                if len(participants) == count and participants.issubset(waiters):
                    print(
                        f"profile barrier root_pid={blocker} "
                        f"participant_pids={sorted(participants)} rooted_pids={sorted(waiters)}"
                    )
                    return


async def test_freeze_restart_and_idempotent_zero_update(
    run_repository: RunRepository, run_chat_database_url: str, run_chat_schema: str
) -> None:
    req = request("profile-first")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    assert (
        await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
        == "frozen"
    )
    before = await _row(run_chat_database_url, run_chat_schema, req.run_id)
    restarted = PostgresRunRepository(run_chat_database_url, 90_000, run_chat_schema)
    assert (
        await restarted.freeze_or_verify_static_recipe(req, lease, _binding())
        == "matched"
    )
    assert await _row(run_chat_database_url, run_chat_schema, req.run_id) == before
    assert before["assembly_recipe_bytes"] == _binding().canonical_bytes


@pytest.mark.parametrize("different", [False, True])
async def test_two_connections_compete_without_last_writer_wins(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    different: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    participants: set[int] = set()
    _observe_profile_connections(monkeypatch, participants)
    req = request("profile-race")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    tasks: list[asyncio.Task[object]] = []
    try:
        async with connect_pg(run_chat_database_url) as conn:
            async with conn.transaction(), conn.cursor() as cur:
                await execute_sql(
                    cur,
                    "SELECT run_id FROM {} WHERE run_id=%s FOR UPDATE".format(
                        qualified(run_chat_schema, "kokoro_agent_run")
                    ),
                    (req.run_id,),
                )
                tasks = [
                    asyncio.create_task(
                        run_repository.freeze_or_verify_static_recipe(
                            req, lease, _binding(n)
                        )
                    )
                    for n in (1, 2 if different else 1)
                ]
                await _blocked(
                    run_chat_database_url, conn.info.backend_pid, participants, 2
                )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert results.count("frozen") == 1
        if different:
            assert sum(isinstance(x, StaticRecipeIncompatible) for x in results) == 1
        else:
            assert results.count("matched") == 1
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_expiry_rechecked_after_actual_row_lock_wait(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    participants: set[int] = set()
    _observe_profile_connections(monkeypatch, participants)
    req = request("profile-expired")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    task: asyncio.Task[object] | None = None
    try:
        async with connect_pg(run_chat_database_url) as conn:
            async with conn.transaction(), conn.cursor() as cur:
                await execute_sql(
                    cur,
                    "SELECT run_id FROM {} WHERE run_id=%s FOR UPDATE".format(
                        qualified(run_chat_schema, "kokoro_agent_run")
                    ),
                    (req.run_id,),
                )
                task = asyncio.create_task(
                    run_repository.freeze_or_verify_static_recipe(
                        req, lease, _binding()
                    )
                )
                await _blocked(
                    run_chat_database_url, conn.info.backend_pid, participants, 1
                )
                await execute_sql(
                    cur,
                    "UPDATE {} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=%s".format(
                        qualified(run_chat_schema, "kokoro_agent_run")
                    ),
                    (req.run_id,),
                )
        with pytest.raises(StaticRecipeAuthorityLost):
            await task
        assert (await _row(run_chat_database_url, run_chat_schema, req.run_id))[
            "assembly_recipe_bytes"
        ] is None
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("already_frozen", [False, True])
async def test_reclaim_preserves_binding_and_rejects_late_generation(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    already_frozen: bool,
) -> None:
    req = request("profile-takeover")
    old = await run_repository.try_claim(req, "old")
    assert old is not None
    if already_frozen:
        await run_repository.freeze_or_verify_static_recipe(req, old, _binding())
    await _mutate(
        run_chat_database_url,
        run_chat_schema,
        req.run_id,
        "lease_expires_at=clock_timestamp()-interval '1 second'",
    )
    adopted = await run_repository.reclaim_expired("next")
    new = next(item.lease for item in adopted if item.request.run_id == req.run_id)
    with pytest.raises(StaticRecipeAuthorityLost):
        await run_repository.freeze_or_verify_static_recipe(req, old, _binding())
    assert await run_repository.freeze_or_verify_static_recipe(
        req, new, _binding()
    ) == ("matched" if already_frozen else "frozen")
    with pytest.raises(StaticRecipeIncompatible):
        await run_repository.freeze_or_verify_static_recipe(req, new, _binding(2))


@pytest.mark.parametrize("freeze_first", [False, True])
async def test_terminal_and_freeze_orders(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    freeze_first: bool,
) -> None:
    req = request("profile-terminal")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    if freeze_first:
        await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    result = await run_repository.finalize_terminal(
        req.run_id,
        ExecutionTerminalAuthority(lease=lease),
        RunTerminalOutcome(payload=RunCompletedPayload(status="completed"), usage=None),
        (),
    )
    assert result.status == "committed"
    with pytest.raises(StaticRecipeAuthorityLost):
        await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    row = await _row(run_chat_database_url, run_chat_schema, req.run_id)
    assert row["assembly_recipe_bytes"] == (
        _binding().canonical_bytes if freeze_first else None
    )


@pytest.mark.parametrize("failure", ["exception", "statement", "cancel"])
async def test_real_update_rolls_back_on_failure(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    req = request("profile-rollback")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    updated = asyncio.Event()
    release = asyncio.Event()
    original = profiles.execute_sql

    async def execute(cursor: object, query: str, params: object = None) -> None:
        await original(cursor, query, params)
        if "UPDATE" in query:
            updated.set()
            if failure == "statement":
                await original(cursor, "SELECT 1 / 0")
            elif failure == "exception":
                raise RuntimeError("injected after actual UPDATE")
            else:
                await release.wait()

    monkeypatch.setattr(profiles, "execute_sql", execute)
    task = asyncio.create_task(
        run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    )
    try:
        async with asyncio.timeout(5):
            await updated.wait()
        if failure == "cancel":
            assert (await _row(run_chat_database_url, run_chat_schema, req.run_id))[
                "assembly_recipe_bytes"
            ] is None
            task.cancel()
        expected_error = {
            "exception": RuntimeError,
            "statement": DivisionByZero,
            "cancel": asyncio.CancelledError,
        }[failure]
        with pytest.raises(expected_error):
            await task
        assert (await _row(run_chat_database_url, run_chat_schema, req.run_id))[
            "assembly_recipe_bytes"
        ] is None
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_commit_ack_loss_recovers_via_new_connection(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    req = request("profile-ack")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    original = profiles.connect_pg

    @asynccontextmanager
    async def lose_ack(url: str) -> AsyncGenerator[Any]:
        async with original(url) as connection:
            yield connection
        raise ConnectionError("injected after transaction committed")

    with monkeypatch.context() as patch:
        patch.setattr(profiles, "connect_pg", lose_ack)
        with pytest.raises(ConnectionError):
            await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    before = await _row(run_chat_database_url, run_chat_schema, req.run_id)
    restarted = PostgresRunRepository(run_chat_database_url, 90_000, run_chat_schema)
    assert (
        await restarted.freeze_or_verify_static_recipe(req, lease, _binding())
        == "matched"
    )
    assert await _row(run_chat_database_url, run_chat_schema, req.run_id) == before


@pytest.mark.parametrize("change", ["order", "space", "escape", "default", "unknown"])
async def test_original_request_text_is_the_authority(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    change: str,
) -> None:
    req = request("profile-text")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    value = json.loads(req.model_dump_json())
    if change == "order":
        value = dict(reversed(tuple(value.items())))
    elif change == "default":
        value.pop("requested_model_label")
    elif change == "unknown":
        value["unknown"] = None
    raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    if change == "space":
        raw += " "
    elif change == "escape":
        raw = raw.replace("profile-text", "profile-\\u0074ext")
    assert raw.encode() != req.model_dump_json().encode()
    await _mutate(
        run_chat_database_url, run_chat_schema, req.run_id, "request_json=%s", (raw,)
    )
    with pytest.raises(StaticRecipeIncompatible):
        await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    assert (await _row(run_chat_database_url, run_chat_schema, req.run_id))[
        "assembly_recipe_bytes"
    ] is None


@pytest.mark.parametrize("authority", ["tenant", "owner", "generation", "paused"])
async def test_invalid_authority_has_no_write(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    authority: str,
) -> None:
    req = request("profile-authority")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    if authority == "tenant":
        req = req.model_copy(
            update={
                "execution_identity": req.execution_identity.model_copy(
                    update={"tenant_ref": "other"}
                )
            }
        )
    elif authority == "owner":
        lease = LeaseFence(owner="other", generation=lease.generation)
    elif authority == "generation":
        lease = LeaseFence(owner=lease.owner, generation=lease.generation + 1)
    else:
        assert await run_repository.pause(req.run_id, lease)
    with pytest.raises(StaticRecipeAuthorityLost):
        await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    assert (await _row(run_chat_database_url, run_chat_schema, req.run_id))[
        "assembly_recipe_bytes"
    ] is None


@pytest.mark.parametrize(
    "fact",
    [
        "event_index_counter=1",
        "durable_counter=1",
        "usage_input_total=1",
        "token_total=1",
        "sandbox_id='box', sandbox_generation=1, sandbox_backend_kind='docker', sandbox_teardown_ref='box'",
    ],
)
async def test_missing_binding_after_execution_is_corruption(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    fact: str,
) -> None:
    req = request("profile-missing")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    await _mutate(run_chat_database_url, run_chat_schema, req.run_id, fact)
    with pytest.raises(StaticRecipeIncompatible):
        await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())


async def test_resume_adopt_compares_existing_frozen_identity(
    run_repository: RunRepository, run_chat_database_url: str, run_chat_schema: str
) -> None:
    req = request("profile-resume")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    assert await run_repository.pause(req.run_id, lease)
    new = await run_repository.adopt(req.run_id, "resume")
    assert new is not None
    assert (
        await run_repository.freeze_or_verify_static_recipe(req, new, _binding())
        == "matched"
    )
    with pytest.raises(StaticRecipeIncompatible):
        await run_repository.freeze_or_verify_static_recipe(req, new, _binding(2))
    assert (await _row(run_chat_database_url, run_chat_schema, req.run_id))[
        "assembly_recipe_bytes"
    ] == _binding().canonical_bytes


@pytest.mark.parametrize("corruption", ["hash", "version", "encoding", "oversize"])
async def test_malformed_recipe_fails_without_write(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    corruption: str,
) -> None:
    req = request("profile-invalid")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    raw = _binding().canonical_bytes
    if corruption == "version":
        raw = raw.replace(b"recipe:1", b"recipe:2")
    elif corruption == "encoding":
        raw += b" "
    elif corruption == "oversize":
        raw = b"x" * (profiles.MAX_STATIC_RECIPE_BYTES + 1)
    binding = StaticRecipeBinding(
        canonical_bytes=raw,
        fingerprint="0" * 64
        if corruption == "hash"
        else hashlib.sha256(raw).hexdigest(),
    )
    with pytest.raises(StaticRecipeIncompatible):
        await run_repository.freeze_or_verify_static_recipe(req, lease, binding)
    assert (await _row(run_chat_database_url, run_chat_schema, req.run_id))[
        "assembly_recipe_bytes"
    ] is None


@pytest.mark.parametrize("corruption", ["digest", "version", "encoding", "partial"])
async def test_persisted_binding_corruption_is_not_repaired(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
    corruption: str,
) -> None:
    req = request("profile-corrupt")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    raw = _binding().canonical_bytes
    if corruption == "version":
        raw = raw.replace(b"recipe:1", b"recipe:2")
    elif corruption == "encoding":
        raw += b" "
    digest = "0" * 64 if corruption == "digest" else hashlib.sha256(raw).hexdigest()
    await _mutate(
        run_chat_database_url,
        run_chat_schema,
        req.run_id,
        "assembly_recipe_bytes=%s, assembly_recipe_fingerprint=%s",
        (raw, digest),
    )
    if corruption == "partial":
        async with connect_pg(run_chat_database_url) as conn, conn.cursor() as cur:
            await execute_sql(
                cur,
                "ALTER TABLE {} DROP CONSTRAINT ck_kokoro_agent_run_static_recipe".format(
                    qualified(run_chat_schema, "kokoro_agent_run")
                ),
            )
        await _mutate(
            run_chat_database_url,
            run_chat_schema,
            req.run_id,
            "assembly_recipe_fingerprint=NULL",
        )
    before = await _row(run_chat_database_url, run_chat_schema, req.run_id)
    with pytest.raises(StaticRecipeIncompatible):
        await run_repository.freeze_or_verify_static_recipe(req, lease, _binding())
    assert await _row(run_chat_database_url, run_chat_schema, req.run_id) == before


async def test_paired_columns_reject_partial_binding_and_accept_exact_size_limit(
    run_repository: RunRepository,
    run_chat_database_url: str,
    run_chat_schema: str,
) -> None:
    from psycopg.errors import CheckViolation

    req = request("profile-limit")
    lease = await run_repository.try_claim(req, "worker")
    assert lease is not None
    with pytest.raises(CheckViolation):
        await _mutate(
            run_chat_database_url,
            run_chat_schema,
            req.run_id,
            "assembly_recipe_fingerprint=%s",
            (_binding().fingerprint,),
        )
    prefix = b'{"domain_tag":"kokoro-agent:assembly-recipe:1","padding":"'
    raw = prefix + b"x" * (profiles.MAX_STATIC_RECIPE_BYTES - len(prefix) - 2) + b'"}'
    assert len(raw) == profiles.MAX_STATIC_RECIPE_BYTES
    binding = StaticRecipeBinding(
        canonical_bytes=raw, fingerprint=hashlib.sha256(raw).hexdigest()
    )
    assert (
        await run_repository.freeze_or_verify_static_recipe(req, lease, binding)
        == "frozen"
    )
    assert (await _row(run_chat_database_url, run_chat_schema, req.run_id))[
        "assembly_recipe_bytes"
    ] == raw
