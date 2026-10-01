"""Run lease, lifecycle, usage, and terminal-state capabilities."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from datetime import UTC, datetime

from kokoro_agent.domain.run.models import (
    RunTerminalOutcome,
    TerminalAuthority,
    TerminalCommitResult,
    ExecutionTerminalAuthority,
    CancelTerminalAuthority,
    QuarantineTerminalAuthority,
    OutboxFrame,
)
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.domain.chat.projection import project_chat_fact
from kokoro_agent.infrastructure.postgres_chat_repository import PostgresChatRepository
from kokoro_agent.infrastructure.postgres_run_interactions import (
    PostgresRunInteractions,
)
from kokoro_agent.infrastructure.postgres_run_events import (
    PostgresRunEvents,
    outbox_row_to_frame,
)

from kokoro_agent.domain.run.repository import (
    LeaseFence,
    LeasedRun,
    UsageIdentityConflict,
)
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
    RUN_CLAIMS_TABLE,
    RUN_CONTROL_COMMANDS_TABLE,
    RUN_DISPATCHES_TABLE,
    RUN_OUTBOX_TABLE,
    RUN_USAGE_SEGMENTS_TABLE,
    RUN_RECEIPTS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.protocol import (
    RunRequest,
    RunStartedPayload,
    RunCompletedPayload,
    RunFailedPayload,
    RunControlReceiptPayload,
    TokenUsage,
)


class PostgresRunLeases:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context

    async def renew(self, run_id: str, lease: LeaseFence) -> bool:
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET lease_expires_at = to_timestamp(%s / 1000.0)
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > to_timestamp(%s / 1000.0)
                      AND terminal = FALSE
                    RETURNING run_id
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (
                        now + self._context.ttl_ms,
                        run_id,
                        lease.owner,
                        lease.generation,
                        now,
                    ),
                )
                return await fetch_one(cur) is not None

    async def adopt(self, run_id: str, owner: str) -> LeaseFence | None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET owner = %s,
                        lease_generation = lease_generation + 1,
                        lease_expires_at = to_timestamp(%s / 1000.0)
                    WHERE run_id = %s
                      AND terminal = FALSE
                      AND lease_expires_at IS NULL
                    RETURNING lease_generation
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (owner, self._context.clock() + self._context.ttl_ms, run_id),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return LeaseFence(owner=owner, generation=int(row["lease_generation"]))

    async def pause(self, run_id: str, lease: LeaseFence) -> bool:
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET lease_expires_at = NULL
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > to_timestamp(%s / 1000.0)
                      AND terminal = FALSE
                    RETURNING run_id
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (run_id, lease.owner, lease.generation, now),
                )
                return await fetch_one(cur) is not None

    async def reclaim_expired(self, owner: str) -> list[LeasedRun]:
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET owner = %s,
                        lease_generation = lease_generation + 1,
                        lease_expires_at = to_timestamp(%s / 1000.0)
                    WHERE terminal = FALSE AND lease_expires_at IS NOT NULL AND lease_expires_at <= to_timestamp(%s / 1000.0)
                    RETURNING request_json, lease_generation
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (owner, now + self._context.ttl_ms, now),
                )
                rows = await fetch_all(cur)
        return [
            LeasedRun(
                request=RunRequest.model_validate_json(row["request_json"]),
                lease=LeaseFence(
                    owner=owner,
                    generation=int(row["lease_generation"]),
                ),
            )
            for row in rows
            if row["request_json"] is not None
        ]

    async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool:
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT 1
                    FROM {}
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > to_timestamp(%s / 1000.0)
                      AND terminal = FALSE
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (run_id, lease.owner, lease.generation, now),
                )
                return await fetch_one(cur) is not None

    async def is_fence_current(self, run_id: str, lease: LeaseFence) -> bool:
        """Check generation ownership even after pause/terminal clears the active expiry."""

        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT 1
                    FROM {}
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (run_id, lease.owner, lease.generation),
                )
                return await fetch_one(cur) is not None

    async def get_fence(self, run_id: str) -> LeaseFence | None:
        row = await self._context.get_claim_row(run_id)
        if row is None or row["owner"] is None:
            return None
        generation = int(row["lease_generation"])
        if generation < 1:
            return None
        return LeaseFence(owner=str(row["owner"]), generation=generation)

    async def get_request(self, run_id: str) -> RunRequest | None:
        row = await self._context.get_claim_row(run_id)
        if row is None or row["request_json"] is None:
            return None
        return RunRequest.model_validate_json(row["request_json"])

    async def get_request_scoped(
        self, run_id: str, tenant_ref: str, namespace: str
    ) -> RunRequest | None:
        """Read a run only when both tenant and derived namespace match."""

        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT claim.request_json
                    FROM {} AS claim
                    INNER JOIN {} AS dispatch
                      ON dispatch.run_id = claim.run_id
                     AND dispatch.tenant_id = claim.tenant_id
                    WHERE claim.run_id = %s
                      AND claim.tenant_id = %s
                      AND dispatch.namespace = %s
                    """.format(
                        qualified(self._context.schema, RUN_CLAIMS_TABLE),
                        qualified(self._context.schema, RUN_DISPATCHES_TABLE),
                    ),
                    (run_id, tenant_ref, namespace),
                )
                row = await fetch_one(cur)
        if row is None or row["request_json"] is None:
            return None
        return RunRequest.model_validate_json(row["request_json"])

    async def list_paused(self) -> list[str]:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT run_id
                    FROM {}
                    WHERE terminal = FALSE AND lease_expires_at IS NULL AND request_json IS NOT NULL
                    ORDER BY run_id ASC
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                )
                rows = await fetch_all(cur)
        return [str(row["run_id"]) for row in rows]

    async def add_tokens(
        self, run_id: str, lease: LeaseFence, count: int
    ) -> int | None:
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET token_total = token_total + %s
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > to_timestamp(%s / 1000.0)
                      AND terminal = FALSE
                    RETURNING token_total
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (count, run_id, lease.owner, lease.generation, now),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return int(row["token_total"])

    async def add_usage(
        self, run_id: str, lease: LeaseFence, input_tokens: int, output_tokens: int
    ) -> tuple[int, int] | None:
        if input_tokens < 0 or output_tokens < 0:
            raise ValueError("usage token counts must be non-negative")
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    return await self._add_usage_on_cursor(
                        cur,
                        run_id,
                        lease,
                        input_tokens,
                        output_tokens,
                    )

    async def _add_usage_on_cursor(
        self,
        cur: Any,
        run_id: str,
        lease: LeaseFence,
        input_tokens: int,
        output_tokens: int,
    ) -> tuple[int, int] | None:
        await execute_sql(
            cur,
            """
            SELECT usage_input_total, usage_output_total, terminal, lease_expires_at
            FROM {}
            WHERE run_id = %s
              AND owner = %s
              AND lease_generation = %s
            FOR UPDATE
            """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
            (run_id, lease.owner, lease.generation),
        )
        claim = await fetch_one(cur)
        if claim is None:
            return None
        database_now = await self._context.database_now(cur)
        if not claim["terminal"] and (
            claim["lease_expires_at"] is None
            or claim["lease_expires_at"] <= database_now
        ):
            return None
        now = int(database_now.timestamp() * 1000)
        await execute_sql(
            cur,
            """
            SELECT input_tokens, output_tokens
            FROM {}
            WHERE run_id = %s AND lease_generation = %s
            """.format(qualified(self._context.schema, RUN_USAGE_SEGMENTS_TABLE)),
            (run_id, lease.generation),
        )
        existing = await fetch_one(cur)
        if existing is not None:
            if (
                int(existing["input_tokens"]) != input_tokens
                or int(existing["output_tokens"]) != output_tokens
            ):
                raise UsageIdentityConflict(
                    "usage identity conflict for "
                    f"run {run_id!r} generation {lease.generation}"
                )
            return (
                int(claim["usage_input_total"]),
                int(claim["usage_output_total"]),
            )
        if bool(claim["terminal"]):
            return None
        await execute_sql(
            cur,
            """
            INSERT INTO {} (
                run_id, lease_generation, input_tokens, output_tokens, created_at
            ) VALUES (%s, %s, %s, %s, to_timestamp(%s / 1000.0))
            """.format(qualified(self._context.schema, RUN_USAGE_SEGMENTS_TABLE)),
            (
                run_id,
                lease.generation,
                input_tokens,
                output_tokens,
                now,
            ),
        )
        await execute_sql(
            cur,
            """
            UPDATE {}
            SET usage_input_total = usage_input_total + %s,
                usage_output_total = usage_output_total + %s
            WHERE run_id = %s
            RETURNING usage_input_total, usage_output_total
            """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
            (input_tokens, output_tokens, run_id),
        )
        updated = await fetch_one(cur)
        if updated is None:
            raise RuntimeError(f"usage aggregate disappeared for run {run_id!r}")
        return (
            int(updated["usage_input_total"]),
            int(updated["usage_output_total"]),
        )

    async def purge_terminal(self, max_age_ms: int) -> int:
        return await self._context.purge_terminal(max_age_ms)

    async def finalize_terminal(
        self,
        run_id: str,
        authority: TerminalAuthority,
        outcome: RunTerminalOutcome,
        delivery_snapshot: tuple[tuple[str, str, str], ...],
    ) -> TerminalCommitResult:
        """Commit one outcome and its public facts, or an explicitly private quarantine."""
        quarantine = isinstance(authority, QuarantineTerminalAuthority)
        cancel = isinstance(authority, CancelTerminalAuthority)
        if quarantine and not (
            isinstance(outcome.payload, RunFailedPayload)
            and outcome.payload.code == "contract_incompatible"
            and not outcome.payload.retryable
            and outcome.usage is None
        ):
            raise ValueError("invalid quarantine outcome")
        if cancel and not (
            isinstance(outcome.payload, RunCompletedPayload)
            and outcome.payload.status == "cancelled"
            and outcome.usage is None
        ):
            raise ValueError("invalid cancellation outcome")
        chat = PostgresChatRepository(self._context.database_url, self._context.schema)
        events = PostgresRunEvents(self._context)
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await execute_sql(
                        cur,
                        "SELECT * FROM {} WHERE run_id = %s FOR UPDATE".format(
                            qualified(self._context.schema, RUN_CLAIMS_TABLE)
                        ),
                        (run_id,),
                    )
                    raw = await fetch_one(cur)
                    if raw is None:
                        return TerminalCommitResult(status="lost", lease=None)
                    row = dict(raw)
                    lease = LeaseFence(
                        owner=str(row["owner"]), generation=int(row["lease_generation"])
                    )
                    now = int(
                        (await self._context.database_now(cur)).timestamp() * 1000
                    )
                    request = RunRequest.model_validate_json(row["request_json"])
                    scope = RunScope.of(request)
                    command: dict[str, Any] | None = None
                    if isinstance(authority, ExecutionTerminalAuthority):
                        if lease != authority.lease:
                            return TerminalCommitResult(status="lost", lease=None)
                    elif isinstance(authority, CancelTerminalAuthority):
                        await execute_sql(
                            cur,
                            "SELECT * FROM {} WHERE run_id=%s AND command_id=%s".format(
                                qualified(
                                    self._context.schema, RUN_CONTROL_COMMANDS_TABLE
                                )
                            ),
                            (run_id, authority.command_id),
                        )
                        command_row = await fetch_one(cur)
                        command = None if command_row is None else dict(command_row)
                        if command is None or command["status"] not in {
                            "persisted",
                            "succeeded",
                        }:
                            return TerminalCommitResult(status="lost", lease=None)
                    else:
                        await execute_sql(
                            cur,
                            """SELECT o.event_id FROM {} r JOIN {} o
                            ON o.run_id=r.run_id AND o.durable_seq=r.durable_seq AND o.event_id=r.event_id
                            WHERE r.run_id=%s AND r.durable_seq=%s AND r.status='rejected'""".format(
                                qualified(self._context.schema, RUN_RECEIPTS_TABLE),
                                qualified(self._context.schema, RUN_OUTBOX_TABLE),
                            ),
                            (run_id, authority.rejected_seq),
                        )
                        if await fetch_one(cur) is None:
                            return TerminalCommitResult(status="lost", lease=None)
                        fence = row["terminal_fence_seq"]
                        if fence is not None and int(fence) != authority.rejected_seq:
                            return TerminalCommitResult(status="lost", lease=None)
                    event_id = (
                        "evt_"
                        + hashlib.sha256(f"terminal\0{run_id}".encode()).hexdigest()
                    )
                    await execute_sql(
                        cur,
                        "SELECT * FROM {} WHERE run_id=%s AND event_id=%s".format(
                            qualified(self._context.schema, RUN_OUTBOX_TABLE)
                        ),
                        (run_id, event_id),
                    )
                    existing_raw = await fetch_one(cur)
                    existing = None if existing_raw is None else dict(existing_raw)
                    if bool(row["terminal"]):
                        if cancel and (
                            command is None or command["status"] != "succeeded"
                        ):
                            return TerminalCommitResult(status="lost", lease=None)
                        if quarantine:
                            if not events.quarantine_audit_matches(
                                row, existing, outcome.payload
                            ):
                                return TerminalCommitResult(status="lost", lease=None)
                            return TerminalCommitResult(status="replayed", lease=lease)
                        terminal_chat = await events.terminal_chat_on_cursor(
                            cur, request, row
                        )
                        replay_payload = outcome.payload
                        if (
                            isinstance(replay_payload, RunCompletedPayload)
                            and replay_payload.status == "completed"
                        ):
                            replay_payload = RunCompletedPayload(
                                status="completed",
                                token_usage=(
                                    TokenUsage(
                                        input_tokens=int(row["usage_input_total"]),
                                        output_tokens=int(row["usage_output_total"]),
                                    )
                                    if row["usage_input_total"]
                                    or row["usage_output_total"]
                                    else None
                                ),
                            )
                        replay_projection = project_chat_fact(
                            tenant_id=request.execution_identity.tenant_ref,
                            namespace=scope.namespace,
                            session_id=request.session_id,
                            run_id=run_id,
                            source_index=0,
                            created_at=datetime.fromtimestamp(now / 1000, tz=UTC),
                            payload=replay_payload,
                        )
                        assert replay_projection is not None
                        if terminal_chat[
                            "event_type"
                        ] != replay_projection.event.event_type or json.loads(
                            str(terminal_chat["payload_json"])
                        ) != json.loads(replay_projection.event.payload_json):
                            return TerminalCommitResult(status="lost", lease=None)
                        if outcome.usage is not None:
                            if (
                                await self._add_usage_on_cursor(
                                    cur,
                                    run_id,
                                    lease,
                                    outcome.usage.input_tokens,
                                    outcome.usage.output_tokens,
                                )
                                is None
                            ):
                                return TerminalCommitResult(status="lost", lease=None)
                        # Return only retained queued facts. Never reconstruct an ACKed/GCed frame.
                        await execute_sql(
                            cur,
                            """SELECT *, floor(extract(epoch FROM occurred_at) * 1000)::bigint AS timestamp FROM {} WHERE run_id=%s
                            AND status='queued' AND (event_id=%s OR event_id=%s)
                            ORDER BY durable_seq""".format(
                                qualified(self._context.schema, RUN_OUTBOX_TABLE)
                            ),
                            (
                                run_id,
                                event_id,
                                "evt_"
                                + hashlib.sha256(
                                    f"control\0{run_id}\0{authority.command_id}".encode()
                                ).hexdigest()
                                if isinstance(authority, CancelTerminalAuthority)
                                else event_id,
                            ),
                        )
                        retained = tuple(
                            outbox_row_to_frame(dict(item))
                            for item in await fetch_all(cur)
                        )
                        return TerminalCommitResult(
                            status="replayed", lease=lease, retained_frames=retained
                        )
                    if isinstance(authority, ExecutionTerminalAuthority):
                        expiry = row["lease_expires_at"]
                        if (
                            expiry is None
                            or int(expiry.timestamp() * 1000) <= now
                            or row["terminal_fence_seq"] is not None
                        ):
                            return TerminalCommitResult(status="lost", lease=None)
                    elif command is not None and command["status"] != "persisted":
                        return TerminalCommitResult(status="lost", lease=None)
                    if not quarantine:
                        await execute_sql(
                            cur,
                            """SELECT 1 FROM {} r JOIN {} o
                            ON o.run_id=r.run_id AND o.durable_seq=r.durable_seq AND o.event_id=r.event_id
                            WHERE r.run_id=%s AND r.status='rejected' LIMIT 1""".format(
                                qualified(self._context.schema, RUN_RECEIPTS_TABLE),
                                qualified(self._context.schema, RUN_OUTBOX_TABLE),
                            ),
                            (run_id,),
                        )
                        if await fetch_one(cur) is not None:
                            return TerminalCommitResult(status="lost", lease=None)
                        if not await events.delivery_ready_on_cursor(
                            cur, request, delivery_snapshot
                        ):
                            return TerminalCommitResult(status="deferred", lease=None)
                    total_in, total_out = (
                        int(row["usage_input_total"]),
                        int(row["usage_output_total"]),
                    )
                    if outcome.usage is not None:
                        totals = await self._add_usage_on_cursor(
                            cur,
                            run_id,
                            lease,
                            outcome.usage.input_tokens,
                            outcome.usage.output_tokens,
                        )
                        if totals is None:
                            return TerminalCommitResult(status="lost", lease=None)
                        total_in, total_out = totals
                    payload = outcome.payload
                    if (
                        isinstance(payload, RunCompletedPayload)
                        and payload.status == "completed"
                    ):
                        payload = RunCompletedPayload(
                            status="completed",
                            token_usage=(
                                TokenUsage(
                                    input_tokens=total_in, output_tokens=total_out
                                )
                                if total_in or total_out
                                else None
                            ),
                        )
                    seq, index = (
                        int(row["durable_counter"]),
                        int(row["event_index_counter"]),
                    )
                    frames: list[OutboxFrame] = []
                    if isinstance(authority, CancelTerminalAuthority):
                        seq += 1
                        receipt = RunControlReceiptPayload(
                            command_id=authority.command_id, control_status="applied"
                        )
                        frames.append(
                            OutboxFrame(
                                run_id=run_id,
                                durable_seq=seq,
                                index=index,
                                event_id="evt_"
                                + hashlib.sha256(
                                    f"control\0{run_id}\0{authority.command_id}".encode()
                                ).hexdigest(),
                                kind="run.control.receipt",
                                timestamp=now,
                                payload_json=receipt.model_dump_json(exclude_none=True),
                            )
                        )
                        index += 1
                    seq += 1
                    terminal_frame = OutboxFrame(
                        run_id=run_id,
                        durable_seq=seq,
                        index=index,
                        event_id=event_id,
                        kind="run.failed"
                        if isinstance(payload, RunFailedPayload)
                        else "run.completed",
                        timestamp=now,
                        payload_json=payload.model_dump_json(exclude_none=True),
                    )
                    frames.append(terminal_frame)
                    if not quarantine:
                        # A failed pre-terminal projection must not later acquire a seq
                        # after terminal. Restore retained started facts on this cursor.
                        await execute_sql(
                            cur,
                            """SELECT index_value,occurred_at,payload_json FROM {}
                            WHERE run_id=%s AND kind='run.started' AND status IN ('queued','published')
                            ORDER BY index_value""".format(
                                qualified(self._context.schema, RUN_OUTBOX_TABLE)
                            ),
                            (run_id,),
                        )
                        for started in await fetch_all(cur):
                            started_projection = project_chat_fact(
                                tenant_id=request.execution_identity.tenant_ref,
                                namespace=scope.namespace,
                                session_id=request.session_id,
                                run_id=run_id,
                                source_index=int(started["index_value"]),
                                created_at=started["occurred_at"],
                                payload=RunStartedPayload.model_validate_json(
                                    started["payload_json"]
                                ),
                            )
                            assert started_projection is not None
                            await chat.append_on_cursor(cur, started_projection)
                    index = await PostgresRunInteractions(
                        self._context
                    ).terminal_on_cursor(
                        cur,
                        request,
                        row,
                        now=datetime.fromtimestamp(now / 1000, tz=UTC),
                        index=index,
                        visible=not quarantine,
                    )
                    terminal_frame = terminal_frame.model_copy(update={"index": index})
                    frames[-1] = terminal_frame
                    if not quarantine:
                        projection = project_chat_fact(
                            tenant_id=request.execution_identity.tenant_ref,
                            namespace=scope.namespace,
                            session_id=request.session_id,
                            run_id=run_id,
                            source_index=index,
                            created_at=datetime.fromtimestamp(now / 1000, tz=UTC),
                            payload=payload,
                        )
                        assert projection is not None
                        await chat.append_on_cursor(cur, projection)
                    if not isinstance(authority, ExecutionTerminalAuthority):
                        lease = LeaseFence(
                            owner=authority.owner, generation=lease.generation + 1
                        )
                    fence_seq = (
                        authority.rejected_seq
                        if isinstance(authority, QuarantineTerminalAuthority)
                        else seq
                    )
                    if quarantine:
                        await execute_sql(
                            cur,
                            """UPDATE {} SET status='superseded', index_value=NULL
                            WHERE run_id=%s AND durable_seq >= %s AND status IN ('queued','published')""".format(
                                qualified(self._context.schema, RUN_OUTBOX_TABLE)
                            ),
                            (run_id, fence_seq),
                        )
                    for frame in frames:
                        await events.insert_frame_on_cursor(
                            cur, frame, superseded=quarantine
                        )
                    if isinstance(authority, CancelTerminalAuthority):
                        await execute_sql(
                            cur,
                            "UPDATE {} SET status='succeeded',updated_at=to_timestamp(%s/1000.0) WHERE run_id=%s AND command_id=%s".format(
                                qualified(
                                    self._context.schema, RUN_CONTROL_COMMANDS_TABLE
                                )
                            ),
                            (now, run_id, authority.command_id),
                        )
                    await execute_sql(
                        cur,
                        """UPDATE {} SET terminal=TRUE,terminal_at=to_timestamp(%s/1000.0),
                        lease_expires_at=NULL,owner=%s,lease_generation=%s,durable_counter=%s,
                        event_index_counter=%s,terminal_fence_seq=%s WHERE run_id=%s""".format(
                            qualified(self._context.schema, RUN_CLAIMS_TABLE)
                        ),
                        (
                            now,
                            lease.owner,
                            lease.generation,
                            seq,
                            index if quarantine else index + 1,
                            fence_seq,
                            run_id,
                        ),
                    )
                    await self._context.queue_bound_sandbox_cleanup(cur, row, now=now)
                    return TerminalCommitResult(
                        status="committed",
                        lease=lease,
                        retained_frames=() if quarantine else tuple(frames),
                    )

    async def is_terminal(self, run_id: str) -> bool:
        row = await self._context.get_claim_row(run_id)
        return bool(row and row["terminal"])
