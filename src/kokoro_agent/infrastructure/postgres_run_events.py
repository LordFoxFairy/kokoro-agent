"""Durable event and outbox PostgreSQL capabilities."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from kokoro_agent.domain.chat.models import chat_event_id
from kokoro_agent.domain.chat.projection import project_chat_fact
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.protocol import RunRequest, RunCompletedPayload, RunFailedPayload
from uuid import uuid4

from kokoro_agent.domain.run.repository import (
    LeaseFence,
    OutboxFrame,
    ReceiptReconcile,
    StagedFrame,
)
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    OutboxFilter,
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
    CHAT_EVENTS_TABLE,
    TOOL_JOURNAL_TABLE,
    RUN_CLAIMS_TABLE,
    RUN_OUTBOX_TABLE,
    RUN_RECEIPT_MANIFESTS_TABLE,
    RUN_RECEIPTS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one


class PostgresRunEvents:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context

    async def stage_critical_frame(
        self,
        run_id: str,
        lease: LeaseFence,
        kind: str,
        timestamp: int,
        payload_json: str,
        *,
        terminal: bool,
        event_id: str | None = None,
    ) -> StagedFrame | None:
        if terminal or kind in {"run.completed", "run.failed"}:
            raise ValueError("terminal facts must use finalize_terminal")
        event_id = event_id or f"evt_{uuid4().hex}"
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    lease_current = (
                        await self._context.lock_active_lease(cur, run_id, lease)
                        if kind == "run.started"
                        else await self._context.lock_fence(cur, run_id, lease)
                    )
                    if not lease_current:
                        return None
                    if kind == "delivery.created":
                        await execute_sql(
                            cur,
                            """
                            SELECT run_id, durable_seq, kind, status, index_value,
                                   occurred_at, payload_json
                            FROM {} WHERE event_id = %s
                            """.format(
                                qualified(self._context.schema, RUN_OUTBOX_TABLE)
                            ),
                            (event_id,),
                        )
                        existing = await fetch_one(cur)
                        if existing is not None:
                            if (
                                existing["run_id"] != run_id
                                or existing["kind"] != kind
                                or existing["payload_json"] != payload_json
                                or existing["index_value"] is None
                            ):
                                raise RuntimeError("delivery event identity drift")
                            return StagedFrame(
                                durable_seq=int(existing["durable_seq"]),
                                event_id=event_id,
                                index=int(existing["index_value"]),
                                timestamp=int(
                                    existing["occurred_at"].timestamp() * 1000
                                ),
                                published=existing["status"] == "published",
                                newly_staged=False,
                            )
                    await execute_sql(
                        cur,
                        """
                        SELECT durable_counter, event_index_counter, terminal_fence_seq
                        FROM {}
                        WHERE run_id = %s
                        FOR UPDATE
                        """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                        (run_id,),
                    )
                    row = await fetch_one(cur)
                    assert row is not None
                    seq = int(row["durable_counter"]) + 1
                    fence = row["terminal_fence_seq"]
                    if terminal and fence is None:
                        fence = seq
                    if fence is not None and seq > int(fence):
                        await execute_sql(
                            cur,
                            """
                            UPDATE {}
                            SET durable_counter = %s, terminal_fence_seq = %s
                            WHERE run_id = %s
                            """.format(
                                qualified(self._context.schema, RUN_CLAIMS_TABLE)
                            ),
                            (seq, fence, run_id),
                        )
                        await execute_sql(
                            cur,
                            """
                            INSERT INTO {} (
                                run_id, durable_seq, event_id, kind, status, index_value,
                                occurred_at, payload_json, published_at
                            ) VALUES (%s, %s, %s, %s, 'superseded', NULL,
                                      to_timestamp(%s / 1000.0), %s, NULL)
                            """.format(
                                qualified(self._context.schema, RUN_OUTBOX_TABLE)
                            ),
                            (
                                run_id,
                                seq,
                                event_id,
                                kind,
                                timestamp,
                                payload_json,
                            ),
                        )
                        return None
                    index = int(row["event_index_counter"])
                    await execute_sql(
                        cur,
                        """
                        UPDATE {}
                        SET durable_counter = %s,
                            event_index_counter = %s,
                            terminal_fence_seq = %s
                        WHERE run_id = %s
                        """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                        (seq, index + 1, fence, run_id),
                    )
                    await self.insert_frame_on_cursor(
                        cur,
                        OutboxFrame(
                            run_id=run_id,
                            durable_seq=seq,
                            event_id=event_id,
                            kind=kind,
                            index=index,
                            timestamp=timestamp,
                            payload_json=payload_json,
                        ),
                    )
        return StagedFrame(
            durable_seq=seq, event_id=event_id, index=index, timestamp=timestamp
        )

    async def insert_frame_on_cursor(
        self, cur: Any, frame: OutboxFrame, *, superseded: bool = False
    ) -> None:
        await execute_sql(
            cur,
            """INSERT INTO {} (
                run_id, durable_seq, event_id, kind, status, index_value,
                occurred_at, payload_json, published_at
            ) VALUES (%s, %s, %s, %s, %s, %s,
                      to_timestamp(%s / 1000.0), %s, NULL)""".format(
                qualified(self._context.schema, RUN_OUTBOX_TABLE)
            ),
            (
                frame.run_id,
                frame.durable_seq,
                frame.event_id,
                frame.kind,
                "superseded" if superseded else "queued",
                None if superseded else frame.index,
                frame.timestamp,
                frame.payload_json,
            ),
        )

    async def next_event_index(self, run_id: str) -> int:
        """Read the claims-owned event-index high-water mark."""

        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT event_index_counter
                    FROM {}
                    WHERE run_id = %s
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (run_id,),
                )
                row = await fetch_one(cur)
        return 0 if row is None else int(row["event_index_counter"])

    async def reserve_event_index(self, run_id: str, lease: LeaseFence) -> int | None:
        """Atomically reserve one live-event index under the active lease row lock."""

        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return None
                    await execute_sql(
                        cur,
                        """
                        UPDATE {}
                        SET event_index_counter = event_index_counter + 1
                        WHERE run_id = %s
                        RETURNING event_index_counter - 1 AS index_value
                        """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                        (run_id,),
                    )
                    row = await fetch_one(cur)
                    if row is None:
                        raise RuntimeError(
                            f"failed to reserve an event index for {run_id!r}"
                        )
                    return int(row["index_value"])

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET status = 'published',
                        published_at = to_timestamp(%s / 1000.0)
                    WHERE run_id = %s AND durable_seq = %s AND status = 'queued'
                    """.format(qualified(self._context.schema, RUN_OUTBOX_TABLE)),
                    (self._context.clock(), run_id, durable_seq),
                )

    async def list_unpublished_outbox(self) -> list[OutboxFrame]:
        rows = await self._context.fetch_outbox(OutboxFilter.QUEUED)
        return [outbox_row_to_frame(row) for row in rows]

    async def list_open_outbox_runs(self) -> list[str]:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT DISTINCT run_id
                    FROM {}
                    WHERE status IN ('queued', 'published')
                    ORDER BY run_id ASC
                    """.format(qualified(self._context.schema, RUN_OUTBOX_TABLE)),
                )
                rows = await fetch_all(cur)
        return [str(row["run_id"]) for row in rows]

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30_000
    ) -> ReceiptReconcile:
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # Every receipt/GC decision serializes with stage/finalize on Run first.
                    await execute_sql(
                        cur,
                        "SELECT terminal FROM {} WHERE run_id=%s FOR UPDATE".format(
                            qualified(self._context.schema, RUN_CLAIMS_TABLE)
                        ),
                        (run_id,),
                    )
                    if await fetch_one(cur) is None:
                        return ReceiptReconcile(receipt_state_lost=True)
                    await execute_sql(
                        cur,
                        """
                        SELECT durable_seq, event_id, kind, status, index_value,
                               (extract(epoch FROM occurred_at) * 1000)::bigint AS timestamp,
                               payload_json,
                               (extract(epoch FROM published_at) * 1000)::bigint AS published_at
                        FROM {}
                        WHERE run_id = %s AND status IN ('queued', 'published')
                        ORDER BY durable_seq ASC
                        """.format(qualified(self._context.schema, RUN_OUTBOX_TABLE)),
                        (run_id,),
                    )
                    live_rows = await fetch_all(cur)
                    if not live_rows:
                        return ReceiptReconcile()
                    await execute_sql(
                        cur,
                        """
                        SELECT durable_seq, event_id, status, reason, created_at
                        FROM {}
                        WHERE run_id = %s
                        ORDER BY durable_seq ASC
                        """.format(qualified(self._context.schema, RUN_RECEIPTS_TABLE)),
                        (run_id,),
                    )
                    receipt_rows = await fetch_all(cur)
                    receipts = {
                        int(row["durable_seq"]): dict(row) for row in receipt_rows
                    }
                    rejected = sorted(
                        seq
                        for seq, row in receipts.items()
                        if row["status"] == "rejected"
                    )
                    if rejected:
                        # The finalizer validates the receipt against its exact outbox
                        # identity and commits quarantine atomically with termination.
                        return ReceiptReconcile(rejected_seq=rejected[0])
                    republish: list[OutboxFrame] = []
                    for row in live_rows:
                        durable_seq = int(row["durable_seq"])
                        published_at = row["published_at"]
                        if (
                            row["status"] == "published"
                            and durable_seq not in receipts
                            and published_at is not None
                            and now - int(published_at) >= republish_grace_ms
                        ):
                            republish.append(outbox_row_to_frame(row, run_id=run_id))
                    for frame in republish:
                        await execute_sql(
                            cur,
                            """
                            UPDATE {}
                            SET published_at = to_timestamp(%s / 1000.0)
                            WHERE run_id = %s AND durable_seq = %s
                            """.format(
                                qualified(self._context.schema, RUN_OUTBOX_TABLE)
                            ),
                            (now, run_id, frame.durable_seq),
                        )
                    await execute_sql(
                        cur,
                        """
                        SELECT run_id, persisted_seq, projected_seq, consumed_seq,
                               producer_close_requested, producer_closed, updated_at
                        FROM {}
                        WHERE run_id = %s
                        """.format(
                            qualified(self._context.schema, RUN_RECEIPT_MANIFESTS_TABLE)
                        ),
                        (run_id,),
                    )
                    manifest = await fetch_one(cur)
                    if manifest is None:
                        return ReceiptReconcile(
                            receipt_state_lost=True, republish=republish
                        )
                    consumed = int(manifest["consumed_seq"])
                    advanced = consumed
                    while True:
                        next_seq = advanced + 1
                        receipt = receipts.get(next_seq)
                        if receipt is None or receipt["status"] != "persisted":
                            break
                        row = next(
                            (
                                row
                                for row in live_rows
                                if int(row["durable_seq"]) == next_seq
                            ),
                            None,
                        )
                        if row is None or row["event_id"] != receipt["event_id"]:
                            break
                        advanced = next_seq
                    if advanced > consumed:
                        await execute_sql(
                            cur,
                            """
                            INSERT INTO {} (
                                run_id, persisted_seq, projected_seq, consumed_seq,
                                producer_close_requested, producer_closed, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s,
                                      to_timestamp(%s / 1000.0))
                            ON CONFLICT (run_id) DO UPDATE SET
                                consumed_seq = EXCLUDED.consumed_seq,
                                updated_at = EXCLUDED.updated_at
                            """.format(
                                qualified(
                                    self._context.schema, RUN_RECEIPT_MANIFESTS_TABLE
                                )
                            ),
                            (
                                run_id,
                                advanced,
                                int(manifest["projected_seq"]),
                                advanced,
                                bool(manifest["producer_close_requested"]),
                                bool(manifest["producer_closed"]),
                                now,
                            ),
                        )
                    # Active delivery mapping is the stable journal-to-Chat identity.
                    # Re-scan the final watermark after terminal even without new ACKs.
                    await execute_sql(
                        cur,
                        """DELETE FROM {} AS frame WHERE frame.run_id=%s AND frame.durable_seq<=%s
                           AND (frame.kind <> 'delivery.created' OR EXISTS (
                               SELECT 1 FROM {} AS claim WHERE claim.run_id=frame.run_id AND claim.terminal=TRUE
                           ))""".format(
                            qualified(self._context.schema, RUN_OUTBOX_TABLE),
                            qualified(self._context.schema, RUN_CLAIMS_TABLE),
                        ),
                        (run_id, advanced),
                    )
                    await execute_sql(
                        cur,
                        """
                        SELECT COUNT(*) AS open_count
                        FROM {}
                        WHERE run_id = %s AND status IN ('queued', 'published')
                        """.format(qualified(self._context.schema, RUN_OUTBOX_TABLE)),
                        (run_id,),
                    )
                    open_count_row = await fetch_one(cur)
                    if open_count_row is None:
                        raise RuntimeError(
                            f"failed to count open outbox rows for {run_id!r}"
                        )
                    open_count = int(open_count_row["open_count"])
                    fence_row = await self._context.select_claim_row(cur, run_id)
                    fence = (
                        fence_row["terminal_fence_seq"]
                        if fence_row is not None
                        else None
                    )
                    close_requested = False
                    if fence is not None and advanced >= int(fence) and open_count == 0:
                        await execute_sql(
                            cur,
                            """
                            UPDATE {}
                            SET producer_close_requested = TRUE,
                                updated_at = to_timestamp(%s / 1000.0)
                            WHERE run_id = %s AND producer_close_requested = FALSE
                            """.format(
                                qualified(
                                    self._context.schema, RUN_RECEIPT_MANIFESTS_TABLE
                                )
                            ),
                            (now, run_id),
                        )
                        close_requested = True
                    return ReceiptReconcile(
                        consumed_through=advanced if advanced > consumed else None,
                        close_requested=close_requested,
                        republish=republish,
                    )

    async def delivery_ready_on_cursor(
        self,
        cur: Any,
        request: RunRequest,
        snapshot: tuple[tuple[str, str, str], ...],
    ) -> bool:
        run_id = request.run_id
        namespace = RunScope.of(request).namespace
        await execute_sql(
            cur,
            "SELECT tool_call_id,status,result FROM {} WHERE run_id=%s AND name='deliver' ORDER BY created_at,tool_call_id".format(
                qualified(self._context.schema, TOOL_JOURNAL_TABLE)
            ),
            (run_id,),
        )
        observed = tuple(
            (str(r["tool_call_id"]), str(r["status"]), str(r["result"]))
            for r in await fetch_all(cur)
        )
        if observed != snapshot or any(
            status == "started" for _, status, _ in observed
        ):
            return False
        for tool_id, status, _ in observed:
            if status != "succeeded":
                continue
            event_id = (
                "evt_"
                + hashlib.sha256(f"delivery\0{run_id}\0{tool_id}".encode()).hexdigest()
            )
            await execute_sql(
                cur,
                """SELECT o.payload_json,o.index_value,c.payload_json AS chat_payload,c.chat_event_id FROM {} o JOIN {} c
                ON c.run_id=o.run_id AND c.source_index=o.index_value
                WHERE o.run_id=%s AND o.event_id=%s AND o.kind='delivery.created'
                AND o.status IN ('queued','published') AND c.namespace=%s AND c.tenant_id=%s
                AND c.session_id=%s AND c.event_type='delivery'""".format(
                    qualified(self._context.schema, RUN_OUTBOX_TABLE),
                    qualified(self._context.schema, CHAT_EVENTS_TABLE),
                ),
                (
                    run_id,
                    event_id,
                    namespace,
                    request.execution_identity.tenant_ref,
                    request.session_id,
                ),
            )
            fact = await fetch_one(cur)
            if fact is None or fact["chat_event_id"] != chat_event_id(
                namespace, run_id, int(fact["index_value"])
            ):
                return False
            if json.loads(str(fact["payload_json"])) != json.loads(
                str(fact["chat_payload"])
            ):
                return False
        return True

    async def terminal_chat_on_cursor(
        self, cur: Any, request: RunRequest, row: dict[str, Any]
    ) -> dict[str, Any]:
        index = int(row["event_index_counter"]) - 1
        namespace = RunScope.of(request).namespace
        await execute_sql(
            cur,
            """SELECT * FROM {} WHERE tenant_id=%s AND namespace=%s
            AND session_id=%s AND run_id=%s AND source_index=%s AND chat_event_id=%s
            AND event_type IN ('run.completed','run.failed') AND created_at=%s""".format(
                qualified(self._context.schema, CHAT_EVENTS_TABLE)
            ),
            (
                request.execution_identity.tenant_ref,
                namespace,
                request.session_id,
                request.run_id,
                index,
                chat_event_id(namespace, request.run_id, index),
                row["terminal_at"],
            ),
        )
        fact = await fetch_one(cur)
        if fact is None:
            raise RuntimeError("terminal Chat identity is missing or corrupt")
        return dict(fact)

    @staticmethod
    def quarantine_audit_matches(
        row: dict[str, Any],
        audit: dict[str, Any] | None,
        payload: RunCompletedPayload | RunFailedPayload,
    ) -> bool:
        if audit is None:
            return False
        try:
            return (
                audit["status"] == "superseded"
                and audit["kind"] == "run.failed"
                and audit["index_value"] is None
                and audit["published_at"] is None
                and audit["occurred_at"] == row["terminal_at"]
                and row["terminal_at"] is not None
                and int(audit["durable_seq"]) == int(row["durable_counter"])
                and int(audit["durable_seq"]) > int(row["terminal_fence_seq"])
                and json.loads(audit["payload_json"])
                == payload.model_dump(exclude_none=True)
            )
        except (TypeError, ValueError):
            return False

    async def verify_terminal_frame(self, frame: OutboxFrame) -> None:
        """Validate retained terminal facts; never repair or allocate Chat state."""
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await execute_sql(
                        cur,
                        "SELECT * FROM {} WHERE run_id=%s FOR UPDATE".format(
                            qualified(self._context.schema, RUN_CLAIMS_TABLE)
                        ),
                        (frame.run_id,),
                    )
                    raw = await fetch_one(cur)
                    if raw is None or not raw["terminal"]:
                        raise RuntimeError("terminal frame has no committed Run")
                    row = dict(raw)
                    if (
                        frame.durable_seq != row["terminal_fence_seq"]
                        or frame.index != int(row["event_index_counter"]) - 1
                        or frame.event_id
                        != "evt_"
                        + hashlib.sha256(
                            f"terminal\0{frame.run_id}".encode()
                        ).hexdigest()
                        or frame.timestamp != int(row["terminal_at"].timestamp() * 1000)
                    ):
                        raise RuntimeError("terminal frame identity drift")
                    await execute_sql(
                        cur,
                        """SELECT *,floor(extract(epoch FROM occurred_at)*1000)::bigint AS timestamp
                        FROM {} WHERE run_id=%s AND durable_seq=%s AND status IN ('queued','published')""".format(
                            qualified(self._context.schema, RUN_OUTBOX_TABLE)
                        ),
                        (frame.run_id, frame.durable_seq),
                    )
                    stored = await fetch_one(cur)
                    if stored is None or outbox_row_to_frame(dict(stored)) != frame:
                        raise RuntimeError("terminal frame is not retained")
                    request = RunRequest.model_validate_json(row["request_json"])
                    fact = await self.terminal_chat_on_cursor(cur, request, row)
                    payload = (
                        RunCompletedPayload.model_validate_json(frame.payload_json)
                        if frame.kind == "run.completed"
                        else RunFailedPayload.model_validate_json(frame.payload_json)
                    )
                    projection = project_chat_fact(
                        tenant_id=request.execution_identity.tenant_ref,
                        namespace=RunScope.of(request).namespace,
                        session_id=request.session_id,
                        run_id=frame.run_id,
                        source_index=frame.index,
                        created_at=row["terminal_at"],
                        payload=payload,
                    )
                    assert projection is not None
                    if fact["event_type"] != projection.event.event_type or json.loads(
                        fact["payload_json"]
                    ) != json.loads(projection.event.payload_json):
                        raise RuntimeError("terminal Chat payload drift")


def outbox_row_to_frame(
    row: dict[str, Any], *, run_id: str | None = None
) -> OutboxFrame:
    return OutboxFrame(
        run_id=str(run_id or row["run_id"]),
        durable_seq=int(row["durable_seq"]),
        event_id=str(row["event_id"]),
        kind=str(row["kind"]),
        index=int(row["index_value"]),
        timestamp=int(row["timestamp"]),
        payload_json=str(row["payload_json"]),
    )
