"""Durable event and outbox PostgreSQL capabilities."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from kokoro_agent.domain.run.repository import (
    LeaseFence,
    OutboxFrame,
    ReceiptReconcile,
    StagedFrame,
)
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
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
    ) -> StagedFrame | None:
        event_id = f"evt_{uuid4().hex}"
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
                    await execute_sql(
                        cur,
                        """
                        INSERT INTO {} (
                            run_id, durable_seq, event_id, kind, status, index_value,
                            occurred_at, payload_json, published_at
                        ) VALUES (%s, %s, %s, %s, 'queued', %s,
                                  to_timestamp(%s / 1000.0), %s, NULL)
                        """.format(qualified(self._context.schema, RUN_OUTBOX_TABLE)),
                        (run_id, seq, event_id, kind, index, timestamp, payload_json),
                    )
        return StagedFrame(durable_seq=seq, event_id=event_id, index=index)

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
        rows = await self._context.fetch_outbox("status = 'queued'")
        return [_outbox_row_to_frame(row) for row in rows]

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
                        seq = rejected[0]
                        await execute_sql(
                            cur,
                            """
                            UPDATE {}
                            SET terminal_fence_seq = CASE
                                WHEN terminal_fence_seq IS NULL OR terminal_fence_seq > %s
                                THEN %s
                                ELSE terminal_fence_seq
                            END
                            WHERE run_id = %s
                            """.format(
                                qualified(self._context.schema, RUN_CLAIMS_TABLE)
                            ),
                            (seq, seq, run_id),
                        )
                        return ReceiptReconcile(rejected_seq=seq)
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
                            republish.append(_outbox_row_to_frame(row, run_id=run_id))
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
                        await execute_sql(
                            cur,
                            """
                            DELETE FROM {}
                            WHERE run_id = %s AND durable_seq <= %s
                            """.format(
                                qualified(self._context.schema, RUN_OUTBOX_TABLE)
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


def _outbox_row_to_frame(
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
