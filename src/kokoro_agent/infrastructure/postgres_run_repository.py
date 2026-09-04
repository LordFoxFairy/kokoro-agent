"""PostgreSQL adapter for the Agent execution repository.

The repository port and transport-neutral records stay in
``kokoro_agent.repositories.run_repository``; this module owns SQL, connection
lifecycle, and PostgreSQL-specific configuration.
"""

# The adapter deliberately builds qualified SQL identifiers at runtime and
# consumes psycopg dict rows. The package stubs currently model only literal
# SQL/tuple rows, so those boundary diagnostics are covered by ruff/tests.
# pyright: reportCallIssue=false, reportArgumentType=false, reportReturnType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportIncompatibleMethodOverride=false, reportUnusedClass=false

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.protocol import RunRequest
from kokoro_agent.infrastructure.postgres import (
    DEFAULT_PG_SCHEMA,
    connect_pg,
    qualified,
)
from kokoro_agent.repositories.run_repository import (
    ControlAdmission,
    ControlAdmissionReceipt,
    ControlAdmissionStatus,
    ControlCommandConflict,
    DispatchAdmission,
    DispatchConflict,
    LeaseFence,
    LeasedRun,
    OutboxFrame,
    ReceiptReconcile,
    RunControlCommandRecord,
    RunRepository,
    SandboxBackendKind,
    SandboxCleanupIntent,
    StagedFrame,
    ToolJournalRecord,
    UsageIdentityConflict,
)
from kokoro_agent.infrastructure.schema import (
    RUN_CLAIMS_TABLE,
    RUN_CONTROL_COMMANDS_TABLE,
    RUN_DISPATCHES_TABLE,
    RUN_DLQ_TABLE,
    RUN_OUTBOX_TABLE,
    RUN_RECEIPT_MANIFESTS_TABLE,
    RUN_RECEIPTS_TABLE,
    RUN_STEERS_TABLE,
    RUN_USAGE_SEGMENTS_TABLE,
    SANDBOX_CLEANUP_INTENTS_TABLE,
    TOOL_JOURNAL_TABLE,
    TOOL_RESULTS_TABLE,
    ensure_run_repository_schema,
)

DEFAULT_LEASE_TTL_S = 90

__all__ = [
    "DEFAULT_LEASE_TTL_S",
    "PostgresRunRepository",
    "RunRepositorySettings",
    "make_run_repository",
]


def _receipt_status(command_status: str) -> ControlAdmissionStatus:
    """Project internal command state onto the small HTTP receipt state set."""

    if command_status in {"admitted", "persisted"}:
        return "pending"
    if command_status in {"applied", "succeeded"}:
        return "succeeded"
    if command_status in {"failed", "superseded"}:
        return "failed"
    raise RuntimeError(f"unknown control command status: {command_status!r}")


class _OutboxEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    durable_seq: int
    event_id: str
    kind: str
    status: str
    index: int | None = None
    timestamp: int | None = None
    payload_json: str | None = None
    published_at: int | None = None


class _DispatchLeaseConflict(Exception):
    """Abort the dispatch transaction when its lease row cannot be created."""


def _sandbox_cleanup_id(
    run_id: str,
    lease_generation: int,
    backend_kind: SandboxBackendKind,
    sandbox_id: str,
) -> str:
    identity = "\0".join(
        (run_id, str(lease_generation), backend_kind, sandbox_id)
    ).encode()
    return f"scu_{hashlib.sha256(identity).hexdigest()}"


def _sandbox_cleanup_from_row(row: dict[str, Any]) -> SandboxCleanupIntent:
    return SandboxCleanupIntent(
        cleanup_id=str(row["cleanup_id"]),
        run_id=str(row["run_id"]),
        lease_generation=int(row["lease_generation"]),
        backend_kind=str(row["backend_kind"]),
        sandbox_id=str(row["sandbox_id"]),
        teardown_ref=str(row["teardown_ref"]),
        attempt_count=int(row["attempt_count"]),
        next_attempt_at=int(row["next_attempt_at"]),
    )


class RunRepositorySettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    database_url: str
    schema_name: str = DEFAULT_PG_SCHEMA
    lease_ttl_ms: Annotated[int, Field(gt=0)]


class PostgresRunRepository:
    def __init__(
        self,
        database_url: str,
        ttl_ms: int,
        schema: str = DEFAULT_PG_SCHEMA,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._database_url = database_url
        self._schema = schema
        self._ttl_ms = ttl_ms
        self._clock = clock or _now_ms

    async def setup(self) -> None:
        async with connect_pg(self._database_url) as conn:
            await ensure_run_repository_schema(conn, self._schema)

    async def enqueue_dispatch(
        self, request: RunRequest, namespace: str, fence: str
    ) -> DispatchAdmission:
        """Create or replay the durable dispatch intent before Redis publication.

        The dispatch row is the admission fence for the worker's claim CAS. A
        repeated request with the same run id and canonical fence is safe to
        republish (for example after a response timeout). A different fence is
        an immutable-identity conflict and is never silently merged.
        """
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (
                        run_id, session_id, namespace, request_json, fence, status,
                        claimed_by, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, 'pending', NULL, %s, %s)
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING run_id
                    """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                    (
                        request.run_id,
                        request.session_id,
                        namespace,
                        request.model_dump_json(),
                        fence,
                        now,
                        now,
                    ),
                )
                if await cur.fetchone() is not None:
                    return DispatchAdmission(replayed=False, publish_required=True)
                await cur.execute(
                    """
                    SELECT fence, status
                    FROM {}
                    WHERE run_id = %s
                    """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                    (request.run_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"dispatch row disappeared for {request.run_id!r}"
                    )
                if str(row["fence"]) != fence:
                    raise DispatchConflict(
                        f"run id {request.run_id!r} was reused with a different launch envelope"
                    )
                status = str(row["status"])
                return DispatchAdmission(
                    replayed=True,
                    publish_required=status == "pending",
                )

    async def admit_control(
        self, run_id: str, command_id: str, request_digest: str, body: str
    ) -> ControlAdmission:
        """Persist one control command before publishing it to Redis.

        ``command_id`` is the durable identity.  A retry may only replay the
        original command when its canonical request digest is unchanged.
        """
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (
                        run_id, command_id, request_digest, status, body,
                        error_code, created_at, updated_at
                    ) VALUES (%s, %s, %s, 'admitted', %s, NULL, %s, %s)
                    ON CONFLICT (run_id, command_id) DO NOTHING
                    RETURNING command_id
                    """.format(qualified(self._schema, RUN_CONTROL_COMMANDS_TABLE)),
                    (run_id, command_id, request_digest, body, now, now),
                )
                if await cur.fetchone() is not None:
                    return ControlAdmission(
                        receipt=ControlAdmissionReceipt(
                            run_id=run_id,
                            command_id=command_id,
                            request_digest=request_digest,
                            status="pending",
                        ),
                        replayed=False,
                        publish_required=True,
                    )
                await cur.execute(
                    """
                    SELECT run_id, command_id, request_digest, status, error_code
                    FROM {}
                    WHERE run_id = %s AND command_id = %s
                    """.format(qualified(self._schema, RUN_CONTROL_COMMANDS_TABLE)),
                    (run_id, command_id),
                )
                row = await cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"control command disappeared for {command_id!r}"
                    )
                if (
                    str(row["run_id"]) != run_id
                    or str(row["request_digest"]) != request_digest
                ):
                    raise ControlCommandConflict(
                        f"command id {command_id!r} was reused with a different request digest"
                    )
                row_data = dict(row)
                receipt_status = _receipt_status(str(row_data["status"]))
                receipt = ControlAdmissionReceipt(
                    run_id=str(row_data["run_id"]),
                    command_id=str(row_data["command_id"]),
                    request_digest=str(row_data["request_digest"]),
                    status=receipt_status,
                    error_code=row_data.get("error_code"),
                )
                return ControlAdmission(
                    receipt=receipt,
                    replayed=True,
                    publish_required=str(row_data["status"])
                    in {"admitted", "persisted"},
                )

    async def mark_control_succeeded(self, run_id: str, command_id: str) -> None:
        await self._update_control_command_status(run_id, command_id, "succeeded")

    async def mark_control_failed(
        self, run_id: str, command_id: str, error_code: str | None = None
    ) -> None:
        await self._update_control_command_status(
            run_id, command_id, "failed", error_code=error_code
        )

    async def try_claim(self, request: RunRequest, owner: str) -> LeaseFence | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (
                        run_id, request_json, owner, lease_generation, lease_expires_at
                    ) VALUES (%s, %s, %s, 1, %s)
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING lease_generation
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (
                        request.run_id,
                        request.model_dump_json(),
                        owner,
                        self._clock() + self._ttl_ms,
                    ),
                )
                row = await cur.fetchone()
        if row is None:
            return None
        return LeaseFence(owner=owner, generation=int(row["lease_generation"]))

    async def claim_dispatch(
        self, request: RunRequest, consumer: str
    ) -> LeaseFence | None:
        now = self._clock()
        try:
            async with connect_pg(self._database_url) as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            UPDATE {}
                            SET status = 'claimed', claimed_by = %s, updated_at = %s
                            WHERE run_id = %s AND status = 'pending' AND request_json = %s
                            RETURNING run_id
                            """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                            (
                                consumer,
                                now,
                                request.run_id,
                                request.model_dump_json(),
                            ),
                        )
                        if await cur.fetchone() is None:
                            return None
                        await cur.execute(
                            """
                            INSERT INTO {} (
                                run_id, request_json, owner, lease_generation,
                                lease_expires_at
                            ) VALUES (%s, %s, %s, 1, %s)
                            ON CONFLICT (run_id) DO NOTHING
                            RETURNING lease_generation
                            """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                            (
                                request.run_id,
                                request.model_dump_json(),
                                consumer,
                                now + self._ttl_ms,
                            ),
                        )
                        row = await cur.fetchone()
                        if row is None:
                            # Returning here would commit the preceding pending→claimed
                            # update. Raising through the transaction context rolls it back.
                            raise _DispatchLeaseConflict
                        return LeaseFence(
                            owner=consumer,
                            generation=int(row["lease_generation"]),
                        )
        except _DispatchLeaseConflict:
            return None

    async def get_pending_dispatch(self, run_id: str) -> RunRequest | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT request_json
                    FROM {}
                    WHERE run_id = %s AND status = 'pending'
                    """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                    (run_id,),
                )
                row = await cur.fetchone()
        if row is None:
            return None
        return RunRequest.model_validate_json(row["request_json"])

    async def list_pending_dispatches(self, limit: int = 100) -> list[RunRequest]:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT request_json
                    FROM {}
                    WHERE status = 'pending'
                    ORDER BY created_at ASC, run_id ASC
                    LIMIT %s
                    """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                    (limit,),
                )
                rows = await cur.fetchall()
        return [RunRequest.model_validate_json(row["request_json"]) for row in rows]

    async def quarantine_dispatch(
        self, raw_hash: str, source: str, reason: str
    ) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (raw_hash, source, reason, at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (raw_hash) DO NOTHING
                    """.format(qualified(self._schema, RUN_DLQ_TABLE)),
                    (raw_hash, source, reason, self._clock()),
                )

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
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    lease_current = (
                        await self._lock_active_lease(cur, run_id, lease)
                        if kind == "run.started"
                        else await self._lock_fence(cur, run_id, lease)
                    )
                    if not lease_current:
                        return None
                    await cur.execute(
                        """
                        SELECT durable_counter, event_index_counter, terminal_fence_seq
                        FROM {}
                        WHERE run_id = %s
                        FOR UPDATE
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (run_id,),
                    )
                    row = await cur.fetchone()
                    assert row is not None
                    seq = int(row["durable_counter"]) + 1
                    fence = row["terminal_fence_seq"]
                    if terminal and fence is None:
                        fence = seq
                    if fence is not None and seq > int(fence):
                        await cur.execute(
                            """
                            UPDATE {}
                            SET durable_counter = %s, terminal_fence_seq = %s
                            WHERE run_id = %s
                            """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                            (seq, fence, run_id),
                        )
                        await cur.execute(
                            """
                            INSERT INTO {} (
                                run_id, durable_seq, event_id, kind, status, index_value,
                                timestamp, payload_json, published_at
                            ) VALUES (%s, %s, %s, %s, 'superseded', NULL, %s, %s, NULL)
                            """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
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
                    await cur.execute(
                        """
                        UPDATE {}
                        SET durable_counter = %s,
                            event_index_counter = %s,
                            terminal_fence_seq = %s
                        WHERE run_id = %s
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (seq, index + 1, fence, run_id),
                    )
                    await cur.execute(
                        """
                        INSERT INTO {} (
                            run_id, durable_seq, event_id, kind, status, index_value,
                            timestamp, payload_json, published_at
                        ) VALUES (%s, %s, %s, %s, 'queued', %s, %s, %s, NULL)
                        """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
                        (run_id, seq, event_id, kind, index, timestamp, payload_json),
                    )
        return StagedFrame(durable_seq=seq, event_id=event_id, index=index)

    async def next_event_index(self, run_id: str) -> int:
        """Read the claims-owned event-index high-water mark."""

        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT event_index_counter
                    FROM {}
                    WHERE run_id = %s
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (run_id,),
                )
                row = await cur.fetchone()
        return 0 if row is None else int(row["event_index_counter"])

    async def reserve_event_index(self, run_id: str, lease: LeaseFence) -> int | None:
        """Atomically reserve one live-event index under the active lease row lock."""

        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._lock_active_lease(cur, run_id, lease):
                        return None
                    await cur.execute(
                        """
                        UPDATE {}
                        SET event_index_counter = event_index_counter + 1
                        WHERE run_id = %s
                        RETURNING event_index_counter - 1 AS index_value
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (run_id,),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise RuntimeError(
                            f"failed to reserve an event index for {run_id!r}"
                        )
                    return int(row["index_value"])

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET status = 'published', published_at = %s
                    WHERE run_id = %s AND durable_seq = %s AND status = 'queued'
                    """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
                    (self._clock(), run_id, durable_seq),
                )

    async def list_unpublished_outbox(self) -> list[OutboxFrame]:
        rows = await self._fetch_outbox("status = 'queued'")
        return [_outbox_row_to_frame(row) for row in rows]

    async def list_open_outbox_runs(self) -> list[str]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT DISTINCT run_id
                    FROM {}
                    WHERE status IN ('queued', 'published')
                    ORDER BY run_id ASC
                    """.format(qualified(self._schema, RUN_OUTBOX_TABLE))
                )
                rows = await cur.fetchall()
        return [str(row["run_id"]) for row in rows]

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30_000
    ) -> ReceiptReconcile:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT durable_seq, event_id, kind, status, index_value, timestamp,
                               payload_json, published_at
                        FROM {}
                        WHERE run_id = %s AND status IN ('queued', 'published')
                        ORDER BY durable_seq ASC
                        """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
                        (run_id,),
                    )
                    live_rows = await cur.fetchall()
                    if not live_rows:
                        return ReceiptReconcile()
                    await cur.execute(
                        """
                        SELECT durable_seq, event_id, status, reason, created_at
                        FROM {}
                        WHERE run_id = %s
                        ORDER BY durable_seq ASC
                        """.format(qualified(self._schema, RUN_RECEIPTS_TABLE)),
                        (run_id,),
                    )
                    receipt_rows = await cur.fetchall()
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
                        await cur.execute(
                            """
                            UPDATE {}
                            SET terminal_fence_seq = CASE
                                WHEN terminal_fence_seq IS NULL OR terminal_fence_seq > %s
                                THEN %s
                                ELSE terminal_fence_seq
                            END
                            WHERE run_id = %s
                            """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                            (seq, seq, run_id),
                        )
                        return ReceiptReconcile(rejected_seq=seq)
                    republish = []
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
                        await cur.execute(
                            """
                            UPDATE {}
                            SET published_at = %s
                            WHERE run_id = %s AND durable_seq = %s
                            """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
                            (now, run_id, frame.durable_seq),
                        )
                    await cur.execute(
                        """
                        SELECT run_id, persisted_seq, projected_seq, consumed_seq,
                               producer_close_requested, producer_closed, updated_at
                        FROM {}
                        WHERE run_id = %s
                        """.format(
                            qualified(self._schema, RUN_RECEIPT_MANIFESTS_TABLE)
                        ),
                        (run_id,),
                    )
                    manifest = await cur.fetchone()
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
                        await cur.execute(
                            """
                            INSERT INTO {} (
                                run_id, persisted_seq, projected_seq, consumed_seq,
                                producer_close_requested, producer_closed, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (run_id) DO UPDATE SET
                                consumed_seq = EXCLUDED.consumed_seq,
                                updated_at = EXCLUDED.updated_at
                            """.format(
                                qualified(self._schema, RUN_RECEIPT_MANIFESTS_TABLE)
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
                        await cur.execute(
                            """
                            DELETE FROM {}
                            WHERE run_id = %s AND durable_seq <= %s
                            """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
                            (run_id, advanced),
                        )
                    await cur.execute(
                        """
                        SELECT COUNT(*) AS open_count
                        FROM {}
                        WHERE run_id = %s AND status IN ('queued', 'published')
                        """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
                        (run_id,),
                    )
                    open_count_row = await cur.fetchone()
                    if open_count_row is None:
                        raise RuntimeError(
                            f"failed to count open outbox rows for {run_id!r}"
                        )
                    open_count = int(open_count_row["open_count"])
                    fence_row = await self._select_claim_row(cur, run_id)
                    fence = (
                        fence_row["terminal_fence_seq"]
                        if fence_row is not None
                        else None
                    )
                    close_requested = False
                    if fence is not None and advanced >= int(fence) and open_count == 0:
                        await cur.execute(
                            """
                            UPDATE {}
                            SET producer_close_requested = TRUE, updated_at = %s
                            WHERE run_id = %s AND producer_close_requested = FALSE
                            """.format(
                                qualified(self._schema, RUN_RECEIPT_MANIFESTS_TABLE)
                            ),
                            (now, run_id),
                        )
                        close_requested = True
                    return ReceiptReconcile(
                        consumed_through=advanced if advanced > consumed else None,
                        close_requested=close_requested,
                        republish=republish,
                    )

    async def record_control_delivery(
        self,
        run_id: str,
        command_id: str,
        request_digest: str | None,
        fingerprint: str | None,
        body: str,
    ) -> bool:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT terminal
                    FROM {}
                    WHERE run_id = %s
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (run_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    return False
                await cur.execute(
                    """
                    SELECT request_digest
                    FROM {}
                    WHERE run_id = %s AND command_id = %s
                    """.format(qualified(self._schema, RUN_CONTROL_COMMANDS_TABLE)),
                    (run_id, command_id),
                )
                command = await cur.fetchone()
                if command is None:
                    return False
                if (
                    request_digest is not None
                    and str(command["request_digest"]) != request_digest
                ):
                    raise ControlCommandConflict(
                        f"command id {command_id!r} was reused with a different request digest"
                    )
                await cur.execute(
                    """
                    UPDATE {}
                    SET status = 'persisted', fingerprint = %s, updated_at = %s
                    WHERE run_id = %s AND command_id = %s AND status = 'admitted'
                    RETURNING command_id
                    """.format(qualified(self._schema, RUN_CONTROL_COMMANDS_TABLE)),
                    (
                        fingerprint,
                        self._clock(),
                        run_id,
                        command_id,
                    ),
                )
                return await cur.fetchone() is not None

    async def mark_control_applied(self, run_id: str, command_id: str) -> None:
        await self._update_control_status(run_id, command_id, "applied")

    async def mark_control_superseded(self, run_id: str, command_id: str) -> None:
        await self._update_control_status(run_id, command_id, "superseded")

    async def list_pending_control_delivery(self) -> list[RunControlCommandRecord]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT i.run_id, i.command_id, i.request_digest, i.fingerprint, i.body
                    FROM {} i
                    JOIN {} r ON r.run_id = i.run_id
                    WHERE i.status = 'persisted' AND r.terminal = FALSE
                    ORDER BY i.run_id ASC, i.command_id ASC
                    """.format(
                        qualified(self._schema, RUN_CONTROL_COMMANDS_TABLE),
                        qualified(self._schema, RUN_CLAIMS_TABLE),
                    )
                )
                rows = await cur.fetchall()
        return [RunControlCommandRecord(**dict(row)) for row in rows]

    async def renew(self, run_id: str, lease: LeaseFence) -> bool:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET lease_expires_at = %s
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > %s
                      AND terminal = FALSE
                    RETURNING run_id
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (
                        now + self._ttl_ms,
                        run_id,
                        lease.owner,
                        lease.generation,
                        now,
                    ),
                )
                return await cur.fetchone() is not None

    async def adopt(self, run_id: str, owner: str) -> LeaseFence | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET owner = %s,
                        lease_generation = lease_generation + 1,
                        lease_expires_at = %s
                    WHERE run_id = %s
                      AND terminal = FALSE
                      AND lease_expires_at IS NULL
                    RETURNING lease_generation
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (owner, self._clock() + self._ttl_ms, run_id),
                )
                row = await cur.fetchone()
        if row is None:
            return None
        return LeaseFence(owner=owner, generation=int(row["lease_generation"]))

    async def pause(self, run_id: str, lease: LeaseFence) -> bool:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET lease_expires_at = NULL
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > %s
                      AND terminal = FALSE
                    RETURNING run_id
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (run_id, lease.owner, lease.generation, now),
                )
                return await cur.fetchone() is not None

    async def reclaim_expired(self, owner: str) -> list[LeasedRun]:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET owner = %s,
                        lease_generation = lease_generation + 1,
                        lease_expires_at = %s
                    WHERE terminal = FALSE AND lease_expires_at IS NOT NULL AND lease_expires_at <= %s
                    RETURNING request_json, lease_generation
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (owner, now + self._ttl_ms, now),
                )
                rows = await cur.fetchall()
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
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT 1
                    FROM {}
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > %s
                      AND terminal = FALSE
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (run_id, lease.owner, lease.generation, now),
                )
                return await cur.fetchone() is not None

    async def is_fence_current(self, run_id: str, lease: LeaseFence) -> bool:
        """Check generation ownership even after pause/terminal clears the active expiry."""

        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT 1
                    FROM {}
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (run_id, lease.owner, lease.generation),
                )
                return await cur.fetchone() is not None

    async def get_fence(self, run_id: str) -> LeaseFence | None:
        row = await self._get_claim_row(run_id)
        if row is None or row["owner"] is None:
            return None
        generation = int(row["lease_generation"])
        if generation < 1:
            return None
        return LeaseFence(owner=str(row["owner"]), generation=generation)

    async def get_request(self, run_id: str) -> RunRequest | None:
        row = await self._get_claim_row(run_id)
        if row is None or row["request_json"] is None:
            return None
        return RunRequest.model_validate_json(row["request_json"])

    async def get_request_scoped(
        self, run_id: str, namespace: str
    ) -> RunRequest | None:
        """Read an ingress-visible run only inside its durable identity scope."""

        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT claim.request_json
                    FROM {} AS claim
                    INNER JOIN {} AS dispatch ON dispatch.run_id = claim.run_id
                    WHERE claim.run_id = %s AND dispatch.namespace = %s
                    """.format(
                        qualified(self._schema, RUN_CLAIMS_TABLE),
                        qualified(self._schema, RUN_DISPATCHES_TABLE),
                    ),
                    (run_id, namespace),
                )
                row = await cur.fetchone()
        if row is None or row["request_json"] is None:
            return None
        return RunRequest.model_validate_json(row["request_json"])

    async def list_paused(self) -> list[str]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT run_id
                    FROM {}
                    WHERE terminal = FALSE AND lease_expires_at IS NULL AND request_json IS NOT NULL
                    ORDER BY run_id ASC
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE))
                )
                rows = await cur.fetchall()
        return [str(row["run_id"]) for row in rows]

    async def add_tokens(
        self, run_id: str, lease: LeaseFence, count: int
    ) -> int | None:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET token_total = token_total + %s
                    WHERE run_id = %s
                      AND owner = %s
                      AND lease_generation = %s
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at > %s
                      AND terminal = FALSE
                    RETURNING token_total
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (count, run_id, lease.owner, lease.generation, now),
                )
                row = await cur.fetchone()
        if row is None:
            return None
        return int(row["token_total"])

    async def add_usage(
        self,
        run_id: str,
        lease: LeaseFence,
        input_tokens: int,
        output_tokens: int,
    ) -> tuple[int, int] | None:
        if input_tokens < 0 or output_tokens < 0:
            raise ValueError("usage token counts must be non-negative")
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT usage_input_total, usage_output_total
                        FROM {}
                        WHERE run_id = %s
                          AND owner = %s
                          AND lease_generation = %s
                          AND (
                            terminal = TRUE
                            OR (lease_expires_at IS NOT NULL AND lease_expires_at > %s)
                          )
                        FOR UPDATE
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (run_id, lease.owner, lease.generation, now),
                    )
                    claim = await cur.fetchone()
                    if claim is None:
                        return None
                    await cur.execute(
                        """
                        SELECT input_tokens, output_tokens
                        FROM {}
                        WHERE run_id = %s AND lease_generation = %s
                        """.format(qualified(self._schema, RUN_USAGE_SEGMENTS_TABLE)),
                        (run_id, lease.generation),
                    )
                    existing = await cur.fetchone()
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
                    await cur.execute(
                        """
                        INSERT INTO {} (
                            run_id, lease_generation, input_tokens, output_tokens, created_at
                        ) VALUES (%s, %s, %s, %s, %s)
                        """.format(qualified(self._schema, RUN_USAGE_SEGMENTS_TABLE)),
                        (
                            run_id,
                            lease.generation,
                            input_tokens,
                            output_tokens,
                            now,
                        ),
                    )
                    await cur.execute(
                        """
                        UPDATE {}
                        SET usage_input_total = usage_input_total + %s,
                            usage_output_total = usage_output_total + %s
                        WHERE run_id = %s
                        RETURNING usage_input_total, usage_output_total
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (input_tokens, output_tokens, run_id),
                    )
                    updated = await cur.fetchone()
                    if updated is None:
                        raise RuntimeError(
                            f"usage aggregate disappeared for run {run_id!r}"
                        )
                    return (
                        int(updated["usage_input_total"]),
                        int(updated["usage_output_total"]),
                    )

    async def purge_terminal(self, max_age_ms: int) -> int:
        cutoff = self._clock() - max_age_ms
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT claim.run_id
                        FROM {} AS claim
                        WHERE claim.terminal = TRUE
                          AND claim.terminal_at IS NOT NULL
                          AND claim.terminal_at <= %s
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {} AS cleanup
                              WHERE cleanup.run_id = claim.run_id
                                AND cleanup.status <> 'completed'
                          )
                        """.format(
                            qualified(self._schema, RUN_CLAIMS_TABLE),
                            qualified(self._schema, SANDBOX_CLEANUP_INTENTS_TABLE),
                        ),
                        (cutoff,),
                    )
                    rows = await cur.fetchall()
                    run_ids = [str(row["run_id"]) for row in rows]
                    if not run_ids:
                        return 0
                    await self._delete_run_rows(cur, run_ids)
                    await cur.execute(
                        "DELETE FROM {} WHERE run_id = ANY(%s)".format(
                            qualified(self._schema, RUN_CLAIMS_TABLE)
                        ),
                        (run_ids,),
                    )
        return len(run_ids)

    async def try_mark_terminal(self, run_id: str, lease: LeaseFence) -> bool:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE {}
                        SET terminal = TRUE,
                            terminal_at = COALESCE(terminal_at, %s),
                            lease_expires_at = NULL
                        WHERE run_id = %s
                          AND owner = %s
                          AND lease_generation = %s
                          AND lease_expires_at IS NOT NULL
                          AND lease_expires_at > %s
                          AND terminal = FALSE
                        RETURNING run_id, sandbox_id, sandbox_generation,
                                  sandbox_backend_kind, sandbox_teardown_ref
                        """.format(
                            qualified(self._schema, RUN_CLAIMS_TABLE),
                        ),
                        (now, run_id, lease.owner, lease.generation, now),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return False
                    await self._queue_bound_sandbox_cleanup(cur, dict(row), now=now)
                    return True

    async def fence_and_mark_terminal(
        self, run_id: str, owner: str
    ) -> LeaseFence | None:
        """Atomically supersede any active/paused owner and claim the sole terminal write."""

        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE {}
                        SET owner = %s,
                            lease_generation = lease_generation + 1,
                            terminal = TRUE,
                            terminal_at = COALESCE(terminal_at, %s),
                            lease_expires_at = NULL
                        WHERE run_id = %s AND terminal = FALSE
                        RETURNING run_id, lease_generation, sandbox_id,
                                  sandbox_generation, sandbox_backend_kind,
                                  sandbox_teardown_ref
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (owner, now, run_id),
                    )
                    row = await cur.fetchone()
                    if row is not None:
                        await self._queue_bound_sandbox_cleanup(cur, dict(row), now=now)
        if row is None:
            return None
        return LeaseFence(owner=owner, generation=int(row["lease_generation"]))

    async def is_terminal(self, run_id: str) -> bool:
        row = await self._get_claim_row(run_id)
        return bool(row and row["terminal"])

    async def execute_active_effect(
        self,
        run_id: str,
        lease: LeaseFence,
        effect: Callable[[], Awaitable[None]],
    ) -> bool:
        """Linearize one bounded external effect with reclaim, pause, and terminal CAS."""

        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._lock_active_lease(cur, run_id, lease):
                        return False
                    await effect()
                    return True

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        row = await self._get_claim_row(run_id)
        if row is None or bool(row["terminal"]):
            return
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (run_id, message_id, content, created_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (run_id, message_id) DO NOTHING
                    """.format(qualified(self._schema, RUN_STEERS_TABLE)),
                    (run_id, message_id, content, self._clock()),
                )

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT message_id, content
                    FROM {}
                    WHERE run_id = %s
                    ORDER BY created_at ASC, message_id ASC
                    """.format(qualified(self._schema, RUN_STEERS_TABLE)),
                    (run_id,),
                )
                rows = await cur.fetchall()
        return [(str(row["message_id"]), str(row["content"])) for row in rows]

    async def ack_steers(
        self, run_id: str, lease: LeaseFence, message_ids: list[str]
    ) -> bool:
        if not message_ids:
            return await self.is_lease_current(run_id, lease)
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._lock_active_lease(cur, run_id, lease):
                        return False
                    await cur.execute(
                        """
                        DELETE FROM {}
                        WHERE run_id = %s AND message_id = ANY(%s)
                        """.format(qualified(self._schema, RUN_STEERS_TABLE)),
                        (run_id, message_ids),
                    )
        return True

    async def put_tool_result(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_id: str,
        result: str,
        is_error: bool,
    ) -> tuple[str, bool] | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._lock_active_lease(cur, run_id, lease):
                        return None
                    await cur.execute(
                        """
                        INSERT INTO {} (run_id, tool_id, result, is_error)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (run_id, tool_id) DO NOTHING
                        """.format(qualified(self._schema, TOOL_RESULTS_TABLE)),
                        (run_id, tool_id, result, is_error),
                    )
                    await cur.execute(
                        """
                        SELECT result, is_error
                        FROM {}
                        WHERE run_id = %s AND tool_id = %s
                        """.format(qualified(self._schema, TOOL_RESULTS_TABLE)),
                        (run_id, tool_id),
                    )
                    row = await cur.fetchone()
        if row is None:
            raise RuntimeError(f"tool result missing after insert for {run_id!r}")
        return str(row["result"]), bool(row["is_error"])

    async def get_tool_result(
        self, run_id: str, tool_id: str
    ) -> tuple[str, bool] | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT result, is_error
                    FROM {}
                    WHERE run_id = %s AND tool_id = %s
                    """.format(qualified(self._schema, TOOL_RESULTS_TABLE)),
                    (run_id, tool_id),
                )
                row = await cur.fetchone()
        if row is None:
            return None
        return str(row["result"]), bool(row["is_error"])

    async def journal_tool_started(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, name: str
    ) -> bool:
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._lock_active_lease(cur, run_id, lease):
                        return False
                    await cur.execute(
                        """
                        INSERT INTO {} (run_id, tool_call_id, name, status, result, is_error)
                        VALUES (%s, %s, %s, 'started', '', FALSE)
                        ON CONFLICT (run_id, tool_call_id) DO NOTHING
                        RETURNING tool_call_id
                        """.format(qualified(self._schema, TOOL_JOURNAL_TABLE)),
                        (run_id, tool_call_id, name),
                    )
                    return await cur.fetchone() is not None

    async def journal_tool_finished(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_call_id: str,
        result: str,
        is_error: bool,
    ) -> bool:
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._lock_active_lease(cur, run_id, lease):
                        return False
                    await cur.execute(
                        """
                        UPDATE {}
                        SET status = %s, result = %s, is_error = %s
                        WHERE run_id = %s AND tool_call_id = %s AND status = 'started'
                        RETURNING tool_call_id
                        """.format(qualified(self._schema, TOOL_JOURNAL_TABLE)),
                        (
                            "failed" if is_error else "succeeded",
                            result,
                            is_error,
                            run_id,
                            tool_call_id,
                        ),
                    )
                    return await cur.fetchone() is not None

    async def clear_tool_journal(
        self, run_id: str, lease: LeaseFence, tool_call_id: str
    ) -> bool:
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._lock_active_lease(cur, run_id, lease):
                        return False
                    await cur.execute(
                        """
                        DELETE FROM {}
                        WHERE run_id = %s AND tool_call_id = %s
                        """.format(qualified(self._schema, TOOL_JOURNAL_TABLE)),
                        (run_id, tool_call_id),
                    )
        return True

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT name, status, result, is_error
                    FROM {}
                    WHERE run_id = %s AND tool_call_id = %s
                    """.format(qualified(self._schema, TOOL_JOURNAL_TABLE)),
                    (run_id, tool_call_id),
                )
                row = await cur.fetchone()
        if row is None:
            return None
        return ToolJournalRecord(**dict(row))

    async def bind_sandbox_id(
        self,
        run_id: str,
        lease: LeaseFence,
        *,
        expected_sandbox_id: str | None,
        sandbox_id: str,
        backend_kind: SandboxBackendKind,
        teardown_ref: str,
    ) -> str | None:
        if not sandbox_id.strip():
            raise ValueError("sandbox_id must be non-empty")
        if backend_kind not in {"docker", "e2b", "custom"}:
            raise ValueError("sandbox backend must have a managed lifecycle")
        if not teardown_ref.strip():
            raise ValueError("sandbox teardown_ref must be non-empty")
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._lock_active_lease(cur, run_id, lease):
                        return None
                    await cur.execute(
                        """
                        SELECT sandbox_id, sandbox_generation,
                               sandbox_backend_kind, sandbox_teardown_ref
                        FROM {}
                        WHERE run_id = %s
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (run_id,),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return None
                    current = row["sandbox_id"]
                    confirmed_generation = row["sandbox_generation"]
                    if (
                        current is not None
                        and confirmed_generation is not None
                        and int(confirmed_generation) == lease.generation
                    ):
                        return str(current)
                    if current != expected_sandbox_id:
                        return None if current is None else str(current)
                    if current is not None and str(current) == sandbox_id:
                        # A later generation reconnecting the same sandbox must keep
                        # the destruction identity captured when that resource was
                        # created, even if deployment configuration has since changed.
                        await cur.execute(
                            """
                            UPDATE {}
                            SET sandbox_generation = %s
                            WHERE run_id = %s
                              AND owner = %s
                              AND lease_generation = %s
                            RETURNING sandbox_id
                            """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                            (lease.generation, run_id, lease.owner, lease.generation),
                        )
                        rebound = await cur.fetchone()
                        return None if rebound is None else str(rebound["sandbox_id"])
                    await cur.execute(
                        """
                        UPDATE {}
                        SET sandbox_id = %s,
                            sandbox_generation = %s,
                            sandbox_backend_kind = %s,
                            sandbox_teardown_ref = %s
                        WHERE run_id = %s
                          AND owner = %s
                          AND lease_generation = %s
                        RETURNING sandbox_id
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (
                            sandbox_id,
                            lease.generation,
                            backend_kind,
                            teardown_ref,
                            run_id,
                            lease.owner,
                            lease.generation,
                        ),
                    )
                    bound = await cur.fetchone()
                    return None if bound is None else str(bound["sandbox_id"])

    async def get_sandbox_id(self, run_id: str) -> str | None:
        row = await self._get_claim_row(run_id)
        if row is None:
            return None
        return row["sandbox_id"]

    async def register_sandbox_cleanup(
        self,
        *,
        run_id: str,
        lease_generation: int,
        backend_kind: SandboxBackendKind,
        sandbox_id: str,
        teardown_ref: str,
    ) -> SandboxCleanupIntent:
        if lease_generation < 1:
            raise ValueError("sandbox cleanup lease_generation must be positive")
        if backend_kind not in {"docker", "e2b", "custom"}:
            raise ValueError("sandbox cleanup backend must have a managed lifecycle")
        if not sandbox_id.strip() or not teardown_ref.strip():
            raise ValueError("sandbox cleanup identity must be non-empty")
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    return await self._insert_sandbox_cleanup(
                        cur,
                        run_id=run_id,
                        lease_generation=lease_generation,
                        backend_kind=backend_kind,
                        sandbox_id=sandbox_id,
                        teardown_ref=teardown_ref,
                        now=now,
                    )

    async def claim_sandbox_cleanups(
        self,
        owner: str,
        *,
        run_id: str | None = None,
        limit: int = 100,
        lease_ms: int = 30_000,
    ) -> list[SandboxCleanupIntent]:
        if not owner.strip():
            raise ValueError("sandbox cleanup owner must be non-empty")
        if limit < 1 or limit > 1_000:
            raise ValueError("sandbox cleanup claim limit must be between 1 and 1000")
        if lease_ms < 1:
            raise ValueError("sandbox cleanup claim lease must be positive")
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        WITH due AS (
                            SELECT cleanup_id
                            FROM {}
                            WHERE status IN ('pending', 'processing')
                              AND next_attempt_at <= %s
                              AND (%s::text IS NULL OR run_id = %s)
                            ORDER BY next_attempt_at ASC, created_at ASC, cleanup_id ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT %s
                        )
                        UPDATE {} AS cleanup
                        SET status = 'processing',
                            cleanup_owner = %s,
                            attempt_count = cleanup.attempt_count + 1,
                            next_attempt_at = %s,
                            updated_at = %s
                        FROM due
                        WHERE cleanup.cleanup_id = due.cleanup_id
                        RETURNING cleanup.cleanup_id, cleanup.run_id,
                                  cleanup.lease_generation, cleanup.backend_kind,
                                  cleanup.sandbox_id, cleanup.teardown_ref,
                                  cleanup.attempt_count, cleanup.next_attempt_at
                        """.format(
                            qualified(self._schema, SANDBOX_CLEANUP_INTENTS_TABLE),
                            qualified(self._schema, SANDBOX_CLEANUP_INTENTS_TABLE),
                        ),
                        (now, run_id, run_id, limit, owner, now + lease_ms, now),
                    )
                    rows = await cur.fetchall()
        return [_sandbox_cleanup_from_row(dict(row)) for row in rows]

    async def complete_sandbox_cleanup(self, cleanup_id: str) -> bool:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET status = 'completed', cleanup_owner = NULL,
                        last_error = NULL, updated_at = %s
                    WHERE cleanup_id = %s AND status <> 'completed'
                    RETURNING cleanup_id
                    """.format(qualified(self._schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
                    (now, cleanup_id),
                )
                return await cur.fetchone() is not None

    async def reschedule_sandbox_cleanup(
        self, cleanup_id: str, error: str, *, retry_delay_ms: int
    ) -> bool:
        if retry_delay_ms < 0:
            raise ValueError("sandbox cleanup retry delay must be non-negative")
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET status = 'pending', cleanup_owner = NULL,
                        next_attempt_at = %s, last_error = %s, updated_at = %s
                    WHERE cleanup_id = %s AND status <> 'completed'
                    RETURNING cleanup_id
                    """.format(qualified(self._schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
                    (now + retry_delay_ms, error[:1_000], now, cleanup_id),
                )
                return await cur.fetchone() is not None

    async def _insert_sandbox_cleanup(
        self,
        cur: Any,
        *,
        run_id: str,
        lease_generation: int,
        backend_kind: SandboxBackendKind,
        sandbox_id: str,
        teardown_ref: str,
        now: int,
    ) -> SandboxCleanupIntent:
        cleanup_id = _sandbox_cleanup_id(
            run_id, lease_generation, backend_kind, sandbox_id
        )
        await cur.execute(
            """
            INSERT INTO {} (
                cleanup_id, run_id, lease_generation, backend_kind, sandbox_id,
                teardown_ref, status, cleanup_owner, attempt_count,
                next_attempt_at, last_error, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 'pending', NULL, 0, %s, NULL, %s, %s)
            ON CONFLICT DO NOTHING
            """.format(qualified(self._schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
            (
                cleanup_id,
                run_id,
                lease_generation,
                backend_kind,
                sandbox_id,
                teardown_ref,
                now,
                now,
                now,
            ),
        )
        await cur.execute(
            """
            SELECT cleanup_id, run_id, lease_generation, backend_kind, sandbox_id,
                   teardown_ref, attempt_count, next_attempt_at
            FROM {}
            WHERE cleanup_id = %s
            """.format(qualified(self._schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
            (cleanup_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise RuntimeError(
                f"sandbox cleanup registration failed for {sandbox_id!r}"
            )
        intent = _sandbox_cleanup_from_row(dict(row))
        if intent.teardown_ref != teardown_ref:
            raise RuntimeError(f"sandbox cleanup identity conflict for {sandbox_id!r}")
        return intent

    async def _queue_bound_sandbox_cleanup(
        self, cur: Any, row: dict[str, Any], *, now: int
    ) -> None:
        sandbox_id = row.get("sandbox_id")
        generation = row.get("sandbox_generation")
        backend_kind = row.get("sandbox_backend_kind")
        teardown_ref = row.get("sandbox_teardown_ref")
        if sandbox_id is None:
            return
        if (
            generation is None
            or backend_kind not in {"docker", "e2b", "custom"}
            or not isinstance(teardown_ref, str)
            or not teardown_ref
        ):
            raise RuntimeError(
                "terminal sandbox binding has incomplete cleanup identity"
            )
        await self._insert_sandbox_cleanup(
            cur,
            run_id=str(row["run_id"]),
            lease_generation=int(generation),
            backend_kind=backend_kind,
            sandbox_id=str(sandbox_id),
            teardown_ref=teardown_ref,
            now=now,
        )

    async def _lock_active_lease(
        self, cur: Any, run_id: str, lease: LeaseFence
    ) -> bool:
        """Serialize one execution effect with lease transfer/terminal fencing."""

        await cur.execute(
            """
            SELECT 1
            FROM {}
            WHERE run_id = %s
              AND owner = %s
              AND lease_generation = %s
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at > %s
              AND terminal = FALSE
            FOR UPDATE
            """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
            (run_id, lease.owner, lease.generation, self._clock()),
        )
        return await cur.fetchone() is not None

    async def _lock_fence(self, cur: Any, run_id: str, lease: LeaseFence) -> bool:
        """Serialize a terminal/control effect with the current generation."""

        await cur.execute(
            """
            SELECT 1
            FROM {}
            WHERE run_id = %s
              AND owner = %s
              AND lease_generation = %s
            FOR UPDATE
            """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
            (run_id, lease.owner, lease.generation),
        )
        return await cur.fetchone() is not None

    async def _get_claim_row(self, run_id: str) -> dict[str, Any] | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                return await self._select_claim_row(cur, run_id)

    async def _select_claim_row(self, cur: Any, run_id: str) -> dict[str, Any] | None:
        await cur.execute(
            """
            SELECT run_id, request_json, owner, lease_generation, lease_expires_at,
                   terminal, terminal_at,
                   durable_counter, event_index_counter, terminal_fence_seq, token_total,
                   usage_input_total, usage_output_total, sandbox_id, sandbox_generation,
                   sandbox_backend_kind, sandbox_teardown_ref
            FROM {}
            WHERE run_id = %s
            """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
            (run_id,),
        )
        row = await cur.fetchone()
        return None if row is None else dict(row)

    async def _get_dispatch_rows(self, run_id: str) -> list[dict[str, Any]]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT run_id, session_id, namespace, request_json, fence, status,
                           claimed_by, created_at, updated_at
                    FROM {}
                    WHERE run_id = %s
                    """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                    (run_id,),
                )
                return [dict(row) for row in await cur.fetchall()]

    async def _update_control_status(
        self, run_id: str, command_id: str, status: str
    ) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET status = %s, updated_at = %s
                    WHERE run_id = %s AND command_id = %s AND status = 'persisted'
                    """.format(qualified(self._schema, RUN_CONTROL_COMMANDS_TABLE)),
                    (status, self._clock(), run_id, command_id),
                )

    async def _update_control_command_status(
        self,
        run_id: str,
        command_id: str,
        status: ControlAdmissionStatus,
        *,
        error_code: str | None = None,
    ) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET status = %s, error_code = %s, updated_at = %s
                    WHERE run_id = %s AND command_id = %s
                      AND status IN ('admitted', 'persisted', 'applied')
                    """.format(qualified(self._schema, RUN_CONTROL_COMMANDS_TABLE)),
                    (status, error_code, self._clock(), run_id, command_id),
                )

    async def _fetch_outbox(self, where_sql: str) -> list[dict[str, Any]]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT run_id, durable_seq, event_id, kind, status, index_value,
                           timestamp, payload_json, published_at
                    FROM {}
                    WHERE {}
                    ORDER BY run_id ASC, durable_seq ASC
                    """.format(qualified(self._schema, RUN_OUTBOX_TABLE), where_sql)
                )
                return [dict(row) for row in await cur.fetchall()]

    async def _delete_run_rows(self, cur: Any, run_ids: list[str]) -> None:
        if not run_ids:
            return
        for table in (
            RUN_OUTBOX_TABLE,
            RUN_RECEIPTS_TABLE,
            RUN_RECEIPT_MANIFESTS_TABLE,
            RUN_CONTROL_COMMANDS_TABLE,
            RUN_STEERS_TABLE,
            RUN_USAGE_SEGMENTS_TABLE,
            SANDBOX_CLEANUP_INTENTS_TABLE,
            TOOL_RESULTS_TABLE,
            TOOL_JOURNAL_TABLE,
            RUN_DISPATCHES_TABLE,
        ):
            await cur.execute(
                "DELETE FROM {} WHERE run_id = ANY(%s)".format(
                    qualified(self._schema, table)
                ),
                (run_ids,),
            )


@asynccontextmanager
async def make_run_repository(
    settings: RunRepositorySettings,
) -> AsyncGenerator[RunRepository, None]:
    repository = PostgresRunRepository(
        settings.database_url, settings.lease_ttl_ms, settings.schema_name
    )
    await repository.setup()
    try:
        yield repository
    finally:
        pass


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


def _now_ms() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)
