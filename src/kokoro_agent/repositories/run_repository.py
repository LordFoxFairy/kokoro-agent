"""RunRepository 契约与 PostgreSQL 后端工厂：多 pod 去重、TTL 租约、HITL 暂停哨兵、终态认领。"""

# The adapter deliberately builds qualified SQL identifiers at runtime and
# consumes psycopg dict rows. The package stubs currently model only literal
# SQL/tuple rows, so those boundary diagnostics are covered by ruff/tests.
# pyright: reportCallIssue=false, reportArgumentType=false, reportReturnType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportIncompatibleMethodOverride=false, reportUnusedClass=false

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.contract import RunRequest
from kokoro_agent.infrastructure.postgres import DEFAULT_PG_SCHEMA, connect_pg, qualified
from kokoro_agent.repositories.schema import (
    RUN_CLAIMS_TABLE,
    RUN_DISPATCHES_TABLE,
    RUN_DLQ_TABLE,
    RUN_CONTROL_COMMANDS_TABLE,
    RUN_OUTBOX_TABLE,
    RUN_RECEIPT_MANIFESTS_TABLE,
    RUN_RECEIPTS_TABLE,
    RUN_STEERS_TABLE,
    TOOL_JOURNAL_TABLE,
    TOOL_RESULTS_TABLE,
    ensure_run_repository_schema,
)

DEFAULT_LEASE_TTL_S = 90

__all__ = [
    "RunControlCommandRecord",
    "ControlAdmission",
    "ControlCommandConflict",
    "ControlAdmissionReceipt",
    "ControlAdmissionStatus",
    "DispatchAdmission",
    "DispatchConflict",
    "DEFAULT_LEASE_TTL_S",
    "RunRepositorySettings",
    "OutboxFrame",
    "PostgresRunRepository",
    "ReceiptReconcile",
    "RunRepository",
    "StagedFrame",
    "ToolJournalRecord",
    "make_run_repository",
]


class RunRepository(Protocol):
    async def enqueue_dispatch(
        self, request: RunRequest, namespace: str, fence: str
    ) -> DispatchAdmission: ...

    async def try_claim(self, request: RunRequest, owner: str) -> bool: ...

    async def claim_dispatch(self, run_id: str, consumer: str) -> bool: ...

    async def quarantine_dispatch(
        self, raw_hash: str, source: str, reason: str
    ) -> None: ...

    async def stage_critical_frame(
        self,
        run_id: str,
        kind: str,
        index: int,
        timestamp: int,
        payload_json: str,
        *,
        terminal: bool,
    ) -> StagedFrame | None: ...

    async def mark_critical_published(self, run_id: str, durable_seq: int) -> None: ...

    async def list_unpublished_outbox(self) -> list[OutboxFrame]: ...

    async def list_open_outbox_runs(self) -> list[str]: ...

    async def reconcile_receipts(
        self, run_id: str, republish_grace_ms: int = 30_000
    ) -> ReceiptReconcile: ...

    async def record_control_delivery(
        self,
        run_id: str,
        command_id: str,
        request_digest: str | None,
        fingerprint: str | None,
        body: str,
    ) -> bool: ...

    async def mark_control_applied(self, run_id: str, command_id: str) -> None: ...

    async def mark_control_superseded(self, run_id: str, command_id: str) -> None: ...

    async def admit_control(
        self, run_id: str, command_id: str, request_digest: str, body: str
    ) -> ControlAdmission: ...

    async def mark_control_succeeded(self, run_id: str, command_id: str) -> None: ...

    async def mark_control_failed(
        self, run_id: str, command_id: str, error_code: str | None = None
    ) -> None: ...

    async def list_pending_control_delivery(self) -> list[RunControlCommandRecord]: ...

    async def renew(self, run_id: str, owner: str) -> bool: ...

    async def adopt(self, run_id: str, owner: str) -> None: ...

    async def pause(self, run_id: str) -> None: ...

    async def reclaim_expired(self, owner: str) -> list[RunRequest]: ...

    async def get_request(self, run_id: str) -> RunRequest | None: ...

    async def list_paused(self) -> list[str]: ...

    async def add_tokens(self, run_id: str, count: int) -> int: ...

    async def add_usage(
        self, run_id: str, input_tokens: int, output_tokens: int
    ) -> tuple[int, int]: ...

    async def purge_terminal(self, max_age_ms: int) -> int: ...

    async def try_mark_terminal(self, run_id: str) -> bool: ...

    async def is_terminal(self, run_id: str) -> bool: ...

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None: ...

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]: ...

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None: ...

    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None: ...

    async def get_tool_result(
        self, run_id: str, tool_id: str
    ) -> tuple[str, bool] | None: ...

    async def journal_tool_started(
        self, run_id: str, tool_call_id: str, name: str
    ) -> bool: ...

    async def journal_tool_finished(
        self, run_id: str, tool_call_id: str, result: str, is_error: bool
    ) -> None: ...

    async def clear_tool_journal(self, run_id: str, tool_call_id: str) -> None: ...

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None: ...

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None: ...

    async def get_sandbox_id(self, run_id: str) -> str | None: ...


class DispatchConflict(RuntimeError):
    """A run id was reused with a different immutable launch envelope."""


class ControlCommandConflict(RuntimeError):
    """A command id was reused with a different immutable request digest."""


class DispatchAdmission(BaseModel):
    """Durable admission result used by the HTTP ingress before Redis publish."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    replayed: bool
    publish_required: bool


ControlAdmissionStatus = Literal["pending", "succeeded", "failed"]


def _receipt_status(command_status: str) -> ControlAdmissionStatus:
    """Project internal command state onto the small HTTP receipt state set."""

    if command_status in {"admitted", "persisted"}:
        return "pending"
    if command_status in {"applied", "succeeded"}:
        return "succeeded"
    if command_status in {"failed", "superseded"}:
        return "failed"
    raise RuntimeError(f"unknown control command status: {command_status!r}")


class ControlAdmissionReceipt(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    command_id: str
    request_digest: str
    status: ControlAdmissionStatus
    error_code: str | None = None


class ControlAdmission(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    receipt: ControlAdmissionReceipt
    replayed: bool
    publish_required: bool


class RunControlCommandRecord(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    command_id: str
    request_digest: str | None = None
    fingerprint: str | None = None
    body: str


class ToolJournalRecord(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    name: str
    status: str
    result: str
    is_error: bool


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


class StagedFrame(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    durable_seq: int
    event_id: str


class OutboxFrame(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    durable_seq: int
    event_id: str
    kind: str
    index: int
    timestamp: int
    payload_json: str


class ReceiptReconcile(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    rejected_seq: int | None = None
    receipt_state_lost: bool = False
    consumed_through: int | None = None
    close_requested: bool = False
    republish: list[OutboxFrame] = Field(default_factory=list)


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
                        run_id, session_id, namespace, fence, status, deadline_at,
                        claimed_by, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, 'pending', %s, NULL, %s, %s)
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING run_id
                    """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                    (
                        request.run_id,
                        request.session_id,
                        namespace,
                        fence,
                        now + self._ttl_ms,
                        now,
                        now,
                    ),
                )
                if await cur.fetchone() is not None:
                    return DispatchAdmission(replayed=False, publish_required=True)
                await cur.execute(
                    """
                    SELECT fence, status, deadline_at
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
                deadline = int(row["deadline_at"])
                if status == "pending" and deadline <= now:
                    await cur.execute(
                        """
                        UPDATE {}
                        SET deadline_at = %s, updated_at = %s
                        WHERE run_id = %s AND status = 'pending'
                        """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                        (now + self._ttl_ms, now, request.run_id),
                    )
                    return DispatchAdmission(replayed=True, publish_required=True)
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

    async def try_claim(self, request: RunRequest, owner: str) -> bool:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (run_id, request_json, owner, lease_expires_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING run_id
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (
                        request.run_id,
                        request.model_dump_json(),
                        owner,
                        self._clock() + self._ttl_ms,
                    ),
                )
                return await cur.fetchone() is not None

    async def claim_dispatch(self, run_id: str, consumer: str) -> bool:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET status = 'claimed', claimed_by = %s, updated_at = %s
                    WHERE run_id = %s AND status = 'pending' AND deadline_at > %s
                    RETURNING run_id
                    """.format(qualified(self._schema, RUN_DISPATCHES_TABLE)),
                    (consumer, now, run_id, now),
                )
                return await cur.fetchone() is not None

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
        kind: str,
        index: int,
        timestamp: int,
        payload_json: str,
        *,
        terminal: bool,
    ) -> StagedFrame | None:
        event_id = f"evt_{uuid4().hex}"
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await self._seed_run_row(cur, run_id)
                    await cur.execute(
                        """
                        SELECT durable_counter, terminal_fence_seq
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
                    await cur.execute(
                        """
                        UPDATE {}
                        SET durable_counter = %s, terminal_fence_seq = %s
                        WHERE run_id = %s
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                        (seq, fence, run_id),
                    )
                    if fence is not None and seq > int(fence):
                        await cur.execute(
                            """
                            INSERT INTO {} (
                                run_id, durable_seq, event_id, kind, status, index_value,
                                timestamp, payload_json, published_at
                            ) VALUES (%s, %s, %s, %s, 'superseded', %s, %s, %s, NULL)
                            """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
                            (
                                run_id,
                                seq,
                                event_id,
                                kind,
                                index,
                                timestamp,
                                payload_json,
                            ),
                        )
                        return None
                    await cur.execute(
                        """
                        INSERT INTO {} (
                            run_id, durable_seq, event_id, kind, status, index_value,
                            timestamp, payload_json, published_at
                        ) VALUES (%s, %s, %s, %s, 'queued', %s, %s, %s, NULL)
                        """.format(qualified(self._schema, RUN_OUTBOX_TABLE)),
                        (run_id, seq, event_id, kind, index, timestamp, payload_json),
                    )
        return StagedFrame(durable_seq=seq, event_id=event_id)

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
                    fence_row = await self._get_claim_row(cur, run_id)
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

    async def renew(self, run_id: str, owner: str) -> bool:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET lease_expires_at = %s
                    WHERE run_id = %s AND owner = %s AND terminal = FALSE
                    RETURNING run_id
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (self._clock() + self._ttl_ms, run_id, owner),
                )
                return await cur.fetchone() is not None

    async def adopt(self, run_id: str, owner: str) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET owner = %s, lease_expires_at = %s
                    WHERE run_id = %s AND terminal = FALSE
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (owner, self._clock() + self._ttl_ms, run_id),
                )

    async def pause(self, run_id: str) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET lease_expires_at = NULL
                    WHERE run_id = %s AND terminal = FALSE
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (run_id,),
                )

    async def reclaim_expired(self, owner: str) -> list[RunRequest]:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET owner = %s, lease_expires_at = %s
                    WHERE terminal = FALSE AND lease_expires_at IS NOT NULL AND lease_expires_at <= %s
                    RETURNING request_json
                    """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
                    (owner, now + self._ttl_ms, now),
                )
                rows = await cur.fetchall()
        return [
            RunRequest.model_validate_json(row["request_json"])
            for row in rows
            if row["request_json"] is not None
        ]

    async def get_request(self, run_id: str) -> RunRequest | None:
        row = await self._get_claim_row(run_id)
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

    async def add_tokens(self, run_id: str, count: int) -> int:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} AS current_run (run_id, token_total)
                    VALUES (%s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET token_total = current_run.token_total + EXCLUDED.token_total
                    RETURNING token_total
                    """.format(
                        qualified(self._schema, RUN_CLAIMS_TABLE),
                    ),
                    (run_id, count),
                )
                row = await cur.fetchone()
        if row is None:
            raise RuntimeError(f"failed to add tokens for {run_id!r}")
        return int(row["token_total"])

    async def add_usage(
        self, run_id: str, input_tokens: int, output_tokens: int
    ) -> tuple[int, int]:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} AS current_run (run_id, usage_input_total, usage_output_total)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET
                        usage_input_total = current_run.usage_input_total + EXCLUDED.usage_input_total,
                        usage_output_total = current_run.usage_output_total + EXCLUDED.usage_output_total
                    RETURNING usage_input_total, usage_output_total
                    """.format(
                        qualified(self._schema, RUN_CLAIMS_TABLE),
                    ),
                    (run_id, input_tokens, output_tokens),
                )
                row = await cur.fetchone()
        if row is None:
            raise RuntimeError(f"failed to add usage for {run_id!r}")
        return int(row["usage_input_total"]), int(row["usage_output_total"])

    async def purge_terminal(self, max_age_ms: int) -> int:
        cutoff = self._clock() - max_age_ms
        async with connect_pg(self._database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT run_id
                        FROM {}
                        WHERE terminal = TRUE AND terminal_at IS NOT NULL AND terminal_at <= %s
                        """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
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

    async def try_mark_terminal(self, run_id: str) -> bool:
        now = self._clock()
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} AS current_run (run_id, terminal, terminal_at, lease_expires_at)
                    VALUES (%s, TRUE, %s, NULL)
                    ON CONFLICT (run_id) DO UPDATE SET
                        terminal = TRUE,
                        terminal_at = COALESCE(current_run.terminal_at, EXCLUDED.terminal_at),
                        lease_expires_at = NULL
                    WHERE current_run.terminal = FALSE
                    RETURNING run_id
                    """.format(
                        qualified(self._schema, RUN_CLAIMS_TABLE),
                    ),
                    (run_id, now),
                )
                return await cur.fetchone() is not None

    async def is_terminal(self, run_id: str) -> bool:
        row = await self._get_claim_row(run_id)
        return bool(row and row["terminal"])

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

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None:
        if not message_ids:
            return
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM {}
                    WHERE run_id = %s AND message_id = ANY(%s)
                    """.format(qualified(self._schema, RUN_STEERS_TABLE)),
                    (run_id, message_ids),
                )

    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} (run_id, tool_id, result, is_error)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (run_id, tool_id) DO NOTHING
                    """.format(qualified(self._schema, TOOL_RESULTS_TABLE)),
                    (run_id, tool_id, result, is_error),
                )

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
        self, run_id: str, tool_call_id: str, name: str
    ) -> bool:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
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
        self, run_id: str, tool_call_id: str, result: str, is_error: bool
    ) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE {}
                    SET status = %s, result = %s, is_error = %s
                    WHERE run_id = %s AND tool_call_id = %s AND status = 'started'
                    """.format(qualified(self._schema, TOOL_JOURNAL_TABLE)),
                    (
                        "failed" if is_error else "succeeded",
                        result,
                        is_error,
                        run_id,
                        tool_call_id,
                    ),
                )

    async def clear_tool_journal(self, run_id: str, tool_call_id: str) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM {}
                    WHERE run_id = %s AND tool_call_id = %s
                    """.format(qualified(self._schema, TOOL_JOURNAL_TABLE)),
                    (run_id, tool_call_id),
                )

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

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO {} AS current_run (run_id, sandbox_id)
                    VALUES (%s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET sandbox_id = COALESCE(current_run.sandbox_id, EXCLUDED.sandbox_id)
                    """.format(
                        qualified(self._schema, RUN_CLAIMS_TABLE),
                    ),
                    (run_id, sandbox_id),
                )

    async def get_sandbox_id(self, run_id: str) -> str | None:
        row = await self._get_claim_row(run_id)
        if row is None:
            return None
        return row["sandbox_id"]

    async def _seed_run_row(self, cur: Any, run_id: str) -> None:
        await cur.execute(
            """
            INSERT INTO {} (run_id)
            VALUES (%s)
            ON CONFLICT (run_id) DO NOTHING
            """.format(qualified(self._schema, RUN_CLAIMS_TABLE)),
            (run_id,),
        )

    async def _get_claim_row(self, run_id: str) -> dict[str, Any] | None:
        async with connect_pg(self._database_url) as conn:
            async with conn.cursor() as cur:
                return await self._select_claim_row(cur, run_id)

    async def _select_claim_row(self, cur: Any, run_id: str) -> dict[str, Any] | None:
        await cur.execute(
            """
            SELECT run_id, request_json, owner, lease_expires_at, terminal, terminal_at,
                   durable_counter, terminal_fence_seq, token_total, usage_input_total,
                   usage_output_total, sandbox_id
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
                    SELECT run_id, session_id, namespace, fence, status, deadline_at,
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
    store = PostgresRunRepository(
        settings.database_url, settings.lease_ttl_ms, settings.schema_name
    )
    await store.setup()
    try:
        yield store
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
