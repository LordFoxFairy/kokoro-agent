"""Shared PostgreSQL context and transactional primitives for the run adapters."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from kokoro_agent.domain.run.repository import (
    ControlAdmissionStatus,
    LeaseFence,
    SandboxBackendKind,
    SandboxCleanupIntent,
)
from kokoro_agent.infrastructure.postgres import (
    DEFAULT_PG_SCHEMA,
    connect_pg,
    qualified,
    utc_to_epoch_millis,
)
from kokoro_agent.infrastructure.schema import (
    RUN_CLAIMS_TABLE,
    RUN_CONTROL_COMMANDS_TABLE,
    RUN_DISPATCHES_TABLE,
    RUN_OUTBOX_TABLE,
    RUN_RECEIPT_MANIFESTS_TABLE,
    RUN_RECEIPTS_TABLE,
    RUN_STEERS_TABLE,
    RUN_USAGE_SEGMENTS_TABLE,
    SANDBOX_CLEANUP_INTENTS_TABLE,
    TOOL_JOURNAL_TABLE,
    TOOL_RESULTS_TABLE,
    verify_agent_schema,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one


class PostgresRunRepositoryContext:
    """Own connection configuration and cross-capability SQL primitives.

    Capability adapters deliberately receive this narrow context rather than
    reaching into one another.  The façade remains the only public repository
    object injected into application code.
    """

    def __init__(
        self,
        database_url: str,
        ttl_ms: int,
        schema: str = DEFAULT_PG_SCHEMA,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self.database_url = database_url
        self.schema = schema
        self.ttl_ms = ttl_ms
        self.clock = clock or _now_ms

    async def setup(self) -> None:
        async with connect_pg(self.database_url) as conn:
            await verify_agent_schema(conn, self.schema)

    async def insert_sandbox_cleanup(
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
        cleanup_id = sandbox_cleanup_id(
            run_id, lease_generation, backend_kind, sandbox_id
        )
        await execute_sql(
            cur,
            """
            INSERT INTO {} (
                cleanup_id, run_id, lease_generation, backend_kind, sandbox_id,
                teardown_ref, status, cleanup_owner, attempt_count,
                next_attempt_at, last_error, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 'pending', NULL, 0,
                      to_timestamp(%s / 1000.0), NULL,
                      to_timestamp(%s / 1000.0), to_timestamp(%s / 1000.0))
            ON CONFLICT DO NOTHING
            """.format(qualified(self.schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
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
        await execute_sql(
            cur,
            """
            SELECT cleanup_id, run_id, lease_generation, backend_kind, sandbox_id,
                   teardown_ref, attempt_count, next_attempt_at
            FROM {}
            WHERE cleanup_id = %s
            """.format(qualified(self.schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
            (cleanup_id,),
        )
        row = await fetch_one(cur)
        if row is None:
            raise RuntimeError(
                f"sandbox cleanup registration failed for {sandbox_id!r}"
            )
        intent = sandbox_cleanup_from_row(dict(row))
        if intent.teardown_ref != teardown_ref:
            raise RuntimeError(f"sandbox cleanup identity conflict for {sandbox_id!r}")
        return intent

    async def queue_bound_sandbox_cleanup(
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
        await self.insert_sandbox_cleanup(
            cur,
            run_id=str(row["run_id"]),
            lease_generation=int(generation),
            backend_kind=sandbox_backend_kind(backend_kind),
            sandbox_id=str(sandbox_id),
            teardown_ref=teardown_ref,
            now=now,
        )

    async def lock_active_lease(self, cur: Any, run_id: str, lease: LeaseFence) -> bool:
        """Serialize one execution effect with lease transfer/terminal fencing."""

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
            FOR UPDATE
            """.format(qualified(self.schema, RUN_CLAIMS_TABLE)),
            (run_id, lease.owner, lease.generation, self.clock()),
        )
        return await fetch_one(cur) is not None

    async def lock_fence(self, cur: Any, run_id: str, lease: LeaseFence) -> bool:
        """Serialize a terminal/control effect with the current generation."""

        await execute_sql(
            cur,
            """
            SELECT 1
            FROM {}
            WHERE run_id = %s
              AND owner = %s
              AND lease_generation = %s
            FOR UPDATE
            """.format(qualified(self.schema, RUN_CLAIMS_TABLE)),
            (run_id, lease.owner, lease.generation),
        )
        return await fetch_one(cur) is not None

    async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool:
        """Read the active lease predicate shared by effect adapters."""

        now = self.clock()
        async with connect_pg(self.database_url) as conn:
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
                    """.format(qualified(self.schema, RUN_CLAIMS_TABLE)),
                    (run_id, lease.owner, lease.generation, now),
                )
                return await fetch_one(cur) is not None

    async def get_claim_row(self, run_id: str) -> dict[str, Any] | None:
        async with connect_pg(self.database_url) as conn:
            async with conn.cursor() as cur:
                return await self.select_claim_row(cur, run_id)

    async def select_claim_row(self, cur: Any, run_id: str) -> dict[str, Any] | None:
        await execute_sql(
            cur,
            """
            SELECT run_id, request_json, owner, lease_generation, lease_expires_at,
                   terminal, terminal_at,
                   durable_counter, event_index_counter, terminal_fence_seq, token_total,
                   usage_input_total, usage_output_total, sandbox_id, sandbox_generation,
                   sandbox_backend_kind, sandbox_teardown_ref
            FROM {}
            WHERE run_id = %s
            """.format(qualified(self.schema, RUN_CLAIMS_TABLE)),
            (run_id,),
        )
        row = await fetch_one(cur)
        return None if row is None else dict(row)

    async def update_control_status(
        self, run_id: str, command_id: str, status: str
    ) -> None:
        async with connect_pg(self.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET status = %s, updated_at = to_timestamp(%s / 1000.0)
                    WHERE run_id = %s AND command_id = %s AND status = 'persisted'
                    """.format(qualified(self.schema, RUN_CONTROL_COMMANDS_TABLE)),
                    (status, self.clock(), run_id, command_id),
                )

    async def update_control_command_status(
        self,
        run_id: str,
        command_id: str,
        status: ControlAdmissionStatus,
        *,
        error_code: str | None = None,
    ) -> None:
        async with connect_pg(self.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET status = %s, error_code = %s,
                        updated_at = to_timestamp(%s / 1000.0)
                    WHERE run_id = %s AND command_id = %s
                      AND status IN ('admitted', 'persisted', 'applied')
                    """.format(qualified(self.schema, RUN_CONTROL_COMMANDS_TABLE)),
                    (status, error_code, self.clock(), run_id, command_id),
                )

    async def fetch_outbox(self, where_sql: str) -> list[dict[str, Any]]:
        async with connect_pg(self.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT run_id, durable_seq, event_id, kind, status, index_value,
                           (extract(epoch FROM occurred_at) * 1000)::bigint AS timestamp,
                           payload_json,
                           (extract(epoch FROM published_at) * 1000)::bigint AS published_at
                    FROM {}
                    WHERE {}
                    ORDER BY run_id ASC, durable_seq ASC
                    """.format(qualified(self.schema, RUN_OUTBOX_TABLE), where_sql),
                )
                return [dict(row) for row in await fetch_all(cur)]

    async def delete_run_rows(self, cur: Any, run_ids: list[str]) -> None:
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
            await execute_sql(
                cur,
                "DELETE FROM {} WHERE run_id = ANY(%s)".format(
                    qualified(self.schema, table)
                ),
                (run_ids,),
            )


def sandbox_cleanup_id(
    run_id: str,
    lease_generation: int,
    backend_kind: SandboxBackendKind,
    sandbox_id: str,
) -> str:
    identity = "\0".join(
        (run_id, str(lease_generation), backend_kind, sandbox_id)
    ).encode()
    return f"scu_{hashlib.sha256(identity).hexdigest()}"


def sandbox_cleanup_from_row(row: dict[str, Any]) -> SandboxCleanupIntent:
    return SandboxCleanupIntent(
        cleanup_id=str(row["cleanup_id"]),
        run_id=str(row["run_id"]),
        lease_generation=int(row["lease_generation"]),
        backend_kind=sandbox_backend_kind(row["backend_kind"]),
        sandbox_id=str(row["sandbox_id"]),
        teardown_ref=str(row["teardown_ref"]),
        attempt_count=int(row["attempt_count"]),
        next_attempt_at=utc_to_epoch_millis(row["next_attempt_at"]) or 0,
    )


def sandbox_backend_kind(value: object) -> SandboxBackendKind:
    """Validate the database enum before it enters the domain model."""

    if value == "docker":
        return "docker"
    if value == "e2b":
        return "e2b"
    if value == "custom":
        return "custom"
    raise ValueError(f"unknown sandbox backend kind: {value!r}")


def _now_ms() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)
