"""Sandbox binding and durable cleanup capabilities."""

from __future__ import annotations

from typing import Any

from kokoro_agent.domain.run.repository import (
    LeaseFence,
    SandboxBackendKind,
    SandboxCleanupIntent,
)
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
    sandbox_backend_kind,
    sandbox_cleanup_id,
    sandbox_cleanup_from_row,
)
from kokoro_agent.infrastructure.schema import (
    RUN_CLAIMS_TABLE,
    SANDBOX_CLEANUP_INTENTS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one


class PostgresRunSandbox:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context

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
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return None
                    await execute_sql(
                        cur,
                        """
                        SELECT sandbox_id, sandbox_generation,
                               sandbox_backend_kind, sandbox_teardown_ref
                        FROM {}
                        WHERE run_id = %s
                        """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                        (run_id,),
                    )
                    row = await fetch_one(cur)
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
                        await execute_sql(
                            cur,
                            """
                            UPDATE {}
                            SET sandbox_generation = %s
                            WHERE run_id = %s
                              AND owner = %s
                              AND lease_generation = %s
                            RETURNING sandbox_id
                            """.format(
                                qualified(self._context.schema, RUN_CLAIMS_TABLE)
                            ),
                            (lease.generation, run_id, lease.owner, lease.generation),
                        )
                        rebound = await fetch_one(cur)
                        return None if rebound is None else str(rebound["sandbox_id"])
                    await execute_sql(
                        cur,
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
                        """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
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
                    bound = await fetch_one(cur)
                    return None if bound is None else str(bound["sandbox_id"])

    async def get_sandbox_id(self, run_id: str) -> str | None:
        row = await self._context.get_claim_row(run_id)
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
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
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
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await execute_sql(
                        cur,
                        """
                        WITH due AS (
                            SELECT cleanup_id
                            FROM {}
                            WHERE status IN ('pending', 'processing')
                              AND next_attempt_at <= to_timestamp(%s / 1000.0)
                              AND (%s::text IS NULL OR run_id = %s)
                            ORDER BY next_attempt_at ASC, created_at ASC, cleanup_id ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT %s
                        )
                        UPDATE {} AS cleanup
                        SET status = 'processing',
                            cleanup_owner = %s,
                            attempt_count = cleanup.attempt_count + 1,
                            next_attempt_at = to_timestamp(%s / 1000.0),
                            updated_at = to_timestamp(%s / 1000.0)
                        FROM due
                        WHERE cleanup.cleanup_id = due.cleanup_id
                        RETURNING cleanup.cleanup_id, cleanup.run_id,
                                  cleanup.lease_generation, cleanup.backend_kind,
                                  cleanup.sandbox_id, cleanup.teardown_ref,
                                  cleanup.attempt_count, cleanup.next_attempt_at
                        """.format(
                            qualified(
                                self._context.schema, SANDBOX_CLEANUP_INTENTS_TABLE
                            ),
                            qualified(
                                self._context.schema, SANDBOX_CLEANUP_INTENTS_TABLE
                            ),
                        ),
                        (now, run_id, run_id, limit, owner, now + lease_ms, now),
                    )
                    rows = await fetch_all(cur)
        return [sandbox_cleanup_from_row(dict(row)) for row in rows]

    async def complete_sandbox_cleanup(self, cleanup_id: str) -> bool:
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                        SET status = 'completed', cleanup_owner = NULL,
                        last_error = NULL, updated_at = to_timestamp(%s / 1000.0)
                    WHERE cleanup_id = %s AND status <> 'completed'
                    RETURNING cleanup_id
                    """.format(
                        qualified(self._context.schema, SANDBOX_CLEANUP_INTENTS_TABLE)
                    ),
                    (now, cleanup_id),
                )
                return await fetch_one(cur) is not None

    async def reschedule_sandbox_cleanup(
        self, cleanup_id: str, error: str, *, retry_delay_ms: int
    ) -> bool:
        if retry_delay_ms < 0:
            raise ValueError("sandbox cleanup retry delay must be non-negative")
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                    SET status = 'pending', cleanup_owner = NULL,
                        next_attempt_at = to_timestamp(%s / 1000.0),
                        last_error = %s, updated_at = to_timestamp(%s / 1000.0)
                    WHERE cleanup_id = %s AND status <> 'completed'
                    RETURNING cleanup_id
                    """.format(
                        qualified(self._context.schema, SANDBOX_CLEANUP_INTENTS_TABLE)
                    ),
                    (now + retry_delay_ms, error[:1_000], now, cleanup_id),
                )
                return await fetch_one(cur) is not None

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
            """.format(qualified(self._context.schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
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
            """.format(qualified(self._context.schema, SANDBOX_CLEANUP_INTENTS_TABLE)),
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
            backend_kind=sandbox_backend_kind(backend_kind),
            sandbox_id=str(sandbox_id),
            teardown_ref=teardown_ref,
            now=now,
        )
