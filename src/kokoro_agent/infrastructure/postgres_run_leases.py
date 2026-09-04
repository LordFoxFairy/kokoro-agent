"""Run lease, lifecycle, usage, and terminal-state capabilities."""

from __future__ import annotations

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
    RUN_DISPATCHES_TABLE,
    RUN_USAGE_SEGMENTS_TABLE,
    SANDBOX_CLEANUP_INTENTS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.protocol import RunRequest


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
        self, run_id: str, namespace: str
    ) -> RunRequest | None:
        """Read an ingress-visible run only inside its durable identity scope."""

        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT claim.request_json
                    FROM {} AS claim
                    INNER JOIN {} AS dispatch ON dispatch.run_id = claim.run_id
                    WHERE claim.run_id = %s AND dispatch.namespace = %s
                    """.format(
                        qualified(self._context.schema, RUN_CLAIMS_TABLE),
                        qualified(self._context.schema, RUN_DISPATCHES_TABLE),
                    ),
                    (run_id, namespace),
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
        self,
        run_id: str,
        lease: LeaseFence,
        input_tokens: int,
        output_tokens: int,
    ) -> tuple[int, int] | None:
        if input_tokens < 0 or output_tokens < 0:
            raise ValueError("usage token counts must be non-negative")
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await execute_sql(
                        cur,
                        """
                        SELECT usage_input_total, usage_output_total
                        FROM {}
                        WHERE run_id = %s
                          AND owner = %s
                          AND lease_generation = %s
                          AND (
                            terminal = TRUE
                            OR (lease_expires_at IS NOT NULL AND lease_expires_at > to_timestamp(%s / 1000.0))
                          )
                        FOR UPDATE
                        """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                        (run_id, lease.owner, lease.generation, now),
                    )
                    claim = await fetch_one(cur)
                    if claim is None:
                        return None
                    await execute_sql(
                        cur,
                        """
                        SELECT input_tokens, output_tokens
                        FROM {}
                        WHERE run_id = %s AND lease_generation = %s
                        """.format(
                            qualified(self._context.schema, RUN_USAGE_SEGMENTS_TABLE)
                        ),
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
                    await execute_sql(
                        cur,
                        """
                        INSERT INTO {} (
                            run_id, lease_generation, input_tokens, output_tokens, created_at
                        ) VALUES (%s, %s, %s, %s, to_timestamp(%s / 1000.0))
                        """.format(
                            qualified(self._context.schema, RUN_USAGE_SEGMENTS_TABLE)
                        ),
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
                        raise RuntimeError(
                            f"usage aggregate disappeared for run {run_id!r}"
                        )
                    return (
                        int(updated["usage_input_total"]),
                        int(updated["usage_output_total"]),
                    )

    async def purge_terminal(self, max_age_ms: int) -> int:
        cutoff = self._context.clock() - max_age_ms
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await execute_sql(
                        cur,
                        """
                        SELECT claim.run_id
                        FROM {} AS claim
                        WHERE claim.terminal = TRUE
                          AND claim.terminal_at IS NOT NULL
                          AND claim.terminal_at <= to_timestamp(%s / 1000.0)
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {} AS cleanup
                              WHERE cleanup.run_id = claim.run_id
                                AND cleanup.status <> 'completed'
                          )
                        """.format(
                            qualified(self._context.schema, RUN_CLAIMS_TABLE),
                            qualified(
                                self._context.schema, SANDBOX_CLEANUP_INTENTS_TABLE
                            ),
                        ),
                        (cutoff,),
                    )
                    rows = await fetch_all(cur)
                    run_ids = [str(row["run_id"]) for row in rows]
                    if not run_ids:
                        return 0
                    await self._context.delete_run_rows(cur, run_ids)
                    await execute_sql(
                        cur,
                        "DELETE FROM {} WHERE run_id = ANY(%s)".format(
                            qualified(self._context.schema, RUN_CLAIMS_TABLE)
                        ),
                        (run_ids,),
                    )
        return len(run_ids)

    async def try_mark_terminal(self, run_id: str, lease: LeaseFence) -> bool:
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await execute_sql(
                        cur,
                        """
                        UPDATE {}
                        SET terminal = TRUE,
                            terminal_at = COALESCE(terminal_at, to_timestamp(%s / 1000.0)),
                            lease_expires_at = NULL
                        WHERE run_id = %s
                          AND owner = %s
                          AND lease_generation = %s
                          AND lease_expires_at IS NOT NULL
                          AND lease_expires_at > to_timestamp(%s / 1000.0)
                          AND terminal = FALSE
                        RETURNING run_id, sandbox_id, sandbox_generation,
                                  sandbox_backend_kind, sandbox_teardown_ref
                        """.format(
                            qualified(self._context.schema, RUN_CLAIMS_TABLE),
                        ),
                        (now, run_id, lease.owner, lease.generation, now),
                    )
                    row = await fetch_one(cur)
                    if row is None:
                        return False
                    await self._context.queue_bound_sandbox_cleanup(
                        cur, dict(row), now=now
                    )
                    return True

    async def fence_and_mark_terminal(
        self, run_id: str, owner: str
    ) -> LeaseFence | None:
        """Atomically supersede any active/paused owner and claim the sole terminal write."""

        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await execute_sql(
                        cur,
                        """
                        UPDATE {}
                        SET owner = %s,
                            lease_generation = lease_generation + 1,
                            terminal = TRUE,
                            terminal_at = COALESCE(terminal_at, to_timestamp(%s / 1000.0)),
                            lease_expires_at = NULL
                        WHERE run_id = %s AND terminal = FALSE
                        RETURNING run_id, lease_generation, sandbox_id,
                                  sandbox_generation, sandbox_backend_kind,
                                  sandbox_teardown_ref
                        """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                        (owner, now, run_id),
                    )
                    row = await fetch_one(cur)
                    if row is not None:
                        await self._context.queue_bound_sandbox_cleanup(
                            cur, dict(row), now=now
                        )
        if row is None:
            return None
        return LeaseFence(owner=owner, generation=int(row["lease_generation"]))

    async def is_terminal(self, run_id: str) -> bool:
        row = await self._context.get_claim_row(run_id)
        return bool(row and row["terminal"])
