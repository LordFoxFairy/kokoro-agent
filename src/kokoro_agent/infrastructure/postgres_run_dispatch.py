"""Dispatch admission and claim PostgreSQL capabilities."""

from __future__ import annotations

from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
    RUN_CLAIMS_TABLE,
    RUN_DISPATCHES_TABLE,
    RUN_DLQ_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.protocol import RunRequest


class _DispatchLeaseConflict(Exception):
    """Abort the dispatch transaction when its lease row cannot be created."""


class PostgresRunDispatch:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context

    async def try_claim(self, request: RunRequest, owner: str) -> LeaseFence | None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    INSERT INTO {} (
                        run_id, tenant_id, request_json, owner, lease_generation,
                        lease_expires_at
                    ) VALUES (%s, %s, %s, %s, 1, to_timestamp(%s / 1000.0))
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING lease_generation
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (
                        request.run_id,
                        request.execution_identity.tenant_ref,
                        request.model_dump_json(),
                        owner,
                        self._context.clock() + self._context.ttl_ms,
                    ),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return LeaseFence(owner=owner, generation=int(row["lease_generation"]))

    async def claim_dispatch(
        self, request: RunRequest, consumer: str
    ) -> LeaseFence | None:
        now = self._context.clock()
        try:
            async with connect_pg(self._context.database_url) as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await execute_sql(
                            cur,
                            """
                            UPDATE {}
                            SET status = 'claimed', claimed_by = %s,
                                updated_at = to_timestamp(%s / 1000.0)
                            WHERE run_id = %s AND status = 'pending' AND request_json = %s
                            RETURNING run_id
                            """.format(
                                qualified(self._context.schema, RUN_DISPATCHES_TABLE)
                            ),
                            (
                                consumer,
                                now,
                                request.run_id,
                                request.model_dump_json(),
                            ),
                        )
                        if await fetch_one(cur) is None:
                            return None
                        await execute_sql(
                            cur,
                            """
                            INSERT INTO {} (
                                run_id, tenant_id, request_json, owner, lease_generation,
                                lease_expires_at
                            ) VALUES (%s, %s, %s, %s, 1,
                                      to_timestamp(%s / 1000.0))
                            ON CONFLICT (run_id) DO NOTHING
                            RETURNING lease_generation
                            """.format(
                                qualified(self._context.schema, RUN_CLAIMS_TABLE)
                            ),
                            (
                                request.run_id,
                                request.execution_identity.tenant_ref,
                                request.model_dump_json(),
                                consumer,
                                now + self._context.ttl_ms,
                            ),
                        )
                        row = await fetch_one(cur)
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
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT request_json
                    FROM {}
                    WHERE run_id = %s AND status = 'pending'
                    """.format(qualified(self._context.schema, RUN_DISPATCHES_TABLE)),
                    (run_id,),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return RunRequest.model_validate_json(row["request_json"])

    async def list_pending_dispatches(self, limit: int = 100) -> list[RunRequest]:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT request_json
                    FROM {}
                    WHERE status = 'pending'
                    ORDER BY created_at ASC, run_id ASC
                    LIMIT %s
                    """.format(qualified(self._context.schema, RUN_DISPATCHES_TABLE)),
                    (limit,),
                )
                rows = await fetch_all(cur)
        return [RunRequest.model_validate_json(row["request_json"]) for row in rows]

    async def quarantine_dispatch(
        self, raw_hash: str, source: str, reason: str
    ) -> None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    INSERT INTO {} (raw_hash, source, reason, occurred_at)
                    VALUES (%s, %s, %s, to_timestamp(%s / 1000.0))
                    ON CONFLICT (raw_hash) DO NOTHING
                    """.format(qualified(self._context.schema, RUN_DLQ_TABLE)),
                    (raw_hash, source, reason, self._context.clock()),
                )
