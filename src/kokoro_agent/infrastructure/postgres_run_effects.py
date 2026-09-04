"""Execution-effect, steer, tool-result, and journal capabilities."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from kokoro_agent.domain.run.repository import LeaseFence, ToolJournalRecord
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
    RUN_STEERS_TABLE,
    TOOL_JOURNAL_TABLE,
    TOOL_RESULTS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one


class PostgresRunEffects:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context

    async def execute_active_effect(
        self,
        run_id: str,
        lease: LeaseFence,
        effect: Callable[[], Awaitable[None]],
    ) -> bool:
        """Linearize one bounded external effect with reclaim, pause, and terminal CAS."""

        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await effect()
                    return True

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        row = await self._context.get_claim_row(run_id)
        if row is None or bool(row["terminal"]):
            return
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    INSERT INTO {} (run_id, message_id, content, created_at)
                    VALUES (%s, %s, %s, to_timestamp(%s / 1000.0))
                    ON CONFLICT (run_id, message_id) DO NOTHING
                    """.format(qualified(self._context.schema, RUN_STEERS_TABLE)),
                    (run_id, message_id, content, self._context.clock()),
                )

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT message_id, content
                    FROM {}
                    WHERE run_id = %s
                    ORDER BY created_at ASC, message_id ASC
                    """.format(qualified(self._context.schema, RUN_STEERS_TABLE)),
                    (run_id,),
                )
                rows = await fetch_all(cur)
        return [(str(row["message_id"]), str(row["content"])) for row in rows]

    async def ack_steers(
        self, run_id: str, lease: LeaseFence, message_ids: list[str]
    ) -> bool:
        if not message_ids:
            return await self._context.is_lease_current(run_id, lease)
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        DELETE FROM {}
                        WHERE run_id = %s AND message_id = ANY(%s)
                        """.format(qualified(self._context.schema, RUN_STEERS_TABLE)),
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
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return None
                    await execute_sql(
                        cur,
                        """
                        INSERT INTO {} (run_id, tool_id, result, is_error)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (run_id, tool_id) DO NOTHING
                        """.format(qualified(self._context.schema, TOOL_RESULTS_TABLE)),
                        (run_id, tool_id, result, is_error),
                    )
                    await execute_sql(
                        cur,
                        """
                        SELECT result, is_error
                        FROM {}
                        WHERE run_id = %s AND tool_id = %s
                        """.format(qualified(self._context.schema, TOOL_RESULTS_TABLE)),
                        (run_id, tool_id),
                    )
                    row = await fetch_one(cur)
        if row is None:
            raise RuntimeError(f"tool result missing after insert for {run_id!r}")
        return str(row["result"]), bool(row["is_error"])

    async def get_tool_result(
        self, run_id: str, tool_id: str
    ) -> tuple[str, bool] | None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT result, is_error
                    FROM {}
                    WHERE run_id = %s AND tool_id = %s
                    """.format(qualified(self._context.schema, TOOL_RESULTS_TABLE)),
                    (run_id, tool_id),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return str(row["result"]), bool(row["is_error"])

    async def journal_tool_started(
        self, run_id: str, lease: LeaseFence, tool_call_id: str, name: str
    ) -> bool:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        INSERT INTO {} (run_id, tool_call_id, name, status, result, is_error)
                        VALUES (%s, %s, %s, 'started', '', FALSE)
                        ON CONFLICT (run_id, tool_call_id) DO NOTHING
                        RETURNING tool_call_id
                        """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                        (run_id, tool_call_id, name),
                    )
                    return await fetch_one(cur) is not None

    async def journal_tool_finished(
        self,
        run_id: str,
        lease: LeaseFence,
        tool_call_id: str,
        result: str,
        is_error: bool,
    ) -> bool:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        UPDATE {}
                        SET status = %s, result = %s, is_error = %s
                        WHERE run_id = %s AND tool_call_id = %s AND status = 'started'
                        RETURNING tool_call_id
                        """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                        (
                            "failed" if is_error else "succeeded",
                            result,
                            is_error,
                            run_id,
                            tool_call_id,
                        ),
                    )
                    return await fetch_one(cur) is not None

    async def clear_tool_journal(
        self, run_id: str, lease: LeaseFence, tool_call_id: str
    ) -> bool:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    if not await self._context.lock_active_lease(cur, run_id, lease):
                        return False
                    await execute_sql(
                        cur,
                        """
                        DELETE FROM {}
                        WHERE run_id = %s AND tool_call_id = %s
                        """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                        (run_id, tool_call_id),
                    )
        return True

    async def get_tool_journal(
        self, run_id: str, tool_call_id: str
    ) -> ToolJournalRecord | None:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT name, status, result, is_error
                    FROM {}
                    WHERE run_id = %s AND tool_call_id = %s
                    """.format(qualified(self._context.schema, TOOL_JOURNAL_TABLE)),
                    (run_id, tool_call_id),
                )
                row = await fetch_one(cur)
        if row is None:
            return None
        return ToolJournalRecord(**dict(row))
