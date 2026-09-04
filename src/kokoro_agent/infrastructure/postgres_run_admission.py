"""Admission and control-command PostgreSQL capabilities."""

from __future__ import annotations

from kokoro_agent.domain.run.repository import (
    ControlAdmission,
    ControlAdmissionReceipt,
    ControlAdmissionStatus,
    ControlCommandConflict,
    DispatchAdmission,
    DispatchConflict,
    RunControlCommandRecord,
)
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
    RUN_CLAIMS_TABLE,
    RUN_CONTROL_COMMANDS_TABLE,
    RUN_DISPATCHES_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.protocol import RunRequest


class PostgresRunAdmission:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context

    async def enqueue_dispatch(
        self, request: RunRequest, namespace: str, fence: str
    ) -> DispatchAdmission:
        """Create or replay the durable dispatch intent before Redis publication.

        The dispatch row is the admission fence for the worker's claim CAS. A
        repeated request with the same run id and canonical fence is safe to
        republish (for example after a response timeout). A different fence is
        an immutable-identity conflict and is never silently merged.
        """
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    INSERT INTO {} (
                        run_id, tenant_id, session_id, namespace, request_json, fence, status,
                        claimed_by, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'pending', NULL,
                              to_timestamp(%s / 1000.0), to_timestamp(%s / 1000.0))
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING run_id
                    """.format(qualified(self._context.schema, RUN_DISPATCHES_TABLE)),
                    (
                        request.run_id,
                        request.execution_identity.tenant_ref,
                        request.session_id,
                        namespace,
                        request.model_dump_json(),
                        fence,
                        now,
                        now,
                    ),
                )
                if await fetch_one(cur) is not None:
                    return DispatchAdmission(replayed=False, publish_required=True)
                await execute_sql(
                    cur,
                    """
                    SELECT fence, status
                    FROM {}
                    WHERE run_id = %s
                    """.format(qualified(self._context.schema, RUN_DISPATCHES_TABLE)),
                    (request.run_id,),
                )
                row = await fetch_one(cur)
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
        now = self._context.clock()
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    INSERT INTO {} (
                        run_id, command_id, request_digest, status, body,
                        error_code, created_at, updated_at
                    ) VALUES (%s, %s, %s, 'admitted', %s, NULL,
                              to_timestamp(%s / 1000.0), to_timestamp(%s / 1000.0))
                    ON CONFLICT (run_id, command_id) DO NOTHING
                    RETURNING command_id
                    """.format(
                        qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                    ),
                    (run_id, command_id, request_digest, body, now, now),
                )
                if await fetch_one(cur) is not None:
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
                await execute_sql(
                    cur,
                    """
                    SELECT run_id, command_id, request_digest, status, error_code
                    FROM {}
                    WHERE run_id = %s AND command_id = %s
                    """.format(
                        qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                    ),
                    (run_id, command_id),
                )
                row = await fetch_one(cur)
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
        await self._context.update_control_command_status(
            run_id, command_id, "succeeded"
        )

    async def mark_control_failed(
        self, run_id: str, command_id: str, error_code: str | None = None
    ) -> None:
        await self._context.update_control_command_status(
            run_id, command_id, "failed", error_code=error_code
        )

    async def record_control_delivery(
        self,
        run_id: str,
        command_id: str,
        request_digest: str | None,
        fingerprint: str | None,
        body: str,
    ) -> bool:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT terminal
                    FROM {}
                    WHERE run_id = %s
                    """.format(qualified(self._context.schema, RUN_CLAIMS_TABLE)),
                    (run_id,),
                )
                row = await fetch_one(cur)
                if row is None:
                    return False
                await execute_sql(
                    cur,
                    """
                    SELECT request_digest
                    FROM {}
                    WHERE run_id = %s AND command_id = %s
                    """.format(
                        qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                    ),
                    (run_id, command_id),
                )
                command = await fetch_one(cur)
                if command is None:
                    return False
                if (
                    request_digest is not None
                    and str(command["request_digest"]) != request_digest
                ):
                    raise ControlCommandConflict(
                        f"command id {command_id!r} was reused with a different request digest"
                    )
                await execute_sql(
                    cur,
                    """
                    UPDATE {}
                        SET status = 'persisted', fingerprint = %s,
                            updated_at = to_timestamp(%s / 1000.0)
                    WHERE run_id = %s AND command_id = %s AND status = 'admitted'
                    RETURNING command_id
                    """.format(
                        qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                    ),
                    (
                        fingerprint,
                        self._context.clock(),
                        run_id,
                        command_id,
                    ),
                )
                return await fetch_one(cur) is not None

    async def mark_control_applied(self, run_id: str, command_id: str) -> None:
        await self._context.update_control_status(run_id, command_id, "applied")

    async def mark_control_superseded(self, run_id: str, command_id: str) -> None:
        await self._context.update_control_status(run_id, command_id, "superseded")

    async def list_pending_control_delivery(self) -> list[RunControlCommandRecord]:
        async with connect_pg(self._context.database_url) as conn:
            async with conn.cursor() as cur:
                await execute_sql(
                    cur,
                    """
                    SELECT i.run_id, i.command_id, i.request_digest, i.fingerprint, i.body
                    FROM {} i
                    JOIN {} r ON r.run_id = i.run_id
                    WHERE i.status = 'persisted' AND r.terminal = FALSE
                    ORDER BY i.run_id ASC, i.command_id ASC
                    """.format(
                        qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE),
                        qualified(self._context.schema, RUN_CLAIMS_TABLE),
                    ),
                )
                rows = await fetch_all(cur)
        return [RunControlCommandRecord(**dict(row)) for row in rows]


def _receipt_status(command_status: str) -> ControlAdmissionStatus:
    """Project internal command state onto the small HTTP receipt state set."""

    if command_status in {"admitted", "persisted"}:
        return "pending"
    if command_status in {"applied", "succeeded"}:
        return "succeeded"
    if command_status in {"failed", "superseded"}:
        return "failed"
    raise RuntimeError(f"unknown control command status: {command_status!r}")
