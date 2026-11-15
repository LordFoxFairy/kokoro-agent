"""Short Agent-only financial delivery transactions. No network or execution permission."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from datetime import datetime
from uuid import UUID, uuid4

from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one, fetch_all
from kokoro_agent.infrastructure.schema import (
    MODEL_CALL_ATTEMPTS_TABLE,
    MODEL_USAGE_EVIDENCE_TABLE,
    MODEL_FINANCIAL_CLOSE_TABLE,
    RUN_CLAIMS_TABLE,
)
from kokoro_agent.infrastructure.postgres_model_call_final import (
    read_final_evidence_in_cursor,
)
from kokoro_agent.model.call_evidence import validate_frozen_attempt
from kokoro_agent.model.usage_evidence_v2 import parse_evidence
from kokoro_agent.protocol.model_usage_v2_generated import (
    AuthorizationGranted,
    AuthorizationRequested,
)
from kokoro_agent.model.budget import ModelBudgetCommand
from kokoro_agent.model.settlement import ModelSettlementCommand, ModelSettlementReceipt
from kokoro_agent.model.financial_reconciliation import ModelFinancialWork
from kokoro_agent.model.financial_receipt import (
    ModelFinancialConflict,
    encode_financial_receipt,
    decode_financial_receipt,
)


from kokoro_agent.protocol import RunRequest
from kokoro_agent.model.call_models import (
    ModelCallEvidenceInvariantFailed,
    AttemptBinding,
)
from kokoro_agent.model.financial_close import (
    ModelFinancialCloseCommand,
    ModelFinancialCloseWork,
    ModelFinancialCloseReceipt,
    encode_close_receipt,
    decode_close_receipt,
    validate_close_binding,
)


class PostgresModelFinancialRepository:
    def __init__(self, database_url: str, schema: str, *, timeout_s: float = 5) -> None:
        if not 0 < timeout_s <= 10:
            raise ValueError("invalid financial SQL bound")
        self._url = database_url
        self._schema = schema
        self._timeout_s = timeout_s
        self._attempt = qualified(schema, MODEL_CALL_ATTEMPTS_TABLE)
        self._evidence = qualified(schema, MODEL_USAGE_EVIDENCE_TABLE)
        self._close = qualified(schema, MODEL_FINANCIAL_CLOSE_TABLE)
        self._run = qualified(schema, RUN_CLAIMS_TABLE)

    @asynccontextmanager
    async def _cursor(self) -> AsyncGenerator[object, None]:
        async with asyncio.timeout(self._timeout_s):
            async with (
                connect_pg(self._url) as connection,
                connection.transaction(),
                connection.cursor() as cursor,
            ):
                await execute_sql(cursor, "SET LOCAL lock_timeout='2000ms'")
                await execute_sql(cursor, "SET LOCAL statement_timeout='4000ms'")
                yield cursor

    async def _attempt_row(self, cursor: object, tenant_id: str, attempt_id: str):
        await execute_sql(
            cursor,
            f"SELECT * FROM {self._attempt} WHERE tenant_id=%s AND attempt_id=%s FOR UPDATE",
            (tenant_id, attempt_id),
        )
        row = await fetch_one(cursor)
        if row is not None:
            validate_frozen_attempt(row)
        return row

    async def _work(
        self, cursor: object, row: dict[str, object], event_id: str
    ) -> ModelFinancialWork:
        tenant = str(row["tenant_id"])
        attempt = str(row["attempt_id"])
        final = await read_final_evidence_in_cursor(
            cursor, self._schema, tenant, attempt, event_id
        )
        if final is None or final.artifact_version != "2.0.0":
            raise ModelFinancialConflict()
        artifact = parse_evidence(final.evidence_bytes)
        auth = artifact.authorization_observation
        if not isinstance(
            auth, (AuthorizationGranted, AuthorizationRequested)
        ) or auth.state not in {"granted", "requested_unknown"}:
            raise ModelFinancialConflict()
        admission = (
            None
            if row["financial_admission_ref"] is None
            else str(row["financial_admission_ref"])
        )
        if auth.state == "granted":
            if admission is not None and admission != auth.admission_ref:
                raise ModelFinancialConflict()
            admission = auth.admission_ref
        if admission is not None and str(UUID(admission)) != admission:
            raise ModelFinancialConflict()
        return ModelFinancialWork(
            tenant_id=tenant,
            budget=ModelBudgetCommand(
                run_id=str(row["run_id"]),
                attempt_id=attempt,
                request_ref=auth.request_ref,
                attempt_binding_digest=str(row["attempt_binding_digest"]),
                planned_digest=str(row["planned_digest"]),
                final_request_digest=str(row["final_request_digest"]),
            ),
            evidence=ModelSettlementCommand(
                run_id=str(row["run_id"]),
                attempt_id=attempt,
                event_id=event_id,
                revision=str(final.revision),
                digest=final.evidence_digest,
            ),
            admission_ref=admission,
        )

    async def _event(self, cursor: object, work: ModelFinancialWork):
        await execute_sql(
            cursor,
            f"SELECT * FROM {self._evidence} WHERE tenant_id=%s AND attempt_id=%s AND event_id=%s",
            (work.tenant_id, work.evidence.attempt_id, work.evidence.event_id),
        )
        row = await fetch_one(cursor)
        if row is None:
            raise ModelFinancialConflict()
        return row

    @staticmethod
    def _ack(
        row: dict[str, object], work: ModelFinancialWork
    ) -> ModelSettlementReceipt | None:
        if row["financial_ack_bytes"] is None:
            if (
                row["financial_ack_digest"] is not None
                or row["financial_acked_at"] is not None
            ):
                raise ModelFinancialConflict()
            return None
        if (
            not isinstance(row["financial_ack_bytes"], bytes)
            or not isinstance(row["financial_ack_digest"], str)
            or not isinstance(row["financial_acked_at"], datetime)
        ):
            raise ModelFinancialConflict()
        value = decode_financial_receipt(
            row["financial_ack_bytes"], row["financial_ack_digest"]
        )
        if (
            value.run_id,
            value.attempt_id,
            value.event_id,
            value.revision,
            value.digest,
        ) != (
            work.evidence.run_id,
            work.evidence.attempt_id,
            work.evidence.event_id,
            work.evidence.revision,
            work.evidence.digest,
        ) or value.admission_ref != work.admission_ref:
            raise ModelFinancialConflict()
        return value

    async def next_work(
        self, tenant_id: str, attempt_id: str
    ) -> ModelFinancialWork | None:
        async with self._cursor() as cursor:
            row = await self._attempt_row(cursor, tenant_id, attempt_id)
            if (
                row is None
                or row["authorization_state"] not in {"granted", "requested_unknown"}
                or row["sealed_at"] is None
            ):
                return None
            if await self._closed(cursor, row) is not None:
                return None
            await execute_sql(
                cursor,
                f"SELECT event_id FROM {self._evidence} WHERE tenant_id=%s AND attempt_id=%s AND financial_ack_bytes IS NULL ORDER BY revision LIMIT 1",
                (tenant_id, attempt_id),
            )
            event = await fetch_one(cursor)
            if event is None:
                return None
            work = await self._work(cursor, row, str(event["event_id"]))
            await self._previous(cursor, row, work)
            return work

    async def _previous(
        self, cursor: object, attempt: dict[str, object], work: ModelFinancialWork
    ) -> None:
        revision = int(work.evidence.revision)
        if revision == 1:
            return
        await execute_sql(
            cursor,
            f"SELECT event_id FROM {self._evidence} WHERE tenant_id=%s AND attempt_id=%s AND revision=%s",
            (work.tenant_id, work.evidence.attempt_id, revision - 1),
        )
        previous = await fetch_one(cursor)
        if previous is None:
            raise ModelFinancialConflict()
        previous_work = await self._work(cursor, attempt, str(previous["event_id"]))
        ack = self._ack(await self._event(cursor, previous_work), previous_work)
        if ack is None or ack.status != "pending_reconciliation":
            raise ModelFinancialConflict()

    async def latest_ack(
        self, tenant_id: str, attempt_id: str
    ) -> ModelSettlementReceipt | None:
        async with self._cursor() as cursor:
            row = await self._attempt_row(cursor, tenant_id, attempt_id)
            if (
                row is None
                or row["latest_event_id"] is None
                or row["authorization_state"] not in {"granted", "requested_unknown"}
            ):
                return None
            work = await self._work(cursor, row, str(row["latest_event_id"]))
            return self._ack(await self._event(cursor, work), work)

    async def _bound(self, cursor: object, work: ModelFinancialWork):
        row = await self._attempt_row(cursor, work.tenant_id, work.evidence.attempt_id)
        if row is None:
            raise ModelFinancialConflict()
        if await self._closed(cursor, row) is not None:
            raise ModelFinancialConflict()
        actual = await self._work(cursor, row, work.evidence.event_id)
        if actual.budget != work.budget or actual.evidence != work.evidence:
            raise ModelFinancialConflict()
        return row, actual

    async def record_admission(
        self, work: ModelFinancialWork, admission_ref: str
    ) -> None:
        if str(UUID(admission_ref)) != admission_ref:
            raise ModelFinancialConflict()
        async with self._cursor() as cursor:
            row, current = await self._bound(cursor, work)
            if (
                current.admission_ref is not None
                and current.admission_ref != admission_ref
            ):
                raise ModelFinancialConflict()
            if row["financial_admission_ref"] is None:
                await execute_sql(
                    cursor,
                    f"UPDATE {self._attempt} SET financial_admission_ref=%s WHERE tenant_id=%s AND attempt_id=%s",
                    (admission_ref, work.tenant_id, work.evidence.attempt_id),
                )

    async def record_ack(
        self, work: ModelFinancialWork, value: ModelSettlementReceipt
    ) -> ModelSettlementReceipt:
        raw, digest = encode_financial_receipt(value)
        async with self._cursor() as cursor:
            attempt, current = await self._bound(cursor, work)
            if current.admission_ref is None or (
                value.admission_ref,
                value.run_id,
                value.attempt_id,
                value.event_id,
                value.revision,
                value.digest,
            ) != (
                current.admission_ref,
                work.evidence.run_id,
                work.evidence.attempt_id,
                work.evidence.event_id,
                work.evidence.revision,
                work.evidence.digest,
            ):
                raise ModelFinancialConflict()
            event = await self._event(cursor, current)
            previous = self._ack(event, current)
            if previous is not None:
                if previous != value:
                    raise ModelFinancialConflict()
                return previous
            await self._previous(cursor, attempt, current)
            await execute_sql(
                cursor,
                f"SELECT event_id FROM {self._evidence} WHERE tenant_id=%s AND attempt_id=%s AND financial_ack_bytes IS NULL ORDER BY revision LIMIT 1",
                (work.tenant_id, work.evidence.attempt_id),
            )
            first = await fetch_one(cursor)
            if first is None or str(first["event_id"]) != work.evidence.event_id:
                raise ModelFinancialConflict()
            await execute_sql(
                cursor,
                f"UPDATE {self._evidence} SET financial_ack_bytes=%s,financial_ack_digest=%s,financial_acked_at=clock_timestamp() WHERE tenant_id=%s AND event_id=%s",
                (raw, digest, work.tenant_id, work.evidence.event_id),
            )
            return value

    async def list_pending(
        self, *, limit: int = 32, after: tuple[str, str] | None = None
    ) -> list[tuple[str, str]]:
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError("invalid financial scan limit")
        if after is not None and (len(after) != 2 or any(not value for value in after)):
            raise ValueError("invalid financial scan cursor")
        async with self._cursor() as cursor:
            await execute_sql(
                cursor,
                f"SELECT a.* FROM {self._attempt} a JOIN {self._close} c ON c.tenant_id=a.tenant_id AND c.attempt_id=a.attempt_id ORDER BY a.tenant_id,a.attempt_id LIMIT 129",
            )
            closed_attempts = await fetch_all(cursor)
            if len(closed_attempts) > 128:
                raise ModelFinancialConflict()  # Explicit recovery slice is bounded, never an unbounded audit scan.
            closed_ids: list[str] = []
            for attempt in closed_attempts:
                try:
                    if await self._closed(cursor, attempt) is not None:
                        closed_ids.append(str(attempt["attempt_id"]))
                except (ModelFinancialConflict, ModelCallEvidenceInvariantFailed):
                    # Invalid close stays pending; the existing per-attempt recovery reports it.
                    continue
            await execute_sql(
                cursor,
                f"""SELECT a.tenant_id,a.attempt_id FROM {self._attempt} a JOIN {self._evidence} e
                ON e.attempt_id=a.attempt_id AND e.tenant_id=a.tenant_id
                WHERE e.financial_ack_bytes IS NULL AND a.authorization_state IN ('granted','requested_unknown')
                AND NOT (a.attempt_id=ANY(%s::uuid[]))
                AND (%s::text IS NULL OR (a.tenant_id,a.attempt_id) > (%s::text,%s::uuid))
                GROUP BY a.tenant_id,a.attempt_id ORDER BY a.tenant_id,a.attempt_id LIMIT %s""",
                (
                    closed_ids,
                    None if after is None else after[0],
                    None if after is None else after[0],
                    None if after is None else after[1],
                    limit,
                ),
            )
            return [
                (str(row["tenant_id"]), str(row["attempt_id"]))
                for row in await fetch_all(cursor)
            ]

    async def _closed(
        self, cursor: object, attempt: dict[str, object]
    ) -> ModelFinancialCloseReceipt | None:
        await execute_sql(
            cursor,
            f"SELECT * FROM {self._close} WHERE tenant_id=%s AND attempt_id=%s",
            (attempt["tenant_id"], attempt["attempt_id"]),
        )
        row = await fetch_one(cursor)
        if row is None:
            return None
        raw = row["receipt_bytes"]
        digest = row["receipt_digest"]
        if not isinstance(raw, bytes) or not isinstance(digest, str):
            raise ModelFinancialConflict()
        receipt = decode_close_receipt(raw, digest)
        work = await self._work(cursor, attempt, str(row["event_id"]))
        command = ModelFinancialCloseCommand(
            tenant_id=work.tenant_id,
            run_id=work.budget.run_id,
            attempt_id=work.budget.attempt_id,
            evidence_event_id=work.evidence.event_id,
            evidence_revision=work.evidence.revision,
            evidence_digest=work.evidence.digest,
        )
        if (
            row["run_id"],
            row["revision"],
            row["evidence_digest"],
            str(attempt["latest_event_id"]),
        ) != (
            command.run_id,
            int(command.evidence_revision),
            command.evidence_digest,
            command.evidence_event_id,
        ):
            raise ModelFinancialConflict()
        await self._close_eligible(cursor, attempt)
        binding = self._close_identity(attempt)
        if row["actor_ref"] != binding.actor.opaque_ref:
            raise ModelFinancialConflict()
        validate_close_binding(
            ModelFinancialCloseWork(
                command=command, financial=work, actor_ref=str(row["actor_ref"])
            ),
            receipt,
        )
        return receipt

    @staticmethod
    def _close_identity(attempt: dict[str, object]) -> AttemptBinding:
        raw = attempt["attempt_binding_bytes"]
        if not isinstance(raw, bytes):
            raise ModelFinancialConflict()
        try:
            binding = AttemptBinding.model_validate_json(raw, strict=True)
        except (ValueError, TypeError):
            raise ModelFinancialConflict() from None
        if binding.actor.kind != "user" or binding.actor != binding.subject:
            raise ModelFinancialConflict()
        return binding

    async def _close_eligible(self, cursor: object, attempt: dict[str, object]) -> None:
        if (
            attempt["status"] != "not_dispatched"
            or attempt["authorization_state"] != "requested_unknown"
            or attempt["dispatch_started_at"] is not None
            or attempt["financial_admission_ref"] is not None
            or attempt["sealed_at"] is None
        ):
            raise ModelFinancialConflict()
        await execute_sql(
            cursor,
            f"SELECT evidence_bytes,financial_ack_bytes,financial_ack_digest,financial_acked_at FROM {self._evidence} WHERE tenant_id=%s AND attempt_id=%s",
            (attempt["tenant_id"], attempt["attempt_id"]),
        )
        for event in await fetch_all(cursor):
            raw = event["evidence_bytes"]
            if not isinstance(raw, bytes):
                raise ModelFinancialConflict()
            artifact = parse_evidence(raw)
            if (
                artifact.execution.outcome != "not_dispatched"
                or artifact.usage.state != "unresolved"
            ):
                raise ModelFinancialConflict()
            if any(
                event[key] is not None
                for key in (
                    "financial_ack_bytes",
                    "financial_ack_digest",
                    "financial_acked_at",
                )
            ):
                raise ModelFinancialConflict()

    async def _prepare_close(
        self, cursor: object, command: ModelFinancialCloseCommand
    ) -> ModelFinancialCloseWork:
        await execute_sql(
            cursor,
            f"SELECT * FROM {self._run} WHERE tenant_id=%s AND run_id=%s FOR UPDATE",
            (command.tenant_id, command.run_id),
        )
        run = await fetch_one(cursor)
        if (
            run is None
            or run["terminal"] is not True
            or run["terminal_at"] is None
            or not isinstance(run["request_json"], str)
        ):
            raise ModelFinancialConflict()
        try:
            request = RunRequest.model_validate_json(run["request_json"])
            identity = request.execution_identity
            if (
                request.run_id != command.run_id
                or identity.tenant_ref != command.tenant_id
                or identity.actor != identity.subject
                or identity.actor.kind != "user"
            ):
                raise ValueError
        except (ValueError, TypeError):
            raise ModelFinancialConflict() from None
        attempt = await self._attempt_row(cursor, command.tenant_id, command.attempt_id)
        if attempt is None or attempt["run_id"] != command.run_id:
            raise ModelFinancialConflict()
        binding = self._close_identity(attempt)
        if (
            binding.tenant_ref,
            binding.run_id,
            binding.session_id,
            binding.identity_assertion_ref,
            binding.actor.model_dump(mode="json"),
            binding.subject.model_dump(mode="json"),
        ) != (
            identity.tenant_ref,
            request.run_id,
            request.session_id,
            identity.identity_assertion_ref,
            identity.actor.model_dump(mode="json"),
            identity.subject.model_dump(mode="json"),
        ):
            raise ModelFinancialConflict()
        await self._close_eligible(cursor, attempt)
        work = await self._work(cursor, attempt, command.evidence_event_id)
        if (
            work.evidence.revision,
            work.evidence.digest,
            str(attempt["latest_event_id"]),
        ) != (
            command.evidence_revision,
            command.evidence_digest,
            command.evidence_event_id,
        ):
            raise ModelFinancialConflict()
        return ModelFinancialCloseWork(
            command=command,
            financial=work,
            actor_ref=identity.actor.opaque_ref,
            receipt=await self._closed(cursor, attempt),
        )

    async def prepare_close(
        self, command: ModelFinancialCloseCommand
    ) -> ModelFinancialCloseWork:
        async with self._cursor() as cursor:
            return await self._prepare_close(cursor, command)

    async def latest_close(
        self, tenant_id: str, attempt_id: str
    ) -> ModelFinancialCloseReceipt | None:
        async with self._cursor() as cursor:
            row = await self._attempt_row(cursor, tenant_id, attempt_id)
            return None if row is None else await self._closed(cursor, row)

    async def record_close(
        self, work: ModelFinancialCloseWork, receipt: ModelFinancialCloseReceipt
    ) -> ModelFinancialCloseReceipt:
        validate_close_binding(work, receipt)
        raw, digest = encode_close_receipt(receipt)
        async with self._cursor() as cursor:
            actual = await self._prepare_close(cursor, work.command)
            if actual.financial != work.financial or actual.actor_ref != work.actor_ref:
                raise ModelFinancialConflict()
            if actual.receipt is not None:
                if actual.receipt != receipt:
                    raise ModelFinancialConflict()
                return actual.receipt
            await execute_sql(
                cursor,
                f"INSERT INTO {self._close} (id,tenant_id,run_id,attempt_id,event_id,revision,evidence_digest,actor_ref,receipt_bytes,receipt_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    str(uuid4()),
                    work.command.tenant_id,
                    work.command.run_id,
                    work.command.attempt_id,
                    work.command.evidence_event_id,
                    int(work.command.evidence_revision),
                    work.command.evidence_digest,
                    work.actor_ref,
                    raw,
                    digest,
                ),
            )
            return receipt
