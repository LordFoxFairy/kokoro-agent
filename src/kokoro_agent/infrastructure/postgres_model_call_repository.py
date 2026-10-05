"""PostgreSQL transaction for the first durable model-call preparation."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from datetime import datetime

from psycopg.errors import UniqueViolation

from kokoro_agent.infrastructure.postgres import (
    connect_pg,
    qualified,
    utc_to_epoch_millis,
)
from kokoro_agent.infrastructure.schema import (
    MODEL_CALL_ATTEMPTS_TABLE,
    MODEL_USAGE_EVIDENCE_TABLE,
    RUN_CLAIMS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.infrastructure.postgres_run_profiles import validate_static_recipe
from kokoro_agent.infrastructure.postgres_model_call_evidence import (
    append_evidence_locked,
    recompute_run_completeness,
)
from kokoro_agent.model.call_models import (
    AppendModelCallEvidence,
    AuthorizationPendingReceipt,
    BeginModelCallAuthorization,
    GrantModelCallAuthorization,
    ModelCallAuthorityLost,
    ModelCallAuthorizationReceipt,
    ModelCallDispatchPermit,
    ModelCallEvidenceReceipt,
    ModelCallIdentityConflict,
    ModelCallInputInvalid,
    ModelCallOrdinalExhausted,
    PrepareModelCall,
    PreparedModelCall,
    StartModelCallDispatch,
)
from kokoro_agent.domain.run.models import StaticRecipeIncompatible
from kokoro_agent.model.call_artifacts import (
    encode_attempt_binding,
    encode_granted_artifact,
    encode_not_requested_artifact,
    encode_planned_artifact,
    encode_requested_artifact,
    recipe_feature_for_peer_path,
    strict_granted_artifact,
)
from kokoro_agent.model.call_evidence import (
    validate_append_evidence,
    validate_frozen_attempt,
)
from kokoro_agent.protocol import RunRequest

_MAX_INT64 = 9_223_372_036_854_775_807


class PostgresModelCallRepository:
    def __init__(
        self,
        database_url: str,
        schema: str,
        *,
        total_timeout_seconds: float = 5.0,
        lock_timeout_ms: int = 5_000,
        statement_timeout_ms: int = 10_000,
    ) -> None:
        if (
            total_timeout_seconds <= 0
            or lock_timeout_ms <= 0
            or statement_timeout_ms <= 0
        ):
            raise ValueError("model-call PostgreSQL bounds must be positive")
        self._database_url = database_url
        self._schema = schema
        self._total_timeout_seconds = total_timeout_seconds
        self._lock_timeout_ms = lock_timeout_ms
        self._statement_timeout_ms = statement_timeout_ms

    async def prepare(self, command: PrepareModelCall) -> PreparedModelCall:
        planned_bytes, planned_digest = encode_planned_artifact(command.planned)
        authorization_bytes, authorization_digest = encode_not_requested_artifact()
        async with asyncio.timeout(self._total_timeout_seconds):
            try:
                return await self._execute_prepare(
                    command,
                    planned_bytes,
                    planned_digest,
                    authorization_bytes,
                    authorization_digest,
                )
            except UniqueViolation as error:
                if await self._same_attempt_scope(command):
                    return await self._execute_prepare(
                        command,
                        planned_bytes,
                        planned_digest,
                        authorization_bytes,
                        authorization_digest,
                    )
                raise ModelCallIdentityConflict(
                    "model-call preparation unique identity conflicts"
                ) from error

    async def begin_authorization(
        self, command: BeginModelCallAuthorization
    ) -> AuthorizationPendingReceipt:
        authorization_bytes, authorization_digest = encode_requested_artifact(
            command.request_ref
        )
        async with asyncio.timeout(self._total_timeout_seconds):
            async with connect_pg(self._database_url) as connection:
                async with connection.transaction(), connection.cursor() as cursor:
                    row, _ = await self._lock_attempt(cursor, command)
                    current = str(row["status"])
                    if current == "prepared":
                        initial_bytes, initial_digest = encode_not_requested_artifact()
                        _require_authorization(row, initial_bytes, initial_digest)
                        await execute_sql(
                            cursor,
                            f"""UPDATE {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)}
                                SET authorization_bytes=%s,authorization_digest=%s,
                                    authorization_state='requested_unknown',
                                    status='authorization_pending',updated_at=clock_timestamp()
                                WHERE attempt_id=%s""",
                            (
                                authorization_bytes,
                                authorization_digest,
                                command.attempt_id,
                            ),
                        )
                        newly_requested = True
                    elif current == "authorization_pending":
                        _require_authorization(
                            row, authorization_bytes, authorization_digest
                        )
                        newly_requested = False
                    else:
                        try:
                            current_bytes = _bytes(row["authorization_bytes"])
                            granted, current_digest = strict_granted_artifact(
                                current_bytes
                            )
                        except (TypeError, ValueError) as error:
                            raise ModelCallIdentityConflict(
                                "authorization request identity conflicts"
                            ) from error
                        if (
                            granted.request_ref != command.request_ref
                            or row["authorization_digest"] != current_digest
                        ):
                            raise ModelCallIdentityConflict(
                                "authorization request identity conflicts"
                            )
                        authorization_bytes = current_bytes
                        authorization_digest = str(row["authorization_digest"])
                        newly_requested = False
        return AuthorizationPendingReceipt(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            attempt_id=command.attempt_id,
            request_ref=command.request_ref,
            authorization_bytes=authorization_bytes,
            authorization_digest=authorization_digest,
            newly_requested=newly_requested,
        )

    async def grant_authorization(
        self, command: GrantModelCallAuthorization
    ) -> ModelCallAuthorizationReceipt:
        authorization_bytes, authorization_digest = encode_granted_artifact(
            command.request_ref, command.admission_ref
        )
        async with asyncio.timeout(self._total_timeout_seconds):
            async with connect_pg(self._database_url) as connection:
                async with connection.transaction(), connection.cursor() as cursor:
                    row, _ = await self._lock_attempt(cursor, command)
                    current = str(row["status"])
                    if current == "authorization_pending":
                        requested_bytes, requested_digest = encode_requested_artifact(
                            command.request_ref
                        )
                        if (
                            row["authorization_bytes"] != requested_bytes
                            or row["authorization_digest"]
                            != command.expected_authorization_digest
                            or command.expected_authorization_digest != requested_digest
                        ):
                            raise ModelCallIdentityConflict(
                                "authorization observation predecessor conflicts"
                            )
                        await execute_sql(
                            cursor,
                            f"""UPDATE {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)}
                                SET authorization_bytes=%s,authorization_digest=%s,
                                    authorization_state='granted',status='authorized',
                                    updated_at=clock_timestamp() WHERE attempt_id=%s""",
                            (
                                authorization_bytes,
                                authorization_digest,
                                command.attempt_id,
                            ),
                        )
                        newly_recorded = True
                    else:
                        _require_authorization(
                            row, authorization_bytes, authorization_digest
                        )
                        newly_recorded = False
        return ModelCallAuthorizationReceipt(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            attempt_id=command.attempt_id,
            request_ref=command.request_ref,
            admission_ref=command.admission_ref,
            authorization_bytes=authorization_bytes,
            authorization_digest=authorization_digest,
            newly_recorded=newly_recorded,
        )

    async def start_dispatch(
        self, command: StartModelCallDispatch
    ) -> ModelCallDispatchPermit:
        authorization_bytes, _ = encode_granted_artifact(
            command.request_ref, command.admission_ref
        )
        async with asyncio.timeout(self._total_timeout_seconds):
            async with connect_pg(self._database_url) as connection:
                async with connection.transaction(), connection.cursor() as cursor:
                    row, clock = await self._lock_attempt(cursor, command)
                    _require_authorization(
                        row, authorization_bytes, command.authorization_digest
                    )
                    current = str(row["status"])
                    if current == "authorized":
                        await execute_sql(
                            cursor,
                            f"""UPDATE {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)}
                                SET status='dispatch_started',dispatch_started_at=%s,
                                    updated_at=%s WHERE attempt_id=%s
                                RETURNING dispatch_started_at""",
                            (clock, clock, command.attempt_id),
                        )
                        persisted = await fetch_one(cursor)
                        if persisted is None:
                            raise RuntimeError("model-call dispatch returned no row")
                        dispatch_started_at = _instant(persisted["dispatch_started_at"])
                        await recompute_run_completeness(
                            cursor,
                            qualified(self._schema, RUN_CLAIMS_TABLE),
                            qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE),
                            qualified(self._schema, MODEL_USAGE_EVIDENCE_TABLE),
                            command.run_id,
                        )
                        newly_started = True
                    elif row["dispatch_started_at"] is not None:
                        dispatch_started_at = _instant(row["dispatch_started_at"])
                        newly_started = False
                    else:
                        raise ModelCallAuthorityLost(
                            "model-call is not authorized for dispatch"
                        )
        millis = utc_to_epoch_millis(dispatch_started_at)
        if millis is None:
            raise ModelCallIdentityConflict("dispatch time is missing")
        return ModelCallDispatchPermit(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            attempt_id=command.attempt_id,
            authorization_digest=command.authorization_digest,
            dispatch_started_at_ms=millis,
            newly_started=newly_started,
        )

    async def append_evidence(
        self, command: AppendModelCallEvidence
    ) -> ModelCallEvidenceReceipt:
        async with asyncio.timeout(self._total_timeout_seconds):
            async with connect_pg(self._database_url) as connection:
                async with connection.transaction(), connection.cursor() as cursor:
                    attempt, database_now, terminal = await self._lock_evidence_attempt(
                        cursor, command
                    )
                    artifact = validate_append_evidence(
                        command.evidence_bytes,
                        attempt_binding_bytes=_bytes(attempt["attempt_binding_bytes"]),
                        planned_bytes=_bytes(attempt["planned_bytes"]),
                        authorization_bytes=_bytes(attempt["authorization_bytes"]),
                        prepared_at=_instant(attempt["prepared_at"]),
                        dispatch_started_at=(
                            None
                            if attempt["dispatch_started_at"] is None
                            else _instant(attempt["dispatch_started_at"])
                        ),
                        database_now=database_now,
                    )
                    return await append_evidence_locked(
                        cursor,
                        run_table=qualified(self._schema, RUN_CLAIMS_TABLE),
                        attempt_table=qualified(
                            self._schema, MODEL_CALL_ATTEMPTS_TABLE
                        ),
                        evidence_table=qualified(
                            self._schema, MODEL_USAGE_EVIDENCE_TABLE
                        ),
                        command=command,
                        attempt=attempt,
                        artifact=artifact,
                        allow_new_revision=not terminal,
                    )

    async def _lock_evidence_attempt(
        self, cursor: object, command: AppendModelCallEvidence
    ) -> tuple[Mapping[str, object], datetime, bool]:
        await execute_sql(cursor, f"SET LOCAL lock_timeout='{self._lock_timeout_ms}ms'")
        await execute_sql(
            cursor, f"SET LOCAL statement_timeout='{self._statement_timeout_ms}ms'"
        )
        await execute_sql(
            cursor,
            f"""SELECT owner,lease_generation,lease_expires_at,terminal
                FROM {qualified(self._schema, RUN_CLAIMS_TABLE)}
                WHERE tenant_id=%s AND run_id=%s FOR UPDATE""",
            (command.tenant_id, command.run_id),
        )
        run = await fetch_one(cursor)
        await execute_sql(cursor, "SELECT clock_timestamp() AS db_now")
        clock_row = await fetch_one(cursor)
        if run is None or clock_row is None:
            raise ModelCallAuthorityLost("model-call evidence Run authority lost")
        database_now = _instant(clock_row["db_now"])
        terminal = bool(run["terminal"])
        if not terminal and (
            run["owner"] != command.lease.owner
            or run["lease_generation"] != command.lease.generation
            or run["lease_expires_at"] is None
            or run["lease_expires_at"] <= database_now
        ):
            raise ModelCallAuthorityLost("model-call evidence lease authority lost")
        await execute_sql(
            cursor,
            f"""SELECT * FROM {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)}
                WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s FOR UPDATE""",
            (command.tenant_id, command.run_id, command.attempt_id),
        )
        attempt = await fetch_one(cursor)
        if attempt is None:
            raise ModelCallAuthorityLost("model-call evidence attempt authority lost")
        if (
            attempt["lease_owner"] != command.lease.owner
            or attempt["lease_generation"] != command.lease.generation
            or attempt["attempt_binding_digest"] != command.attempt_binding_digest
            or attempt["planned_digest"] != command.planned_digest
            or attempt["authorization_digest"] != command.authorization_digest
        ):
            raise ModelCallIdentityConflict("model-call evidence identity conflicts")
        validate_frozen_attempt(attempt)
        return attempt, database_now, terminal

    async def _lock_attempt(
        self,
        cursor: object,
        command: BeginModelCallAuthorization
        | GrantModelCallAuthorization
        | StartModelCallDispatch,
    ) -> tuple[Mapping[str, object], datetime]:
        await execute_sql(cursor, f"SET LOCAL lock_timeout='{self._lock_timeout_ms}ms'")
        await execute_sql(
            cursor, f"SET LOCAL statement_timeout='{self._statement_timeout_ms}ms'"
        )
        await execute_sql(
            cursor,
            f"""SELECT owner,lease_generation,lease_expires_at,terminal
                FROM {qualified(self._schema, RUN_CLAIMS_TABLE)}
                WHERE tenant_id=%s AND run_id=%s FOR UPDATE""",
            (command.tenant_id, command.run_id),
        )
        run = await fetch_one(cursor)
        await execute_sql(cursor, "SELECT clock_timestamp() AS db_now")
        clock_row = await fetch_one(cursor)
        if clock_row is None:
            raise RuntimeError("model-call database clock is missing")
        clock = _instant(clock_row["db_now"])
        if (
            run is None
            or run["owner"] != command.lease.owner
            or run["lease_generation"] != command.lease.generation
            or run["terminal"]
            or run["lease_expires_at"] is None
            or run["lease_expires_at"] <= clock
        ):
            raise ModelCallAuthorityLost("model-call transition authority lost")
        await execute_sql(
            cursor,
            f"""SELECT * FROM {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)}
                WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s FOR UPDATE""",
            (command.tenant_id, command.run_id, command.attempt_id),
        )
        attempt = await fetch_one(cursor)
        if attempt is None:
            raise ModelCallAuthorityLost("model-call attempt authority lost")
        if (
            attempt["lease_owner"] != command.lease.owner
            or attempt["lease_generation"] != command.lease.generation
            or attempt["attempt_binding_digest"] != command.attempt_binding_digest
            or attempt["planned_digest"] != command.planned_digest
        ):
            raise ModelCallIdentityConflict("model-call transition identity conflicts")
        validate_frozen_attempt(attempt)
        return attempt, clock

    async def _execute_prepare(
        self,
        command: PrepareModelCall,
        planned_bytes: bytes,
        planned_digest: str,
        authorization_bytes: bytes,
        authorization_digest: str,
    ) -> PreparedModelCall:
        async with connect_pg(self._database_url) as connection:
            async with connection.transaction():
                async with connection.cursor() as cursor:
                    await execute_sql(
                        cursor, f"SET LOCAL lock_timeout='{self._lock_timeout_ms}ms'"
                    )
                    await execute_sql(
                        cursor,
                        f"SET LOCAL statement_timeout='{self._statement_timeout_ms}ms'",
                    )
                    return await self._prepare_locked(
                        cursor,
                        command,
                        planned_bytes,
                        planned_digest,
                        authorization_bytes,
                        authorization_digest,
                    )

    async def _same_attempt_scope(self, command: PrepareModelCall) -> bool:
        async with connect_pg(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await execute_sql(
                    cursor,
                    f"SELECT tenant_id,run_id FROM {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)} WHERE attempt_id=%s",
                    (command.attempt_id,),
                )
                row = await fetch_one(cursor)
        return row is not None and (
            row["tenant_id"] == command.request.execution_identity.tenant_ref
            and row["run_id"] == command.request.run_id
        )

    async def _prepare_locked(
        self,
        cursor: object,
        command: PrepareModelCall,
        planned_bytes: bytes,
        planned_digest: str,
        authorization_bytes: bytes,
        authorization_digest: str,
    ) -> PreparedModelCall:
        run_table = qualified(self._schema, RUN_CLAIMS_TABLE)
        attempt_table = qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)
        tenant = command.request.execution_identity.tenant_ref
        await execute_sql(
            cursor,
            f"""SELECT tenant_id,request_json,owner,lease_generation,lease_expires_at,
                       terminal,assembly_recipe_bytes,assembly_recipe_fingerprint
                FROM {run_table} WHERE run_id=%s AND tenant_id=%s FOR UPDATE""",
            (command.request.run_id, tenant),
        )
        run = await fetch_one(cursor)
        await execute_sql(cursor, "SELECT clock_timestamp() AS db_now")
        clock = await fetch_one(cursor)
        if (
            run is None
            or clock is None
            or run["owner"] != command.lease.owner
            or run["lease_generation"] != command.lease.generation
            or run["terminal"]
            or run["lease_expires_at"] is None
            or run["lease_expires_at"] <= clock["db_now"]
        ):
            raise ModelCallAuthorityLost("model-call preparation authority lost")
        durable_request = _durable_request(run["request_json"])
        if durable_request != command.request:
            raise ModelCallAuthorityLost("model-call request authority differs")
        try:
            validate_static_recipe(command.recipe, command.lease)
        except StaticRecipeIncompatible as error:
            raise ModelCallInputInvalid("model-call recipe is invalid") from error
        if (
            run["assembly_recipe_bytes"] != command.recipe.canonical_bytes
            or run["assembly_recipe_fingerprint"] != command.recipe.fingerprint
            or hashlib.sha256(command.recipe.canonical_bytes).hexdigest()
            != command.recipe.fingerprint
        ):
            raise ModelCallAuthorityLost("model-call recipe authority differs")
        recipe_feature = recipe_feature_for_peer_path(
            command.recipe.canonical_bytes, command.peer_path
        )
        if recipe_feature != durable_request.feature_key:
            raise ModelCallInputInvalid("recipe feature differs from durable Run")
        if command.planned.feature_key != durable_request.feature_key:
            raise ModelCallInputInvalid("planned feature differs from durable Run")

        await execute_sql(
            cursor,
            f"SELECT * FROM {attempt_table} WHERE attempt_id=%s AND tenant_id=%s AND run_id=%s",
            (command.attempt_id, tenant, durable_request.run_id),
        )
        existing = await fetch_one(cursor)
        if existing is not None:
            binding_bytes, binding_digest = encode_attempt_binding(
                command, durable_request, _integer(existing["attempt_ordinal"])
            )
            return _replay(
                existing,
                command,
                binding_bytes,
                binding_digest,
                planned_bytes,
                planned_digest,
            )

        await execute_sql(
            cursor,
            f"SELECT max(attempt_ordinal) AS ordinal FROM {attempt_table} WHERE run_id=%s AND call_id=%s",
            (durable_request.run_id, command.call_id),
        )
        maximum_row = await fetch_one(cursor)
        maximum = None if maximum_row is None else maximum_row["ordinal"]
        if maximum == _MAX_INT64:
            raise ModelCallOrdinalExhausted("model-call attempt ordinal exhausted")
        ordinal = 1 if maximum is None else int(maximum) + 1
        binding_bytes, binding_digest = encode_attempt_binding(
            command, durable_request, ordinal
        )
        prepared_at = clock["db_now"]
        await execute_sql(
            cursor,
            f"""INSERT INTO {attempt_table} (
                attempt_id,tenant_id,run_id,call_id,attempt_ordinal,lease_owner,
                lease_generation,attempt_binding_bytes,attempt_binding_digest,
                planned_bytes,planned_digest,authorization_bytes,authorization_digest,
                final_request_binding_version,final_request_digest,
                authorization_state,status,prepared_at,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,'not_requested','prepared',%s,%s,%s) RETURNING prepared_at""",
            (
                command.attempt_id,
                tenant,
                durable_request.run_id,
                command.call_id,
                ordinal,
                command.lease.owner,
                command.lease.generation,
                binding_bytes,
                binding_digest,
                planned_bytes,
                planned_digest,
                authorization_bytes,
                authorization_digest,
                command.final_request_binding_version,
                command.final_request_digest,
                prepared_at,
                prepared_at,
                prepared_at,
            ),
        )
        persisted = await fetch_one(cursor)
        if persisted is None:
            raise RuntimeError("model-call prepare insert returned no row")
        prepared_at = _instant(persisted["prepared_at"])
        await recompute_run_completeness(
            cursor,
            run_table,
            attempt_table,
            qualified(self._schema, MODEL_USAGE_EVIDENCE_TABLE),
            durable_request.run_id,
        )
        return _receipt(
            command,
            tenant,
            ordinal,
            binding_bytes,
            binding_digest,
            planned_bytes,
            planned_digest,
            prepared_at,
            replayed=False,
        )


def _durable_request(value: object) -> RunRequest:
    if not isinstance(value, str):
        raise ModelCallAuthorityLost("durable Run request is missing")
    try:
        return RunRequest.model_validate_json(value, strict=True)
    except ValueError as error:
        raise ModelCallAuthorityLost("durable Run request is invalid") from error


def _replay(
    row: Mapping[str, object],
    command: PrepareModelCall,
    binding_bytes: bytes,
    binding_digest: str,
    planned_bytes: bytes,
    planned_digest: str,
) -> PreparedModelCall:
    tenant = command.request.execution_identity.tenant_ref
    expected = (
        tenant,
        command.request.run_id,
        str(command.call_id),
        command.lease.owner,
        command.lease.generation,
        binding_bytes,
        binding_digest,
        planned_bytes,
        planned_digest,
        command.final_request_binding_version,
        command.final_request_digest,
    )
    actual = tuple(
        row[name]
        for name in (
            "tenant_id",
            "run_id",
            "call_id",
            "lease_owner",
            "lease_generation",
            "attempt_binding_bytes",
            "attempt_binding_digest",
            "planned_bytes",
            "planned_digest",
            "final_request_binding_version",
            "final_request_digest",
        )
    )
    actual = (actual[0], actual[1], str(actual[2]), *actual[3:])
    if actual != expected:
        raise ModelCallIdentityConflict("model-call preparation identity conflicts")
    return _receipt(
        command,
        tenant,
        _integer(row["attempt_ordinal"]),
        binding_bytes,
        str(row["attempt_binding_digest"]),
        planned_bytes,
        planned_digest,
        _instant(row["prepared_at"]),
        replayed=True,
    )


def _require_authorization(
    row: Mapping[str, object], expected_bytes: bytes, expected_digest: str
) -> None:
    if (
        row["authorization_bytes"] != expected_bytes
        or row["authorization_digest"] != expected_digest
    ):
        raise ModelCallIdentityConflict("model-call authorization identity conflicts")


def _receipt(
    command: PrepareModelCall,
    tenant: str,
    ordinal: int,
    binding_bytes: bytes,
    binding_digest: str,
    planned_bytes: bytes,
    planned_digest: str,
    prepared_at: datetime,
    *,
    replayed: bool,
) -> PreparedModelCall:
    millis = utc_to_epoch_millis(prepared_at)
    if millis is None:
        raise ModelCallIdentityConflict("prepared time is missing")
    return PreparedModelCall(
        tenant_id=tenant,
        run_id=command.request.run_id,
        call_id=command.call_id,
        attempt_id=command.attempt_id,
        attempt_ordinal=ordinal,
        lease=command.lease,
        attempt_binding_bytes=binding_bytes,
        attempt_binding_digest=binding_digest,
        planned_bytes=planned_bytes,
        planned_digest=planned_digest,
        final_request_binding_version=command.final_request_binding_version,
        final_request_digest=command.final_request_digest,
        prepared_at_ms=millis,
        replayed=replayed,
    )


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ModelCallIdentityConflict("model-call ordinal is invalid")
    return int(value)


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ModelCallIdentityConflict("model-call canonical bytes are invalid")
    return value


def _instant(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise ModelCallIdentityConflict("model-call prepared time is invalid")
    return value


__all__ = ["PostgresModelCallRepository"]
