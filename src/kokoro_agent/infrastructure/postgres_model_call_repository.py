"""PostgreSQL transaction for the first durable model-call preparation."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime

import rfc8785
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
    AttemptBinding,
    CurrentModelCallBound,
    ModelCallBoundStale,
    ModelCallBoundExpired,
    AppendModelCallEvidence,
    AuthorizationPendingReceipt,
    BeginModelCallAuthorization,
    GrantModelCallAuthorization,
    FinalModelCallEvidence,
    ModelCallAuthorityLost,
    ModelCallAuthorizationReceipt,
    ModelCallDispatchPermit,
    ModelCallEvidenceInvariantFailed,
    ModelCallEvidenceReceipt,
    ModelCallIdentityConflict,
    ModelCallInputInvalid,
    ModelCallOrdinalExhausted,
    PrepareModelCall,
    PreparedModelCall,
    StartModelCallDispatch,
)
from kokoro_agent.model.call_bound import (
    build_bound,
    parse_bound,
    validate_prepare_bound,
    utc_millis,
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
    parse_stored_evidence,
    validate_append_evidence,
    validate_frozen_attempt,
)
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding

_MAX_INT64 = 9_223_372_036_854_775_807


def _utc_millis_text(value: object) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("model-call timestamp is invalid")
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def _optional_utc_millis_text(value: object) -> str | None:
    return None if value is None else _utc_millis_text(value)


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

    async def assert_current_producer_ready(self) -> None:
        """Reject unsealed legacy attempts before the worker consumes commands.

        Sealed history remains readable. Valid current attempts remain available
        for ordinary lease recovery; this is not an all-attempts drain gate.
        Deployment must separately stop old producers before starting this worker.
        """
        invalid = False
        async with asyncio.timeout(self._total_timeout_seconds):
            async with connect_pg(self._database_url) as connection:
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await execute_sql(cursor, "SET TRANSACTION READ ONLY")
                        await execute_sql(
                            cursor,
                            f"SET LOCAL lock_timeout='{self._lock_timeout_ms}ms'",
                        )
                        await execute_sql(
                            cursor,
                            f"SET LOCAL statement_timeout='{self._statement_timeout_ms}ms'",
                        )
                    async with connection.cursor(
                        name="model_producer_startup"
                    ) as cursor:
                        await execute_sql(
                            cursor,
                            "SELECT CASE WHEN octet_length(planned_bytes) <= 65536 "
                            "THEN planned_bytes END AS planned_bytes "
                            f"FROM {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)} "
                            "WHERE sealed_at IS NULL",
                        )
                        while (row := await fetch_one(cursor)) is not None:
                            try:
                                raw = bytes(row["planned_bytes"])
                                if len(raw) > 65_536:
                                    raise ValueError
                                planned = PlannedBinding.model_validate_json(
                                    raw, strict=True
                                )
                                if (
                                    rfc8785.dumps(planned.model_dump(mode="json"))
                                    != raw
                                ):
                                    raise ValueError
                            except (TypeError, ValueError, RecursionError):
                                invalid = True
                                break
        if invalid:
            raise RuntimeError(
                "unsealed legacy model attempts prevent current producer startup"
            )

    async def read_final_evidence(
        self, tenant_id: str, attempt_id: str, event_id: str
    ) -> FinalModelCallEvidence | None:
        """Read only the exact immutable sealed head for one tenant attempt."""
        async with asyncio.timeout(self._total_timeout_seconds):
            async with connect_pg(self._database_url) as connection:
                async with connection.cursor() as cursor:
                    await execute_sql(
                        cursor,
                        f"""SELECT a.tenant_id,a.run_id,a.call_id,a.attempt_id,
                                   a.attempt_ordinal,a.lease_owner,a.lease_generation,
                                   a.attempt_binding_bytes,a.attempt_binding_digest,
                                   a.planned_bytes,a.planned_digest,
                                   a.final_request_binding_version,
                                   a.final_request_digest,
                                   a.authorization_bytes,a.authorization_digest,
                                   a.authorization_state,a.prepared_at,
                                   a.dispatch_started_at,a.latest_revision,
                                   a.latest_event_id,a.latest_evidence_digest,
                                   e.event_id AS evidence_event_id,
                                   e.tenant_id AS evidence_tenant_id,
                                   e.run_id AS evidence_run_id,
                                   e.attempt_id AS evidence_attempt_id,
                                   e.call_id AS evidence_call_id,
                                   e.revision AS evidence_revision,
                                   e.evidence_bytes,e.evidence_digest,
                                   e.sealed AS evidence_sealed,
                                   e.observed_at AS evidence_observed_at
                            FROM {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)} a
                            JOIN {qualified(self._schema, MODEL_USAGE_EVIDENCE_TABLE)} e
                              ON e.attempt_id=a.attempt_id AND e.tenant_id=a.tenant_id
                            WHERE a.tenant_id=%s AND a.attempt_id=%s
                              AND a.latest_event_id=%s AND a.sealed_at IS NOT NULL
                              AND e.event_id=a.latest_event_id AND e.sealed=true""",
                        (tenant_id, attempt_id, event_id),
                    )
                    row = await fetch_one(cursor)
        if row is None:
            return None
        try:
            evidence_bytes = bytes(row["evidence_bytes"])
            artifact = parse_stored_evidence(evidence_bytes)
            validate_frozen_attempt(row, artifact_version=artifact.artifact_version)
            observed_at = datetime.fromisoformat(
                artifact.observed_at.replace("Z", "+00:00")
            )
            frozen_identity = json.loads(bytes(row["attempt_binding_bytes"]))
            if (
                rfc8785.dumps(artifact.model_dump(mode="json")) != evidence_bytes
                or artifact.execution.seal is not True
                or row["latest_revision"] != row["evidence_revision"]
                or row["latest_evidence_digest"] != row["evidence_digest"]
                or row["tenant_id"] != row["evidence_tenant_id"]
                or row["run_id"] != row["evidence_run_id"]
                or row["attempt_id"] != row["evidence_attempt_id"]
                or row["call_id"] != row["evidence_call_id"]
                or artifact.identity.model_dump(mode="json", exclude={"event_id"})
                != frozen_identity
                or artifact.identity.event_id != str(row["evidence_event_id"])
                or int(artifact.revision) != row["evidence_revision"]
                or artifact.digest != row["evidence_digest"]
                or observed_at != row["evidence_observed_at"]
                or artifact.planned.model_dump(mode="json")
                != json.loads(bytes(row["planned_bytes"]))
                or artifact.authorization_observation.model_dump(mode="json")
                != json.loads(bytes(row["authorization_bytes"]))
                or artifact.execution.prepared_at
                != _utc_millis_text(row["prepared_at"])
                or artifact.execution.dispatch_started_at
                != _optional_utc_millis_text(row["dispatch_started_at"])
            ):
                raise ValueError
            return FinalModelCallEvidence.model_validate(
                {
                    "tenant_id": tenant_id,
                    "artifact_version": artifact.artifact_version,
                    "attempt_id": attempt_id,
                    "event_id": str(row["evidence_event_id"]),
                    "revision": row["evidence_revision"],
                    "evidence_bytes": evidence_bytes,
                    "evidence_digest": row["evidence_digest"],
                    "observed_at": observed_at,
                },
                strict=True,
            )
        except Exception as error:
            raise ModelCallEvidenceInvariantFailed(
                "persisted final model-call evidence violates owner invariants"
            ) from error

    async def read_bound(
        self, tenant_id: str, bound_ref: str
    ) -> CurrentModelCallBound | None:
        """One bounded read-only snapshot closes planned, bound and live fence."""
        async with asyncio.timeout(self._total_timeout_seconds):
            async with connect_pg(self._database_url) as connection:
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await execute_sql(
                            cursor,
                            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
                        )
                        await execute_sql(
                            cursor,
                            f"SET LOCAL statement_timeout='{self._statement_timeout_ms}ms'",
                        )
                        await execute_sql(
                            cursor,
                            f"SET LOCAL lock_timeout='{self._lock_timeout_ms}ms'",
                        )
                        await execute_sql(
                            cursor,
                            f"""SELECT a.*,r.owner AS current_owner,
                            r.lease_generation AS current_generation,r.lease_expires_at AS current_expiry,
                            r.terminal AS current_terminal,
                            date_trunc('milliseconds',clock_timestamp()) AS db_now
                            FROM {qualified(self._schema, MODEL_CALL_ATTEMPTS_TABLE)} a
                            JOIN {qualified(self._schema, RUN_CLAIMS_TABLE)} r
                              ON r.run_id=a.run_id AND r.tenant_id=a.tenant_id
                            WHERE a.tenant_id=%s AND a.execution_bound_ref=%s""",
                            (tenant_id, bound_ref),
                        )
                        row = await fetch_one(cursor)
                        if row is None:
                            return None
                        try:
                            _validate_bound_row(row)
                            now = _instant(row["db_now"])
                            expiry = _instant(row["execution_bound_valid_until"])
                            if (
                                row["current_terminal"]
                                or row["current_owner"] != row["lease_owner"]
                                or row["current_generation"] != row["lease_generation"]
                                or row["current_expiry"] is None
                                or _instant(row["current_expiry"]) < expiry
                            ):
                                raise ModelCallBoundStale("model-call bound is stale")
                            if now >= expiry:
                                raise ModelCallBoundExpired(
                                    "model-call bound is expired"
                                )
                            return CurrentModelCallBound(
                                planned_binding_ref="mpb_" + str(row["attempt_id"]),
                                planned_binding_digest=str(row["planned_digest"]),
                                planned_binding_bytes=_bytes(row["planned_bytes"]),
                                execution_bound_ref=bound_ref,
                                execution_bound_digest=str(
                                    row["execution_bound_digest"]
                                ),
                                execution_bound_artifact_version="1.0.0",
                                execution_bound_revision="1",
                                execution_bound_bytes=_bytes(
                                    row["execution_bound_bytes"]
                                ),
                            )
                        except (ModelCallBoundStale, ModelCallBoundExpired):
                            raise
                        except (ValueError, TypeError, KeyError):
                            raise ModelCallEvidenceInvariantFailed(
                                "persisted model-call bound is invalid"
                            ) from None

    async def prepare(self, command: PrepareModelCall) -> PreparedModelCall:
        validate_prepare_bound(command)
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
        await execute_sql(
            cursor, "SELECT date_trunc('milliseconds', clock_timestamp()) AS db_now"
        )
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
        prepared_at = utc_millis(_instant(clock["db_now"]))
        bound_bytes, bound_digest, bound_expiry = build_bound(
            command,
            ordinal=ordinal,
            prepared_at=prepared_at,
            lease_expires_at=_instant(run["lease_expires_at"]),
        )
        await execute_sql(
            cursor,
            f"""INSERT INTO {attempt_table} (
                attempt_id,tenant_id,run_id,call_id,attempt_ordinal,lease_owner,
                lease_generation,attempt_binding_bytes,attempt_binding_digest,
                planned_bytes,planned_digest,authorization_bytes,authorization_digest,
                final_request_binding_version,final_request_digest,
                execution_bound_artifact_version,execution_bound_ref,execution_bound_revision,
                execution_bound_bytes,execution_bound_digest,execution_bound_valid_until,
                authorization_state,status,prepared_at,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,'1.0.0',%s,1,%s,%s,%s,'not_requested','prepared',%s,%s,%s) RETURNING prepared_at""",
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
                "mcb_" + str(command.attempt_id),
                bound_bytes,
                bound_digest,
                bound_expiry,
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
            bound_bytes=bound_bytes,
            bound_digest=bound_digest,
            bound_expiry=bound_expiry,
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
    _validate_bound_row(row)
    return _receipt(
        command,
        tenant,
        _integer(row["attempt_ordinal"]),
        binding_bytes,
        str(row["attempt_binding_digest"]),
        planned_bytes,
        planned_digest,
        _instant(row["prepared_at"]),
        bound_bytes=_bytes(row["execution_bound_bytes"]),
        bound_digest=str(row["execution_bound_digest"]),
        bound_expiry=_instant(row["execution_bound_valid_until"]),
        replayed=True,
    )


def _validate_bound_row(row: Mapping[str, object]) -> None:
    bound = parse_bound(
        _bytes(row["execution_bound_bytes"]),
        planned_bytes=_bytes(row["planned_bytes"]),
        expected_digest=str(row["execution_bound_digest"]),
    )
    expected = (
        row["tenant_id"],
        row["run_id"],
        str(row["call_id"]),
        str(row["attempt_id"]),
        str(row["attempt_ordinal"]),
        row["lease_owner"],
        str(row["lease_generation"]),
        row["planned_digest"],
        row["final_request_binding_version"],
        row["final_request_digest"],
        row["execution_bound_ref"],
        row["execution_bound_artifact_version"],
        str(row["execution_bound_revision"]),
        _utc_millis_text(row["prepared_at"]),
        _utc_millis_text(row["execution_bound_valid_until"]),
    )
    actual = (
        bound.tenant_ref,
        bound.run_id,
        bound.call_id,
        bound.attempt_id,
        bound.attempt_ordinal,
        bound.lease_owner,
        bound.lease_generation,
        bound.planned_binding_digest,
        bound.final_request_binding_version,
        bound.final_request_digest,
        bound.execution_bound_ref,
        bound.artifact_version,
        bound.revision,
        bound.sealed_at,
        bound.valid_until,
    )
    if actual != expected:
        raise ModelCallInputInvalid("persisted model-call bound identity differs")
    # Bound identity is immutable; mutable authorization state is independent.
    binding_raw = _bytes(row["attempt_binding_bytes"])
    binding = AttemptBinding.model_validate_json(binding_raw, strict=True)
    if (
        hashlib.sha256(binding_raw).hexdigest() != row["attempt_binding_digest"]
        or rfc8785.dumps(binding.model_dump(mode="json")) != binding_raw
    ):
        raise ModelCallInputInvalid("persisted model-call attempt identity differs")
    if (
        binding.tenant_ref,
        binding.run_id,
        binding.call_id,
        binding.attempt_id,
        binding.attempt_ordinal,
        binding.lease.owner,
        binding.lease.generation,
    ) != (
        bound.tenant_ref,
        bound.run_id,
        bound.call_id,
        bound.attempt_id,
        bound.attempt_ordinal,
        bound.lease_owner,
        bound.lease_generation,
    ):
        raise ModelCallInputInvalid("persisted model-call attempt identity differs")


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
    bound_bytes: bytes,
    bound_digest: str,
    bound_expiry: datetime,
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
        execution_bound_ref="mcb_" + str(command.attempt_id),
        execution_bound_bytes=bound_bytes,
        execution_bound_digest=bound_digest,
        execution_bound_valid_until=bound_expiry,
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
