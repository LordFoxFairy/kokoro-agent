"""PostgreSQL transaction for the first durable model-call preparation."""

from __future__ import annotations

import asyncio
import hashlib
import math
from collections.abc import Mapping
from datetime import datetime

import rfc8785
from pydantic import JsonValue, TypeAdapter
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
from kokoro_agent.model.call_models import (
    AttemptBinding,
    ModelCallAuthorityLost,
    ModelCallIdentityConflict,
    ModelCallInputInvalid,
    ModelCallOrdinalExhausted,
    PrepareModelCall,
    PreparedModelCall,
)
from kokoro_agent.domain.run.models import StaticRecipeIncompatible
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_generated import (
    AuthorizationNotRequested,
    IdentityRef,
    Lease,
)

_MAX_INT64 = 9_223_372_036_854_775_807
_RAW_OBJECT = TypeAdapter(dict[str, object])
_RAW_ARRAY = TypeAdapter(list[object])
_RAW_TUPLE = TypeAdapter(tuple[object, ...])


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
        planned_bytes = _canonical(command.planned.model_dump(mode="json"))
        planned_digest = _digest(planned_bytes)
        authorization_bytes = _canonical(
            AuthorizationNotRequested(state="not_requested").model_dump(mode="json")
        )
        authorization_digest = _digest(authorization_bytes)
        _bounded(planned_bytes)
        _bounded(authorization_bytes)
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
            or _digest(command.recipe.canonical_bytes) != command.recipe.fingerprint
        ):
            raise ModelCallAuthorityLost("model-call recipe authority differs")
        recipe_feature = _validate_peer_path(
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
            binding_bytes = _binding_bytes(
                command, durable_request, _integer(existing["attempt_ordinal"])
            )
            return _replay(
                existing, command, binding_bytes, planned_bytes, planned_digest
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
        binding_bytes = _binding_bytes(command, durable_request, ordinal)
        binding_digest = _digest(binding_bytes)
        prepared_at = clock["db_now"]
        await execute_sql(
            cursor,
            f"""INSERT INTO {attempt_table} (
                attempt_id,tenant_id,run_id,call_id,attempt_ordinal,lease_owner,
                lease_generation,attempt_binding_bytes,attempt_binding_digest,
                planned_bytes,planned_digest,authorization_bytes,authorization_digest,
                authorization_state,status,prepared_at,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        'not_requested','prepared',%s,%s,%s) RETURNING prepared_at""",
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
                prepared_at,
                prepared_at,
                prepared_at,
            ),
        )
        persisted = await fetch_one(cursor)
        if persisted is None:
            raise RuntimeError("model-call prepare insert returned no row")
        prepared_at = _instant(persisted["prepared_at"])
        await _recompute_run(
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


def _binding_bytes(
    command: PrepareModelCall, request: RunRequest, ordinal: int
) -> bytes:
    identity = request.execution_identity
    binding = AttemptBinding(
        tenant_ref=identity.tenant_ref,
        actor=IdentityRef(
            kind=identity.actor.kind, opaque_ref=identity.actor.opaque_ref
        ),
        subject=IdentityRef(
            kind=identity.subject.kind, opaque_ref=identity.subject.opaque_ref
        ),
        identity_assertion_ref=identity.identity_assertion_ref,
        run_id=request.run_id,
        session_id=request.session_id,
        call_id=command.call_id,
        attempt_id=command.attempt_id,
        attempt_ordinal=str(ordinal),
        lease=Lease(
            owner=command.lease.owner, generation=str(command.lease.generation)
        ),
        input_digest=_digest(_canonical(request.input.model_dump(mode="json"))),
        peer_path=command.peer_path,
    )
    raw = _canonical(binding.model_dump(mode="json"))
    _bounded(raw)
    return raw


def _validate_peer_path(recipe_bytes: bytes, path: tuple[str, ...]) -> str:
    try:
        recipe = _json_object(_RAW_OBJECT.validate_json(recipe_bytes, strict=True))
        feature = _require_object(recipe, "feature")
        agent_items = _require_list(feature, "agents")
        agents = {
            _require_str(item, "key"): item
            for value in agent_items
            if (item := _as_object(value)) is not None
        }
        if len(agents) != len(agent_items):
            raise ValueError
        handoffs = {
            (_require_string(edge[0]), _require_string(edge[1]))
            for value in _require_list(feature, "handoffs")
            if len(edge := _require_array(value)) == 2
        }
        entry_agent = _require_str(feature, "entry_agent")
        if not path or path[0] != entry_agent or path[0] not in agents:
            raise ValueError
        current = path[0]
        for target in path[1:]:
            agent = agents.get(current)
            if agent is None:
                raise ValueError
            subagents = {
                _require_str(item, "name")
                for value in _require_list(agent, "subagents")
                if (item := _as_object(value)) is not None
            }
            if target in subagents:
                current = target
                continue
            if (current, target) not in handoffs or target not in agents:
                raise ValueError
            current = target
        return _require_str(feature, "key")
    except (KeyError, TypeError, ValueError) as error:
        raise ModelCallInputInvalid("peer path is outside the frozen recipe") from error


def _replay(
    row: Mapping[str, object],
    command: PrepareModelCall,
    binding_bytes: bytes,
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
        _digest(binding_bytes),
        planned_bytes,
        planned_digest,
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


async def _recompute_run(
    cursor: object,
    run_table: str,
    attempt_table: str,
    evidence_table: str,
    run_id: str,
) -> None:
    await execute_sql(
        cursor,
        f"""SELECT count(*) AS attempts,
          count(*) FILTER (WHERE a.dispatch_started_at IS NOT NULL) AS dispatched,
          count(*) FILTER (WHERE a.sealed_at IS NOT NULL) AS sealed,
          count(*) FILTER (WHERE a.sealed_at IS NOT NULL AND (
            e.event_id IS NULL OR e.tenant_id<>a.tenant_id OR e.run_id<>a.run_id OR
            e.call_id<>a.call_id OR (e.execution_outcome<>'not_dispatched' AND
              (e.execution_outcome='unknown' OR e.usage_state='unresolved' OR
               e.classification_state='unknown')))) AS unknown_count,
          count(*) FILTER (WHERE e.execution_outcome<>'not_dispatched' AND
            e.classification_state='partial') AS partial_count
        FROM {attempt_table} a LEFT JOIN {evidence_table} e
          ON e.attempt_id=a.attempt_id AND e.revision=a.latest_revision
        WHERE a.run_id=%s""",
        (run_id,),
    )
    counts = await fetch_one(cursor)
    if counts is None:
        raise RuntimeError("model-call completeness query returned no row")
    attempts, dispatched, sealed, unknown, partial = (
        int(counts[name])
        for name in (
            "attempts",
            "dispatched",
            "sealed",
            "unknown_count",
            "partial_count",
        )
    )
    completeness = (
        "none"
        if attempts == 0
        else "unknown"
        if sealed < attempts or unknown > 0
        else "partial"
        if partial > 0
        else "complete"
    )
    await execute_sql(
        cursor,
        f"""UPDATE {run_table} SET model_attempt_count=%s,
          model_dispatched_attempt_count=%s,model_sealed_attempt_count=%s,
          model_unknown_attempt_count=%s,model_usage_completeness=%s,
          updated_at=clock_timestamp() WHERE run_id=%s""",
        (attempts, dispatched, sealed, unknown, completeness, run_id),
    )


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
        prepared_at_ms=millis,
        replayed=replayed,
    )


def _canonical(value: object) -> bytes:
    try:
        validated = _json_value(value)
        return rfc8785.dumps(validated)
    except (TypeError, ValueError) as error:
        raise ModelCallInputInvalid("model-call canonical value is invalid") from error


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _bounded(value: bytes) -> None:
    if not 0 < len(value) <= 65_536:
        raise ModelCallInputInvalid("model-call canonical value exceeds its bound")


def _json_value(value: object, depth: int = 1) -> JsonValue:
    if depth > 16:
        raise ValueError("model-call JSON exceeds its depth bound")
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("model-call JSON number is not finite")
        return value
    if isinstance(value, list):
        return [
            _json_value(item, depth + 1)
            for item in _RAW_ARRAY.validate_python(value, strict=True)
        ]
    if isinstance(value, tuple):
        return [
            _json_value(item, depth + 1)
            for item in _RAW_TUPLE.validate_python(value, strict=True)
        ]
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in _RAW_OBJECT.validate_python(value, strict=True).items():
            result[key] = _json_value(item, depth + 1)
        return result
    raise ValueError("model-call value is not JSON")


def _json_object(value: Mapping[str, object]) -> dict[str, JsonValue]:
    normalized = _json_value(value)
    if not isinstance(normalized, dict):
        raise ValueError("model-call recipe root is not an object")
    return normalized


def _as_object(value: JsonValue) -> dict[str, JsonValue] | None:
    return value if isinstance(value, dict) else None


def _require_object(value: Mapping[str, JsonValue], key: str) -> dict[str, JsonValue]:
    result = _as_object(value.get(key))
    if result is None:
        raise ValueError
    return result


def _require_list(value: Mapping[str, JsonValue], key: str) -> list[JsonValue]:
    result = value.get(key)
    if not isinstance(result, list):
        raise ValueError
    return result


def _require_array(value: JsonValue) -> list[JsonValue]:
    if not isinstance(value, list):
        raise ValueError
    return value


def _require_string(value: JsonValue) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError
    return value


def _require_str(value: Mapping[str, JsonValue], key: str) -> str:
    return _require_string(value.get(key))


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ModelCallIdentityConflict("model-call ordinal is invalid")
    return int(value)


def _instant(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise ModelCallIdentityConflict("model-call prepared time is invalid")
    return value


__all__ = ["PostgresModelCallRepository"]
