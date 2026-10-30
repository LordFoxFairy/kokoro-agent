"""Current Agent-owned admission facts; no reservation, row lock, or dispatch."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from datetime import datetime

from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_run_profiles import validate_static_recipe
from kokoro_agent.infrastructure.schema import (
    MODEL_CALL_ATTEMPTS_TABLE,
    RUN_CLAIMS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.model.call_artifacts import (
    encode_attempt_binding,
    recipe_feature_for_peer_path,
)
from kokoro_agent.model.call_evidence import validate_frozen_attempt
from kokoro_agent.model.call_models import (
    AttemptBinding,
    ModelCallAuthorityLost,
    ModelCallEvidenceInvariantFailed,
    PrepareModelCall,
    PreparedModelCallSnapshot,
)
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding


async def read_prepared(
    database_url: str,
    schema: str,
    tenant_id: str,
    attempt_id: str,
    *,
    total_timeout_seconds: float,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
) -> PreparedModelCallSnapshot | None:
    async with asyncio.timeout(total_timeout_seconds):
        async with (
            connect_pg(database_url) as connection,
            connection.transaction(),
            connection.cursor() as cursor,
        ):
            await execute_sql(cursor, "SET TRANSACTION READ ONLY")
            await execute_sql(cursor, f"SET LOCAL lock_timeout='{lock_timeout_ms}ms'")
            await execute_sql(
                cursor, f"SET LOCAL statement_timeout='{statement_timeout_ms}ms'"
            )
            await execute_sql(
                cursor,
                f"""
                SELECT a.tenant_id,a.run_id,a.call_id,a.attempt_id,a.attempt_ordinal,
                       a.lease_owner,a.lease_generation,a.attempt_binding_bytes,
                       a.attempt_binding_digest,a.planned_bytes,a.planned_digest,
                       a.authorization_bytes,a.authorization_digest,a.authorization_state,
                       a.final_request_binding_version,a.final_request_digest,
                       a.status,a.prepared_at,a.dispatch_started_at,a.sealed_at,
                       a.latest_revision,a.latest_event_id,a.latest_evidence_digest,
                       r.owner AS current_owner,r.lease_generation AS current_generation,
                       r.lease_expires_at,r.terminal,r.request_json,
                       r.assembly_recipe_bytes,r.assembly_recipe_fingerprint,
                       date_trunc('milliseconds',clock_timestamp()) AS observed_at
                FROM {qualified(schema, MODEL_CALL_ATTEMPTS_TABLE)} a
                JOIN {qualified(schema, RUN_CLAIMS_TABLE)} r
                  ON r.tenant_id=a.tenant_id AND r.run_id=a.run_id
                WHERE a.tenant_id=%s AND a.attempt_id=%s
                """,
                (tenant_id, attempt_id),
            )
            row = await fetch_one(cursor)
    return None if row is None else _snapshot(row)


def _snapshot(row: Mapping[str, object]) -> PreparedModelCallSnapshot:
    try:
        validate_frozen_attempt(row)
        binding = AttemptBinding.model_validate_json(
            _bytes(row["attempt_binding_bytes"]), strict=True
        )
        planned = PlannedBinding.model_validate_json(
            _bytes(row["planned_bytes"]), strict=True
        )
        request = RunRequest.model_validate_json(
            _text(row["request_json"]), strict=True
        )
        lease = LeaseFence(
            owner=binding.lease.owner, generation=int(binding.lease.generation)
        )
        recipe = StaticRecipeBinding(
            canonical_bytes=_bytes(row["assembly_recipe_bytes"]),
            fingerprint=_text(row["assembly_recipe_fingerprint"]),
        )
        validate_static_recipe(recipe, lease)
        if hashlib.sha256(recipe.canonical_bytes).hexdigest() != recipe.fingerprint:
            raise ValueError("recipe digest differs")
        if (
            recipe_feature_for_peer_path(recipe.canonical_bytes, binding.peer_path)
            != request.feature_key
            or planned.feature_key != request.feature_key
        ):
            raise ValueError("feature binding differs")
        command = PrepareModelCall(
            request=request,
            lease=lease,
            call_id=binding.call_id,
            attempt_id=binding.attempt_id,
            planned=planned,
            peer_path=binding.peer_path,
            recipe=recipe,
            final_request_binding_version=1,
            final_request_digest=_text(row["final_request_digest"]),
        )
        expected = encode_attempt_binding(
            command, request, int(binding.attempt_ordinal)
        )
        if expected != (row["attempt_binding_bytes"], row["attempt_binding_digest"]):
            raise ValueError("durable request identity differs")
        observed_at = _instant(row["observed_at"])
        prepared_at = _instant(row["prepared_at"])
        if prepared_at > observed_at:
            raise ValueError("preparation follows observation")
        status = _text(row["status"])
        states = {
            "prepared": "not_requested",
            "authorization_pending": "requested_unknown",
            "authorized": "granted",
        }
        if status in states and (
            row["authorization_state"] != states[status]
            or row["dispatch_started_at"] is not None
            or row["sealed_at"] is not None
            or row["latest_revision"] != 0
            or row["latest_event_id"] is not None
            or row["latest_evidence_digest"] is not None
        ):
            raise ValueError("current preparation state differs")
    except Exception as error:
        raise ModelCallEvidenceInvariantFailed(
            "persisted prepared model-call facts violate owner invariants"
        ) from error
    if (
        status not in states
        or row["terminal"] is not False
        or row["current_owner"] != binding.lease.owner
        or str(row["current_generation"]) != binding.lease.generation
        or row["lease_expires_at"] is None
    ):
        raise ModelCallAuthorityLost(
            "prepared model-call is not available for admission"
        )
    expires_at = _instant(row["lease_expires_at"])
    if expires_at <= observed_at:
        raise ModelCallAuthorityLost("prepared model-call lease has expired")
    return PreparedModelCallSnapshot.model_validate(
        {
            "attempt_id": str(row["attempt_id"]),
            "status": status,
            **{
                name: row[name]
                for name in (
                    "attempt_binding_bytes",
                    "attempt_binding_digest",
                    "planned_bytes",
                    "planned_digest",
                    "authorization_bytes",
                    "authorization_digest",
                    "final_request_binding_version",
                    "final_request_digest",
                )
            },
            "prepared_at": prepared_at,
            "observed_at": observed_at,
            "lease_expires_at": expires_at,
        },
        strict=True,
    )


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ValueError("persisted bytes are invalid")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("persisted text is invalid")
    return value


def _instant(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("persisted instant is invalid")
    return value
