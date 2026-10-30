"""Exact sealed event reads and independent head integrity in one owner snapshot."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import rfc8785

from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.schema import (
    MODEL_CALL_ATTEMPTS_TABLE,
    MODEL_USAGE_EVIDENCE_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all
from kokoro_agent.model.call_evidence import (
    parse_stored_evidence,
    validate_frozen_attempt,
)
from kokoro_agent.model.call_models import (
    FinalModelCallEvidence,
    ModelCallEvidenceInvariantFailed,
)


def _utc_millis_text(value: object) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("model-call timestamp is invalid")
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def _optional_utc_millis_text(value: object) -> str | None:
    return None if value is None else _utc_millis_text(value)


async def read_final_evidence(
    database_url: str,
    schema: str,
    tenant_id: str,
    attempt_id: str,
    event_id: str,
    *,
    total_timeout_seconds: float,
) -> FinalModelCallEvidence | None:
    async with asyncio.timeout(total_timeout_seconds):
        async with (
            connect_pg(database_url) as connection,
            connection.cursor() as cursor,
        ):
            return await read_final_evidence_in_cursor(
                cursor, schema, tenant_id, attempt_id, event_id
            )


async def read_final_evidence_in_cursor(
    cursor: object,
    schema: str,
    tenant_id: str,
    attempt_id: str,
    event_id: str,
) -> FinalModelCallEvidence | None:
    await execute_sql(
        cursor,
        f"""
        SELECT a.tenant_id,a.run_id,a.call_id,a.attempt_id,
               a.attempt_ordinal,a.lease_owner,a.lease_generation,
               a.attempt_binding_bytes,a.attempt_binding_digest,
               a.planned_bytes,a.planned_digest,
               a.final_request_binding_version,a.final_request_digest,
               a.authorization_bytes,a.authorization_digest,
               a.authorization_state,a.prepared_at,a.dispatch_started_at,a.sealed_at,
               a.latest_revision,a.latest_event_id,a.latest_evidence_digest,
               e.event_id AS evidence_event_id,e.tenant_id AS evidence_tenant_id,
               e.run_id AS evidence_run_id,e.attempt_id AS evidence_attempt_id,
               e.call_id AS evidence_call_id,e.revision AS evidence_revision,
               e.evidence_bytes,e.evidence_digest,e.sealed AS evidence_sealed,
               e.observed_at AS evidence_observed_at
        FROM {qualified(schema, MODEL_CALL_ATTEMPTS_TABLE)} a
        JOIN {qualified(schema, MODEL_USAGE_EVIDENCE_TABLE)} e
          ON e.attempt_id=a.attempt_id AND e.tenant_id=a.tenant_id
        WHERE a.tenant_id=%s AND a.attempt_id=%s AND a.sealed_at IS NOT NULL
          AND (e.event_id=%s OR e.event_id=a.latest_event_id) AND e.sealed=true
        """,
        (tenant_id, attempt_id, event_id),
    )
    rows = await fetch_all(cursor)
    requested = next((r for r in rows if str(r["evidence_event_id"]) == event_id), None)
    if requested is None:
        return None
    try:
        head = next(
            (r for r in rows if r["evidence_event_id"] == r["latest_event_id"]), None
        )
        if (
            head is None
            or head["latest_revision"] != head["evidence_revision"]
            or head["latest_evidence_digest"] != head["evidence_digest"]
        ):
            raise ValueError("current model-call evidence head is inconsistent")
        current = _decode_row(head, tenant_id, attempt_id)
        if current.observed_at != head["sealed_at"]:
            raise ValueError("current model-call evidence head time is inconsistent")
        return (
            current
            if requested is head
            else _decode_row(requested, tenant_id, attempt_id)
        )
    except Exception as error:
        raise ModelCallEvidenceInvariantFailed(
            "persisted final model-call evidence violates owner invariants"
        ) from error


def _decode_row(
    row: dict[str, Any], tenant_id: str, attempt_id: str
) -> FinalModelCallEvidence:
    evidence_bytes = bytes(row["evidence_bytes"])
    artifact = parse_stored_evidence(evidence_bytes)
    validate_frozen_attempt(row, artifact_version=artifact.artifact_version)
    observed_at = datetime.fromisoformat(artifact.observed_at.replace("Z", "+00:00"))
    frozen_identity = json.loads(bytes(row["attempt_binding_bytes"]))
    if (
        rfc8785.dumps(artifact.model_dump(mode="json")) != evidence_bytes
        or artifact.execution.seal is not True
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
        or artifact.execution.prepared_at != _utc_millis_text(row["prepared_at"])
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
