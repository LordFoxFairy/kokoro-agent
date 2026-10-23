"""Read financial completion under the caller's existing Run lock before new dispatch."""

from __future__ import annotations

from datetime import datetime

from kokoro_agent.infrastructure.postgres import qualified
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all
from kokoro_agent.infrastructure.postgres_model_call_final import (
    read_final_evidence_in_cursor,
)
from kokoro_agent.model.call_models import ModelCallAuthorizationUnavailable
from kokoro_agent.model.financial_receipt import decode_financial_receipt
from kokoro_agent.model.usage_evidence_v2 import parse_evidence


async def require_run_financial_clear(
    cursor: object, *, schema: str, tenant_id: str, run_id: str, attempt_id: str
) -> None:
    attempts = qualified(schema, "kokoro_agent_model_call_attempt")
    evidence = qualified(schema, "kokoro_agent_model_usage_evidence")
    await execute_sql(
        cursor,
        f"""SELECT a.attempt_id,a.latest_event_id,a.financial_admission_ref,
        e.financial_ack_bytes,e.financial_ack_digest,e.financial_acked_at,
        EXISTS(SELECT 1 FROM {evidence} pending WHERE pending.tenant_id=a.tenant_id
          AND pending.attempt_id=a.attempt_id AND pending.financial_ack_bytes IS NULL) AS unacked
        FROM {attempts} a LEFT JOIN {evidence} e ON e.tenant_id=a.tenant_id
          AND e.attempt_id=a.attempt_id AND e.event_id=a.latest_event_id
        WHERE a.tenant_id=%s AND a.run_id=%s AND a.attempt_id<>%s
          AND a.sealed_at IS NOT NULL AND a.authorization_state IN ('granted','requested_unknown')""",
        (tenant_id, run_id, attempt_id),
    )
    rows = await fetch_all(cursor)
    for row in rows:
        raw, digest, at = (
            row["financial_ack_bytes"],
            row["financial_ack_digest"],
            row["financial_acked_at"],
        )
        if (
            row["unacked"]
            or not isinstance(raw, bytes)
            or not isinstance(digest, str)
            or not isinstance(at, datetime)
            or row["financial_admission_ref"] is None
        ):
            raise ModelCallAuthorizationUnavailable(
                "prior model financial evidence is unresolved"
            )
        final = await read_final_evidence_in_cursor(
            cursor,
            schema,
            tenant_id,
            str(row["attempt_id"]),
            str(row["latest_event_id"]),
        )
        if final is None or final.artifact_version != "2.0.0":
            raise ModelCallAuthorizationUnavailable(
                "prior model financial evidence conflicts"
            )
        original = parse_evidence(final.evidence_bytes)
        auth = original.authorization_observation
        ref = str(row["financial_admission_ref"])
        if auth.state == "granted" and auth.admission_ref != ref:
            raise ModelCallAuthorizationUnavailable(
                "prior model financial authorization conflicts"
            )
        ack = decode_financial_receipt(raw, digest)
        if ack.status not in {"captured", "released"} or (
            ack.admission_ref,
            ack.run_id,
            ack.attempt_id,
            ack.event_id,
            ack.revision,
            ack.digest,
        ) != (
            ref,
            run_id,
            final.attempt_id,
            final.event_id,
            str(final.revision),
            final.evidence_digest,
        ):
            raise ModelCallAuthorizationUnavailable(
                "prior model financial evidence is unresolved"
            )
