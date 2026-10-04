"""Package-private PostgreSQL primitives for immutable model-call evidence."""

from __future__ import annotations

from collections.abc import Mapping

from kokoro_agent.infrastructure.postgres import utc_to_epoch_millis
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one
from kokoro_agent.model.call_evidence import ValidatedModelCallEvidence
from kokoro_agent.model.call_models import (
    AppendModelCallEvidence,
    ModelCallAuthorityLost,
    ModelCallEvidenceReceipt,
    ModelCallIdentityConflict,
)
from kokoro_agent.model.usage_evidence import parse_evidence, validate_successor


async def append_evidence_locked(
    cursor: object,
    *,
    run_table: str,
    attempt_table: str,
    evidence_table: str,
    command: AppendModelCallEvidence,
    attempt: Mapping[str, object],
    artifact: ValidatedModelCallEvidence,
    allow_new_revision: bool,
) -> ModelCallEvidenceReceipt:
    current_revision = _integer(attempt["latest_revision"])
    if artifact.revision <= current_revision:
        return await _replay_locked(
            cursor, evidence_table, command, artifact, allow_new_revision
        )
    if not allow_new_revision:
        raise ModelCallAuthorityLost("terminal Run rejects new model-call evidence")
    if artifact.revision != current_revision + 1:
        raise ModelCallIdentityConflict(
            "model-call evidence revision is not contiguous"
        )
    status = str(attempt["status"])
    if current_revision == 0:
        if status != "dispatch_started" or artifact.revision != 1:
            raise ModelCallIdentityConflict("model-call first evidence state conflicts")
        if artifact.evidence.predecessor is not None:
            raise ModelCallIdentityConflict("first model-call evidence has predecessor")
    else:
        if status not in {"observed", "unknown"}:
            raise ModelCallIdentityConflict("model-call successor state conflicts")
        await execute_sql(
            cursor,
            f"SELECT evidence_bytes FROM {evidence_table} WHERE attempt_id=%s AND revision=%s",
            (command.attempt_id, current_revision),
        )
        previous_row = await fetch_one(cursor)
        if previous_row is None:
            raise ModelCallIdentityConflict("model-call evidence head is missing")
        previous = parse_evidence(_bytes(previous_row["evidence_bytes"]))
        try:
            validate_successor(previous, artifact.evidence)
        except ValueError as error:
            raise ModelCallIdentityConflict(
                "model-call evidence successor conflicts"
            ) from error
    predecessor = artifact.evidence.predecessor
    await execute_sql(
        cursor,
        f"""INSERT INTO {evidence_table} (
            event_id,tenant_id,run_id,attempt_id,call_id,revision,
            predecessor_event_id,predecessor_digest,evidence_bytes,evidence_digest,
            execution_outcome,usage_state,totals_state,classification_state,
            sealed,observed_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s)""",
        (
            artifact.event_id,
            command.tenant_id,
            command.run_id,
            command.attempt_id,
            artifact.evidence.identity.call_id,
            artifact.revision,
            None if predecessor is None else predecessor.event_id,
            None if predecessor is None else predecessor.digest,
            command.evidence_bytes,
            artifact.digest,
            artifact.outcome,
            artifact.usage_state,
            artifact.totals_state,
            artifact.classification_state,
            artifact.observed_at,
        ),
    )
    next_status = "unknown" if artifact.outcome == "unknown" else "observed"
    await execute_sql(
        cursor,
        f"""UPDATE {attempt_table}
            SET status=%s,sealed_at=%s,latest_revision=%s,latest_event_id=%s,
                latest_evidence_digest=%s,updated_at=clock_timestamp()
            WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s""",
        (
            next_status,
            artifact.observed_at,
            artifact.revision,
            artifact.event_id,
            artifact.digest,
            command.tenant_id,
            command.run_id,
            command.attempt_id,
        ),
    )
    await recompute_run_completeness(
        cursor, run_table, attempt_table, evidence_table, command.run_id
    )
    return _receipt(command, artifact, replayed=False)


async def _replay_locked(
    cursor: object,
    evidence_table: str,
    command: AppendModelCallEvidence,
    artifact: ValidatedModelCallEvidence,
    allow_new_revision: bool,
) -> ModelCallEvidenceReceipt:
    await execute_sql(
        cursor,
        f"""SELECT event_id,evidence_bytes,evidence_digest,observed_at
            FROM {evidence_table}
            WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s AND revision=%s""",
        (
            command.tenant_id,
            command.run_id,
            command.attempt_id,
            artifact.revision,
        ),
    )
    existing = await fetch_one(cursor)
    if existing is None and not allow_new_revision:
        raise ModelCallAuthorityLost("terminal Run rejects new model-call evidence")
    if (
        existing is None
        or str(existing["event_id"]) != artifact.event_id
        or existing["evidence_bytes"] != command.evidence_bytes
        or existing["evidence_digest"] != artifact.digest
        or existing["observed_at"] != artifact.observed_at
    ):
        raise ModelCallIdentityConflict("model-call evidence replay conflicts")
    return _receipt(command, artifact, replayed=True)


async def recompute_run_completeness(
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
    command: AppendModelCallEvidence,
    artifact: ValidatedModelCallEvidence,
    *,
    replayed: bool,
) -> ModelCallEvidenceReceipt:
    observed_at_ms = utc_to_epoch_millis(artifact.observed_at)
    if observed_at_ms is None:
        raise ModelCallIdentityConflict("model-call evidence time is missing")
    return ModelCallEvidenceReceipt(
        tenant_id=command.tenant_id,
        run_id=command.run_id,
        attempt_id=command.attempt_id,
        event_id=artifact.event_id,
        revision=artifact.revision,
        evidence_digest=artifact.digest,
        observed_at_ms=observed_at_ms,
        replayed=replayed,
    )


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ModelCallIdentityConflict("model-call evidence revision is invalid")
    return int(value)


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ModelCallIdentityConflict("model-call evidence bytes are invalid")
    return value


__all__: list[str] = []
