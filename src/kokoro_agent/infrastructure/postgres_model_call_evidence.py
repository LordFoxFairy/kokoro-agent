"""Package-private PostgreSQL primitives for immutable model-call evidence."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from kokoro_agent.infrastructure.postgres import qualified, utc_to_epoch_millis
from kokoro_agent.infrastructure.schema import (
    MODEL_CALL_ATTEMPTS_TABLE,
    MODEL_USAGE_EVIDENCE_TABLE,
    RUN_CLAIMS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.model.call_evidence import (
    ValidatedModelCallEvidence,
    build_terminal_closure_evidence,
    validate_sealed_evidence,
    validate_frozen_attempt,
)
from kokoro_agent.model.call_models import (
    AppendModelCallEvidence,
    ModelCallAuthorityLost,
    ModelCallEvidenceReceipt,
    ModelCallIdentityConflict,
)
from kokoro_agent.model.usage_evidence_v2 import parse_evidence, validate_successor


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
    if artifact.evidence.artifact_version != "2.0.0":
        raise ModelCallIdentityConflict("current writer rejects historical evidence")
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


async def close_model_call_attempts_for_terminal(
    cursor: object,
    *,
    schema: str,
    tenant_id: str,
    run_id: str,
    database_now: datetime,
    allow_create: bool,
) -> None:
    run_table = qualified(schema, RUN_CLAIMS_TABLE)
    attempt_table = qualified(schema, MODEL_CALL_ATTEMPTS_TABLE)
    evidence_table = qualified(schema, MODEL_USAGE_EVIDENCE_TABLE)
    if database_now.tzinfo is None:
        raise ModelCallIdentityConflict("terminal model-call time is invalid")
    await execute_sql(
        cursor,
        f"SELECT * FROM {attempt_table} WHERE tenant_id=%s AND run_id=%s ORDER BY attempt_id FOR UPDATE",
        (tenant_id, run_id),
    )
    attempts = await fetch_all(cursor)
    changed = False
    for attempt in attempts:
        revision = _integer(attempt["latest_revision"])
        if revision > 0:
            await _validate_sealed_head(cursor, evidence_table, attempt, database_now)
            continue
        validate_frozen_attempt(attempt)
        if not allow_create:
            raise ModelCallIdentityConflict(
                "terminal Run has an open model-call attempt"
            )
        raw, artifact = build_terminal_closure_evidence(attempt, database_now)
        predecessor = artifact.evidence.predecessor
        if predecessor is not None:
            raise ModelCallIdentityConflict("terminal closure has a predecessor")
        await execute_sql(
            cursor,
            f"""INSERT INTO {evidence_table} (
                event_id,tenant_id,run_id,attempt_id,call_id,revision,
                predecessor_event_id,predecessor_digest,evidence_bytes,evidence_digest,
                execution_outcome,usage_state,totals_state,classification_state,
                sealed,observed_at)
                VALUES (%s,%s,%s,%s,%s,1,NULL,NULL,%s,%s,%s,%s,%s,%s,TRUE,%s)""",
            (
                artifact.event_id,
                tenant_id,
                run_id,
                attempt["attempt_id"],
                artifact.evidence.identity.call_id,
                raw,
                artifact.digest,
                artifact.outcome,
                artifact.usage_state,
                artifact.totals_state,
                artifact.classification_state,
                artifact.observed_at,
            ),
        )
        next_status = "unknown" if artifact.outcome == "unknown" else "not_dispatched"
        await execute_sql(
            cursor,
            f"""UPDATE {attempt_table}
                SET status=%s,sealed_at=%s,latest_revision=1,latest_event_id=%s,
                    latest_evidence_digest=%s,updated_at=clock_timestamp()
                WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s
                  AND latest_revision=0 AND status=%s RETURNING attempt_id""",
            (
                next_status,
                artifact.observed_at,
                artifact.event_id,
                artifact.digest,
                tenant_id,
                run_id,
                attempt["attempt_id"],
                attempt["status"],
            ),
        )
        if await fetch_one(cursor) is None:
            raise ModelCallIdentityConflict("terminal model-call closure lost its head")
        changed = True
    if not allow_create:
        return
    if changed or attempts:
        await recompute_run_completeness(
            cursor, run_table, attempt_table, evidence_table, run_id
        )
    await execute_sql(
        cursor,
        f"SELECT model_attempt_count,model_sealed_attempt_count FROM {run_table} WHERE run_id=%s",
        (run_id,),
    )
    counts = await fetch_one(cursor)
    if (
        counts is None
        or counts["model_attempt_count"] != counts["model_sealed_attempt_count"]
    ):
        raise ModelCallIdentityConflict("terminal model-call closure is incomplete")


async def _validate_sealed_head(
    cursor: object,
    evidence_table: str,
    attempt: Mapping[str, object],
    database_now: datetime,
) -> None:
    await execute_sql(
        cursor,
        f"SELECT * FROM {evidence_table} WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s AND revision=%s",
        (
            attempt["tenant_id"],
            attempt["run_id"],
            attempt["attempt_id"],
            attempt["latest_revision"],
        ),
    )
    evidence = await fetch_one(cursor)
    if evidence is None:
        raise ModelCallIdentityConflict("terminal model-call evidence head is missing")
    artifact = validate_sealed_evidence(
        _bytes(evidence["evidence_bytes"]),
        attempt_binding_bytes=_bytes(attempt["attempt_binding_bytes"]),
        planned_bytes=_bytes(attempt["planned_bytes"]),
        authorization_bytes=_bytes(attempt["authorization_bytes"]),
        prepared_at=_datetime(attempt["prepared_at"]),
        dispatch_started_at=_optional_datetime(attempt["dispatch_started_at"]),
        database_now=database_now,
    )
    validate_frozen_attempt(
        attempt, artifact_version=artifact.evidence.artifact_version
    )
    expected_status = "observed"
    if artifact.outcome == "unknown":
        expected_status = "unknown"
    elif artifact.outcome == "not_dispatched":
        expected_status = (
            "authorization_denied"
            if attempt["authorization_state"] == "denied"
            else "not_dispatched"
        )
    predecessor = artifact.evidence.predecessor
    if (
        evidence["tenant_id"] != attempt["tenant_id"]
        or evidence["run_id"] != attempt["run_id"]
        or str(evidence["attempt_id"]) != str(attempt["attempt_id"])
        or str(evidence["call_id"]) != str(attempt["call_id"])
        or _integer(evidence["revision"]) != artifact.revision
        or artifact.revision != _integer(attempt["latest_revision"])
        or (None if predecessor is None else predecessor.event_id)
        != (
            None
            if evidence["predecessor_event_id"] is None
            else str(evidence["predecessor_event_id"])
        )
        or (None if predecessor is None else predecessor.digest)
        != evidence["predecessor_digest"]
        or str(evidence["event_id"]) != artifact.event_id
        or evidence["evidence_digest"] != artifact.digest
        or str(attempt["latest_event_id"]) != artifact.event_id
        or attempt["latest_evidence_digest"] != artifact.digest
        or attempt["status"] != expected_status
        or evidence["execution_outcome"] != artifact.outcome
        or evidence["usage_state"] != artifact.usage_state
        or evidence["totals_state"] != artifact.totals_state
        or evidence["classification_state"] != artifact.classification_state
        or evidence["sealed"] is not True
        or evidence["observed_at"] != artifact.observed_at
        or attempt["sealed_at"] != artifact.observed_at
    ):
        raise ModelCallIdentityConflict("terminal model-call evidence head conflicts")


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ModelCallIdentityConflict("model-call evidence bytes are invalid")
    return value


def _datetime(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise ModelCallIdentityConflict("model-call evidence time is invalid")
    return value


def _optional_datetime(value: object) -> datetime | None:
    return None if value is None else _datetime(value)


__all__: list[str] = []
