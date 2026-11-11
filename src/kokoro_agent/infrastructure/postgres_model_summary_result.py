"""Original summary sealing/replay under the existing Run → Attempt lock order."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from datetime import datetime

from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_model_call_final import (
    read_final_evidence_in_cursor,
)
from kokoro_agent.infrastructure.postgres_run_profiles import validate_static_recipe
from kokoro_agent.infrastructure.sql import execute_sql, fetch_all, fetch_one
from kokoro_agent.model.call_artifacts import (
    encode_attempt_binding,
    encode_planned_artifact,
    recipe_feature_for_peer_path,
)
from kokoro_agent.model.call_evidence import (
    ValidatedModelCallEvidence,
    validate_frozen_attempt,
)
from kokoro_agent.model.call_models import (
    AttemptBinding,
    ModelCallAuthorityLost,
    ModelCallAuthorizationUnavailable,
    ModelCallIdentityConflict,
    PrepareModelCall,
    ReadSummaryResult,
    SealModelCallObservation,
)
from kokoro_agent.model.financial_receipt import decode_financial_receipt
from kokoro_agent.model.summary_result import (
    SummaryMessage,
    SummaryReplayReceipt,
    SummaryResult,
    SummaryUsage,
    decode_summary_result,
    encode_summary_result,
)
from kokoro_agent.model.usage_evidence_v2 import parse_evidence, validate_successor
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_v2_generated import (
    ModelUsageEvidence,
    PlannedBinding,
)

_MAX_INT64 = (1 << 63) - 1


def _usage(artifact: ModelUsageEvidence) -> SummaryUsage:
    usage = artifact.usage
    if usage.state != "normalized":
        return SummaryUsage(input_tokens=None, output_tokens=None, total_tokens=None)
    counts = (usage.value.input_total, usage.value.output_total, usage.value.total)
    values = [count.count if count.state == "reported" else None for count in counts]
    return SummaryUsage(
        input_tokens=values[0], output_tokens=values[1], total_tokens=values[2]
    )


def _candidate(
    command: SealModelCallObservation,
    attempt: Mapping[str, object],
    artifact: ValidatedModelCallEvidence,
) -> tuple[bytes, str] | None:
    summary = command.summary
    if summary is None:
        return None
    binding = summary.binding
    if (
        binding.tenant_ref,
        binding.run_id,
        binding.call_id,
        binding.attempt_id,
        binding.planned_digest,
    ) != (
        command.tenant_id,
        command.run_id,
        str(attempt["call_id"]),
        command.attempt_id,
        command.planned_digest,
    ):
        raise ModelCallIdentityConflict("native summary seal binding conflicts")
    if artifact.evidence.artifact_version != "2.0.0":
        raise ModelCallIdentityConflict("native summary evidence version conflicts")
    if summary.text is None or command.outcome != "completed":
        return None
    value = SummaryResult(
        binding=binding,
        attempt_binding_digest=command.attempt_binding_digest,
        final_request_digest=_text(attempt["final_request_digest"]),
        event_id=artifact.event_id,
        event_digest=artifact.digest,
        usage=_usage(artifact.evidence),
        message=SummaryMessage(content=summary.text),
    )
    try:
        return encode_summary_result(value)
    except ValueError:
        # The response still happened: retain its usage/financial facts, but no
        # invalid/truncated result is ever eligible for successful replay.
        return None


async def seal_summary_result_locked(
    cursor: object,
    *,
    schema: str,
    command: SealModelCallObservation,
    attempt: Mapping[str, object],
    artifact: ValidatedModelCallEvidence,
    replayed: bool,
) -> None:
    candidate = _candidate(command, attempt, artifact)
    prior = (attempt.get("summary_result_bytes"), attempt.get("summary_result_digest"))
    if replayed:
        expected = (None, None) if candidate is None else candidate
        if prior != expected:
            raise ModelCallIdentityConflict(
                "native summary observation replay conflicts"
            )
        return
    if command.summary is None:
        return
    await execute_sql(
        cursor,
        f"SELECT assembly_recipe_fingerprint,token_total FROM {qualified(schema, 'kokoro_agent_run')} WHERE tenant_id=%s AND run_id=%s",
        (command.tenant_id, command.run_id),
    )
    run = await fetch_one(cursor)
    if (
        run is None
        or run["assembly_recipe_fingerprint"] != command.summary.binding.recipe_digest
    ):
        raise ModelCallIdentityConflict("native summary recipe conflicts")
    if artifact.evidence.artifact_version != "2.0.0":
        raise ModelCallIdentityConflict("native summary evidence version conflicts")
    total = _usage(artifact.evidence).reported_total
    current = _integer(run["token_total"])
    if total is not None and total > _MAX_INT64 - current:
        raise ModelCallIdentityConflict(
            "native summary token total exceeds storage bound"
        )
    # First evidence CAS is owned by seal_observation; this helper runs only
    # under that same transaction and never increments on an evidence replay.
    if candidate is not None:
        await execute_sql(
            cursor,
            f"UPDATE {qualified(schema, 'kokoro_agent_model_call_attempt')} SET summary_result_bytes=%s,summary_result_digest=%s WHERE tenant_id=%s AND attempt_id=%s AND summary_result_bytes IS NULL AND summary_result_digest IS NULL",
            (*candidate, command.tenant_id, command.attempt_id),
        )
    if total is not None:
        await execute_sql(
            cursor,
            f"UPDATE {qualified(schema, 'kokoro_agent_run')} SET token_total=token_total+%s WHERE tenant_id=%s AND run_id=%s",
            (total, command.tenant_id, command.run_id),
        )


async def read_summary_result(
    database_url: str,
    schema: str,
    command: ReadSummaryResult,
    *,
    total_timeout_seconds: float,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
) -> SummaryReplayReceipt | None:
    async with asyncio.timeout(total_timeout_seconds):
        async with connect_pg(database_url) as connection:
            async with connection.transaction(), connection.cursor() as cursor:
                await execute_sql(
                    cursor, f"SET LOCAL lock_timeout='{lock_timeout_ms}ms'"
                )
                await execute_sql(
                    cursor, f"SET LOCAL statement_timeout='{statement_timeout_ms}ms'"
                )
                await execute_sql(
                    cursor,
                    f"SELECT *,clock_timestamp() AS summary_read_at FROM {qualified(schema, 'kokoro_agent_run')} WHERE tenant_id=%s AND run_id=%s FOR UPDATE",
                    (command.binding.tenant_ref, command.binding.run_id),
                )
                run = await fetch_one(cursor)
                _require_run(run, command)
                assert run is not None
                await execute_sql(
                    cursor,
                    f"SELECT * FROM {qualified(schema, 'kokoro_agent_model_call_attempt')} WHERE tenant_id=%s AND run_id=%s AND attempt_id=%s FOR UPDATE",
                    (
                        command.binding.tenant_ref,
                        command.binding.run_id,
                        command.binding.attempt_id,
                    ),
                )
                attempt = await fetch_one(cursor)
                if attempt is None:
                    return None
                _require_attempt(attempt, command)
                if attempt["dispatch_started_at"] is None:
                    if (attempt["lease_owner"], attempt["lease_generation"]) != (
                        command.lease.owner,
                        command.lease.generation,
                    ):
                        raise ModelCallAuthorityLost(
                            "native summary undispatched attempt has an old lease"
                        )
                    return None
                raw, digest = (
                    attempt["summary_result_bytes"],
                    attempt["summary_result_digest"],
                )
                if raw is None and digest is None:
                    raise ModelCallAuthorizationUnavailable(
                        "native summary result is unavailable"
                    )
                if not isinstance(raw, bytes) or not isinstance(digest, str):
                    raise ModelCallIdentityConflict(
                        "native summary result columns conflict"
                    )
                try:
                    result = decode_summary_result(raw, digest)
                except ValueError:
                    raise ModelCallIdentityConflict(
                        "native summary result integrity conflicts"
                    ) from None
                if result.binding != command.binding or (
                    result.attempt_binding_digest,
                    result.final_request_digest,
                ) != (
                    attempt["attempt_binding_digest"],
                    attempt["final_request_digest"],
                ):
                    raise ModelCallIdentityConflict(
                        "native summary result execution binding conflicts"
                    )
                receipt = await _validate_events(
                    cursor,
                    schema,
                    command,
                    attempt,
                    result,
                    _integer(run["token_total"]),
                )
                task = asyncio.current_task()
                if task is not None and task.cancelling():
                    raise asyncio.CancelledError
                return receipt


def _require_run(run: Mapping[str, object] | None, command: ReadSummaryResult) -> None:
    if (
        run is None
        or run["terminal"] is not False
        or (run["owner"], run["lease_generation"])
        != (command.lease.owner, command.lease.generation)
        or not isinstance(run["lease_expires_at"], datetime)
        or not isinstance(run["summary_read_at"], datetime)
        or run["lease_expires_at"] <= run["summary_read_at"]
    ):
        raise ModelCallAuthorityLost("native summary current Run lease is unavailable")
    try:
        request = RunRequest.model_validate_json(
            _text(run["request_json"]), strict=True
        )
        validate_static_recipe(command.recipe, command.lease)
        planned = encode_planned_artifact(command.planned)
        if (
            request != command.request
            or command.binding.tenant_ref != request.execution_identity.tenant_ref
            or command.binding.run_id != request.run_id
            or run["assembly_recipe_bytes"] != command.recipe.canonical_bytes
            or run["assembly_recipe_fingerprint"] != command.recipe.fingerprint
            or hashlib.sha256(command.recipe.canonical_bytes).hexdigest()
            != command.recipe.fingerprint
            or command.binding.recipe_digest != command.recipe.fingerprint
            or command.binding.planned_digest != planned[1]
            or recipe_feature_for_peer_path(
                command.recipe.canonical_bytes, command.binding.peer_path
            )
            != request.feature_key
        ):
            raise ValueError
    except (ValueError, TypeError):
        raise ModelCallIdentityConflict(
            "native summary current request or recipe conflicts"
        ) from None


def _require_attempt(attempt: Mapping[str, object], command: ReadSummaryResult) -> None:
    validate_frozen_attempt(attempt)
    original = AttemptBinding.model_validate_json(
        _bytes(attempt["attempt_binding_bytes"]), strict=True
    )
    planned = encode_planned_artifact(command.planned)
    if (
        str(attempt["call_id"]),
        str(attempt["attempt_id"]),
        attempt["planned_bytes"],
        attempt["planned_digest"],
        original.peer_path,
    ) != (
        command.binding.call_id,
        command.binding.attempt_id,
        planned[0],
        planned[1],
        command.binding.peer_path,
    ):
        raise ModelCallIdentityConflict(
            "native summary original route or locator conflicts"
        )
    # Reconstruct only the original frozen identity, never prepare with this lease.
    original_command = PrepareModelCall(
        request=command.request,
        lease=LeaseFence(
            owner=original.lease.owner, generation=int(original.lease.generation)
        ),
        call_id=original.call_id,
        attempt_id=original.attempt_id,
        planned=PlannedBinding.model_validate_json(
            _bytes(attempt["planned_bytes"]), strict=True
        ),
        peer_path=original.peer_path,
        recipe=command.recipe,
        final_request_binding_version=1,
        final_request_digest=_text(attempt["final_request_digest"]),
    )
    if encode_attempt_binding(
        original_command, command.request, int(original.attempt_ordinal)
    ) != (attempt["attempt_binding_bytes"], attempt["attempt_binding_digest"]):
        raise ModelCallIdentityConflict(
            "native summary original request identity conflicts"
        )


async def _validate_events(
    cursor: object,
    schema: str,
    command: ReadSummaryResult,
    attempt: Mapping[str, object],
    result: SummaryResult,
    token_total: int,
) -> SummaryReplayReceipt:
    await execute_sql(
        cursor,
        f"SELECT event_id,revision,financial_ack_bytes,financial_ack_digest,financial_acked_at FROM {qualified(schema, 'kokoro_agent_model_usage_evidence')} WHERE tenant_id=%s AND attempt_id=%s ORDER BY revision LIMIT 65",
        (command.binding.tenant_ref, command.binding.attempt_id),
    )
    events = await fetch_all(cursor)
    if (
        not events
        or len(events) > 64
        or len(events) != _integer(attempt["latest_revision"])
    ):
        raise ModelCallIdentityConflict("native summary evidence chain is incomplete")
    previous: ModelUsageEvidence | None = None
    pending = False
    known = False
    for index, row in enumerate(events, 1):
        final = await read_final_evidence_in_cursor(
            cursor,
            schema,
            command.binding.tenant_ref,
            command.binding.attempt_id,
            str(row["event_id"]),
        )
        if (
            final is None
            or final.artifact_version != "2.0.0"
            or final.revision != index
        ):
            raise ModelCallIdentityConflict("native summary evidence binding conflicts")
        event = parse_evidence(final.evidence_bytes)
        if previous is not None:
            validate_successor(previous, event)
        previous = event
        if index == 1:
            usage = _usage(event)
            if (result.event_id, result.event_digest, result.usage) != (
                final.event_id,
                final.evidence_digest,
                usage,
            ):
                raise ModelCallIdentityConflict(
                    "native summary first observation conflicts"
                )
            known = (
                event.execution.outcome == "completed"
                and event.actual.state == "verified"
                and event.usage.state == "normalized"
                and event.usage.value.profile_assessment.state == "supported"
                and usage.known_total is not None
            )
            if usage.reported_total is not None and token_total < usage.reported_total:
                raise ModelCallIdentityConflict(
                    "native summary durable token total conflicts"
                )
        raw, digest, at = (
            row["financial_ack_bytes"],
            row["financial_ack_digest"],
            row["financial_acked_at"],
        )
        if raw is None and digest is None and at is None:
            pending = True
            continue
        if (
            not isinstance(raw, bytes)
            or not isinstance(digest, str)
            or not isinstance(at, datetime)
        ):
            raise ModelCallIdentityConflict(
                "native summary financial ACK is incomplete"
            )
        ack = decode_financial_receipt(raw, digest)
        if (
            ack.admission_ref,
            ack.run_id,
            ack.attempt_id,
            ack.event_id,
            ack.revision,
            ack.digest,
        ) != (
            str(attempt["financial_admission_ref"]),
            command.binding.run_id,
            command.binding.attempt_id,
            final.event_id,
            str(final.revision),
            final.evidence_digest,
        ):
            raise ModelCallIdentityConflict(
                "native summary financial ACK binding conflicts"
            )
        if index < len(events) and ack.status != "pending_reconciliation":
            raise ModelCallIdentityConflict(
                "native summary financial predecessor conflicts"
            )
        if index == len(events):
            if ack.status == "released":
                raise ModelCallAuthorizationUnavailable(
                    "native summary financial ACK is not captured"
                )
            pending = pending or ack.status != "captured"
    if not known and not pending:
        raise ModelCallAuthorizationUnavailable(
            "native summary original usage is not replayable"
        )
    return SummaryReplayReceipt(
        result=result,
        token_total=token_total,
        state="pending" if pending else "captured",
    )


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ModelCallIdentityConflict("native summary frozen bytes are invalid")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ModelCallIdentityConflict("native summary frozen text is invalid")
    return value


def _integer(value: object) -> int:
    if type(value) is not int or not 0 <= value <= _MAX_INT64:
        raise ModelCallIdentityConflict("native summary frozen integer is invalid")
    return value
