"""Strict run-local evidence records derived from Agent's durable event authority."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from typing import Literal, cast

from google.protobuf.message import DecodeError
from pydantic import BaseModel, ConfigDict, Field, model_validator

from kokoro.agent.execution.v1 import agent_execution_evidence_pb2 as wire
from kokoro_agent.contract import (
    DeliveryCreatedPayload,
    MessageCompletedPayload,
    MessageDeltaPayload,
    PlanProposedPayload,
    RunCompletedPayload,
    RunFailedPayload,
    RunOwnerCompletedPayload,
    RunStartedPayload,
    SubagentFinishedPayload,
    SubagentStartedPayload,
    SubagentTextCompletedPayload,
    SubagentTextDeltaPayload,
    SubagentThinkingDeltaPayload,
    SubagentToolInvokedPayload,
    SubagentToolReturnedPayload,
    ThinkingDeltaPayload,
    ToolAwaitingApprovalPayload,
    ToolInvokedPayload,
    ToolReturnedPayload,
)

MAX_CANONICAL_PAYLOAD_BYTES = 64 * 1024
MAX_SAFE_JSON_BYTES = 16 * 1024
MAX_SAFE_RESULT_PREVIEW_BYTES = 16 * 1024
_MAX_SAFE_JSON_DEPTH = 6
_MAX_SAFE_JSON_KEYS = 128
_MAX_SAFE_JSON_ARRAY_ITEMS = 32
_MAX_SAFE_JSON_STRING_CHARS = 1024

_SECRET_KEY_NAMES = frozenset(
    {
        "apikey",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "password",
        "passwd",
        "privatekey",
        "pwd",
        "refreshtoken",
        "secret",
        "secretkey",
        "setcookie",
        "token",
        "accesstoken",
    }
)
_SECRET_KEY_SUFFIXES = (
    "apikey",
    "credential",
    "password",
    "privatekey",
    "secret",
    "token",
)
_SCHEMA_SAMPLE_KEYS = frozenset({"default", "example", "examples"})
_SECRET_TEXT_PATTERNS = (
    re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]+"),
    re.compile(r"\bsk-[a-zA-Z0-9_-]{8,}"),
    re.compile(
        r"(?i)\b(password|passwd|pwd|secret|api[_-]?key|access[_-]?token)"
        r"\s*[:=]\s*[^\s,;]+"
    ),
)
_OUTPUT_REF_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,255}$")
_CONTENT_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
EvidenceKind = Literal[
    "run.started",
    "action_owner",
    "plan_owner",
    "run.owner.completed",
    "run.completed",
    "run.failed",
]

_EVIDENCE_KIND_BY_EVENT: dict[str, EvidenceKind] = {
    "run.started": "run.started",
    "tool.awaiting_approval": "action_owner",
    "plan.proposed": "plan_owner",
    "run.owner.completed": "run.owner.completed",
    "run.completed": "run.completed",
    "run.failed": "run.failed",
}
_ONEOF_BY_KIND: dict[EvidenceKind, str] = {
    "run.started": "run_started",
    "action_owner": "action_owner",
    "plan_owner": "plan_owner",
    "run.owner.completed": "run_owner_completed",
    "run.completed": "run_completed",
    "run.failed": "run_failed",
}


class EvidencePayloadTooLarge(ValueError):
    """The typed canonical evidence envelope exceeded its public wire cap."""


class DurableOutputDraft(BaseModel):
    """Safe typed output waiting for storage to allocate its independent sequence."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    canonical_payload: bytes = Field(
        min_length=1, max_length=MAX_CANONICAL_PAYLOAD_BYTES
    )
    text_part_ref: str | None = Field(default=None, min_length=1, max_length=256)
    is_text_snapshot: bool = False

    @model_validator(mode="after")
    def validate_canonical_payload(self) -> DurableOutputDraft:
        try:
            payload = wire.DurableOutputCanonicalPayloadV1.FromString(
                self.canonical_payload
            )
        except DecodeError as error:
            raise ValueError("output draft is not V1 protobuf") from error
        kind = payload.WhichOneof("payload")
        if kind is None:
            raise ValueError("output draft payload is missing")
        if self.is_text_snapshot != (kind == "text_snapshot"):
            raise ValueError("output draft snapshot marker mismatch")
        canonical_part_ref = (
            payload.text_delta.part_ref
            if kind == "text_delta"
            else payload.text_snapshot.part_ref
            if kind == "text_snapshot"
            else None
        )
        if self.text_part_ref != canonical_part_ref:
            raise ValueError("output draft text part marker mismatch")
        return self

    @property
    def source_payload_sha256(self) -> str:
        return hashlib.sha256(self.canonical_payload).hexdigest()


class DurableOutputRecord(BaseModel):
    """One append-only Agent output fact, independent of lifecycle durable_seq."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    output_ref: str = Field(min_length=1, max_length=256)
    output_version: Literal[1] = 1
    run_id: str = Field(min_length=1, max_length=128)
    output_seq: int = Field(gt=0)
    canonical_payload: bytes = Field(
        min_length=1, max_length=MAX_CANONICAL_PAYLOAD_BYTES
    )
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    recorded_at_ms: int = Field(ge=0)
    producer_instance_ref: str = Field(min_length=1, max_length=256)
    producer_generation: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_canonical_payload(self) -> DurableOutputRecord:
        if hashlib.sha256(self.canonical_payload).hexdigest() != self.payload_sha256:
            raise ValueError("output payload sha256 does not match canonical payload")
        try:
            payload = wire.DurableOutputCanonicalPayloadV1.FromString(
                self.canonical_payload
            )
        except DecodeError as error:
            raise ValueError("output canonical payload is not V1 protobuf") from error
        if payload.WhichOneof("payload") is None:
            raise ValueError("output canonical payload is missing")
        return self


class DurableRetentionStats(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    output_records: int = Field(ge=0)
    evidence_records: int = Field(ge=0)


def initial_output_digest(run_id: str) -> str:
    return hashlib.sha256(b"kokoro-output-chain-v1\0" + run_id.encode()).hexdigest()


def append_output_digest(
    previous_digest_sha256: str, output_seq: int, payload_sha256: str
) -> str:
    if output_seq < 1:
        raise ValueError("OUTPUT_SEQUENCE_INVALID")
    try:
        previous = bytes.fromhex(previous_digest_sha256)
        payload = bytes.fromhex(payload_sha256)
    except ValueError as error:
        raise ValueError("OUTPUT_DIGEST_INVALID") from error
    if len(previous) != 32 or len(payload) != 32:
        raise ValueError("OUTPUT_DIGEST_INVALID")
    return hashlib.sha256(
        previous + output_seq.to_bytes(8, "big") + payload
    ).hexdigest()


def _utf8_chunks(value: str, maximum_bytes: int) -> tuple[str, ...]:
    """Split text on UTF-8 boundaries without dropping any bytes."""

    if maximum_bytes < 4:
        raise ValueError("UTF8_CHUNK_LIMIT_INVALID")
    remaining = value.encode()
    chunks: list[str] = []
    while remaining:
        candidate = remaining[:maximum_bytes]
        try:
            chunk = candidate.decode()
        except UnicodeDecodeError as error:
            if error.start == 0:
                raise ValueError("UTF8_CHUNK_LIMIT_INVALID") from error
            candidate = candidate[: error.start]
            chunk = candidate.decode()
        chunks.append(chunk)
        remaining = remaining[len(candidate) :]
    return tuple(chunks)


def _bounded_output_text(
    value: str, maximum_bytes: int, *, allow_empty: bool = False
) -> str | None:
    if not value:
        return "" if allow_empty else None
    chunks = _utf8_chunks(value, maximum_bytes)
    return chunks[0]


def _safe_output_ref(value: str) -> str:
    if _OUTPUT_REF_PATTERN.fullmatch(value) is not None:
        return value
    return f"opaque_ref_{hashlib.sha256(value.encode()).hexdigest()}"


def _output_draft(
    payload: wire.DurableOutputCanonicalPayloadV1,
    *,
    text_part_ref: str | None = None,
    is_text_snapshot: bool = False,
) -> DurableOutputDraft:
    return DurableOutputDraft(
        canonical_payload=payload.SerializeToString(deterministic=True),
        text_part_ref=text_part_ref,
        is_text_snapshot=is_text_snapshot,
    )


def _text_delta_drafts(part_ref: str, value: str) -> tuple[DurableOutputDraft, ...]:
    safe_part_ref = _safe_output_ref(part_ref)
    return tuple(
        _output_draft(
            wire.DurableOutputCanonicalPayloadV1(
                text_delta=wire.TextDeltaOutputV1(
                    part_ref=safe_part_ref,
                    delta=delta,
                )
            ),
            text_part_ref=safe_part_ref,
        )
        for delta in _utf8_chunks(value, MAX_SAFE_RESULT_PREVIEW_BYTES)
    )


def _text_snapshot_drafts(part_ref: str, value: str) -> tuple[DurableOutputDraft, ...]:
    safe_part_ref = _safe_output_ref(part_ref)
    if not value:
        return (
            _output_draft(
                wire.DurableOutputCanonicalPayloadV1(
                    text_snapshot=wire.TextSnapshotOutputV1(
                        part_ref=safe_part_ref,
                        text="",
                    )
                ),
                text_part_ref=safe_part_ref,
                is_text_snapshot=True,
            ),
        )
    snapshot_chunks = _utf8_chunks(value, 60 * 1024)
    snapshot_text = snapshot_chunks[0]
    remainder = "".join(snapshot_chunks[1:])
    snapshot = _output_draft(
        wire.DurableOutputCanonicalPayloadV1(
            text_snapshot=wire.TextSnapshotOutputV1(
                part_ref=safe_part_ref,
                text=snapshot_text,
            )
        ),
        text_part_ref=safe_part_ref,
        is_text_snapshot=True,
    )
    return (snapshot, *_text_delta_drafts(safe_part_ref, remainder))


def _tool_started_draft(tool_call_id: str, label: str) -> DurableOutputDraft | None:
    safe_tool_call_id = _safe_output_ref(tool_call_id)
    safe_label = _bounded_output_text(label, 256)
    if safe_label is None:
        return None
    return _output_draft(
        wire.DurableOutputCanonicalPayloadV1(
            tool_started=wire.ToolStartedOutputV1(
                tool_call_id=safe_tool_call_id,
                tool_label=safe_label,
            )
        )
    )


def _tool_finished_drafts(
    tool_call_id: str,
    *,
    is_error: bool,
    truncated: bool,
) -> tuple[DurableOutputDraft, ...]:
    safe_tool_call_id = _safe_output_ref(tool_call_id)
    finished = _output_draft(
        wire.DurableOutputCanonicalPayloadV1(
            tool_finished=wire.ToolFinishedOutputV1(
                tool_call_id=safe_tool_call_id,
                is_error=is_error,
                truncated=truncated,
            )
        )
    )
    if not is_error:
        return (finished,)
    error = _output_draft(
        wire.DurableOutputCanonicalPayloadV1(
            error=wire.ErrorOutputV1(
                error_ref=safe_tool_call_id,
                code="tool.failed",
                message="Tool execution failed",
                retry_class=wire.OUTPUT_RETRY_CLASS_V1_NEVER,
            )
        )
    )
    return finished, error


def durable_output_drafts_for_event(
    payload: BaseModel,
) -> tuple[DurableOutputDraft, ...]:
    """Fail-closed projection from live events into typed durable output facts."""

    if isinstance(payload, MessageDeltaPayload):
        return _text_delta_drafts(payload.segment_id, payload.delta)
    if isinstance(payload, MessageCompletedPayload):
        return _text_snapshot_drafts(payload.segment_id, payload.content)
    if isinstance(payload, SubagentTextDeltaPayload):
        return _text_delta_drafts(payload.segment_id, payload.text)
    if isinstance(payload, SubagentTextCompletedPayload):
        return _text_snapshot_drafts(payload.segment_id, payload.text)
    if isinstance(payload, ThinkingDeltaPayload | SubagentThinkingDeltaPayload):
        return ()
    if isinstance(payload, ToolInvokedPayload | SubagentToolInvokedPayload):
        draft = _tool_started_draft(payload.tool_id, payload.name)
        return (draft,) if draft is not None else ()
    if isinstance(payload, ToolReturnedPayload | SubagentToolReturnedPayload):
        return _tool_finished_drafts(
            payload.tool_id,
            is_error=payload.is_error,
            truncated=bool(payload.truncated),
        )
    if isinstance(payload, PlanProposedPayload):
        plan_ref = _safe_output_ref(payload.owner_ref)
        summary = _bounded_output_text(payload.proposal.summary, 4096)
        if summary is None or len(payload.proposal.steps) > 256:
            return ()
        steps: list[wire.PlanStepV1] = []
        for step in payload.proposal.steps:
            step_ref = _safe_output_ref(step.step_ref)
            label = _bounded_output_text(step.label, 1024)
            if label is None:
                return ()
            steps.append(
                wire.PlanStepV1(
                    step_ref=step_ref,
                    label=label,
                    status=_PLAN_STATUS[step.status],
                )
            )
        return (
            _output_draft(
                wire.DurableOutputCanonicalPayloadV1(
                    plan_progress=wire.PlanProgressOutputV1(
                        plan_ref=plan_ref,
                        safe_summary=summary,
                        steps=steps,
                    )
                )
            ),
        )
    if isinstance(payload, SubagentStartedPayload | SubagentFinishedPayload):
        subagent_ref = _safe_output_ref(payload.subagent_id)
        failed = isinstance(payload, SubagentFinishedPayload) and bool(payload.failed)
        status = (
            wire.SUBAGENT_PROGRESS_STATUS_V1_FAILED
            if failed
            else wire.SUBAGENT_PROGRESS_STATUS_V1_COMPLETED
            if isinstance(payload, SubagentFinishedPayload)
            else wire.SUBAGENT_PROGRESS_STATUS_V1_RUNNING
        )
        progress = _output_draft(
            wire.DurableOutputCanonicalPayloadV1(
                subagent_progress=wire.SubagentProgressOutputV1(
                    subagent_ref=subagent_ref,
                    status=status,
                )
            )
        )
        if not failed:
            return (progress,)
        error = _output_draft(
            wire.DurableOutputCanonicalPayloadV1(
                error=wire.ErrorOutputV1(
                    error_ref=subagent_ref,
                    code="subagent.failed",
                    message="Subagent execution failed",
                    retry_class=wire.OUTPUT_RETRY_CLASS_V1_NEVER,
                )
            )
        )
        return progress, error
    if isinstance(payload, DeliveryCreatedPayload):
        if _CONTENT_HASH_PATTERN.fullmatch(payload.content_hash) is None:
            return ()
        delivery_ref = f"delivery:sha256:{payload.content_hash}"
        return (
            _output_draft(
                wire.DurableOutputCanonicalPayloadV1(
                    notice=wire.NoticeOutputV1(
                        notice_ref=delivery_ref,
                        code="delivery.created",
                        message="Delivery created",
                        severity=wire.NOTICE_SEVERITY_V1_INFO,
                        retry_class=wire.OUTPUT_RETRY_CLASS_V1_NEVER,
                    )
                )
            ),
        )
    return ()


def durable_output_draft_for_event(payload: BaseModel) -> DurableOutputDraft | None:
    """Return the first mapped output for callers that only accept one record."""

    drafts = durable_output_drafts_for_event(payload)
    return drafts[0] if drafts else None


def make_durable_output_record(
    *,
    run_id: str,
    output_seq: int,
    draft: DurableOutputDraft,
    replaces_through_output_seq: int,
    recorded_at_ms: int,
    producer_instance_ref: str,
    producer_generation: int,
) -> DurableOutputRecord:
    payload = wire.DurableOutputCanonicalPayloadV1.FromString(draft.canonical_payload)
    if draft.is_text_snapshot:
        payload.text_snapshot.replaces_through_output_seq = replaces_through_output_seq
    canonical_payload = payload.SerializeToString(deterministic=True)
    if len(canonical_payload) > MAX_CANONICAL_PAYLOAD_BYTES:
        raise EvidencePayloadTooLarge("OUTPUT_PAYLOAD_TOO_LARGE")
    payload_sha256 = hashlib.sha256(canonical_payload).hexdigest()
    identity = f"v1\0{run_id}\0{output_seq}\0{payload_sha256}".encode()
    return DurableOutputRecord(
        output_ref=f"ado_{hashlib.sha256(identity).hexdigest()}",
        run_id=run_id,
        output_seq=output_seq,
        canonical_payload=canonical_payload,
        payload_sha256=payload_sha256,
        recorded_at_ms=recorded_at_ms,
        producer_instance_ref=producer_instance_ref,
        producer_generation=producer_generation,
    )


class DurableExecutionEvidence(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    evidence_ref: str = Field(min_length=1, max_length=256)
    evidence_version: Literal[1] = 1
    run_id: str = Field(min_length=1, max_length=128)
    durable_seq: int = Field(gt=0)
    event_id: str = Field(min_length=1, max_length=256)
    kind: EvidenceKind
    canonical_payload: bytes = Field(max_length=MAX_CANONICAL_PAYLOAD_BYTES)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    recorded_at_ms: int = Field(ge=0)
    producer_instance_ref: str = Field(min_length=1, max_length=256)
    producer_generation: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_canonical_payload(self) -> DurableExecutionEvidence:
        actual = hashlib.sha256(self.canonical_payload).hexdigest()
        if actual != self.payload_sha256:
            raise ValueError("payload sha256 does not match canonical payload")
        try:
            payload = wire.DurableExecutionCanonicalPayloadV1.FromString(
                self.canonical_payload
            )
        except DecodeError as error:
            raise ValueError("canonical payload is not V1 protobuf") from error
        if payload.WhichOneof("payload") != _ONEOF_BY_KIND[self.kind]:
            raise ValueError("canonical payload kind does not match evidence kind")
        return self


def evidence_kind_for_event(event_kind: str) -> EvidenceKind | None:
    return _EVIDENCE_KIND_BY_EVENT.get(event_kind)


def _canonical_event_json(payload: BaseModel) -> bytes:
    return json.dumps(
        payload.model_dump(mode="json", exclude_none=True),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _safe_text(value: str, max_chars: int) -> str:
    text = value
    for pattern in _SECRET_TEXT_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 1]}…"


def _sensitive_key(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", value.casefold())
    return normalized in _SECRET_KEY_NAMES or normalized.endswith(
        _SECRET_KEY_SUFFIXES
    )


def _safe_json_value(
    value: object,
    *,
    depth: int,
    key_budget: list[int],
    schema_mode: bool,
) -> object:
    if depth > _MAX_SAFE_JSON_DEPTH:
        return "[REDACTED_DEPTH]"
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else "[REDACTED_NON_FINITE_NUMBER]"
    if isinstance(value, str):
        return _safe_text(value, _MAX_SAFE_JSON_STRING_CHARS)
    if isinstance(value, list):
        items = cast(list[object], value)
        return [
            _safe_json_value(
                item,
                depth=depth + 1,
                key_budget=key_budget,
                schema_mode=schema_mode,
            )
            for item in items[:_MAX_SAFE_JSON_ARRAY_ITEMS]
        ]
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        result: dict[str, object] = {}
        keys = sorted(key for key in mapping if isinstance(key, str))
        for key in keys:
            if key_budget[0] <= 0:
                break
            if len(key) > 256:
                continue
            key_budget[0] -= 1
            redact = (
                key.casefold() in _SCHEMA_SAMPLE_KEYS
                if schema_mode
                else _sensitive_key(key)
            )
            result[key] = (
                "[REDACTED]"
                if redact
                else _safe_json_value(
                    mapping[key],
                    depth=depth + 1,
                    key_budget=key_budget,
                    schema_mode=schema_mode,
                )
            )
        return result
    return "[REDACTED_TYPE]"


def _safe_json_object(
    value: Mapping[str, object], *, schema_mode: bool = False
) -> bytes:
    safe = _safe_json_value(
        value,
        depth=0,
        key_budget=[_MAX_SAFE_JSON_KEYS],
        schema_mode=schema_mode,
    )
    if not isinstance(safe, dict):
        raise ValueError("EVIDENCE_SAFE_JSON_NOT_OBJECT")
    encoded = json.dumps(
        safe,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    if len(encoded) <= MAX_SAFE_JSON_BYTES:
        return encoded
    return b'{"_kokoro_redacted":"payload_exceeded_safe_limit"}'


_ACTION_KIND = {
    "tool_approval": wire.ACTION_AWAITING_KIND_V1_TOOL_APPROVAL,
    "ask_user_question": wire.ACTION_AWAITING_KIND_V1_ASK_USER_QUESTION,
    "result_review": wire.ACTION_AWAITING_KIND_V1_RESULT_REVIEW,
    "input": wire.ACTION_AWAITING_KIND_V1_INPUT,
}
_ACTION_DECISION = {
    "approve": wire.ACTION_DECISION_V1_APPROVE,
    "edit": wire.ACTION_DECISION_V1_EDIT,
    "reject": wire.ACTION_DECISION_V1_REJECT,
    "respond": wire.ACTION_DECISION_V1_RESPOND,
    "submit": wire.ACTION_DECISION_V1_SUBMIT,
}
_PLAN_STATUS = {
    "pending": wire.PLAN_STEP_STATUS_V1_PENDING,
    "in_progress": wire.PLAN_STEP_STATUS_V1_IN_PROGRESS,
    "completed": wire.PLAN_STEP_STATUS_V1_COMPLETED,
}
_PLAN_DECISION = {
    "accept": wire.PLAN_DECISION_V1_ACCEPT,
    "reject": wire.PLAN_DECISION_V1_REJECT,
}


def _typed_payload(
    event_kind: str,
    payload_json: str,
    *,
    output_high_watermark: int,
    output_digest_sha256: str,
) -> bytes:
    try:
        raw = json.loads(payload_json)
    except (TypeError, ValueError) as error:
        raise ValueError("EVIDENCE_PAYLOAD_INVALID") from error
    try:
        if event_kind == "run.started":
            RunStartedPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                run_started=wire.RunStartedEvidenceV1()
            )
        elif event_kind == "tool.awaiting_approval":
            owner = ToolAwaitingApprovalPayload.model_validate(raw)
            safe_request = _safe_json_object(owner.args)
            pending_owner_refs = list(dict.fromkeys(owner.pending_tool_ids))
            if len(pending_owner_refs) > 64:
                raise ValueError("EVIDENCE_ACTION_OWNER_SET_TOO_LARGE")
            action = wire.ActionOwnerEvidenceV1(
                owner_ref=owner.tool_id,
                owner_version=1,
                segment_id=owner.segment_id,
                action_name=owner.name,
                awaiting_kind=_ACTION_KIND[owner.kind],
                action_payload_sha256=hashlib.sha256(
                    _canonical_event_json(owner)
                ).hexdigest(),
                description=_safe_text(owner.description, 4096),
                allowed_decisions=list(
                    dict.fromkeys(
                        _ACTION_DECISION[decision]
                        for decision in owner.allowed_decisions
                    )
                ),
                pending_owner_refs=pending_owner_refs,
                editable=owner.editable,
                safe_request_json=safe_request,
            )
            if owner.risk is not None:
                action.risk.CopyFrom(
                    wire.ActionRiskSummaryV1(
                        level=_safe_text(owner.risk.level, 64),
                        source=_safe_text(owner.risk.source, 128),
                        reason=_safe_text(owner.risk.reason, 4096),
                    )
                )
            if owner.input_schema is not None:
                safe_schema = _safe_json_object(owner.input_schema, schema_mode=True)
                action.input_schema_ref = (
                    f"sha256:{hashlib.sha256(safe_schema).hexdigest()}"
                )
                action.safe_input_schema_json = safe_schema
            payload = wire.DurableExecutionCanonicalPayloadV1(
                action_owner=action
            )
        elif event_kind == "plan.proposed":
            owner = PlanProposedPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                plan_owner=wire.PlanOwnerEvidenceV1(
                    owner_ref=owner.owner_ref,
                    owner_version=owner.owner_version,
                    segment_id=owner.segment_id,
                    proposal_payload_sha256=hashlib.sha256(
                        _canonical_event_json(owner)
                    ).hexdigest(),
                    summary=_safe_text(owner.proposal.summary, 4096),
                    steps=[
                        wire.PlanStepV1(
                            step_ref=step.step_ref,
                            label=_safe_text(step.label, 1024),
                            status=_PLAN_STATUS[step.status],
                        )
                        for step in owner.proposal.steps
                    ],
                    allowed_decisions=list(
                        dict.fromkeys(
                            _PLAN_DECISION[decision]
                            for decision in owner.proposal.allowed_actions
                        )
                    ),
                )
            )
        elif event_kind == "run.owner.completed":
            owner = RunOwnerCompletedPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                run_owner_completed=wire.RunOwnerCompletedEvidenceV1(
                    execution_context_anchor=owner.execution_context_anchor,
                    execution_context_digest=owner.execution_context_digest,
                    owner_revision=owner.owner_revision,
                )
            )
        elif event_kind == "run.completed":
            completed = RunCompletedPayload.model_validate(raw)
            status = (
                wire.RUN_COMPLETED_EVIDENCE_STATUS_COMPLETED
                if completed.status == "completed"
                else wire.RUN_COMPLETED_EVIDENCE_STATUS_CANCELLED
            )
            result = wire.RunCompletedEvidenceV1(
                status=status,
                output_high_watermark=output_high_watermark,
                output_digest_sha256=output_digest_sha256,
            )
            if completed.token_usage is not None:
                result.token_usage.CopyFrom(
                    wire.TokenUsageEvidenceV1(
                        input_tokens=completed.token_usage.input_tokens,
                        output_tokens=completed.token_usage.output_tokens,
                    )
                )
            payload = wire.DurableExecutionCanonicalPayloadV1(run_completed=result)
        elif event_kind == "run.failed":
            failed = RunFailedPayload.model_validate(raw)
            payload = wire.DurableExecutionCanonicalPayloadV1(
                run_failed=wire.RunFailedEvidenceV1(
                    code=failed.code,
                    error_kind=failed.error_kind,
                    message=failed.message,
                    output_high_watermark=output_high_watermark,
                    output_digest_sha256=output_digest_sha256,
                )
            )
        else:
            raise ValueError("EVIDENCE_KIND_UNSUPPORTED")
    except (TypeError, ValueError) as error:
        if str(error) in {"EVIDENCE_KIND_UNSUPPORTED", "EVIDENCE_PAYLOAD_INVALID"}:
            raise
        raise ValueError("EVIDENCE_PAYLOAD_INVALID") from error
    encoded = payload.SerializeToString(deterministic=True)
    if len(encoded) > MAX_CANONICAL_PAYLOAD_BYTES:
        raise EvidencePayloadTooLarge("EVIDENCE_PAYLOAD_TOO_LARGE")
    return encoded


def make_durable_execution_evidence(
    *,
    run_id: str,
    durable_seq: int,
    event_id: str,
    event_kind: str,
    payload_json: str,
    recorded_at_ms: int,
    producer_instance_ref: str,
    producer_generation: int,
    output_high_watermark: int = 0,
    output_digest_sha256: str | None = None,
) -> DurableExecutionEvidence:
    kind = evidence_kind_for_event(event_kind)
    if kind is None:
        raise ValueError("EVIDENCE_KIND_UNSUPPORTED")
    canonical_payload = _typed_payload(
        event_kind,
        payload_json,
        output_high_watermark=output_high_watermark,
        output_digest_sha256=(output_digest_sha256 or initial_output_digest(run_id)),
    )
    identity = f"v1\0{run_id}\0{event_id}".encode()
    return DurableExecutionEvidence(
        evidence_ref=f"aee_{hashlib.sha256(identity).hexdigest()}",
        run_id=run_id,
        durable_seq=durable_seq,
        event_id=event_id,
        kind=kind,
        canonical_payload=canonical_payload,
        payload_sha256=hashlib.sha256(canonical_payload).hexdigest(),
        recorded_at_ms=recorded_at_ms,
        producer_instance_ref=producer_instance_ref,
        producer_generation=producer_generation,
    )
