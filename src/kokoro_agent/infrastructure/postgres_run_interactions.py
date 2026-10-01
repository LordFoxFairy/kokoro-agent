"""Run-owned HITL transactions and strict private codecs; no native execution."""

from __future__ import annotations

import base64
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json
from typing import Annotated, Any, Literal, NoReturn

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from kokoro_agent.domain.chat.models import ChatEventDraft, ChatProjection
from kokoro_agent.domain.run.interactions import (
    CheckpointObservation,
    ObservationTarget,
    ObservationStored,
    ConsumedPauseEvidence,
    UnknownResumeEvidence,
    QuiescentProbe,
    ReconcileProbeResult,
    InteractionRecoveryTarget,
    ResumeReadContext,
    AcceptedResume,
    Decision,
    DecisionGroup,
    DurablePauseSnapshot,
    InteractionAuthorityLost,
    InteractionCommitted,
    InteractionConflict,
    InteractionCorrupt,
    InteractionRunMissing,
    InteractionSnapshot,
    InteractionState,
    IntentStatus,
    PendingGroup,
    PendingItem,
    Phase,
    ReplayedResume,
    ResumeDispatchPlan,
    ResumeIntent,
    StartedResume,
    Submission,
    ValidationIssue,
)
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.infrastructure.postgres import connect_pg, qualified
from kokoro_agent.infrastructure.postgres_chat_repository import PostgresChatRepository
from kokoro_agent.infrastructure.postgres_run_context import (
    PostgresRunRepositoryContext,
)
from kokoro_agent.infrastructure.schema import (
    RUN_CHECKPOINT_OBSERVATIONS_TABLE,
    RUN_CLAIMS_TABLE,
    RUN_CONTROL_COMMANDS_TABLE,
)
from kokoro_agent.infrastructure.sql import execute_sql, fetch_one, fetch_all
from kokoro_agent.protocol.control import RunRequest, RunResume, control_request_digest
from kokoro_agent.protocol.events import (
    ChatInteractionState,
    InteractionGroup,
    InteractionActionResult,
)

_LIMIT = 8388608
_OBJECT = TypeAdapter(dict[str, JsonValue])
Nonempty = Annotated[str, Field(min_length=1)]
HexDigest = Annotated[
    str, Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
]


class _Strict(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )


class _TaskLocator(_Strict):
    task_id: Nonempty
    interrupt_id: Nonempty
    item_ids: Annotated[list[Nonempty], Field(min_length=1)]


class _GroupLocator(_Strict):
    group_id: Nonempty
    thread_id: Nonempty
    checkpoint_ns: str
    checkpoint_id: Nonempty
    tasks: Annotated[list[_TaskLocator], Field(min_length=1)]


class _Locator(_Strict):
    groups: Annotated[list[_GroupLocator], Field(min_length=1)]


class _Pause(_Strict):
    format: Literal["kokoro-agent:pause-collection:1"]
    groups: Annotated[list[InteractionGroup], Field(min_length=1)]
    locator: _Locator


class _StoredDecision(_Strict):
    item_id: Nonempty
    kind: Literal["approve", "edit", "reject", "respond", "submit"]
    payload_b64: str


class _StoredGroup(_Strict):
    group_id: Nonempty
    decisions: Annotated[list[_StoredDecision], Field(min_length=1)]


class _Decisions(_Strict):
    format: Literal["kokoro-agent:resume-decisions:1"]
    groups: Annotated[list[_StoredGroup], Field(min_length=1)]


class _TaskPlan(_Strict):
    task_id: Nonempty
    interrupt_id: Nonempty
    pre_resume_length: Annotated[int, Field(ge=0)]
    pre_resume_digest: HexDigest
    expected_append_digest: HexDigest


class _GroupPlan(_Strict):
    group_id: Nonempty
    tasks: Annotated[list[_TaskPlan], Field(min_length=1)]


class StoredResumePlan(_Strict):
    format: Literal["kokoro-agent:resume-dispatch:1"]
    attempt_id: Nonempty
    groups: Annotated[list[_GroupPlan], Field(min_length=1)]


class _RunRow(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="ignore", hide_input_in_errors=True
    )
    run_id: str
    tenant_id: str
    request_json: str
    owner: str | None
    lease_generation: int
    lease_expires_at: datetime | None
    terminal: bool
    event_index_counter: int
    interaction_revision: int
    interaction_phase: Literal["active", "waiting", "resuming", "terminal"]
    interaction_source_index: int | None
    pause_revision: int
    pause_ref: str | None
    pending_groups_json: list[InteractionGroup]
    pause_snapshot_json: dict[str, JsonValue] | None
    pause_collection_digest: str | None
    interaction_command_id: str | None


class _CommandRow(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="ignore", hide_input_in_errors=True
    )
    run_id: str
    command_id: str
    request_digest: str
    body: str
    resume_pause_revision: int | None
    resume_pause_ref: str | None
    resume_decisions_bytes: bytes | None
    resume_decisions_digest: str | None
    resume_intent_status: (
        Literal[
            "accepted",
            "dispatch_started",
            "native_observed",
            "unknown",
            "reconciled",
            "terminal",
        ]
        | None
    )
    resume_pause_snapshot_json: dict[str, JsonValue] | None
    resume_pause_collection_digest: str | None
    resume_accepted_revision: int | None
    resume_accepted_source_index: int | None
    resume_attempt_id: str | None
    resume_attempt_generation: int | None
    resume_dispatch_plan_json: dict[str, JsonValue] | None
    resume_accepted_at: datetime | None
    resume_started_at: datetime | None
    resume_observation_digest: str | None
    resume_result_kind: (
        Literal[
            "accepted", "native_consumed", "validation_failed", "unknown", "cancelled"
        ]
        | None
    )
    resume_result_revision: int | None
    resume_result_source_index: int | None
    resume_probe_count: int
    resume_probe_progress_digest: str | None
    resume_probe_quiescence_json: dict[str, JsonValue] | None
    resume_probe_last_read_id: str | None
    resume_probe_checked_at: datetime | None


def canonical_interaction_bytes(value: object) -> bytes:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if not 1 <= len(raw) <= _LIMIT:
        raise InteractionCorrupt("interaction value exceeds its byte limit")
    return raw


def _unique(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise InteractionCorrupt("duplicate JSON field")
        result[key] = value
    return result


def _invalid_constant(_value: str) -> NoReturn:
    raise InteractionCorrupt("nonfinite JSON value")


def decode_interaction_bytes(raw: bytes) -> dict[str, JsonValue]:
    if type(raw) is not bytes or not 1 <= len(raw) <= _LIMIT:
        raise InteractionCorrupt("invalid interaction bytes")
    value = _OBJECT.validate_python(
        json.loads(raw, object_pairs_hook=_unique, parse_constant=_invalid_constant)
    )
    if canonical_interaction_bytes(value) != raw:
        raise InteractionCorrupt("noncanonical interaction bytes")
    return value


def decode_resume_command(
    body: str, digest: str, *, run_id: str, session_id: str, command_id: str
) -> Submission:
    """One stored typed command is the only authority for accepted decisions."""
    message = RunResume.model_validate_json(body)
    if (
        message.model_dump_json().encode("utf-8") != body.encode("utf-8")
        or message.run_id != run_id
        or message.session_id != session_id
        or message.command_id != command_id
        or message.request_digest != digest
        or control_request_digest(message) != digest
    ):
        raise InteractionCorrupt("stored resume identity or digest mismatch")
    return Submission(
        command_id=message.command_id,
        pause_revision=message.expected_pause_revision,
        pause_ref=message.pause_ref,
        decisions=tuple(
            Decision(
                item_id=d.item_id,
                kind=d.type,
                payload=canonical_interaction_bytes(
                    d.model_dump(
                        mode="json", exclude={"item_id", "type"}, exclude_none=True
                    )
                ),
            )
            for d in message.decisions
        ),
    )


def decode_pause_snapshot(value: DurablePauseSnapshot) -> _Pause:
    raw = decode_interaction_bytes(value.canonical_bytes)
    if hashlib.sha256(value.canonical_bytes).hexdigest() != value.digest:
        raise InteractionCorrupt("pause digest mismatch")
    result = _Pause.model_validate(raw)
    if len(result.groups) != len(result.locator.groups):
        raise InteractionCorrupt("pause locator group coverage mismatch")
    for group, locator in zip(result.groups, result.locator.groups, strict=True):
        expected = [item.item_id for item in group.items]
        actual = [item for task in locator.tasks for item in task.item_ids]
        task_ids = [(task.task_id, task.interrupt_id) for task in locator.tasks]
        if (
            group.group_id != locator.group_id
            or actual != expected
            or len(set(task_ids)) != len(task_ids)
        ):
            raise InteractionCorrupt("pause locator item coverage mismatch")
    # The domain rejects duplicate group/item identities and invalid action sets.
    InteractionState().pause(pause_ref=value.pause_ref, groups=_groups(result))
    return result


def _groups(pause: _Pause) -> tuple[PendingGroup, ...]:
    return tuple(
        PendingGroup(
            group_id=g.group_id,
            items=tuple(
                PendingItem(
                    item_id=i.item_id,
                    request_id=i.request_id,
                    allowed_decisions=tuple(i.allowed_decisions),
                    validation=None
                    if i.validation is None
                    else ValidationIssue(
                        instance_path=tuple(i.validation.instance_path)
                    ),
                )
                for i in g.items
            ),
        )
        for g in pause.groups
    )


def _decision_bytes(intent: ResumeIntent) -> bytes:
    return canonical_interaction_bytes(
        {
            "format": "kokoro-agent:resume-decisions:1",
            "groups": [
                {
                    "group_id": g.group_id,
                    "decisions": [
                        {
                            "item_id": d.item_id,
                            "kind": d.kind,
                            "payload_b64": base64.b64encode(d.payload).decode("ascii"),
                        }
                        for d in g.decisions
                    ],
                }
                for g in intent.groups
            ],
        }
    )


def _intent(row: _CommandRow, request: RunRequest) -> ResumeIntent:
    if (
        row.resume_intent_status is None
        or row.resume_pause_revision is None
        or row.resume_pause_ref is None
        or row.resume_decisions_bytes is None
        or row.resume_decisions_digest is None
        or row.resume_pause_snapshot_json is None
        or row.resume_pause_collection_digest is None
        or row.resume_accepted_revision is None
        or row.resume_accepted_source_index is None
        or row.resume_accepted_at is None
    ):
        raise InteractionCorrupt("incomplete stored resume intent")
    pause = decode_pause_snapshot(
        DurablePauseSnapshot(
            pause_ref=row.resume_pause_ref,
            canonical_bytes=canonical_interaction_bytes(row.resume_pause_snapshot_json),
            digest=row.resume_pause_collection_digest,
        )
    )
    decoded = _Decisions.model_validate(
        decode_interaction_bytes(row.resume_decisions_bytes)
    )
    if (
        hashlib.sha256(row.resume_decisions_bytes).hexdigest()
        != row.resume_decisions_digest
    ):
        raise InteractionCorrupt("decision digest mismatch")
    groups: list[DecisionGroup] = []
    for group in decoded.groups:
        decisions: list[Decision] = []
        for item in group.decisions:
            payload = base64.b64decode(item.payload_b64, validate=True)
            if base64.b64encode(payload).decode("ascii") != item.payload_b64:
                raise InteractionCorrupt("noncanonical decision encoding")
            decisions.append(
                Decision(item_id=item.item_id, kind=item.kind, payload=payload)
            )
        groups.append(
            DecisionGroup(group_id=group.group_id, decisions=tuple(decisions))
        )
    submission = decode_resume_command(
        row.body,
        row.request_digest,
        run_id=request.run_id,
        session_id=request.session_id,
        command_id=row.command_id,
    )
    original = InteractionState(
        interaction_revision=row.resume_accepted_revision - 1,
        pause_revision=row.resume_pause_revision,
        pause_ref=row.resume_pause_ref,
        phase=Phase.WAITING,
        groups=_groups(pause),
    ).accept(submission)
    if original.intent is None or original.intent.groups != tuple(groups):
        raise InteractionCorrupt("stored decision vector differs from admitted command")
    status = IntentStatus(row.resume_intent_status)
    started = row.resume_attempt_id is not None
    if any(
        value is not None
        for value in (
            row.resume_attempt_id,
            row.resume_attempt_generation,
            row.resume_dispatch_plan_json,
            row.resume_started_at,
        )
    ) and not all(
        value is not None
        for value in (
            row.resume_attempt_id,
            row.resume_attempt_generation,
            row.resume_dispatch_plan_json,
            row.resume_started_at,
        )
    ):
        raise InteractionCorrupt("incomplete attempt identity")
    if (
        status is IntentStatus.ACCEPTED
        and started
        or status
        in (
            IntentStatus.DISPATCH_STARTED,
            IntentStatus.NATIVE_OBSERVED,
            IntentStatus.UNKNOWN,
            IntentStatus.RECONCILED,
        )
        and not started
    ):
        raise InteractionCorrupt("intent status and attempt disagree")
    if row.resume_dispatch_plan_json is not None:
        plan = StoredResumePlan.model_validate(
            decode_interaction_bytes(
                canonical_interaction_bytes(row.resume_dispatch_plan_json)
            )
        )
        expected = [
            (g.group_id, [(t.task_id, t.interrupt_id) for t in g.tasks])
            for g in pause.locator.groups
        ]
        actual = [
            (g.group_id, [(t.task_id, t.interrupt_id) for t in g.tasks])
            for g in plan.groups
        ]
        if plan.attempt_id != row.resume_attempt_id or expected != actual:
            raise InteractionCorrupt("stored dispatch plan differs from its pause")
    return ResumeIntent(
        command_id=row.command_id,
        pause_revision=row.resume_pause_revision,
        pause_ref=row.resume_pause_ref,
        groups=tuple(groups),
        status=status,
        attempt_id=row.resume_attempt_id,
    )


class _ObservedTask(_Strict):
    task_id: Nonempty
    interrupt_id: Nonempty
    item_ids: Annotated[list[Nonempty], Field(min_length=1)]
    resume_length: Annotated[int, Field(ge=0)]
    resume_digest: HexDigest
    error: bool
    interrupt_digest: HexDigest


class _Successor(_Strict):
    checkpoint_id: Nonempty
    parent_checkpoint_id: str | None
    current_task_ids: list[Nonempty]
    current_interrupt_ids: list[Nonempty]


class _ObservedGroup(_Strict):
    group_id: Nonempty
    thread_id: Nonempty
    checkpoint_ns: str
    checkpoint_id: Nonempty
    parent_checkpoint_id: str | None
    tasks: Annotated[list[_ObservedTask], Field(min_length=1)]
    successors: list[_Successor]


class _Quiescence(_Strict):
    kind: Literal["local_drained"]
    worker_boot_id: Nonempty
    invocation_id: Nonempty


class _Observation(_Strict):
    format: Literal["kokoro-agent:checkpoint-observation:1"]
    run_id: Nonempty
    generation: Annotated[int, Field(gt=0)]
    command_id: str | None
    attempt_id: str | None
    kind: Literal["pause", "read", "resume", "probe"]
    probe_read_id: str | None
    facts: list[_ObservedGroup]
    pause: _Pause | None = None
    pause_ref: str | None = None
    pause_revision: int | None = None
    collection_digest: str | None = None
    next_pause: _Pause | None = None
    next_pause_ref: str | None = None
    quiescence: _Quiescence | None = None
    progress_digest: str | None = None


def decode_checkpoint_observation(
    value: CheckpointObservation, request: RunRequest
) -> _Observation:
    decoded = _Observation.model_validate(
        decode_interaction_bytes(value.canonical_bytes)
    )
    if (
        hashlib.sha256(value.canonical_bytes).hexdigest() != value.digest
        or (
            decoded.run_id,
            decoded.generation,
            decoded.command_id,
            decoded.attempt_id,
            decoded.kind,
        )
        != (
            request.run_id,
            value.target.generation,
            value.target.command_id,
            value.target.attempt_id,
            value.kind,
        )
        or (decoded.command_id is None) != (decoded.attempt_id is None)
        or (
            decoded.command_id is not None
            and (not decoded.command_id or not decoded.attempt_id)
        )
        or (decoded.kind == "pause" and decoded.command_id is not None)
        or (decoded.kind in ("resume", "probe") and decoded.command_id is None)
        or (decoded.kind == "probe") != (decoded.probe_read_id is not None)
        or (
            decoded.kind == "probe"
            and (
                not decoded.probe_read_id
                or decoded.quiescence is None
                or decoded.progress_digest is None
            )
        )
        or (
            decoded.kind != "probe"
            and (decoded.quiescence is not None or decoded.progress_digest is not None)
        )
    ):
        raise InteractionCorrupt("invalid checkpoint observation identity")
    ids = [g.group_id for g in decoded.facts]
    if len(set(ids)) != len(ids):
        raise InteractionCorrupt("duplicate observation group")
    thread = RunScope.of(request).scoped_thread_id
    for group in decoded.facts:
        task_ids = [(t.task_id, t.interrupt_id) for t in group.tasks]
        items = [item for t in group.tasks for item in t.item_ids]
        if (
            group.thread_id != thread
            or len(set(task_ids)) != len(task_ids)
            or len(set(items)) != len(items)
        ):
            raise InteractionCorrupt("invalid observation coverage")
        prior = group.checkpoint_id
        for successor in group.successors:
            if (
                successor.parent_checkpoint_id != prior
                or successor.checkpoint_id == prior
            ):
                raise InteractionCorrupt("noncausal checkpoint successor")
            prior = successor.checkpoint_id
    if decoded.kind == "pause":
        if decoded.pause is None or not decoded.pause_ref:
            raise InteractionCorrupt("pause observation missing collection")
        original = decode_pause_snapshot(
            DurablePauseSnapshot(
                pause_ref=decoded.pause_ref,
                canonical_bytes=canonical_interaction_bytes(
                    decoded.pause.model_dump(mode="json", exclude_unset=True)
                ),
                digest=hashlib.sha256(
                    canonical_interaction_bytes(
                        decoded.pause.model_dump(mode="json", exclude_unset=True)
                    )
                ).hexdigest(),
            )
        )
        if [
            (
                g.group_id,
                g.thread_id,
                g.checkpoint_ns,
                g.checkpoint_id,
                [(t.task_id, t.interrupt_id, t.item_ids) for t in g.tasks],
            )
            for g in original.locator.groups
        ] != [
            (
                g.group_id,
                g.thread_id,
                g.checkpoint_ns,
                g.checkpoint_id,
                [(t.task_id, t.interrupt_id, t.item_ids) for t in g.tasks],
            )
            for g in decoded.facts
        ]:
            raise InteractionCorrupt("pause observation collection coverage differs")
    elif decoded.pause is not None:
        raise InteractionCorrupt("unexpected original pause payload")
    if decoded.kind == "resume":
        if (
            not decoded.pause_ref
            or not decoded.pause_revision
            or not decoded.collection_digest
            or not decoded.facts
        ):
            raise InteractionCorrupt("incomplete consumption identity")
    elif any(
        v is not None
        for v in (
            decoded.pause_revision,
            decoded.collection_digest,
            decoded.next_pause,
            decoded.next_pause_ref,
        )
    ):
        raise InteractionCorrupt("read observation claims consumption")
    if decoded.kind in ("read", "probe") and decoded.pause_ref is not None:
        raise InteractionCorrupt("read observation contains pause claim")
    if (decoded.next_pause is None) != (decoded.next_pause_ref is None):
        raise InteractionCorrupt("incomplete successor pause")
    return decoded


class PostgresRunInteractions:
    def __init__(self, context: PostgresRunRepositoryContext) -> None:
        self._context = context
        self._chat = PostgresChatRepository(context.database_url, context.schema)

    async def _run(self, cur: Any, request: RunRequest) -> _RunRow:
        await execute_sql(
            cur,
            "SELECT * FROM {} WHERE run_id=%s FOR UPDATE".format(
                qualified(self._context.schema, RUN_CLAIMS_TABLE)
            ),
            (request.run_id,),
        )
        raw = await fetch_one(cur)
        if raw is None or raw["tenant_id"] != request.execution_identity.tenant_ref:
            raise InteractionRunMissing("Run was not found")
        row = _RunRow.model_validate(raw)
        if row.request_json.encode("utf-8") != request.model_dump_json().encode(
            "utf-8"
        ):
            raise InteractionCorrupt("Run request bytes differ")
        return row

    async def _command(self, cur: Any, run_id: str, command_id: str) -> _CommandRow:
        await execute_sql(
            cur,
            "SELECT * FROM {} WHERE run_id=%s AND command_id=%s FOR UPDATE".format(
                qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
            ),
            (run_id, command_id),
        )
        raw = await fetch_one(cur)
        if raw is None:
            raise InteractionConflict("command_missing")
        return _CommandRow.model_validate(raw)

    async def _snapshot(
        self, cur: Any, row: _RunRow, request: RunRequest
    ) -> InteractionSnapshot:
        if row.terminal != (row.interaction_phase == "terminal"):
            raise InteractionCorrupt("Run terminal and interaction head disagree")
        pause: DurablePauseSnapshot | None = None
        pending: tuple[PendingGroup, ...] = ()
        if row.pause_revision:
            if (
                row.pause_ref is None
                or row.pause_snapshot_json is None
                or row.pause_collection_digest is None
            ):
                raise InteractionCorrupt("incomplete pause identity")
            pause = DurablePauseSnapshot(
                pause_ref=row.pause_ref,
                canonical_bytes=canonical_interaction_bytes(row.pause_snapshot_json),
                digest=row.pause_collection_digest,
            )
            value = decode_pause_snapshot(pause)
            if row.interaction_phase in ("waiting", "resuming"):
                if [
                    g.model_dump(mode="json", exclude_unset=True)
                    for g in row.pending_groups_json
                ] != [
                    g.model_dump(mode="json", exclude_unset=True) for g in value.groups
                ]:
                    raise InteractionCorrupt("head and immutable pause disagree")
                pending = _groups(value)
            elif row.pending_groups_json:
                raise InteractionCorrupt("released phase still has pending groups")
        elif (
            row.pause_ref is not None
            or row.pause_snapshot_json is not None
            or row.pause_collection_digest is not None
            or row.pending_groups_json
        ):
            raise InteractionCorrupt("initial state has pause evidence")
        intent = None
        if row.interaction_command_id is not None:
            intent = _intent(
                await self._command(cur, request.run_id, row.interaction_command_id),
                request,
            )
        state = InteractionState(
            interaction_revision=row.interaction_revision,
            pause_revision=row.pause_revision,
            pause_ref=row.pause_ref,
            phase=Phase(row.interaction_phase),
            groups=pending,
            intent=intent,
        )
        return InteractionSnapshot(
            state=state, pause=pause, source_index=row.interaction_source_index
        )

    async def _active(self, cur: Any, row: _RunRow, lease: LeaseFence) -> datetime:
        now = await self._context.database_now(cur)
        if (
            row.terminal
            or (row.owner, row.lease_generation) != (lease.owner, lease.generation)
            or row.lease_expires_at is None
            or row.lease_expires_at <= now
        ):
            raise InteractionAuthorityLost("Run interaction authority lost")
        return now

    async def _write(
        self,
        cur: Any,
        request: RunRequest,
        row: _RunRow,
        state: InteractionState,
        pause: DurablePauseSnapshot | None,
        now: datetime,
        *,
        visible: bool = True,
        result_kind: Literal[
            "accepted", "native_consumed", "validation_failed", "unknown", "cancelled"
        ]
        | None = None,
    ) -> InteractionSnapshot:
        value = decode_pause_snapshot(pause) if pause is not None else None
        groups = (
            value.groups
            if value is not None and state.phase in (Phase.WAITING, Phase.RESUMING)
            else []
        )
        source_index = row.event_index_counter if visible else None
        action = None
        if state.intent is not None:
            command = await self._command(cur, request.run_id, state.intent.command_id)
            kind = result_kind or command.resume_result_kind
            if kind is not None:
                action = InteractionActionResult(
                    command_id=state.intent.command_id,
                    pause_revision=state.intent.pause_revision,
                    kind=kind,
                )
                if visible:
                    await execute_sql(
                        cur,
                        "UPDATE {} SET resume_result_kind=%s,resume_result_revision=%s,resume_result_source_index=%s WHERE run_id=%s AND command_id=%s".format(
                            qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                        ),
                        (
                            kind,
                            state.interaction_revision,
                            row.event_index_counter,
                            request.run_id,
                            state.intent.command_id,
                        ),
                    )
        payload = ChatInteractionState(
            interaction_revision=state.interaction_revision,
            pause_revision=state.pause_revision,
            pause_ref=state.pause_ref,
            phase=state.phase.value,
            groups=groups,
            action_result=action,
        )
        await execute_sql(
            cur,
            """UPDATE {} SET interaction_revision=%s,interaction_phase=%s,interaction_source_index=%s,
            pause_revision=%s,pause_ref=%s,pending_groups_json=%s::jsonb,pause_snapshot_json=%s::jsonb,pause_collection_digest=%s,
            interaction_command_id=%s,event_index_counter=%s,updated_at=%s,
            terminal=CASE WHEN %s THEN TRUE ELSE terminal END,
            terminal_at=CASE WHEN %s THEN %s ELSE terminal_at END WHERE run_id=%s""".format(
                qualified(self._context.schema, RUN_CLAIMS_TABLE)
            ),
            (
                state.interaction_revision,
                state.phase.value,
                source_index,
                state.pause_revision,
                state.pause_ref,
                canonical_interaction_bytes(
                    [g.model_dump(mode="json", exclude_unset=True) for g in groups]
                ).decode(),
                pause.canonical_bytes.decode() if pause is not None else None,
                pause.digest if pause else None,
                state.intent.command_id if state.intent else None,
                row.event_index_counter + int(visible),
                now,
                state.phase is Phase.TERMINAL,
                state.phase is Phase.TERMINAL,
                now,
                request.run_id,
            ),
        )
        if visible:
            scope = RunScope.of(request)
            await self._chat.append_on_cursor(
                cur,
                ChatProjection(
                    event=ChatEventDraft(
                        tenant_id=request.execution_identity.tenant_ref,
                        namespace=scope.namespace,
                        session_id=request.session_id,
                        run_id=request.run_id,
                        source_index=row.event_index_counter,
                        event_type="interaction.state",
                        payload_json=payload.model_dump_json(exclude_unset=True),
                        created_at=now,
                    )
                ),
            )
        return InteractionSnapshot(state=state, pause=pause, source_index=source_index)

    async def read_interaction(self, request: RunRequest) -> InteractionSnapshot | None:
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            try:
                row = await self._run(cur, request)
            except InteractionRunMissing:
                return None
            return await self._snapshot(cur, row, request)

    async def record_pause(
        self, request: RunRequest, lease: LeaseFence, pause: DurablePauseSnapshot
    ) -> InteractionCommitted | ReplayedResume:
        value = decode_pause_snapshot(pause)
        if any(
            g.thread_id != RunScope.of(request).scoped_thread_id
            for g in value.locator.groups
        ):
            raise InteractionCorrupt("pause thread differs from Run")
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            row = await self._run(cur, request)
            snapshot = await self._snapshot(cur, row, request)
            if (
                not row.terminal
                and row.owner == lease.owner
                and row.lease_generation == lease.generation
                and snapshot.state.phase is Phase.WAITING
                and snapshot.pause == pause
            ):
                return ReplayedResume(
                    snapshot=snapshot,
                    original_intent=snapshot.state.intent,
                    accepted_source_index=snapshot.source_index,
                )
            now = await self._active(cur, row, lease)
            state = snapshot.state.pause(
                pause_ref=pause.pause_ref, groups=_groups(value)
            )
            if state.intent is not None:
                await execute_sql(
                    cur,
                    "UPDATE {} SET resume_intent_status='reconciled',updated_at=%s WHERE run_id=%s AND command_id=%s".format(
                        qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                    ),
                    (now, request.run_id, state.intent.command_id),
                )
            result = await self._write(cur, request, row, state, pause, now)
            # Chat sequence locking can wait behind another Run in this session.
            # Recheck after that wait; raising rolls the entire source/head back.
            await self._active(cur, row, lease)
            await execute_sql(
                cur,
                "UPDATE {} SET lease_expires_at=NULL WHERE run_id=%s".format(
                    qualified(self._context.schema, RUN_CLAIMS_TABLE)
                ),
                (request.run_id,),
            )
            return InteractionCommitted(snapshot=result)

    async def accept_resume(
        self, request: RunRequest, command_id: str, owner: str
    ) -> AcceptedResume | ReplayedResume:
        if not owner:
            raise ValueError("empty interaction owner")
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            row = await self._run(cur, request)
            command = await self._command(cur, request.run_id, command_id)
            snapshot = await self._snapshot(cur, row, request)
            submission = decode_resume_command(
                command.body,
                command.request_digest,
                run_id=request.run_id,
                session_id=request.session_id,
                command_id=command_id,
            )
            if command.resume_intent_status is not None:
                original = _intent(command, request)
                original.verify_replay(submission)
                return ReplayedResume(
                    snapshot=snapshot,
                    original_intent=original,
                    accepted_source_index=command.resume_accepted_source_index,
                )
            now = await self._context.database_now(cur)
            if (
                row.terminal
                or snapshot.state.phase is not Phase.WAITING
                or row.lease_expires_at is not None
            ):
                raise InteractionConflict("not_waiting")
            state = snapshot.state.accept(submission)
            if state.intent is None or snapshot.pause is None:
                raise InteractionCorrupt("accepted state lacks intent or pause")
            encoded = _decision_bytes(state.intent)
            lease = LeaseFence(owner=owner, generation=row.lease_generation + 1)
            await execute_sql(
                cur,
                """UPDATE {} SET resume_pause_revision=%s,resume_pause_ref=%s,resume_decisions_bytes=%s,
                resume_decisions_digest=%s,resume_intent_status='accepted',resume_pause_snapshot_json=%s::jsonb,
                resume_pause_collection_digest=%s,resume_accepted_revision=%s,resume_accepted_source_index=%s,
                resume_accepted_at=%s,updated_at=%s WHERE run_id=%s AND command_id=%s""".format(
                    qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                ),
                (
                    state.pause_revision,
                    state.pause_ref,
                    encoded,
                    hashlib.sha256(encoded).hexdigest(),
                    snapshot.pause.canonical_bytes.decode(),
                    snapshot.pause.digest,
                    state.interaction_revision,
                    row.event_index_counter,
                    now,
                    now,
                    request.run_id,
                    command_id,
                ),
            )
            result = await self._write(
                cur, request, row, state, snapshot.pause, now, result_kind="accepted"
            )
            now = await self._context.database_now(cur)
            await execute_sql(
                cur,
                "UPDATE {} SET owner=%s,lease_generation=%s,lease_expires_at=%s WHERE run_id=%s".format(
                    qualified(self._context.schema, RUN_CLAIMS_TABLE)
                ),
                (
                    owner,
                    lease.generation,
                    now + timedelta(milliseconds=self._context.ttl_ms),
                    request.run_id,
                ),
            )
            return AcceptedResume(snapshot=result, lease=lease)

    async def start_resume(
        self,
        request: RunRequest,
        lease: LeaseFence,
        command_id: str,
        plan: ResumeDispatchPlan,
    ) -> StartedResume | ReplayedResume:
        value = StoredResumePlan.model_validate(
            decode_interaction_bytes(plan.canonical_bytes)
        )
        if value.attempt_id != plan.attempt_id:
            raise InteractionCorrupt("dispatch attempt differs")
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            row = await self._run(cur, request)
            command = await self._command(cur, request.run_id, command_id)
            snapshot = await self._snapshot(cur, row, request)
            now = await self._active(cur, row, lease)
            if (
                snapshot.pause is None
                or snapshot.state.intent is None
                or snapshot.state.intent.command_id != command_id
            ):
                raise InteractionConflict("intent_conflict")
            original = decode_pause_snapshot(snapshot.pause)
            expected = [
                (g.group_id, [(t.task_id, t.interrupt_id) for t in g.tasks])
                for g in original.locator.groups
            ]
            actual = [
                (g.group_id, [(t.task_id, t.interrupt_id) for t in g.tasks])
                for g in value.groups
            ]
            if expected != actual:
                raise InteractionCorrupt("dispatch plan coverage differs")
            if (
                command.resume_intent_status == "dispatch_started"
                and command.resume_attempt_id == plan.attempt_id
            ):
                if (
                    command.resume_attempt_generation != lease.generation
                    or command.resume_dispatch_plan_json is None
                    or canonical_interaction_bytes(command.resume_dispatch_plan_json)
                    != plan.canonical_bytes
                ):
                    raise InteractionConflict("attempt_conflict")
                return ReplayedResume(
                    snapshot=snapshot,
                    original_intent=snapshot.state.intent,
                    accepted_source_index=command.resume_accepted_source_index,
                )
            state = snapshot.state.start(
                command_id=command_id, attempt_id=plan.attempt_id
            )
            await execute_sql(
                cur,
                """UPDATE {} SET resume_intent_status='dispatch_started',resume_attempt_id=%s,
                resume_attempt_generation=%s,resume_dispatch_plan_json=%s::jsonb,resume_started_at=%s,updated_at=%s
                WHERE run_id=%s AND command_id=%s""".format(
                    qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                ),
                (
                    plan.attempt_id,
                    lease.generation,
                    plan.canonical_bytes.decode(),
                    now,
                    now,
                    request.run_id,
                    command_id,
                ),
            )
            return StartedResume(
                snapshot=InteractionSnapshot(
                    state=state,
                    pause=snapshot.pause,
                    source_index=snapshot.source_index,
                ),
                attempt_id=plan.attempt_id,
            )

    async def mark_resume_unknown(
        self, request: RunRequest, lease: LeaseFence, command_id: str, attempt_id: str
    ) -> InteractionCommitted | ReplayedResume:
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            row = await self._run(cur, request)
            command = await self._command(cur, request.run_id, command_id)
            snapshot = await self._snapshot(cur, row, request)
            now = await self._active(cur, row, lease)
            if (
                command.resume_attempt_id != attempt_id
                or command.resume_attempt_generation != lease.generation
            ):
                raise InteractionConflict("attempt_conflict")
            state = snapshot.state.unknown(command_id=command_id)
            if state == snapshot.state:
                return ReplayedResume(
                    snapshot=snapshot,
                    original_intent=state.intent,
                    accepted_source_index=command.resume_accepted_source_index,
                )
            await execute_sql(
                cur,
                "UPDATE {} SET resume_intent_status='unknown',updated_at=%s WHERE run_id=%s AND command_id=%s".format(
                    qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                ),
                (now, request.run_id, command_id),
            )
            return InteractionCommitted(
                snapshot=InteractionSnapshot(
                    state=state,
                    pause=snapshot.pause,
                    source_index=snapshot.source_index,
                )
            )

    async def terminal_on_cursor(
        self,
        cur: Any,
        request: RunRequest,
        raw: dict[str, Any],
        *,
        now: datetime,
        index: int,
        visible: bool,
    ) -> int:
        """Only the existing finalizer calls this while holding its Run authority."""
        row = _RunRow.model_validate({**raw, "event_index_counter": index})
        snapshot = await self._snapshot(cur, row, request)
        state = snapshot.state.terminal()
        await execute_sql(
            cur,
            "UPDATE {} SET resume_intent_status='terminal',updated_at=%s WHERE run_id=%s AND resume_intent_status IS NOT NULL AND resume_intent_status <> 'terminal'".format(
                qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
            ),
            (now, request.run_id),
        )
        await self._write(
            cur,
            request,
            row,
            state,
            snapshot.pause,
            now,
            visible=visible,
            result_kind="cancelled"
            if snapshot.state.intent is not None
            and snapshot.state.intent.status
            not in (IntentStatus.RECONCILED, IntentStatus.TERMINAL)
            else None,
        )
        return index + int(visible)

    async def read_resume_context(
        self, request: RunRequest, command_id: str
    ) -> ResumeReadContext | None:
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            try:
                row = await self._run(cur, request)
            except InteractionRunMissing:
                return None
            command = await self._command(cur, request.run_id, command_id)
            if command.resume_intent_status is None:
                return None
            intent = _intent(command, request)
            if (
                command.resume_pause_snapshot_json is None
                or command.resume_pause_ref is None
                or command.resume_pause_collection_digest is None
            ):
                raise InteractionCorrupt("missing original collection")
            return ResumeReadContext(
                snapshot=await self._snapshot(cur, row, request),
                original_pause=DurablePauseSnapshot(
                    pause_ref=command.resume_pause_ref,
                    canonical_bytes=canonical_interaction_bytes(
                        command.resume_pause_snapshot_json
                    ),
                    digest=command.resume_pause_collection_digest,
                ),
                intent=intent,
                dispatch_plan=ResumeDispatchPlan(
                    attempt_id=command.resume_attempt_id,
                    canonical_bytes=canonical_interaction_bytes(
                        command.resume_dispatch_plan_json
                    ),
                )
                if command.resume_attempt_id is not None
                and command.resume_dispatch_plan_json is not None
                else None,
                attempt_generation=command.resume_attempt_generation,
                observation_digest=command.resume_observation_digest,
            )

    def _observation_command(
        self, value: _Observation, command: _CommandRow, request: RunRequest
    ) -> None:
        _intent(command, request)
        if (value.attempt_id, value.generation) != (
            command.resume_attempt_id,
            command.resume_attempt_generation,
        ):
            raise InteractionCorrupt("observation attempt differs")
        if command.resume_pause_snapshot_json is None:
            raise InteractionCorrupt("missing original pause")
        original = _Pause.model_validate(command.resume_pause_snapshot_json)
        expected = [
            (
                g.group_id,
                g.thread_id,
                g.checkpoint_ns,
                g.checkpoint_id,
                [(t.task_id, t.interrupt_id, t.item_ids) for t in g.tasks],
            )
            for g in original.locator.groups
        ]
        actual = [
            (
                g.group_id,
                g.thread_id,
                g.checkpoint_ns,
                g.checkpoint_id,
                [(t.task_id, t.interrupt_id, t.item_ids) for t in g.tasks],
            )
            for g in value.facts
        ]
        if expected != actual:
            raise InteractionCorrupt("observation differs from original locator")
        if value.kind == "resume":
            if (value.pause_revision, value.pause_ref, value.collection_digest) != (
                command.resume_pause_revision,
                command.resume_pause_ref,
                command.resume_pause_collection_digest,
            ) or command.resume_dispatch_plan_json is None:
                raise InteractionCorrupt("consumption identity differs")
            plan = StoredResumePlan.model_validate(command.resume_dispatch_plan_json)
            for group, planned in zip(value.facts, plan.groups, strict=True):
                for task, expected_task in zip(group.tasks, planned.tasks, strict=True):
                    if (
                        task.error
                        or task.resume_length != expected_task.pre_resume_length + 1
                        or task.resume_digest != expected_task.expected_append_digest
                    ):
                        raise InteractionCorrupt(
                            "consumption vector differs from immutable plan"
                        )
                # Validation may re-interrupt in the original checkpoint. A full
                # exact appended vector proves that round; success needs successor.
                if value.next_pause is None and (
                    not group.successors
                    or group.successors[-1].current_task_ids
                    or group.successors[-1].current_interrupt_ids
                ):
                    raise InteractionCorrupt("missing stable causal successor")
            if value.next_pause is not None:
                decode_pause_snapshot(
                    DurablePauseSnapshot(
                        pause_ref=value.next_pause_ref or "",
                        canonical_bytes=canonical_interaction_bytes(
                            value.next_pause.model_dump(mode="json", exclude_unset=True)
                        ),
                        digest=hashlib.sha256(
                            canonical_interaction_bytes(
                                value.next_pause.model_dump(
                                    mode="json", exclude_unset=True
                                )
                            )
                        ).hexdigest(),
                    )
                )

    async def _store_observation(
        self,
        cur: Any,
        request: RunRequest,
        lease: LeaseFence,
        row: _RunRow,
        observation: CheckpointObservation,
        command: _CommandRow | None,
    ) -> ObservationStored:
        value = decode_checkpoint_observation(observation, request)
        if value.generation != lease.generation:
            raise InteractionCorrupt("observation fence differs")
        if command is not None:
            self._observation_command(value, command, request)
        elif value.command_id is not None or value.generation != row.lease_generation:
            raise InteractionCorrupt("unattributed observation")
        table = qualified(self._context.schema, RUN_CHECKPOINT_OBSERVATIONS_TABLE)
        await execute_sql(
            cur,
            f"SELECT * FROM {table} WHERE run_id=%s AND observation_digest=%s FOR UPDATE",
            (request.run_id, observation.digest),
        )
        existing = await fetch_one(cur)
        now = await self._context.database_now(cur)
        if existing is not None:
            if bytes(existing["evidence_bytes"]) != observation.canonical_bytes:
                raise InteractionCorrupt("observation digest collision")
            return ObservationStored(
                digest=observation.digest,
                disposition=existing["disposition"],
                inserted=False,
            )
        current = (
            not row.terminal
            and (row.owner, row.lease_generation) == (lease.owner, lease.generation)
            and row.lease_expires_at is not None
            and row.lease_expires_at > now
        )
        disposition: Literal["current", "audit"] = "current" if current else "audit"
        await execute_sql(
            cur,
            f"INSERT INTO {table} (run_id,observation_digest,generation,command_id,attempt_id,kind,disposition,evidence_bytes,probe_read_id,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                request.run_id,
                observation.digest,
                value.generation,
                value.command_id,
                value.attempt_id,
                value.kind,
                disposition,
                observation.canonical_bytes,
                value.probe_read_id,
                now,
            ),
        )
        if current and command is not None and value.kind == "resume":
            if command.resume_intent_status not in (
                "dispatch_started",
                "unknown",
                "native_observed",
            ):
                raise InteractionConflict("observation_not_unsettled")
            if command.resume_observation_digest not in (None, observation.digest):
                raise InteractionConflict("observation_conflict")
            await execute_sql(
                cur,
                "UPDATE {} SET resume_intent_status='native_observed',resume_observation_digest=%s,updated_at=%s WHERE run_id=%s AND command_id=%s".format(
                    qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                ),
                (observation.digest, now, request.run_id, command.command_id),
            )
        return ObservationStored(
            digest=observation.digest, disposition=disposition, inserted=True
        )

    async def record_checkpoint_observation(
        self, request: RunRequest, lease: LeaseFence, observation: CheckpointObservation
    ) -> ObservationStored:
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            row = await self._run(cur, request)
            command = (
                await self._command(cur, request.run_id, observation.target.command_id)
                if observation.target.command_id is not None
                else None
            )
            return await self._store_observation(
                cur, request, lease, row, observation, command
            )

    async def read_checkpoint_observations(
        self, request: RunRequest, target: ObservationTarget
    ) -> tuple[CheckpointObservation, ...]:
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            await self._run(cur, request)
            if target.command_id is not None:
                command = await self._command(cur, request.run_id, target.command_id)
                if (command.resume_attempt_id, command.resume_attempt_generation) != (
                    target.attempt_id,
                    target.generation,
                ):
                    raise InteractionConflict("attempt_conflict")
            await execute_sql(
                cur,
                "SELECT * FROM {} WHERE run_id=%s AND generation=%s AND command_id IS NOT DISTINCT FROM %s AND attempt_id IS NOT DISTINCT FROM %s ORDER BY observation_digest".format(
                    qualified(self._context.schema, RUN_CHECKPOINT_OBSERVATIONS_TABLE)
                ),
                (
                    request.run_id,
                    target.generation,
                    target.command_id,
                    target.attempt_id,
                ),
            )
            result: list[CheckpointObservation] = []
            for raw in await fetch_all(cur):
                value = CheckpointObservation(
                    target=target,
                    kind=raw["kind"],
                    canonical_bytes=bytes(raw["evidence_bytes"]),
                    digest=raw["observation_digest"],
                )
                decode_checkpoint_observation(value, request)
                result.append(value)
            return tuple(result)

    async def reconcile_resume(
        self,
        request: RunRequest,
        lease: LeaseFence,
        evidence: ConsumedPauseEvidence | UnknownResumeEvidence,
    ) -> InteractionCommitted | ReplayedResume:
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            row = await self._run(cur, request)
            command = await self._command(cur, request.run_id, evidence.command_id)
            snapshot = await self._snapshot(cur, row, request)
            original = _intent(command, request)
            if (
                evidence.attempt_id,
                evidence.attempt_generation,
                evidence.pause_revision,
                evidence.pause_ref,
                evidence.collection_digest,
            ) != (
                command.resume_attempt_id,
                command.resume_attempt_generation,
                command.resume_pause_revision,
                command.resume_pause_ref,
                command.resume_pause_collection_digest,
            ):
                raise InteractionCorrupt("reconcile identity differs")
            if row.terminal or original.status in (
                IntentStatus.RECONCILED,
                IntentStatus.TERMINAL,
            ):
                return ReplayedResume(
                    snapshot=snapshot,
                    original_intent=original,
                    accepted_source_index=command.resume_accepted_source_index,
                )
            if (
                row.interaction_command_id != evidence.command_id
                or evidence.attempt_generation != lease.generation
            ):
                raise InteractionAuthorityLost("reconcile attempt no longer current")
            if (
                not evidence.observation_digests
                or tuple(sorted(set(evidence.observation_digests)))
                != evidence.observation_digests
            ):
                raise InteractionCorrupt("invalid observation references")
            positive = None
            for digest in evidence.observation_digests:
                await execute_sql(
                    cur,
                    "SELECT * FROM {} WHERE run_id=%s AND observation_digest=%s FOR UPDATE".format(
                        qualified(
                            self._context.schema, RUN_CHECKPOINT_OBSERVATIONS_TABLE
                        )
                    ),
                    (request.run_id, digest),
                )
                raw = await fetch_one(cur)
                if raw is None:
                    raise InteractionCorrupt("missing observation")
                observed = CheckpointObservation(
                    target=ObservationTarget(
                        generation=evidence.attempt_generation,
                        command_id=evidence.command_id,
                        attempt_id=evidence.attempt_id,
                    ),
                    kind=raw["kind"],
                    canonical_bytes=bytes(raw["evidence_bytes"]),
                    digest=digest,
                )
                decoded = decode_checkpoint_observation(observed, request)
                self._observation_command(decoded, command, request)
                if raw["disposition"] != "current":
                    raise InteractionAuthorityLost("audit evidence cannot advance")
                if (
                    decoded.kind == "resume"
                    and digest == command.resume_observation_digest
                ):
                    positive = decoded
            now = await self._active(cur, row, lease)
            if isinstance(evidence, UnknownResumeEvidence):
                if original.status not in (
                    IntentStatus.DISPATCH_STARTED,
                    IntentStatus.UNKNOWN,
                ):
                    raise InteractionConflict("not_dispatched")
                if command.resume_result_kind == "unknown":
                    return ReplayedResume(
                        snapshot=snapshot,
                        original_intent=original,
                        accepted_source_index=command.resume_accepted_source_index,
                    )
                state = replace(
                    snapshot.state,
                    intent=replace(original, status=IntentStatus.UNKNOWN),
                    interaction_revision=snapshot.state.interaction_revision + 1,
                )
                result_kind = "unknown"
                pause = snapshot.pause
            else:
                if (
                    positive is None
                    or original.status is not IntentStatus.NATIVE_OBSERVED
                ):
                    raise InteractionCorrupt("missing durable consumption evidence")
                pause = evidence.next_pause
                if (evidence.disposition == "waiting") != (pause is not None):
                    raise InteractionCorrupt("invalid consumption disposition")
                if pause is not None:
                    if (
                        positive.next_pause is None
                        or positive.next_pause_ref != pause.pause_ref
                        or canonical_interaction_bytes(
                            positive.next_pause.model_dump(
                                mode="json", exclude_unset=True
                            )
                        )
                        != pause.canonical_bytes
                    ):
                        raise InteractionCorrupt("successor collection differs")
                    state = snapshot.state.pause(
                        pause_ref=pause.pause_ref,
                        groups=_groups(decode_pause_snapshot(pause)),
                    )
                    result_kind = (
                        "validation_failed"
                        if any(
                            item.validation is not None
                            for group in state.groups
                            for item in group.items
                        )
                        else "native_consumed"
                    )
                else:
                    if positive.next_pause is not None:
                        raise InteractionCorrupt("successor still waiting")
                    state = replace(
                        snapshot.state,
                        phase=Phase.ACTIVE,
                        groups=(),
                        intent=replace(original, status=IntentStatus.RECONCILED),
                        interaction_revision=snapshot.state.interaction_revision + 1,
                    )
                    pause = snapshot.pause
                    result_kind = "native_consumed"
            await execute_sql(
                cur,
                "UPDATE {} SET resume_intent_status=%s,updated_at=%s WHERE run_id=%s AND command_id=%s".format(
                    qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                ),
                (
                    state.intent.status.value if state.intent else None,
                    now,
                    request.run_id,
                    evidence.command_id,
                ),
            )
            result = await self._write(
                cur, request, row, state, pause, now, result_kind=result_kind
            )
            await self._active(cur, row, lease)
            if state.phase is Phase.WAITING:
                await execute_sql(
                    cur,
                    "UPDATE {} SET lease_expires_at=NULL WHERE run_id=%s".format(
                        qualified(self._context.schema, RUN_CLAIMS_TABLE)
                    ),
                    (request.run_id,),
                )
            return InteractionCommitted(snapshot=result)

    async def list_unsettled_interactions(
        self, limit: int
    ) -> tuple[InteractionRecoveryTarget, ...]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("invalid recovery limit")
        async with connect_pg(self._context.database_url) as conn, conn.cursor() as cur:
            await execute_sql(
                cur,
                "SELECT run_id,command_id FROM {} WHERE resume_intent_status IN ('accepted','dispatch_started','native_observed','unknown') ORDER BY run_id,command_id LIMIT %s".format(
                    qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                ),
                (limit,),
            )
            return tuple(
                InteractionRecoveryTarget(
                    run_id=r["run_id"], command_id=r["command_id"]
                )
                for r in await fetch_all(cur)
            )

    async def reset_reconcile_probe(
        self, request: RunRequest, lease: LeaseFence, command_id: str, attempt_id: str
    ) -> None:
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            row = await self._run(cur, request)
            command = await self._command(cur, request.run_id, command_id)
            await self._active(cur, row, lease)
            if (command.resume_attempt_id, command.resume_attempt_generation) != (
                attempt_id,
                lease.generation,
            ):
                raise InteractionConflict("attempt_conflict")
            await execute_sql(
                cur,
                "UPDATE {} SET resume_probe_count=0,resume_probe_progress_digest=NULL,resume_probe_quiescence_json=NULL,resume_probe_last_read_id=NULL,resume_probe_checked_at=NULL WHERE run_id=%s AND command_id=%s".format(
                    qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                ),
                (request.run_id, command_id),
            )

    async def record_reconcile_probe(
        self, request: RunRequest, lease: LeaseFence, probe: QuiescentProbe
    ) -> ReconcileProbeResult:
        value = decode_checkpoint_observation(probe.observation, request)
        quiescence = _Quiescence(
            kind=probe.quiescence.kind,
            worker_boot_id=probe.quiescence.worker_boot_id,
            invocation_id=probe.quiescence.invocation_id,
        )
        if (
            value.kind != "probe"
            or value.command_id is None
            or value.probe_read_id != probe.read_id
            or value.progress_digest != probe.progress_digest
            or value.quiescence != quiescence
        ):
            raise InteractionCorrupt("probe identity differs")
        async with (
            connect_pg(self._context.database_url) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            row = await self._run(cur, request)
            command = await self._command(cur, request.run_id, value.command_id)
            snapshot = await self._snapshot(cur, row, request)
            if (
                command.resume_intent_status not in ("dispatch_started", "unknown")
                or row.interaction_command_id != value.command_id
            ):
                raise InteractionConflict("probe_not_unsettled")
            await self._active(cur, row, lease)
            stored = await self._store_observation(
                cur, request, lease, row, probe.observation, command
            )
            if not stored.inserted:
                return ReconcileProbeResult(
                    count=command.resume_probe_count,
                    exhausted=command.resume_probe_count == 3,
                    snapshot=snapshot,
                )
            now = await self._active(cur, row, lease)
            q = quiescence.model_dump(mode="json")
            count = (
                1
                if command.resume_probe_progress_digest is None
                else min(3, command.resume_probe_count + 1)
                if (
                    command.resume_probe_progress_digest,
                    command.resume_probe_quiescence_json,
                )
                == (probe.progress_digest, q)
                else 0
            )
            await execute_sql(
                cur,
                "UPDATE {} SET resume_probe_count=%s,resume_probe_progress_digest=%s,resume_probe_quiescence_json=%s::jsonb,resume_probe_last_read_id=%s,resume_probe_checked_at=%s WHERE run_id=%s AND command_id=%s".format(
                    qualified(self._context.schema, RUN_CONTROL_COMMANDS_TABLE)
                ),
                (
                    count,
                    probe.progress_digest,
                    canonical_interaction_bytes(q).decode(),
                    probe.read_id,
                    now,
                    request.run_id,
                    value.command_id,
                ),
            )
            return ReconcileProbeResult(
                count=count, exhausted=count == 3, snapshot=snapshot
            )
