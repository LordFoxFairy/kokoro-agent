"""Plan raw Agent owner facts into ordered Presentation Submission batches."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence

from kokoro_agent.contract import (
    AgentEvent,
    MessageCompleted,
    MessageDelta,
    PlanProposed,
    RunCompleted,
    RunFailed,
    RunStarted,
    SubagentFinished,
    SubagentStarted,
    SubagentToolInvoked,
    SubagentToolReturned,
    ToolAwaitingApproval,
    ToolInvoked,
    ToolReturned,
)
from kokoro_agent.presentation.adapters.ag_ui import (
    OfficialEvent,
    build_submission,
    make_activity_snapshot,
    make_run_error,
    make_run_finished,
    make_run_started,
    make_text_content,
    make_text_end,
    make_text_start,
)
from kokoro_agent.presentation.model import (
    MAX_UINT64_DECIMAL,
    PresentationDecisionGroupState,
    PresentationMessageState,
    PresentationOwnerState,
    PresentationState,
    PresentationSubmission,
    SubmissionBatch,
    SubmissionRoute,
    SubmissionSource,
    agent_thread_ref,
    activity_message_ref,
    fingerprint,
    derive_message_ref,
    private_ref,
    canonical_recorded_at,
)

def _increment_uint64_decimal(value: str) -> str:
    if value == MAX_UINT64_DECIMAL:
        raise ValueError("PRESENTATION_OWNER_VERSION_OVERFLOW")
    digits = list(value)
    carry = 1
    for index in range(len(digits) - 1, -1, -1):
        next_digit = ord(digits[index]) - ord("0") + carry
        digits[index] = chr(ord("0") + next_digit % 10)
        carry = next_digit // 10
        if carry == 0:
            break
    if carry:
        digits.insert(0, "1")
    return "".join(digits)


def _owner_terminal_state(activity_type: str, content: dict[str, object]) -> str | None:
    status = content.get("status")
    if activity_type == "kokoro.safe-summary.v1":
        return None if status == "streaming" else str(status)
    if activity_type in {
        "kokoro.tool-preview.v1",
        "kokoro.plan.v1",
        "kokoro.subagent.v1",
    }:
        return str(status) if status in {"completed", "failed", "canceled"} else None
    if activity_type in {"kokoro.notice.v1", "kokoro.error.v1"}:
        return "terminal"
    return None


def _plan_owner_activity(
    *,
    run_id: str,
    timestamp: int,
    activity_type: str,
    raw_owner_key: str,
    owner_identity: dict[str, object],
    semantic_content: dict[str, object],
    owners: dict[str, PresentationOwnerState],
) -> tuple[OfficialEvent | None, str]:
    owner_key = private_ref("presentation-owner", activity_type, raw_owner_key)
    message_ref = activity_message_ref(run_id, activity_type, owner_key)
    identity_fingerprint = fingerprint(
        "kokoro-agent-presentation-owner-identity-v1",
        {"activityType": activity_type, **owner_identity},
    )
    semantic_fingerprint = fingerprint(
        "kokoro-agent-presentation-owner-semantic-v1",
        {"activityType": activity_type, **semantic_content},
    )
    updated_at = canonical_recorded_at(timestamp)
    current = owners.get(owner_key)
    if current is None:
        owner_version = "1"
    else:
        if current.activity_type != activity_type or current.identity_fingerprint != identity_fingerprint:
            raise ValueError("PRESENTATION_OWNER_IDENTITY_CONFLICT")
        if current.message_ref != message_ref:
            raise ValueError("PRESENTATION_OWNER_PLACEMENT_CONFLICT")
        if updated_at < current.updated_at:
            raise ValueError("PRESENTATION_OWNER_TIME_REGRESSION")
        if current.semantic_fingerprint == semantic_fingerprint:
            return None, message_ref
        if current.terminal_state is not None:
            raise ValueError("PRESENTATION_OWNER_TERMINAL")
        owner_version = _increment_uint64_decimal(current.owner_version)
    terminal_state = _owner_terminal_state(activity_type, semantic_content)
    owners[owner_key] = PresentationOwnerState(
        owner_key=owner_key,
        activity_type=activity_type,
        message_ref=message_ref,
        identity_fingerprint=identity_fingerprint,
        semantic_fingerprint=semantic_fingerprint,
        owner_version=owner_version,
        updated_at=updated_at,
        terminal_state=terminal_state,
    )
    return (
        _activity(
            message_id=message_ref,
            timestamp=timestamp,
            activity_type=activity_type,
            content={
                **semantic_content,
                "ownerVersion": owner_version,
                "updatedAt": updated_at,
            },
        ),
        message_ref,
    )


def _hitl_group(
    event: ToolAwaitingApproval,
    groups: dict[str, PresentationDecisionGroupState],
) -> tuple[PresentationDecisionGroupState, str]:
    pending_tool_ids = tuple(event.payload.pending_tool_ids)
    if (
        not pending_tool_ids
        or len(pending_tool_ids) != len(set(pending_tool_ids))
        or event.payload.tool_id not in pending_tool_ids
    ):
        raise ValueError("PRESENTATION_HITL_GROUP_INVALID")
    group_key = private_ref(
        "decision-group-key",
        event.run_id,
        event.payload.segment_id,
        event.payload.kind,
        *pending_tool_ids,
    )
    decision_group_ref = private_ref("decision-group", group_key)
    control_ref = private_ref("control-proposal", group_key)
    owner_refs = tuple(
        private_ref("hitl-owner", group_key, tool_id) for tool_id in pending_tool_ids
    )
    expected = PresentationDecisionGroupState(
        group_key=group_key,
        decision_group_ref=decision_group_ref,
        control_ref=control_ref,
        required_owner_refs=owner_refs,
    )
    current = groups.get(group_key)
    if current is not None and current != expected:
        raise ValueError("PRESENTATION_HITL_GROUP_CONFLICT")
    groups[group_key] = expected
    return expected, owner_refs[pending_tool_ids.index(event.payload.tool_id)]


def _safe_ref(value: str, *, domain: str) -> str:
    if (
        1 <= len(value) <= 128
        and value[0].isalnum()
        and all(char.isalnum() or char in "._:-" for char in value)
    ):
        return value
    return f"agent.{domain}:{hashlib.sha256(value.encode()).hexdigest()}"


def _clip(value: str, maximum: int = 16_384) -> str:
    if len(value) <= maximum:
        return value
    return value[: maximum - 1] + "…"


def _presentation_actions(actions: Sequence[str]) -> list[str]:
    projected = ("respond" if action == "submit" else action for action in actions)
    return list(dict.fromkeys(projected))


def _tool_activity_identity(
    event: ToolInvoked | ToolReturned | SubagentToolInvoked | SubagentToolReturned,
) -> tuple[str, str]:
    if isinstance(event, SubagentToolInvoked | SubagentToolReturned):
        return (
            f"subagent\0{event.payload.subagent_id}\0{event.payload.tool_id}",
            private_ref(
                "subagent-tool-call",
                event.payload.subagent_id,
                event.payload.tool_id,
            ),
        )
    return f"main\0{event.payload.tool_id}", _safe_ref(
        event.payload.tool_id, domain="tool"
    )


def _source_event_ref(event: AgentEvent) -> str:
    stable = event.event_id or f"index:{event.index}"
    material = f"v1\0{event.run_id}\0{event.kind}\0{stable}".encode()
    return f"agent.source:{hashlib.sha256(material).hexdigest()}"


def _submission_source_ref(source_event_ref: str, member_ordinal: int) -> str:
    material = f"v1\0{source_event_ref}\0{member_ordinal}".encode()
    return f"agent.presentation.source:{hashlib.sha256(material).hexdigest()}"


def _event_payload_digest(event: AgentEvent) -> str:
    encoded = json.dumps(
        event.model_dump(mode="json", exclude_none=True),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _message_state(
    states: dict[str, PresentationMessageState], run_id: str, segment_ref: str
) -> PresentationMessageState | None:
    return states.get(derive_message_ref(run_id, segment_ref))


def _activity(
    *, message_id: str, timestamp: int, activity_type: str, content: dict[str, object]
) -> OfficialEvent:
    return make_activity_snapshot(
        message_id=message_id,
        activity_type=activity_type,
        content=content,
        timestamp=timestamp,
    )


def _events_for_source(
    event: AgentEvent,
    state: PresentationState,
) -> tuple[tuple[tuple[OfficialEvent, str | None], ...], PresentationState]:
    if state.run_state in {"finished", "failed"}:
        raise ValueError("PRESENTATION_RUN_TERMINAL")
    presentation_scoped = isinstance(
        event,
        (
            MessageDelta,
            MessageCompleted,
            ToolInvoked,
            ToolReturned,
            ToolAwaitingApproval,
            PlanProposed,
            SubagentStarted,
            SubagentFinished,
            SubagentToolInvoked,
            SubagentToolReturned,
        ),
    )
    if state.run_state == "new" and presentation_scoped:
        raise ValueError("PRESENTATION_RUN_START_REQUIRED")
    if state.run_state == "running" and isinstance(event, RunStarted):
        raise ValueError("PRESENTATION_RUN_ALREADY_STARTED")

    states = {message.internal_message_ref: message for message in state.messages}
    owners = {owner.owner_key: owner for owner in state.owners}
    groups = {group.group_key: group for group in state.decision_groups}
    planned: list[tuple[OfficialEvent, str | None]] = []
    next_run_state = state.run_state
    thread_ref = state.internal_thread_ref
    if thread_ref is None:
        raise ValueError("PRESENTATION_THREAD_SCOPE_INVALID")

    if isinstance(event, RunStarted):
        planned.append(
            (
                make_run_started(
                    thread_id=thread_ref,
                    run_id=event.run_id,
                    timestamp=event.timestamp,
                ),
                None,
            )
        )
        next_run_state = "running"
    elif isinstance(event, MessageDelta | MessageCompleted):
        segment_ref = event.payload.segment_id
        message_ref = derive_message_ref(event.run_id, segment_ref)
        message = _message_state(states, event.run_id, segment_ref)
        if message is not None and message.state == "closed":
            raise ValueError("PRESENTATION_MESSAGE_CLOSED")
        if message is None:
            planned.append(
                (
                    make_text_start(
                        message_id=message_ref,
                        role="assistant",
                        timestamp=event.timestamp,
                    ),
                    message_ref,
                )
            )
            message = PresentationMessageState(
                internal_message_ref=message_ref,
                source_segment_ref=segment_ref,
                state="open",
                opened_ordinal=state.next_ordinal,
            )
        text = event.payload.delta if isinstance(event, MessageDelta) else event.payload.content
        if text and (isinstance(event, MessageDelta) or not message.text_seen):
            for offset in range(0, len(text), 16_384):
                planned.append(
                    (
                        make_text_content(
                            message_id=message_ref,
                            delta=text[offset : offset + 16_384],
                            timestamp=event.timestamp,
                        ),
                        message_ref,
                    )
                )
            message = message.model_copy(update={"text_seen": True})
        if isinstance(event, MessageCompleted):
            planned.append(
                (
                    make_text_end(
                        message_id=message_ref, timestamp=event.timestamp
                    ),
                    message_ref,
                )
            )
            message = message.model_copy(update={"state": "closed"})
        states[message_ref] = message
    elif isinstance(event, ToolInvoked | SubagentToolInvoked):
        raw_owner_key, tool_call_ref = _tool_activity_identity(event)
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.tool-preview.v1",
            raw_owner_key=raw_owner_key,
            owner_identity={"toolCallRef": tool_call_ref},
            semantic_content={
                "toolCallRef": tool_call_ref,
                "label": _clip(event.payload.name, 1_024),
                "status": "running",
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, ToolReturned | SubagentToolReturned):
        raw_owner_key, tool_call_ref = _tool_activity_identity(event)
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.tool-preview.v1",
            raw_owner_key=raw_owner_key,
            owner_identity={"toolCallRef": tool_call_ref},
            semantic_content={
                "toolCallRef": tool_call_ref,
                "label": _clip(event.payload.name, 1_024),
                "status": "failed" if event.payload.is_error else "completed",
                "isError": event.payload.is_error,
                **({"truncated": True} if event.payload.truncated else {}),
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, ToolAwaitingApproval):
        group, owner_ref = _hitl_group(event, groups)
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.hitl.v1",
            raw_owner_key=f"{group.group_key}\0{event.payload.tool_id}",
            owner_identity={
                "ownerRef": owner_ref,
                "decisionGroupRef": group.decision_group_ref,
                "requiredOwnerRefs": group.required_owner_refs,
                "controlRef": group.control_ref,
            },
            semantic_content={
                "ownerRef": owner_ref,
                "decisionGroupRef": group.decision_group_ref,
                "requiredOwnerRefs": list(group.required_owner_refs),
                "controlRef": group.control_ref,
                "kind": (
                    "approval"
                    if event.payload.kind == "tool_approval"
                    else "interaction"
                ),
                "title": _clip(event.payload.name, 1_024),
                "description": _clip(event.payload.description),
                "allowedActions": _presentation_actions(event.payload.allowed_decisions),
                "status": "pending",
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, PlanProposed):
        plan_ref = _safe_ref(event.payload.owner_ref, domain="plan")
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.plan.v1",
            raw_owner_key=event.payload.owner_ref,
            owner_identity={"planRef": plan_ref},
            semantic_content={
                "planRef": plan_ref,
                "summary": _clip(event.payload.proposal.summary),
                "status": "proposed",
                "steps": [
                    {
                        "stepRef": _safe_ref(step.step_ref, domain="plan.step"),
                        "label": _clip(step.label, 1_024),
                        "status": step.status.replace("_", "-"),
                    }
                    for step in event.payload.proposal.steps[:256]
                ],
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, SubagentStarted | SubagentFinished):
        subagent_ref = _safe_ref(event.payload.subagent_id, domain="subagent")
        activity, message_ref = _plan_owner_activity(
            run_id=event.run_id,
            timestamp=event.timestamp,
            activity_type="kokoro.subagent.v1",
            raw_owner_key=event.payload.subagent_id,
            owner_identity={"subagentRef": subagent_ref},
            semantic_content={
                "subagentRef": subagent_ref,
                "status": (
                    "failed"
                    if isinstance(event, SubagentFinished) and event.payload.failed
                    else "completed"
                    if isinstance(event, SubagentFinished)
                    else "running"
                ),
            },
            owners=owners,
        )
        if activity is not None:
            planned.append((activity, message_ref))
    elif isinstance(event, RunCompleted | RunFailed):
        if state.run_state == "new":
            planned.append(
                (
                    make_run_started(
                        thread_id=thread_ref,
                        run_id=event.run_id,
                        timestamp=event.timestamp,
                    ),
                    None,
                )
            )
        for message in sorted(states.values(), key=lambda item: item.opened_ordinal):
            if message.state == "open":
                planned.append(
                    (
                        make_text_end(
                            message_id=message.internal_message_ref,
                            timestamp=event.timestamp,
                        ),
                        message.internal_message_ref,
                    )
                )
                states[message.internal_message_ref] = message.model_copy(
                    update={"state": "closed"}
                )
        if isinstance(event, RunCompleted) and event.payload.status == "completed":
            planned.append(
                (
                    make_run_finished(
                        thread_id=thread_ref,
                        run_id=event.run_id,
                        timestamp=event.timestamp,
                        outcome=None,
                    ),
                    None,
                )
            )
            next_run_state = "finished"
        else:
            message = (
                "The agent run failed."
                if isinstance(event, RunFailed)
                else "Run cancelled."
            )
            code = (
                event.payload.code if isinstance(event, RunFailed) else "run_cancelled"
            )
            planned.append(
                (
                    make_run_error(
                        message=_clip(message), code=code, timestamp=event.timestamp
                    ),
                    None,
                )
            )
            next_run_state = "failed"

    next_state = PresentationState(
        internal_run_ref=state.internal_run_ref,
        internal_thread_ref=state.internal_thread_ref,
        run_state=next_run_state,
        next_ordinal=state.next_ordinal + len(planned),
        messages=tuple(sorted(states.values(), key=lambda item: item.opened_ordinal)),
        owners=tuple(sorted(owners.values(), key=lambda item: item.owner_key)),
        decision_groups=tuple(sorted(groups.values(), key=lambda item: item.group_key)),
    )
    return tuple(planned), next_state


def plan_presentation_batch(
    event: AgentEvent,
    state: PresentationState,
    agent_thread_ref: str,
) -> SubmissionBatch:
    """Plan one complete source batch; persistence must commit the batch and state together."""

    if not agent_thread_ref.startswith("agent.thread:"):
        raise ValueError("PRESENTATION_THREAD_SCOPE_INVALID")
    if state.run_state == "new":
        state = state.model_copy(
            update={
                "internal_run_ref": event.run_id,
                "internal_thread_ref": agent_thread_ref,
            }
        )
    elif (
        state.internal_run_ref != event.run_id
        or state.internal_thread_ref != agent_thread_ref
    ):
        raise ValueError("PRESENTATION_SCOPE_CONFLICT")

    source_event_ref = _source_event_ref(event)
    planned, next_state = _events_for_source(event, state)
    submissions: list[PresentationSubmission] = []
    for member, (official, message_ref) in enumerate(planned):
        source = SubmissionSource(
            source_event_ref=_submission_source_ref(source_event_ref, member),
            event_ordinal=str(state.next_ordinal + member),
            recorded_at=canonical_recorded_at(event.timestamp),
            route=SubmissionRoute(
                internal_run_ref=event.run_id,
                internal_thread_ref=agent_thread_ref,
                **(
                    {}
                    if message_ref is None
                    else {"internal_message_ref": message_ref}
                ),
            ),
        )
        submissions.append(build_submission(official, source=source))
    return SubmissionBatch(
        source_event_ref=source_event_ref,
        source_payload_sha256=_event_payload_digest(event),
        submissions=tuple(submissions),
        next_state=next_state,
    )

__all__ = ["agent_thread_ref", "plan_presentation_batch"]
