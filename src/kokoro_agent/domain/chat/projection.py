"""Allowlisted projection from execution payloads to user-visible chat facts."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue

from kokoro_agent.domain.chat.models import (
    ChatEventDraft,
    ChatMessageDraft,
    ChatProjection,
    assistant_message_id,
)
from kokoro_agent.protocol import (
    ArtifactKind,
    ChatFailure,
    DeliveryCreatedPayload,
    MessageCompletedPayload,
    MessageDeltaPayload,
    RunCompletedPayload,
    RunFailedPayload,
    RunStartedPayload,
    SubagentFinishedPayload,
    SubagentStartedPayload,
    ToolAwaitingApprovalPayload,
    ToolInvokedPayload,
    ToolReturnedPayload,
    TodoUpdatedPayload,
    SkillProgressPayload,
)


class _Payload(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class _RunPhase(_Payload):
    status: Literal["running"]


class _AssistantDelta(_Payload):
    delta: str


class _AssistantCompleted(_Payload):
    content: str


class _ToolActivity(_Payload):
    activity: Literal["tool"] = "tool"
    activity_id: str
    segment_id: str
    status: Literal["running", "completed", "failed"]
    display_code: Literal["tool.execution"] = "tool.execution"


class _SubagentActivity(_Payload):
    activity: Literal["subagent"] = "subagent"
    activity_id: str
    segment_id: str
    status: Literal["running", "completed", "failed"]
    display_code: Literal["subagent.execution"] = "subagent.execution"


class _SkillActivity(_Payload):
    activity: Literal["skill"] = "skill"
    activity_id: str
    preflight_id: str
    source_refs: list[str]
    phase: Literal["resolving", "loading", "ready", "failed"]
    error_code: Literal["skill_resolve_failed", "skill_load_failed"] | None = None


class _Delivery(_Payload):
    tool_call_id: str
    artifact_id: str
    asset_id: str
    artifact_kind: ArtifactKind
    path: str
    title: str
    mime: str
    size: int
    content_hash: str
    note: str | None = None


class _Terminal(_Payload):
    status: Literal["completed", "cancelled"]
    token_usage: dict[str, JsonValue] | None = None


ProjectablePayload = (
    RunStartedPayload
    | MessageDeltaPayload
    | MessageCompletedPayload
    | ToolInvokedPayload
    | ToolReturnedPayload
    | ToolAwaitingApprovalPayload
    | SubagentStartedPayload
    | SubagentFinishedPayload
    | DeliveryCreatedPayload
    | RunCompletedPayload
    | RunFailedPayload
    | BaseModel
)


def project_chat_fact(
    *,
    tenant_id: str,
    namespace: str,
    session_id: str,
    run_id: str,
    source_index: int,
    created_at: datetime,
    payload: ProjectablePayload,
) -> ChatProjection | None:
    """Project only allowlisted product semantics; unknown/private payloads disappear."""

    event_type: str
    chat_message_id: str | None = None
    safe_payload: _Payload | ChatFailure | TodoUpdatedPayload
    message: ChatMessageDraft | None = None

    def opaque(tag: str, *parts: str) -> str:
        value = [
            "kokoro-agent.safe-progress.v1",
            tag,
            tenant_id,
            namespace,
            session_id,
            run_id,
            *parts,
        ]
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")
        return hashlib.sha256(encoded).hexdigest()

    if isinstance(payload, TodoUpdatedPayload):
        event_type = "todo.updated"
        safe_payload = TodoUpdatedPayload.model_validate(
            payload.model_dump(mode="json")
        )
    elif isinstance(payload, SkillProgressPayload):
        event_type = "activity"
        safe_payload = _SkillActivity(
            activity_id="act_" + opaque("skill-activity"),
            preflight_id="spf_"
            + opaque("skill-preflight", str(payload.anchor_source_index)),
            source_refs=payload.source_refs,
            phase=payload.phase,
            error_code=payload.error_code,
        )
    elif isinstance(payload, RunStartedPayload):
        event_type = "run.started"
        safe_payload = _RunPhase(status="running")
    elif isinstance(payload, MessageDeltaPayload):
        event_type = "assistant.delta"
        chat_message_id = assistant_message_id(namespace, run_id, payload.segment_id)
        safe_payload = _AssistantDelta(delta=payload.delta)
    elif isinstance(payload, MessageCompletedPayload):
        event_type = "assistant.completed"
        chat_message_id = assistant_message_id(namespace, run_id, payload.segment_id)
        safe_payload = _AssistantCompleted(content=payload.content)
        message = ChatMessageDraft(
            chat_message_id=chat_message_id,
            tenant_id=tenant_id,
            namespace=namespace,
            session_id=session_id,
            run_id=run_id,
            role="assistant",
            content=payload.content,
            status="completed",
            created_at=created_at,
            updated_at=created_at,
        )
    elif isinstance(payload, ToolInvokedPayload | ToolReturnedPayload):
        event_type = "activity"
        safe_payload = _ToolActivity(
            activity_id="act_"
            + opaque("tool-activity", payload.segment_id, payload.tool_id),
            segment_id="seg_" + opaque("segment", payload.segment_id),
            status="running"
            if isinstance(payload, ToolInvokedPayload)
            else "failed"
            if payload.is_error
            else "completed",
        )
    elif isinstance(payload, ToolAwaitingApprovalPayload):
        # Partial execution notifications cannot supply the committed collection,
        # pause identity or revision. Only the Run transaction writes this source.
        raise ValueError("interaction source requires a complete durable pause")
    elif isinstance(payload, SubagentStartedPayload | SubagentFinishedPayload):
        event_type = "activity"
        safe_payload = _SubagentActivity(
            activity_id="act_"
            + opaque("subagent-activity", payload.segment_id, payload.subagent_id),
            segment_id="seg_" + opaque("segment", payload.segment_id),
            status="running"
            if isinstance(payload, SubagentStartedPayload)
            else "failed"
            if payload.failed
            else "completed",
        )
    elif isinstance(payload, DeliveryCreatedPayload):
        event_type = "delivery"
        safe_payload = _Delivery(**payload.model_dump())
    elif isinstance(payload, RunCompletedPayload):
        event_type = "run.completed"
        safe_payload = _Terminal(
            status=payload.status,
            token_usage=None
            if payload.token_usage is None
            else payload.token_usage.model_dump(mode="json"),
        )
    elif isinstance(payload, RunFailedPayload):
        event_type = "run.failed"
        safe_payload = ChatFailure(
            status="failed", code=payload.code, retryable=payload.retryable
        )
    else:
        return None
    return ChatProjection(
        event=ChatEventDraft(
            tenant_id=tenant_id,
            namespace=namespace,
            session_id=session_id,
            run_id=run_id,
            source_index=source_index,
            chat_message_id=chat_message_id,
            event_type=event_type,
            payload_json=safe_payload.canonical_bytes().decode("utf-8")
            if isinstance(safe_payload, TodoUpdatedPayload)
            else safe_payload.model_dump_json(exclude_none=True),
            created_at=created_at,
        ),
        message=message,
    )


__all__ = ["project_chat_fact"]
