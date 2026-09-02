"""Allowlisted projection from execution payloads to user-visible chat facts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue

from kokoro_agent.chat.models import (
    ChatEventDraft,
    ChatMessageDraft,
    ChatProjection,
    assistant_message_id,
)
from kokoro_agent.contract import (
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
)


class _Payload(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class _RunPhase(_Payload):
    status: Literal["running"]


class _AssistantDelta(_Payload):
    delta: str


class _AssistantCompleted(_Payload):
    content: str


class _Activity(_Payload):
    activity: Literal["tool", "subagent"]
    segment_id: str
    tool_id: str | None = None
    subagent_id: str | None = None
    name: str
    status: Literal["started", "completed", "failed"]
    description: str | None = None
    subagent_type: str | None = None
    source: str | None = None
    args: dict[str, JsonValue] | None = None
    result: str | None = None
    is_error: bool | None = None
    truncated: bool | None = None
    error: str | None = None


class _Interaction(_Payload):
    interaction_id: str
    segment_id: str
    tool_id: str
    name: str
    kind: str
    description: str
    allowed_decisions: list[str]
    args: dict[str, JsonValue]
    editable: bool
    pending_tool_ids: list[str]
    risk: dict[str, JsonValue] | None = None
    input_schema: dict[str, JsonValue] | None = None
    result: str | None = None


class _Delivery(_Payload):
    path: str
    title: str
    mime: str
    size: int
    content_hash: str
    note: str | None = None


class _Terminal(_Payload):
    status: Literal["completed", "cancelled", "failed"]
    code: str | None = None
    error_kind: str | None = None
    message: str | None = None
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
    namespace: str,
    session_id: str,
    run_id: str,
    source_index: int,
    timestamp: int,
    payload: ProjectablePayload,
) -> ChatProjection | None:
    """Project only allowlisted product semantics; unknown/private payloads disappear."""

    event_type: str
    chat_message_id: str | None = None
    safe_payload: _Payload
    message: ChatMessageDraft | None = None
    if isinstance(payload, RunStartedPayload):
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
            namespace=namespace,
            session_id=session_id,
            run_id=run_id,
            role="assistant",
            content=payload.content,
            status="completed",
            created_at=timestamp,
            updated_at=timestamp,
        )
    elif isinstance(payload, ToolInvokedPayload):
        event_type = "activity"
        safe_payload = _Activity(
            activity="tool",
            segment_id=payload.segment_id,
            tool_id=payload.tool_id,
            name=payload.name,
            status="started",
        )
    elif isinstance(payload, ToolReturnedPayload):
        event_type = "activity"
        safe_payload = _Activity(
            activity="tool",
            segment_id=payload.segment_id,
            tool_id=payload.tool_id,
            name=payload.name,
            status="failed" if payload.is_error else "completed",
            result=payload.result,
            is_error=payload.is_error,
            truncated=payload.truncated,
        )
    elif isinstance(payload, ToolAwaitingApprovalPayload):
        event_type = "interaction"
        safe_payload = _Interaction(
            interaction_id=payload.tool_id,
            segment_id=payload.segment_id,
            tool_id=payload.tool_id,
            name=payload.name,
            kind=payload.kind,
            description=payload.description,
            allowed_decisions=list(payload.allowed_decisions),
            # Tool arguments are intentionally not persisted in the chat
            # projection.  Approval UI gets the redacted public shape; raw
            # arguments remain in the execution-owned event stream.
            args={},
            editable=payload.editable,
            pending_tool_ids=list(payload.pending_tool_ids),
            risk=None if payload.risk is None else payload.risk.model_dump(mode="json"),
            input_schema=payload.input_schema,
            result=payload.result,
        )
    elif isinstance(payload, SubagentStartedPayload):
        event_type = "activity"
        safe_payload = _Activity(
            activity="subagent",
            segment_id=payload.segment_id,
            subagent_id=payload.subagent_id,
            name=payload.name,
            status="started",
            description=payload.description,
            subagent_type=payload.subagent_type,
            source=payload.source,
        )
    elif isinstance(payload, SubagentFinishedPayload):
        event_type = "activity"
        safe_payload = _Activity(
            activity="subagent",
            segment_id=payload.segment_id,
            subagent_id=payload.subagent_id,
            name=payload.name,
            status="failed" if payload.failed else "completed",
            subagent_type=payload.subagent_type,
            source=payload.source,
            error=payload.error,
        )
    elif isinstance(payload, DeliveryCreatedPayload):
        event_type = "delivery"
        safe_payload = _Delivery(**payload.model_dump())
    elif isinstance(payload, RunCompletedPayload):
        event_type = "run.completed"
        safe_payload = _Terminal(
            status=payload.status,
            token_usage=None if payload.token_usage is None else payload.token_usage.model_dump(mode="json"),
        )
    elif isinstance(payload, RunFailedPayload):
        event_type = "run.failed"
        # Keep diagnostics out of the durable chat projection.  The stable
        # code is enough for the Web error catalogue; detailed exception
        # fields remain in Agent-owned execution logs.
        safe_payload = _Terminal(status="failed", code=payload.code)
    else:
        return None
    return ChatProjection(
        event=ChatEventDraft(
            namespace=namespace,
            session_id=session_id,
            run_id=run_id,
            source_index=source_index,
            chat_message_id=chat_message_id,
            event_type=event_type,
            payload_json=safe_payload.model_dump_json(exclude_none=True),
            created_at=timestamp,
        ),
        message=message,
    )


__all__ = ["project_chat_fact"]
