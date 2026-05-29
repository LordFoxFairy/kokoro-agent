from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, TypedDict
from uuid import uuid4

EventName = Literal[
    "session.created",
    "message.delta",
    "message.completed",
    "run.completed",
    "run.failed",
]


class SessionEvent(TypedDict):
    event: EventName
    event_id: str
    session_id: str
    conversation_id: str
    run_id: str
    cursor: str
    timestamp: str
    payload: dict[str, object]


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _event_id() -> str:
    return f"evt_{uuid4().hex[:12]}"


def _cursor(run_id: str, sequence: int) -> str:
    return f"{run_id}:{sequence:04d}"


# 统一输出协议 envelope，避免 session 层再猜字段语义。
def _envelope(
    *,
    event: EventName,
    session_id: str,
    conversation_id: str,
    run_id: str,
    sequence: int,
    payload: dict[str, object],
) -> SessionEvent:
    return {
        "event": event,
        "event_id": _event_id(),
        "session_id": session_id,
        "conversation_id": conversation_id,
        "run_id": run_id,
        "cursor": _cursor(run_id, sequence),
        "timestamp": _timestamp(),
        "payload": payload,
    }


def session_created(
    *,
    session_id: str,
    conversation_id: str,
    run_id: str,
    sequence: int,
    title: str,
    owner_id: str,
) -> SessionEvent:
    return _envelope(
        event="session.created",
        session_id=session_id,
        conversation_id=conversation_id,
        run_id=run_id,
        sequence=sequence,
        payload={
            "session_id": session_id,
            "conversation_id": conversation_id,
            "owner_id": owner_id,
            "title": title,
        },
    )


def message_delta(
    *,
    session_id: str,
    conversation_id: str,
    run_id: str,
    sequence: int,
    message_id: str,
    delta: str,
    role: Literal["assistant", "user"],
) -> SessionEvent:
    return _envelope(
        event="message.delta",
        session_id=session_id,
        conversation_id=conversation_id,
        run_id=run_id,
        sequence=sequence,
        payload={
            "message_id": message_id,
            "delta": delta,
            "role": role,
        },
    )


def message_completed(
    *,
    session_id: str,
    conversation_id: str,
    run_id: str,
    sequence: int,
    message_id: str,
    content: str,
    role: Literal["assistant", "user"],
) -> SessionEvent:
    return _envelope(
        event="message.completed",
        session_id=session_id,
        conversation_id=conversation_id,
        run_id=run_id,
        sequence=sequence,
        payload={
            "message_id": message_id,
            "content": content,
            "role": role,
        },
    )


def run_completed(
    *,
    session_id: str,
    conversation_id: str,
    run_id: str,
    sequence: int,
    final_message_id: str,
) -> SessionEvent:
    return _envelope(
        event="run.completed",
        session_id=session_id,
        conversation_id=conversation_id,
        run_id=run_id,
        sequence=sequence,
        payload={
            "run_id": run_id,
            "status": "completed",
            "final_message_id": final_message_id,
        },
    )


def run_failed(
    *,
    session_id: str,
    conversation_id: str,
    run_id: str,
    sequence: int,
    error_kind: str,
    message: str,
    retryable: bool = False,
) -> SessionEvent:
    return _envelope(
        event="run.failed",
        session_id=session_id,
        conversation_id=conversation_id,
        run_id=run_id,
        sequence=sequence,
        payload={
            "run_id": run_id,
            "error_kind": error_kind,
            "message": message,
            "retryable": retryable,
        },
    )
