from __future__ import annotations

import pytest
from pydantic import ValidationError

from kokoro_agent.events import AgentEvent, RunRequest


def test_run_request_requires_input() -> None:
    with pytest.raises(ValidationError):
        RunRequest(
            kind="run.request",
            run_id="run_01",
            session_id="ses_01",
            conversation_id="conv_01",
        )  # type: ignore[call-arg]


def test_run_request_defaults_execution_style_fast() -> None:
    req = RunRequest(
        kind="run.request",
        run_id="run_01",
        session_id="ses_01",
        conversation_id="conv_01",
        input="hello",
    )
    assert req.execution_style == "fast"


def test_run_request_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        RunRequest(
            kind="run.request",
            run_id="run_01",
            session_id="ses_01",
            conversation_id="conv_01",
            input="hello",
            owner_id="kokoro-agent",  # type: ignore[call-arg]
        )


def test_run_request_strict_rejects_coerced_input() -> None:
    with pytest.raises(ValidationError):
        RunRequest(
            kind="run.request",
            run_id="run_01",
            session_id="ses_01",
            conversation_id="conv_01",
            input=123,  # type: ignore[arg-type]
        )


def test_agent_event_requires_seq() -> None:
    with pytest.raises(ValidationError):
        AgentEvent(
            kind="run.started",
            run_id="run_01",
            payload={},
        )  # type: ignore[call-arg]


def test_agent_event_rejects_unknown_kind() -> None:
    with pytest.raises(ValidationError):
        AgentEvent(
            kind="session.created",  # type: ignore[arg-type]
            run_id="run_01",
            seq=1,
            payload={},
        )


def test_text_delta_roundtrip() -> None:
    event = AgentEvent(
        kind="text.delta",
        run_id="run_01",
        seq=2,
        payload={"message_ref": "m1", "text": "hello"},
    )
    dumped = event.model_dump()
    assert dumped == {
        "kind": "text.delta",
        "run_id": "run_01",
        "seq": 2,
        "payload": {"message_ref": "m1", "text": "hello"},
    }
    restored = AgentEvent.model_validate(dumped)
    assert restored == event


def test_agent_event_forbids_extra_fields() -> None:
    # extra=forbid: a stray key (e.g. a leaked envelope field) must be rejected,
    # not silently absorbed — the agent never assigns event_id/cursor/owner_id.
    with pytest.raises(ValidationError):
        AgentEvent(
            kind="text.delta",
            run_id="run_01",
            seq=2,
            payload={"message_ref": "m1", "text": "hi"},
            event_id="evt_01",  # type: ignore[call-arg]
        )


def test_agent_event_strict_rejects_coerced_seq() -> None:
    # strict mode: seq is a monotonic int; a numeric string must NOT coerce.
    with pytest.raises(ValidationError):
        AgentEvent(
            kind="text.delta",
            run_id="run_01",
            seq="2",  # type: ignore[arg-type]
            payload={"message_ref": "m1", "text": "hi"},
        )


# Activity event families added for the agent-activity goal: every new kind must
# be accepted with its documented payload (thinking/tool/todo/subagent).
@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        ("thinking.delta", {"message_ref": "m1", "text": "let me think"}),
        ("tool.invoked", {"tool_id": "t1", "name": "write_todos", "args": {"todos": []}}),
        ("tool.returned", {"tool_id": "t1", "name": "write_todos", "result": "ok"}),
        ("subagent.started", {"subagent_id": "s1", "name": "researcher", "description": "dig"}),
        ("subagent.finished", {"subagent_id": "s1", "name": "researcher"}),
    ],
)
def test_activity_kinds_accepted(kind: str, payload: dict[str, object]) -> None:
    event = AgentEvent(kind=kind, run_id="run_01", seq=3, payload=payload)  # type: ignore[arg-type]
    assert event.kind == kind
    assert AgentEvent.model_validate(event.model_dump()) == event


def test_todo_updated_roundtrip_preserves_statuses() -> None:
    # CC-style todo: the ordered list with per-item status is the whole point —
    # it must survive dump/validate unchanged so the web checklist renders truthfully.
    payload: dict[str, object] = {
        "todos": [
            {"content": "扫描上下文", "status": "completed"},
            {"content": "写契约", "status": "in_progress"},
            {"content": "接前端", "status": "pending"},
        ]
    }
    event = AgentEvent(kind="todo.updated", run_id="run_01", seq=4, payload=payload)
    restored = AgentEvent.model_validate(event.model_dump())
    assert restored == event
    assert restored.payload["todos"] == payload["todos"]
