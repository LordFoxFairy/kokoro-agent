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


def test_tool_invoked_carries_args() -> None:
    ev = AgentEvent(
        kind="tool.invoked",
        run_id="run_1",
        seq=3,
        payload={
            "tool_call_ref": "call_1",
            "tool_name": "write_todos",
            "args": {"todos": [{"content": "step 1", "status": "pending"}]},
        },
    )
    todos = ev.payload["args"]  # type: ignore[index]
    assert isinstance(todos, dict)
    assert todos["todos"][0]["status"] == "pending"  # type: ignore[index]


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
