import pytest
from pydantic import ValidationError

from kokoro_agent.interfaces.envelope import AgentEvent


def test_envelope_shape_strict() -> None:
    ev = AgentEvent(event="text_chunk", request_id="r1", timestamp=123, data={"content": []})
    assert ev.model_dump() == {
        "event": "text_chunk",
        "request_id": "r1",
        "timestamp": 123,
        "data": {"content": []},
    }


def test_timestamp_autostamped_when_omitted() -> None:
    ev = AgentEvent.model_validate(
        {"event": "agent_done", "request_id": "r2", "data": {"status": "completed"}}
    )
    assert isinstance(ev.timestamp, int) and ev.timestamp > 0


def test_unknown_event_rejected() -> None:
    with pytest.raises(ValidationError):
        AgentEvent.model_validate({"event": "bogus", "request_id": "r", "data": {}})


def test_extra_field_forbidden() -> None:
    with pytest.raises(ValidationError):
        AgentEvent.model_validate(
            {"event": "agent_status", "request_id": "r", "data": {}, "seq": 1}
        )
