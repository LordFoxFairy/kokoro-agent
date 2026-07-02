import pytest
from pydantic import ValidationError

from kokoro_agent.run.events import AgentEvent


def test_envelope_shape_strict() -> None:
    ev = AgentEvent(
        event="text_chunk",
        request_id="r1",
        timestamp=123,
        data={"segment_id": "s", "text": "hi", "final": False},
    )
    assert ev.model_dump() == {
        "event": "text_chunk",
        "request_id": "r1",
        "timestamp": 123,
        "data": {"segment_id": "s", "text": "hi", "final": False},
    }


def test_text_chunk_carries_string_text() -> None:
    ev = AgentEvent.model_validate(
        {"event": "text_chunk", "request_id": "r", "data": {"segment_id": "s", "text": "hi", "final": False}}
    )
    assert ev.model_dump()["data"] == {"segment_id": "s", "text": "hi", "final": False}


def test_reasoning_chunk_is_valid_event() -> None:
    ev = AgentEvent.model_validate(
        {"event": "reasoning_chunk", "request_id": "r", "data": {"segment_id": "s", "text": "t", "final": False}}
    )
    assert ev.event == "reasoning_chunk"


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
            {"event": "agent_status", "request_id": "r", "data": {}, "index": 1}
        )
