from kokoro_agent.events.agent_event import AgentEvent, is_agent_kind


def test_agent_event_has_no_seq_and_strict():
    ev = AgentEvent(kind="text.delta", run_id="r1", payload={"segment_id": "s", "text": "hi"})
    assert ev.model_dump() == {"kind": "text.delta", "run_id": "r1", "payload": {"segment_id": "s", "text": "hi"}}
    assert "seq" not in ev.model_dump()
    assert is_agent_kind("run.failed") and not is_agent_kind("bogus")
