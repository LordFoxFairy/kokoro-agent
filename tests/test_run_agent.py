from kokoro_agent.application.run_agent import run_agent


def test_run_agent_emits_message_and_completion_events() -> None:
    events = list(run_agent("hello kokoro"))

    assert events[0]["event"] == "run.created"
    assert any(event["event"] == "message.delta" for event in events)
    assert any(event["event"] == "message.completed" for event in events)
    assert events[-1]["event"] == "run.completed"
