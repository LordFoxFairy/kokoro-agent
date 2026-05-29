from kokoro_agent.run_agent import RunAgentInput, run_agent


def test_run_agent_emits_replayable_session_events() -> None:
    events = list(
        run_agent(
            RunAgentInput(
                session_id="ses_01",
                conversation_id="conv_01",
                user_input="hello kokoro",
            )
        )
    )

    assert events[0]["event"] == "session.created"
    assert events[0]["session_id"] == "ses_01"
    assert events[0]["conversation_id"] == "conv_01"
    assert events[1]["event"] == "message.delta"
    assert events[1]["payload"]["role"] == "assistant"
    assert events[-1]["event"] == "run.completed"
    assert events[-1]["payload"]["status"] == "completed"
