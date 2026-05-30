from __future__ import annotations

from collections.abc import Iterator

from kokoro_agent.events import AgentEvent, RunRequest


def run_agent(req: RunRequest) -> Iterator[AgentEvent]:
    """Deterministic echo brain.

    Emits the raw agent-event sequence defined by the agent-events contract:
    ``run.started`` -> ``text.delta`` -> ``text.completed`` -> ``run.completed``.
    No real LLM is invoked. The agent only fills execution semantics; it does
    not assign cursors/ids/owner — that is kokoro-session's responsibility.
    """
    text = f"Kokoro received: {req.input}"
    message_ref = "m1"

    yield AgentEvent(kind="run.started", run_id=req.run_id, seq=1, payload={})
    yield AgentEvent(
        kind="text.delta",
        run_id=req.run_id,
        seq=2,
        payload={"message_ref": message_ref, "text": text},
    )
    yield AgentEvent(
        kind="text.completed",
        run_id=req.run_id,
        seq=3,
        payload={"message_ref": message_ref, "text": text},
    )
    yield AgentEvent(
        kind="run.completed",
        run_id=req.run_id,
        seq=4,
        payload={"status": "completed"},
    )
