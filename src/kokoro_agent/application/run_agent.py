from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

from kokoro_agent.domain import events


def run_agent(user_input: str) -> Iterator[dict[str, object]]:
    run_id = f"run_{uuid4().hex[:8]}"

    yield events.run_created(run_id)
    yield events.message_delta(run_id, f"Kokoro received: {user_input}")
    yield events.message_completed(run_id, f"Kokoro received: {user_input}")
    yield events.run_completed(run_id)
