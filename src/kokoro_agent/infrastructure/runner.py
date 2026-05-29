from __future__ import annotations

from collections.abc import Iterator

from kokoro_agent.application.run_agent import run_agent


def run(user_input: str) -> Iterator[dict[str, object]]:
    return run_agent(user_input)
