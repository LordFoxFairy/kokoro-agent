from __future__ import annotations

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from kokoro_agent.events import RunRequest
from kokoro_agent.run_agent import run_agent


def _req(text: str = "hi") -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id="run_1",
        session_id="s",
        conversation_id="c",
        input=text,
        execution_style="fast",
    )


@pytest.mark.asyncio
async def test_run_agent_streams_text_then_completes() -> None:
    # GenericFakeChatModel streams "Hello world" as ["Hello", " ", "world"].
    model = GenericFakeChatModel(messages=iter([AIMessage(content="Hello world")]))
    events = [e async for e in run_agent(_req(), model)]
    kinds = [e.kind for e in events]
    assert kinds[0] == "run.started"
    assert "text.delta" in kinds
    assert kinds[-2] == "text.completed"
    assert kinds[-1] == "run.completed"

    # seq strictly increasing from 1, no duplicates.
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs)
    assert seqs[0] == 1
    assert len(set(seqs)) == len(seqs)

    # Delta concatenation == completed text == full message (robust to chunking).
    deltas = "".join(
        str(e.payload["text"]) for e in events if e.kind == "text.delta"
    )
    completed = next(e for e in events if e.kind == "text.completed")
    assert completed.payload["text"] == deltas == "Hello world"

    # run.completed carries terminal status.
    assert events[-1].payload == {"status": "completed"}
