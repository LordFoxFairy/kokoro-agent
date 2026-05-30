from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, AIMessageChunk

from kokoro_agent.events import RunRequest
from kokoro_agent.run_agent import run_agent


class _ChunkFake(GenericFakeChatModel):
    """Fake brain that emits one ``on_chat_model_stream`` event with given content.

    GenericFakeChatModel itself rejects non-string / empty content at the
    generation layer, so to exercise ``_text_of``'s list-content branch we
    override ``astream_events`` directly with the exact event shape LangChain
    produces (``on_chat_model_stream`` carrying an ``AIMessageChunk``).
    """

    chunk_content: Any = ""

    async def astream_events(  # type: ignore[override]
        self, *args: object, **kwargs: object
    ) -> AsyncIterator[dict[str, Any]]:
        yield {
            "event": "on_chat_model_stream",
            "data": {"chunk": AIMessageChunk(content=self.chunk_content)},
        }


class _EmptyStreamFake(GenericFakeChatModel):
    """Fake brain whose stream yields no model-stream events at all."""

    async def astream_events(  # type: ignore[override]
        self, *args: object, **kwargs: object
    ) -> AsyncIterator[dict[str, Any]]:
        return
        yield  # pragma: no cover — marks this an async generator


class _BoomFake(GenericFakeChatModel):
    """Fake brain that fails as soon as streaming starts."""

    async def astream_events(  # type: ignore[override]
        self, *args: object, **kwargs: object
    ) -> AsyncIterator[dict[str, Any]]:
        raise RuntimeError("model down")
        yield  # pragma: no cover — marks this an async generator


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


@pytest.mark.asyncio
async def test_list_content_extracts_only_text_blocks() -> None:
    # A multi-block chunk: only the text block may surface; thinking must not.
    model = _ChunkFake(
        messages=iter([]),
        chunk_content=[
            {"type": "text", "text": "Hi"},
            {"type": "thinking", "thinking": "secret"},
        ],
    )
    events = [e async for e in run_agent(_req(), model)]
    full = "".join(
        str(e.payload["text"]) for e in events if e.kind == "text.delta"
    )
    assert "secret" not in full
    assert full == "Hi"
    completed = next(e for e in events if e.kind == "text.completed")
    assert completed.payload["text"] == "Hi"


@pytest.mark.asyncio
async def test_empty_stream_still_completes() -> None:
    # No stream chunks at all -> still a well-formed run with empty completion.
    model = _EmptyStreamFake(messages=iter([]))
    events = [e async for e in run_agent(_req(""), model)]
    kinds = [e.kind for e in events]
    assert kinds == ["run.started", "text.completed", "run.completed"]
    assert "text.delta" not in kinds
    completed = next(e for e in events if e.kind == "text.completed")
    assert completed.payload["text"] == ""


@pytest.mark.asyncio
async def test_brain_error_yields_run_failed() -> None:
    # Any brain failure is caught and surfaced, never re-raised.
    model = _BoomFake(messages=iter([]))
    events = [e async for e in run_agent(_req(), model)]
    assert events[0].kind == "run.started"
    assert events[-1].kind == "run.failed"
    assert events[-1].payload["error_kind"] == "RuntimeError"
    assert "model down" in str(events[-1].payload["message"])
    # No completion event when the run failed.
    assert "run.completed" not in [e.kind for e in events]
