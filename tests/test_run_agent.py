from __future__ import annotations

from typing import Any

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, BaseMessage

from kokoro_agent.events import RunRequest
from kokoro_agent.run_agent import run_agent


class _BoomFake(GenericFakeChatModel):
    """Fake brain that fails as soon as it is invoked."""

    async def ainvoke(  # type: ignore[override]
        self, *args: object, **kwargs: object
    ) -> BaseMessage:
        raise RuntimeError("model down")


class _LoopFake(GenericFakeChatModel):
    """Fake brain that always asks for a tool call -> drives the loop cap."""

    async def ainvoke(  # type: ignore[override]
        self, *args: object, **kwargs: object
    ) -> BaseMessage:
        return AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "echo_search",
                    "args": {"query": "x"},
                    "id": "call_loop",
                    "type": "tool_call",
                }
            ],
        )


def _req(text: str = "hi", style: str = "fast") -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id="run_1",
        session_id="s",
        conversation_id="c",
        input=text,
        execution_style=style,
    )


def _tc(name: str, args: dict[str, Any], call_id: str) -> dict[str, Any]:
    return {"name": name, "args": args, "id": call_id, "type": "tool_call"}


def _assert_seq_monotonic(events: list[Any]) -> None:
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs)
    assert seqs[0] == 1
    assert len(set(seqs)) == len(seqs)


@pytest.mark.asyncio
async def test_plain_text_run_completes() -> None:
    model = GenericFakeChatModel(messages=iter([AIMessage(content="Final")]))
    events = [e async for e in run_agent(_req(), model)]
    kinds = [e.kind for e in events]
    assert kinds == [
        "run.started",
        "text.delta",
        "text.completed",
        "run.completed",
    ]
    _assert_seq_monotonic(events)
    completed = next(e for e in events if e.kind == "text.completed")
    assert completed.payload["text"] == "Final"
    assert events[-1].payload == {"status": "completed"}


@pytest.mark.asyncio
async def test_tool_call_then_final_text() -> None:
    model = GenericFakeChatModel(
        messages=iter(
            [
                AIMessage(
                    content="",
                    tool_calls=[_tc("echo_search", {"query": "cats"}, "call_1")],
                ),
                AIMessage(content="Final"),
            ]
        )
    )
    events = [e async for e in run_agent(_req(), model)]
    kinds = [e.kind for e in events]
    assert kinds == [
        "run.started",
        "tool.invoked",
        "tool.returned",
        "text.delta",
        "text.completed",
        "run.completed",
    ]
    _assert_seq_monotonic(events)

    invoked = next(e for e in events if e.kind == "tool.invoked")
    returned = next(e for e in events if e.kind == "tool.returned")
    assert invoked.payload["tool_call_ref"] == returned.payload["tool_call_ref"] == "call_1"
    assert invoked.payload["tool_name"] == "echo_search"
    assert returned.payload["status"] == "ok"


@pytest.mark.asyncio
async def test_tool_error_surfaces_error_status() -> None:
    model = GenericFakeChatModel(
        messages=iter(
            [
                AIMessage(
                    content="",
                    tool_calls=[_tc("nope", {"query": "x"}, "call_e")],
                ),
                AIMessage(content="Done"),
            ]
        )
    )
    events = [e async for e in run_agent(_req(), model)]
    returned = next(e for e in events if e.kind == "tool.returned")
    assert returned.payload["status"] == "error"
    assert events[-1].kind == "run.completed"


@pytest.mark.asyncio
async def test_thinking_style_emits_thinking_before_text() -> None:
    model = GenericFakeChatModel(
        messages=iter(
            [
                AIMessage(
                    content=[
                        {"type": "thinking", "thinking": "reasoning"},
                        {"type": "text", "text": "Final"},
                    ]
                )
            ]
        )
    )
    events = [e async for e in run_agent(_req(style="thinking"), model)]
    kinds = [e.kind for e in events]
    assert "thinking.delta" in kinds
    assert kinds.index("thinking.delta") < kinds.index("text.delta")

    thinking = next(e for e in events if e.kind == "thinking.delta")
    assert thinking.payload == {"text": "reasoning"}

    # Reasoning must never leak into the visible answer.
    text_blob = "".join(
        str(e.payload["text"]) for e in events if e.kind == "text.delta"
    )
    assert "reasoning" not in text_blob
    assert text_blob == "Final"


@pytest.mark.asyncio
async def test_fast_style_has_no_thinking() -> None:
    model = GenericFakeChatModel(
        messages=iter(
            [
                AIMessage(
                    content=[
                        {"type": "thinking", "thinking": "reasoning"},
                        {"type": "text", "text": "Final"},
                    ]
                )
            ]
        )
    )
    events = [e async for e in run_agent(_req(style="fast"), model)]
    assert "thinking.delta" not in [e.kind for e in events]


@pytest.mark.asyncio
async def test_brain_error_yields_run_failed() -> None:
    model = _BoomFake(messages=iter([]))
    events = [e async for e in run_agent(_req(), model)]
    assert events[0].kind == "run.started"
    assert events[-1].kind == "run.failed"
    assert events[-1].payload["error_kind"] == "RuntimeError"
    assert "model down" in str(events[-1].payload["message"])
    assert "run.completed" not in [e.kind for e in events]


@pytest.mark.asyncio
async def test_tool_loop_limit_fails_loud() -> None:
    model = _LoopFake(messages=iter([]))
    events = [e async for e in run_agent(_req(), model)]
    assert events[-1].kind == "run.failed"
    assert events[-1].payload["error_kind"] == "ToolLoopLimit"
    assert "run.completed" not in [e.kind for e in events]
