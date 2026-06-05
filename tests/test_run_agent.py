from __future__ import annotations

from collections.abc import AsyncIterator, Mapping

import pytest
from langchain_core.messages import AIMessage

from kokoro_agent.run_agent import drive_agent_events, translate_stream_event


# --- pure mapper: one astream_events(v2) event -> (kind, payload) intents -----


def test_write_todos_start_maps_to_todo_updated() -> None:
    todos = [
        {"content": "查天气", "status": "in_progress"},
        {"content": "作答", "status": "pending"},
    ]
    ev: Mapping[str, object] = {
        "event": "on_tool_start",
        "name": "write_todos",
        "run_id": "t1",
        "data": {"input": {"todos": todos}},
    }
    assert translate_stream_event(ev) == [("todo.updated", {"todos": todos})]


def test_write_todos_end_is_silent() -> None:
    # The list is emitted on start; the end carries the same list -> no duplicate.
    ev: Mapping[str, object] = {
        "event": "on_tool_end",
        "name": "write_todos",
        "run_id": "t1",
        "data": {"output": "ok"},
    }
    assert translate_stream_event(ev) == []


def test_generic_tool_start_and_end_pair() -> None:
    start: Mapping[str, object] = {
        "event": "on_tool_start",
        "name": "get_weather",
        "run_id": "tw",
        "data": {"input": {"city": "北京"}},
    }
    assert translate_stream_event(start) == [
        ("tool.invoked", {"tool_id": "tw", "name": "get_weather", "args": {"city": "北京"}})
    ]
    end: Mapping[str, object] = {
        "event": "on_tool_end",
        "name": "get_weather",
        "run_id": "tw",
        "data": {"output": AIMessage(content="北京: sunny")},
    }
    # tool result text is correlated by the same tool_id (event run_id).
    assert translate_stream_event(end) == [
        ("tool.returned", {"tool_id": "tw", "name": "get_weather", "result": "北京: sunny"})
    ]


def test_task_tool_maps_to_subagent_lifecycle() -> None:
    start: Mapping[str, object] = {
        "event": "on_tool_start",
        "name": "task",
        "run_id": "sa1",
        "data": {"input": {"subagent_type": "researcher", "description": "查资料"}},
    }
    assert translate_stream_event(start) == [
        ("subagent.started", {"subagent_id": "sa1", "name": "researcher", "description": "查资料"})
    ]
    end: Mapping[str, object] = {
        "event": "on_tool_end",
        "name": "task",
        "run_id": "sa1",
        "data": {"output": "done"},
    }
    assert translate_stream_event(end) == [
        ("subagent.finished", {"subagent_id": "sa1", "name": "task"})
    ]


def test_final_model_message_becomes_text_intent() -> None:
    ev: Mapping[str, object] = {
        "event": "on_chat_model_end",
        "name": "ChatOpenAI",
        "data": {"output": AIMessage(content="适合出门。")},
    }
    assert translate_stream_event(ev) == [("text", {"text": "适合出门。"})]


def test_intermediate_tool_call_turn_emits_no_text() -> None:
    # An intermediate model turn carries tool_calls (and here empty content);
    # it must NOT surface as a user-visible message.
    msg = AIMessage(
        content="",
        tool_calls=[{"name": "get_weather", "args": {"city": "北京"}, "id": "c1", "type": "tool_call"}],
    )
    ev: Mapping[str, object] = {
        "event": "on_chat_model_end",
        "name": "ChatOpenAI",
        "data": {"output": msg},
    }
    assert translate_stream_event(ev) == []


def test_reasoning_content_surfaces_as_thinking() -> None:
    # Reasoning models expose reasoning_content; it must precede the answer text.
    msg = AIMessage(content="答案", additional_kwargs={"reasoning_content": "我在推理"})
    ev: Mapping[str, object] = {
        "event": "on_chat_model_end",
        "name": "ChatOpenAI",
        "data": {"output": msg},
    }
    assert translate_stream_event(ev) == [
        ("thinking.delta", {"text": "我在推理"}),
        ("text", {"text": "答案"}),
    ]


@pytest.mark.parametrize(
    "ev",
    [
        {"event": "on_chain_start", "name": "LangGraph", "data": {}},
        {"event": "on_chain_stream", "name": "TodoListMiddleware.after_model", "data": {}},
        {"event": "on_chat_model_start", "name": "ChatOpenAI", "data": {}},
        {"event": "on_chain_end", "name": "tools", "data": {}},
    ],
)
def test_internal_graph_nodes_are_skipped(ev: Mapping[str, object]) -> None:
    assert translate_stream_event(ev) == []


# --- envelope: run.started ... run.completed | run.failed ---------------------


async def _aiter(items: list[Mapping[str, object]]) -> AsyncIterator[Mapping[str, object]]:
    for item in items:
        yield item


async def _boom() -> AsyncIterator[Mapping[str, object]]:
    raise RuntimeError("model down")
    yield {}  # pragma: no cover — marks this an async generator


async def test_empty_stream_is_started_then_completed() -> None:
    events = [e async for e in drive_agent_events("run_1", _aiter([]))]
    assert [e.kind for e in events] == ["run.started", "run.completed"]
    assert [e.seq for e in events] == [1, 2]
    assert events[-1].payload == {"status": "completed"}


async def test_activity_stream_envelope_and_text_expansion() -> None:
    todos = [{"content": "查天气", "status": "in_progress"}]
    raw: list[Mapping[str, object]] = [
        {"event": "on_tool_start", "name": "write_todos", "run_id": "t", "data": {"input": {"todos": todos}}},
        {"event": "on_chat_model_end", "name": "ChatOpenAI", "data": {"output": AIMessage(content="晴，适合。")}},
    ]
    events = [e async for e in drive_agent_events("run_1", _aiter(raw))]
    kinds = [e.kind for e in events]
    assert kinds == [
        "run.started",
        "todo.updated",
        "text.delta",
        "text.completed",
        "run.completed",
    ]
    # seq strictly increasing from 1, unique.
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs) and seqs[0] == 1 and len(set(seqs)) == len(seqs)
    # text.delta and text.completed are the same logical message (shared ref + text).
    delta = next(e for e in events if e.kind == "text.delta")
    completed = next(e for e in events if e.kind == "text.completed")
    assert delta.payload == completed.payload
    assert delta.payload["text"] == "晴，适合。"
    assert delta.payload["message_ref"] == completed.payload["message_ref"]


async def test_stream_failure_yields_run_failed_not_completed() -> None:
    events = [e async for e in drive_agent_events("run_1", _boom())]
    assert events[0].kind == "run.started"
    assert events[-1].kind == "run.failed"
    assert events[-1].payload["error_kind"] == "RuntimeError"
    assert "model down" in str(events[-1].payload["message"])
    assert "run.completed" not in [e.kind for e in events]
