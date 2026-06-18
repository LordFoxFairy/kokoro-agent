from __future__ import annotations

from typing import TypedDict

from langchain_core.messages import AIMessage
from langchain_core.runnables.schema import EventData, StandardStreamEvent, StreamEvent
from pydantic import JsonValue

from kokoro_agent.infrastructure.stream_events import stream_intent_contract, translate_stream_event


class EventSeed(TypedDict, total=False):
    event: str
    name: str
    run_id: str
    data: EventData
    metadata: dict[str, str]


def _event(
    event: str,
    name: str,
    run_id: str,
    data: EventData,
    metadata: dict[str, str] | None = None,
) -> StandardStreamEvent:
    return {
        "event": event,
        "name": name,
        "run_id": run_id,
        "data": data,
        "metadata": dict(metadata or {}),
        "tags": [],
        "parent_ids": [],
    }


def _tuples(ev: StreamEvent) -> list[tuple[str, dict[str, JsonValue]]]:
    return [
        (contract.kind, contract.payload.model_dump(exclude_defaults=True))
        for contract in map(stream_intent_contract, translate_stream_event(ev))
    ]


# --- pure mapper: one astream_events(v2) event -> (kind, payload) intents -----


def test_write_todos_start_maps_to_todo_updated() -> None:
    todos = [
        {"content": "查天气", "status": "in_progress"},
        {"content": "作答", "status": "pending"},
    ]
    ev = _event(
        "on_tool_start",
        "write_todos",
        "t1",
        {"input": {"todos": todos}},
    )
    assert _tuples(ev) == [("todo.updated", {"todos": todos})]


def test_write_todos_end_is_silent() -> None:
    ev = _event("on_tool_end", "write_todos", "t1", {"output": "ok"})
    assert _tuples(ev) == []


def test_generic_tool_start_and_end_pair() -> None:
    start = _event(
        "on_tool_start",
        "get_weather",
        "tw",
        {"input": {"city": "北京"}},
    )
    assert _tuples(start) == [
        ("tool.invoked", {"tool_id": "tw", "name": "get_weather", "args": {"city": "北京"}})
    ]
    end = _event(
        "on_tool_end",
        "get_weather",
        "tw",
        {"output": AIMessage(content="北京: sunny")},
    )
    assert _tuples(end) == [
        ("tool.returned", {"tool_id": "tw", "name": "get_weather", "result": "北京: sunny", "is_error": False})
    ]


def test_rejected_gated_tool_marks_tool_returned_rejected() -> None:
    from kokoro_agent.infrastructure.control import rejection_result

    ev = _event(
        "on_tool_end",
        "fetch_url",
        "tr",
        {"output": AIMessage(content=rejection_result("fetch_url"))},
    )
    assert _tuples(ev) == [
        (
            "tool.returned",
            {
                "tool_id": "tr",
                "name": "fetch_url",
                "result": rejection_result("fetch_url"),
                "is_error": False,
                "rejected": True,
            },
        )
    ]


def test_tool_error_maps_to_tool_returned_with_is_error() -> None:
    ev = _event(
        "on_tool_error",
        "fetch_url",
        "te",
        {"error": ValueError("connection refused"), "input": {"url": "x"}},
    )
    assert _tuples(ev) == [
        (
            "tool.returned",
            {"tool_id": "te", "name": "fetch_url", "result": "connection refused", "is_error": True},
        )
    ]


def test_tool_error_on_subagent_tool_maps_to_subagent_finished_not_a_fake_tool() -> None:
    ev = _event(
        "on_tool_error",
        "task",
        "sa1",
        {"error": ValueError("subagent crashed"), "input": {"subagent_type": "researcher"}},
    )
    [(kind, payload)] = _tuples(ev)
    assert kind == "subagent.finished"
    assert payload.get("subagent_id") == "sa1"
    assert payload.get("subagent_type") == "researcher"


def test_tool_error_on_runtime_subagent_tool_maps_to_subagent_finished() -> None:
    ev = _event(
        "on_tool_error",
        "agent",
        "rt1",
        {"error": ValueError("boom"), "input": {"name": "helper"}},
    )
    [(kind, payload)] = _tuples(ev)
    assert kind == "subagent.finished"
    assert payload.get("subagent_type") == "helper"
    assert payload.get("source") == "runtime-custom"


def test_tool_error_on_write_todos_is_silent() -> None:
    ev = _event(
        "on_tool_error",
        "write_todos",
        "wt",
        {"error": ValueError("x"), "input": {}},
    )
    assert _tuples(ev) == []


def test_tool_error_with_empty_message_falls_back_to_exception_type_name() -> None:
    ev = _event(
        "on_tool_error",
        "fetch_url",
        "te",
        {"error": RuntimeError()},
    )
    [(kind, payload)] = _tuples(ev)
    assert kind == "tool.returned" and payload.get("is_error") is True
    assert payload.get("result") == "RuntimeError"
