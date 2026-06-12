from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence

import pytest
from _pytest.monkeypatch import MonkeyPatch
from langchain_core.messages import AIMessage, AIMessageChunk

from kokoro_agent.infrastructure.stream_translator import translate_stream_event
from kokoro_agent.application.run_agent import drive_agent_events
from kokoro_agent.infrastructure.subagent_registry import CUSTOM_SUBAGENTS_ENV


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


async def test_drive_agent_events_keeps_segment_activity_on_current_segment_id() -> None:
    # 同一段内：工具/子智能体先到（真实 ReAct 顺序），随后该段的思考+正文落定，
    # 全部共享同一个 segment_id。
    events = [
        {
            "event": "on_tool_start",
            "name": "get_weather",
            "run_id": "tool_x",
            "data": {"input": {"city": "北京"}},
        },
        {
            "event": "on_tool_end",
            "name": "get_weather",
            "run_id": "tool_x",
            "data": {"output": "晴"},
        },
        {
            "event": "on_tool_start",
            "name": "task",
            "run_id": "subagent_x",
            "data": {"input": {"subagent_type": "researcher", "description": "查资料"}},
        },
        {
            "event": "on_tool_end",
            "name": "task",
            "run_id": "subagent_x",
            "data": {"output": "done"},
        },
        {
            "event": "on_chat_model_end",
            "name": "ChatOpenAI",
            "data": {
                "output": AIMessage(
                    content="第一段",
                    additional_kwargs={"reasoning_content": "先想一下"},
                )
            },
        },
    ]

    out = [event async for event in drive_agent_events("run_1", _aiter(events))]
    segment_ref = next(event.payload["segment_id"] for event in out if event.kind == "text.completed")

    for kind in (
        "thinking.delta",
        "text.delta",
        "tool.invoked",
        "tool.returned",
        "subagent.started",
        "subagent.finished",
    ):
        payload = next(event.payload for event in out if event.kind == kind)
        assert payload["segment_id"] == segment_ref


async def test_drive_agent_events_attaches_activity_to_the_following_segment() -> None:
    # 工具出现在上一段「已落定」之后，属于即将到来的下一段，不再挂回旧段。
    raw = [
        {
            "event": "on_chat_model_end",
            "name": "ChatOpenAI",
            "data": {"output": AIMessage(content="第一段")},
        },
        {
            "event": "on_tool_start",
            "name": "get_weather",
            "run_id": "tool_x",
            "data": {"input": {"city": "北京"}},
        },
        {
            "event": "on_chat_model_end",
            "name": "ChatOpenAI",
            "data": {"output": AIMessage(content="第二段")},
        },
    ]

    out = [event async for event in drive_agent_events("run_1", _aiter(raw))]
    completed_refs = [event.payload["segment_id"] for event in out if event.kind == "text.completed"]
    tool_ref = next(event.payload["segment_id"] for event in out if event.kind == "tool.invoked")

    assert completed_refs == ["run_1:seg_0001", "run_1:seg_0002"]
    # 工具属于第二段（它后面那条消息），而不是第一段。
    assert tool_ref == completed_refs[1]


async def test_drive_agent_events_interleaved_tool_text_tool_text_groups_each_tool_with_following_text() -> None:
    # 真实交错流：工具1 → 文本1 → 工具2 → 文本2。
    # 每个工具属于它「后面」那条消息，分成两段、各挂各的工具，绝不塌缩成一段。
    raw = [
        {"event": "on_tool_start", "name": "tool_a", "run_id": "ta", "data": {"input": {"q": "a"}}},
        {"event": "on_tool_end", "name": "tool_a", "run_id": "ta", "data": {"output": "ra"}},
        {"event": "on_chat_model_end", "name": "ChatOpenAI", "data": {"output": AIMessage(content="第一段")}},
        {"event": "on_tool_start", "name": "tool_b", "run_id": "tb", "data": {"input": {"q": "b"}}},
        {"event": "on_tool_end", "name": "tool_b", "run_id": "tb", "data": {"output": "rb"}},
        {"event": "on_chat_model_end", "name": "ChatOpenAI", "data": {"output": AIMessage(content="第二段")}},
    ]

    out = [event async for event in drive_agent_events("run_1", _aiter(raw))]
    tool_refs = [event.payload["segment_id"] for event in out if event.kind == "tool.invoked"]
    text_refs = [event.payload["segment_id"] for event in out if event.kind == "text.completed"]

    assert text_refs == ["run_1:seg_0001", "run_1:seg_0002"]
    assert tool_refs == ["run_1:seg_0001", "run_1:seg_0002"]
    # tool_b 与第二段同段（run_1:seg_0002），不是第一段。
    assert tool_refs[1] == text_refs[1]


def test_task_tool_maps_to_subagent_lifecycle() -> None:
    start: Mapping[str, object] = {
        "event": "on_tool_start",
        "name": "task",
        "run_id": "sa1",
        "data": {"input": {"subagent_type": "researcher", "description": "查资料"}},
    }
    assert translate_stream_event(start) == [
        (
            "subagent.started",
            {
                "subagent_id": "sa1",
                "name": "researcher",
                "description": "查资料",
                "subagent_type": "researcher",
                "source": "built-in",
            },
        )
    ]
    end: Mapping[str, object] = {
        "event": "on_tool_end",
        "name": "task",
        "run_id": "sa1",
        "data": {"input": {"subagent_type": "researcher"}, "output": "done"},
    }
    assert translate_stream_event(end) == [
        (
            "subagent.finished",
            {
                "subagent_id": "sa1",
                "name": "researcher",
                "subagent_type": "researcher",
                "source": "built-in",
            },
        )
    ]


def test_task_tool_marks_env_registered_subagent_as_custom(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        CUSTOM_SUBAGENTS_ENV,
        '[{"name":"reviewer","description":"审稿","system_prompt":"检查内容质量"}]',
    )
    start: Mapping[str, object] = {
        "event": "on_tool_start",
        "name": "task",
        "run_id": "sa2",
        "data": {"input": {"subagent_type": "reviewer", "description": "审稿"}},
    }

    assert translate_stream_event(start) == [
        (
            "subagent.started",
            {
                "subagent_id": "sa2",
                "name": "reviewer",
                "description": "审稿",
                "subagent_type": "reviewer",
                "source": "config-custom",
            },
        )
    ]


async def test_drive_agent_events_routes_subagent_text_into_nested_subagent_stream() -> None:
    raw = [
        {
            "event": "on_tool_start",
            "name": "task",
            "run_id": "subagent_x",
            "data": {"input": {"subagent_type": "researcher", "description": "查资料"}},
        },
        {
            "event": "on_chat_model_end",
            "name": "ChatOpenAI",
            "metadata": {"lc_agent_name": "researcher"},
            "data": {"output": AIMessage(content="子智能体结论")},
        },
        {
            "event": "on_tool_end",
            "name": "task",
            "run_id": "subagent_x",
            "data": {"input": {"subagent_type": "researcher"}, "output": "done"},
        },
        {
            "event": "on_chat_model_end",
            "name": "ChatOpenAI",
            "data": {"output": AIMessage(content="主助手总结")},
        },
    ]

    out = [event async for event in drive_agent_events("run_1", _aiter(raw))]
    kinds = [event.kind for event in out]
    assert "subagent.text.delta" in kinds
    assert "subagent.text.completed" in kinds
    assert kinds.count("text.completed") == 1

    sub_delta = next(event for event in out if event.kind == "subagent.text.delta")
    sub_done = next(event for event in out if event.kind == "subagent.text.completed")
    final_done = [event for event in out if event.kind == "text.completed"][0]

    assert sub_delta.payload["subagent_id"] == "subagent_x"
    assert sub_done.payload["text"] == "子智能体结论"
    assert final_done.payload["text"] == "主助手总结"


def test_runtime_agent_tool_maps_to_runtime_custom_lifecycle() -> None:
    start: Mapping[str, object] = {
        "event": "on_tool_start",
        "name": "agent",
        "run_id": "sa3",
        "data": {
            "input": {
                "name": "runtime-reviewer",
                "description": "运行时审稿",
                "system_prompt": "检查一致性",
                "task": "核查 1+1=2",
            }
        },
    }
    end: Mapping[str, object] = {
        "event": "on_tool_end",
        "name": "agent",
        "run_id": "sa3",
        "data": {
            "input": {
                "name": "runtime-reviewer",
                "description": "运行时审稿",
                "system_prompt": "检查一致性",
                "task": "核查 1+1=2",
            },
            "output": "done",
        },
    }

    assert translate_stream_event(start) == [
        (
            "subagent.started",
            {
                "subagent_id": "sa3",
                "name": "runtime-reviewer",
                "description": "运行时审稿",
                "subagent_type": "runtime-reviewer",
                "source": "runtime-custom",
            },
        )
    ]
    assert translate_stream_event(end) == [
        (
            "subagent.finished",
            {
                "subagent_id": "sa3",
                "name": "runtime-reviewer",
                "subagent_type": "runtime-reviewer",
                "source": "runtime-custom",
            },
        )
    ]



def test_final_model_message_becomes_text_intent() -> None:
    ev: Mapping[str, object] = {
        "event": "on_chat_model_end",
        "name": "ChatOpenAI",
        "data": {"output": AIMessage(content="适合出门。")},
    }
    assert translate_stream_event(ev) == [("text", {"text": "适合出门。"})]


def test_model_stream_chunk_becomes_text_stream_intent() -> None:
    # A streamed token chunk carries only its incremental slice of the answer.
    ev: Mapping[str, object] = {
        "event": "on_chat_model_stream",
        "name": "ChatOpenAI",
        "data": {"chunk": AIMessageChunk(content="适合")},
    }
    assert translate_stream_event(ev) == [("text.stream", {"text": "适合"})]


def test_empty_model_stream_chunk_is_silent() -> None:
    ev: Mapping[str, object] = {
        "event": "on_chat_model_stream",
        "name": "ChatOpenAI",
        "data": {"chunk": AIMessageChunk(content="")},
    }
    assert translate_stream_event(ev) == []


def test_tool_call_only_stream_chunk_is_silent() -> None:
    # Tool-call argument chunks carry no user-visible text; they must not leak.
    chunk = AIMessageChunk(
        content="",
        tool_call_chunks=[
            {"name": "get_weather", "args": '{"ci', "id": "c1", "index": 0, "type": "tool_call_chunk"}
        ],
    )
    ev: Mapping[str, object] = {
        "event": "on_chat_model_stream",
        "name": "ChatOpenAI",
        "data": {"chunk": chunk},
    }
    assert translate_stream_event(ev) == []


def test_model_stream_chunk_surfaces_incremental_reasoning() -> None:
    ev: Mapping[str, object] = {
        "event": "on_chat_model_stream",
        "name": "ChatOpenAI",
        "data": {"chunk": AIMessageChunk(content="答", additional_kwargs={"reasoning_content": "想"})},
    }
    assert translate_stream_event(ev) == [
        ("thinking.delta", {"text": "想"}),
        ("text.stream", {"text": "答"}),
    ]


async def test_drive_agent_events_streams_incremental_deltas_then_single_completed() -> None:
    # Real token streaming: several stream chunks, then the final end carries the
    # whole message. The driver must emit one delta per chunk (each incremental)
    # and exactly one completed with the full accumulated text — all on one ref.
    raw: list[Mapping[str, object]] = [
        {"event": "on_chat_model_stream", "name": "ChatOpenAI", "data": {"chunk": AIMessageChunk(content="晴，")}},
        {"event": "on_chat_model_stream", "name": "ChatOpenAI", "data": {"chunk": AIMessageChunk(content="适合")}},
        {"event": "on_chat_model_stream", "name": "ChatOpenAI", "data": {"chunk": AIMessageChunk(content="出门。")}},
        {"event": "on_chat_model_end", "name": "ChatOpenAI", "data": {"output": AIMessage(content="晴，适合出门。")}},
    ]
    events = [e async for e in drive_agent_events("run_1", _aiter(raw))]
    kinds = [e.kind for e in events]
    assert kinds == [
        "run.started",
        "text.delta",
        "text.delta",
        "text.delta",
        "text.completed",
        "run.completed",
    ]
    deltas = [e for e in events if e.kind == "text.delta"]
    completed = next(e for e in events if e.kind == "text.completed")
    # Each delta is the incremental slice, not the cumulative buffer.
    assert [d.payload["text"] for d in deltas] == ["晴，", "适合", "出门。"]
    # One completed carrying the full accumulated text — not another delta.
    assert completed.payload["text"] == "晴，适合出门。"
    # Every event in the segment shares one segment_id.
    refs = {e.payload["segment_id"] for e in events if e.kind in ("text.delta", "text.completed")}
    assert len(refs) == 1


async def test_drive_agent_events_streamed_then_fresh_segment_uses_new_ref() -> None:
    # After a streamed segment completes, a new streamed segment must get a new
    # ref (the streamed completed closes the segment just like the fallback path).
    raw: list[Mapping[str, object]] = [
        {"event": "on_chat_model_stream", "name": "ChatOpenAI", "data": {"chunk": AIMessageChunk(content="第一")}},
        {"event": "on_chat_model_end", "name": "ChatOpenAI", "data": {"output": AIMessage(content="第一")}},
        {"event": "on_chat_model_stream", "name": "ChatOpenAI", "data": {"chunk": AIMessageChunk(content="第二")}},
        {"event": "on_chat_model_end", "name": "ChatOpenAI", "data": {"output": AIMessage(content="第二")}},
    ]
    events = [e async for e in drive_agent_events("run_1", _aiter(raw))]
    completed_refs = [e.payload["segment_id"] for e in events if e.kind == "text.completed"]
    assert completed_refs == ["run_1:seg_0001", "run_1:seg_0002"]


async def test_drive_agent_events_routes_streamed_subagent_chunks_into_subagent_stream() -> None:
    # A streaming sub-agent's chunks must route to subagent.text.delta, and the
    # sub-agent's end must produce one subagent.text.completed with the full text.
    raw: list[Mapping[str, object]] = [
        {
            "event": "on_tool_start",
            "name": "task",
            "run_id": "subagent_x",
            "data": {"input": {"subagent_type": "researcher", "description": "查资料"}},
        },
        {
            "event": "on_chat_model_stream",
            "name": "ChatOpenAI",
            "metadata": {"lc_agent_name": "researcher"},
            "data": {"chunk": AIMessageChunk(content="子")},
        },
        {
            "event": "on_chat_model_stream",
            "name": "ChatOpenAI",
            "metadata": {"lc_agent_name": "researcher"},
            "data": {"chunk": AIMessageChunk(content="结论")},
        },
        {
            "event": "on_chat_model_end",
            "name": "ChatOpenAI",
            "metadata": {"lc_agent_name": "researcher"},
            "data": {"output": AIMessage(content="子结论")},
        },
        {
            "event": "on_tool_end",
            "name": "task",
            "run_id": "subagent_x",
            "data": {"input": {"subagent_type": "researcher"}, "output": "done"},
        },
    ]
    events = [e async for e in drive_agent_events("run_1", _aiter(raw))]
    sub_deltas = [e for e in events if e.kind == "subagent.text.delta"]
    sub_completed = [e for e in events if e.kind == "subagent.text.completed"]
    assert [d.payload["text"] for d in sub_deltas] == ["子", "结论"]
    assert len(sub_completed) == 1
    assert sub_completed[0].payload["text"] == "子结论"
    assert sub_completed[0].payload["subagent_id"] == "subagent_x"
    sub_refs = {
        e.payload["segment_id"]
        for e in events
        if e.kind in ("subagent.text.delta", "subagent.text.completed")
    }
    assert len(sub_refs) == 1
    # The parent thread never gets a text.delta/completed for a sub-agent segment.
    assert "text.completed" not in [e.kind for e in events]


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


async def _aiter(items: Sequence[Mapping[str, object]]) -> AsyncIterator[Mapping[str, object]]:
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
    assert delta.payload["segment_id"] == completed.payload["segment_id"]


async def test_stream_failure_yields_run_failed_not_completed() -> None:
    events = [e async for e in drive_agent_events("run_1", _boom())]
    assert events[0].kind == "run.started"
    assert events[-1].kind == "run.failed"
    assert events[-1].payload["error_kind"] == "RuntimeError"
    assert "model down" in str(events[-1].payload["message"])
    assert "run.completed" not in [e.kind for e in events]
