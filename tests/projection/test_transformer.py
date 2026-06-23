from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.runnables.schema import EventData, StandardStreamEvent, StreamEvent

from kokoro_agent.application.projection.attribution import SubagentAttribution
from kokoro_agent.application.projection.transformer import (
    TOOL_RESULT_MAX_CHARS,
    project,
    usage_delta,
)


def _ev(
    event: str,
    *,
    name: str = "model",
    run_id: str = "run-1",
    data: EventData | None = None,
    metadata: dict[str, str] | None = None,
) -> StreamEvent:
    seed: StandardStreamEvent = {
        "event": event,
        "run_id": run_id,
        "parent_ids": [],
        "name": name,
        "data": data or {},
        "metadata": metadata or {},
    }
    return seed


KORO = "kokoro-run"


def test_chat_model_stream_emits_text_chunk_with_content_blocks() -> None:
    ev = _ev("on_chat_model_stream", run_id="seg-7", data={"chunk": AIMessageChunk(content="hi")})
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.event for e in out] == ["text_chunk"]
    assert out[0].request_id == KORO
    assert out[0].data == {
        "segment_id": "seg-7",
        "content": [{"type": "text", "text": "hi"}],
        "final": False,
    }


def test_reasoning_blocks_pass_through_in_content() -> None:
    chunk = AIMessageChunk(content=[{"type": "reasoning", "reasoning": "think"}])
    out = project(_ev("on_chat_model_stream", data={"chunk": chunk}), SubagentAttribution(), KORO)
    assert [e.event for e in out] == ["text_chunk"]
    assert out[0].data == {
        "segment_id": "run-1",
        "content": [{"type": "reasoning", "reasoning": "think"}],
        "final": False,
    }


def test_subagent_text_carries_subagent_id() -> None:
    attribution = SubagentAttribution()
    attribution.started("sub-9", "researcher")
    ev = _ev(
        "on_chat_model_stream",
        run_id="seg-3",
        data={"chunk": AIMessageChunk(content="sub")},
        metadata={"agent_name": "researcher"},
    )
    out = project(ev, attribution, KORO)
    assert [e.event for e in out] == ["text_chunk"]
    assert out[0].data == {
        "segment_id": "seg-3",
        "content": [{"type": "text", "text": "sub"}],
        "final": False,
        "subagent_id": "sub-9",
    }


def test_empty_stream_chunk_skipped() -> None:
    out = project(
        _ev("on_chat_model_stream", data={"chunk": AIMessageChunk(content="")}),
        SubagentAttribution(),
        KORO,
    )
    assert out == []


def test_chat_model_end_emits_final_text_chunk() -> None:
    ev = _ev("on_chat_model_end", run_id="seg-z", data={"output": AIMessage(content="final")})
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.event for e in out] == ["text_chunk"]
    assert out[0].data == {
        "segment_id": "seg-z",
        "content": [{"type": "text", "text": "final"}],
        "final": True,
    }


def test_chat_model_end_subagent_final() -> None:
    attribution = SubagentAttribution()
    attribution.started("sub-1", "researcher")
    ev = _ev(
        "on_chat_model_end",
        data={"output": AIMessage(content="done")},
        metadata={"agent_name": "researcher"},
    )
    out = project(ev, attribution, KORO)
    assert out[0].data == {
        "segment_id": "run-1",
        "content": [{"type": "text", "text": "done"}],
        "final": True,
        "subagent_id": "sub-1",
    }


def test_tool_start_todo_emits_agent_status() -> None:
    ev = _ev(
        "on_tool_start",
        name="write_todos",
        data={"input": {"todos": [{"content": "a", "status": "pending"}]}},
    )
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.event for e in out] == ["agent_status"]
    assert out[0].data == {
        "status": "todo_updated",
        "segment_id": "run-1",
        "todos": [{"content": "a", "status": "pending"}],
    }


def test_tool_start_emits_tool_call_start() -> None:
    ev = _ev(
        "on_tool_start",
        name="search",
        run_id="tool-5",
        data={"input": {"query": "kokoro", "limit": 3}},
    )
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.event for e in out] == ["tool_call_start"]
    assert out[0].request_id == KORO
    assert out[0].data == {
        "segment_id": "tool-5",
        "tool_id": "tool-5",
        "name": "search",
        "args": {"query": "kokoro", "limit": 3},
    }


def test_tool_start_subagent_started_status_registers_attribution() -> None:
    attribution = SubagentAttribution()
    ev = _ev(
        "on_tool_start",
        name="task",
        run_id="sub-x",
        data={"input": {"subagent_type": "researcher", "description": "查资料"}},
    )
    out = project(ev, attribution, KORO)
    assert [e.event for e in out] == ["agent_status"]
    assert out[0].data == {
        "status": "subagent_started",
        "segment_id": "sub-x",
        "subagent_id": "sub-x",
        "name": "researcher",
        "description": "查资料",
        "subagent_type": "researcher",
        "source": "built-in",
    }
    text_ev = _ev(
        "on_chat_model_stream",
        data={"chunk": AIMessageChunk(content="x")},
        metadata={"agent_name": "researcher"},
    )
    assert project(text_ev, attribution, KORO)[0].data["subagent_id"] == "sub-x"


def test_tool_start_runtime_subagent_status() -> None:
    attribution = SubagentAttribution()
    ev = _ev(
        "on_tool_start",
        name="agent",
        run_id="sa3",
        data={"input": {"name": "runtime-reviewer", "description": "运行时审稿"}},
    )
    out = project(ev, attribution, KORO)
    assert [e.event for e in out] == ["agent_status"]
    assert out[0].data == {
        "status": "subagent_started",
        "segment_id": "sa3",
        "subagent_id": "sa3",
        "name": "runtime-reviewer",
        "description": "运行时审稿",
        "subagent_type": "runtime-reviewer",
        "source": "runtime-custom",
    }


def test_tool_end_emits_tool_call_end() -> None:
    ev = _ev(
        "on_tool_end",
        name="search",
        run_id="tool-9",
        data={"input": {}, "output": "result-text"},
    )
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.event for e in out] == ["tool_call_end"]
    assert out[0].data == {
        "segment_id": "tool-9",
        "tool_id": "tool-9",
        "name": "search",
        "result": "result-text",
        "is_error": False,
        "rejected": False,
    }


def test_tool_end_result_is_truncated() -> None:
    huge = "x" * (TOOL_RESULT_MAX_CHARS + 100)
    ev = _ev("on_tool_end", name="search", data={"input": {}, "output": huge})
    out = project(ev, SubagentAttribution(), KORO)
    result = out[0].data["result"]
    assert isinstance(result, str)
    assert len(result) < len(huge)
    assert result.startswith("x" * TOOL_RESULT_MAX_CHARS)


def test_tool_error_is_error_true() -> None:
    ev = _ev(
        "on_tool_error",
        name="search",
        run_id="t-err",
        data={"input": {}, "error": Exception("boom")},
    )
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.event for e in out] == ["tool_call_end"]
    assert out[0].data["is_error"] is True
    assert out[0].data["result"] == "boom"
    assert out[0].data["rejected"] is False


def test_tool_end_subagent_finished_clears_attribution() -> None:
    attribution = SubagentAttribution()
    attribution.started("sub-x", "researcher")
    ev = _ev(
        "on_tool_end",
        name="task",
        run_id="sub-x",
        data={"input": {"subagent_type": "researcher"}, "output": "done"},
    )
    out = project(ev, attribution, KORO)
    assert [e.event for e in out] == ["agent_status"]
    assert out[0].data == {
        "status": "subagent_finished",
        "segment_id": "sub-x",
        "subagent_id": "sub-x",
        "name": "researcher",
        "subagent_type": "researcher",
        "source": "built-in",
    }
    text_ev = _ev(
        "on_chat_model_stream",
        data={"chunk": AIMessageChunk(content="x")},
        metadata={"agent_name": "researcher"},
    )
    assert "subagent_id" not in project(text_ev, attribution, KORO)[0].data


def test_unknown_event_returns_empty() -> None:
    assert project(_ev("on_chain_start"), SubagentAttribution(), KORO) == []


def test_request_id_separate_from_segment_and_tool_id() -> None:
    lc_id = "lc-runnable-id"
    text_out = project(
        _ev("on_chat_model_stream", run_id=lc_id, data={"chunk": AIMessageChunk(content="hi")}),
        SubagentAttribution(),
        KORO,
    )
    assert text_out[0].request_id == KORO
    assert text_out[0].data["segment_id"] == lc_id
    tool_out = project(
        _ev("on_tool_start", name="search", run_id=lc_id, data={"input": {"q": "x"}}),
        SubagentAttribution(),
        KORO,
    )
    assert tool_out[0].request_id == KORO
    assert tool_out[0].data["tool_id"] == lc_id
    assert tool_out[0].data["segment_id"] == lc_id


def test_usage_delta_from_chat_model_end() -> None:
    msg = AIMessage(
        content="x", usage_metadata={"input_tokens": 3, "output_tokens": 5, "total_tokens": 8}
    )
    assert usage_delta(_ev("on_chat_model_end", data={"output": msg})) == {
        "input_tokens": 3,
        "output_tokens": 5,
        "total_tokens": 8,
    }


def test_usage_delta_empty_for_non_end() -> None:
    ev = _ev("on_chat_model_stream", data={"chunk": AIMessageChunk(content="hi")})
    assert usage_delta(ev) == {}
