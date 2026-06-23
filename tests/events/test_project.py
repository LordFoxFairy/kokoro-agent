from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.runnables.schema import EventData, StandardStreamEvent, StreamEvent

from kokoro_agent.application.projection.attribution import SubagentAttribution
from kokoro_agent.application.projection.transformer import TOOL_RESULT_MAX_CHARS, project


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


def test_chat_model_stream_text_delta_uses_run_id_as_segment_id() -> None:
    ev = _ev("on_chat_model_stream", run_id="seg-7", data={"chunk": AIMessageChunk(content="hi")})
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.kind for e in out] == ["text.delta"]
    assert out[0].run_id == KORO
    assert out[0].payload == {"segment_id": "seg-7", "text": "hi"}


def test_chat_model_stream_reasoning_emits_thinking_delta() -> None:
    chunk = AIMessageChunk(content=[{"type": "reasoning", "reasoning": "think"}])
    out = project(_ev("on_chat_model_stream", data={"chunk": chunk}), SubagentAttribution(), KORO)
    assert [e.kind for e in out] == ["thinking.delta"]
    assert out[0].run_id == KORO
    assert out[0].payload == {"segment_id": "run-1", "text": "think"}


def test_chat_model_stream_subagent_text_goes_to_subagent_branch() -> None:
    attribution = SubagentAttribution()
    attribution.started("sub-9", "researcher")
    ev = _ev(
        "on_chat_model_stream",
        run_id="seg-3",
        data={"chunk": AIMessageChunk(content="sub")},
        metadata={"agent_name": "researcher"},
    )
    out = project(ev, attribution, KORO)
    assert [e.kind for e in out] == ["subagent.text.delta"]
    assert out[0].run_id == KORO
    assert out[0].payload == {"segment_id": "seg-3", "subagent_id": "sub-9", "text": "sub"}


def test_chat_model_end_emits_text_completed() -> None:
    ev = _ev("on_chat_model_end", run_id="seg-z", data={"output": AIMessage(content="final")})
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.kind for e in out] == ["text.completed"]
    assert out[0].run_id == KORO
    assert out[0].payload == {"segment_id": "seg-z", "text": "final"}


def test_chat_model_end_subagent_emits_subagent_text_completed() -> None:
    attribution = SubagentAttribution()
    attribution.started("sub-1", "researcher")
    ev = _ev(
        "on_chat_model_end",
        data={"output": AIMessage(content="done")},
        metadata={"agent_name": "researcher"},
    )
    out = project(ev, attribution, KORO)
    assert [e.kind for e in out] == ["subagent.text.completed"]
    assert out[0].run_id == KORO
    assert out[0].payload == {"segment_id": "run-1", "subagent_id": "sub-1", "text": "done"}


def test_tool_start_todo_emits_todo_updated() -> None:
    ev = _ev(
        "on_tool_start",
        name="write_todos",
        data={"input": {"todos": [{"content": "a", "status": "pending"}]}},
    )
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.kind for e in out] == ["todo.updated"]
    assert out[0].run_id == KORO
    assert out[0].payload == {"todos": [{"content": "a", "status": "pending"}]}


def test_tool_start_tool_invoked_args_from_data_input() -> None:
    ev = _ev(
        "on_tool_start",
        name="search",
        run_id="tool-5",
        data={"input": {"query": "kokoro", "limit": 3}},
    )
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.kind for e in out] == ["tool.invoked"]
    # 信封 run_id 恒为 kokoro run_id；tool_id/segment_id 用 LC 每工具调用 id（确为不同值）。
    assert out[0].run_id == KORO
    assert out[0].payload == {
        "segment_id": "tool-5",
        "tool_id": "tool-5",
        "name": "search",
        "args": {"query": "kokoro", "limit": 3},
    }


def test_tool_start_subagent_started_registers_attribution() -> None:
    attribution = SubagentAttribution()
    ev = _ev(
        "on_tool_start",
        name="task",
        run_id="sub-x",
        data={"input": {"subagent_type": "researcher", "description": "查资料"}},
    )
    out = project(ev, attribution, KORO)
    assert [e.kind for e in out] == ["subagent.started"]
    assert out[0].run_id == KORO
    assert out[0].payload == {
        "segment_id": "sub-x",
        "subagent_id": "sub-x",
        "name": "researcher",
        "description": "查资料",
        "subagent_type": "researcher",
        "source": "built-in",
    }
    # attribution 已登记：后续 researcher 文本可命中
    text_ev = _ev(
        "on_chat_model_stream",
        data={"chunk": AIMessageChunk(content="x")},
        metadata={"agent_name": "researcher"},
    )
    assert project(text_ev, attribution, KORO)[0].kind == "subagent.text.delta"


def test_tool_start_runtime_subagent_started() -> None:
    attribution = SubagentAttribution()
    ev = _ev(
        "on_tool_start",
        name="agent",
        run_id="sa3",
        data={"input": {"name": "runtime-reviewer", "description": "运行时审稿"}},
    )
    out = project(ev, attribution, KORO)
    assert [e.kind for e in out] == ["subagent.started"]
    assert out[0].run_id == KORO
    assert out[0].payload == {
        "segment_id": "sa3",
        "subagent_id": "sa3",
        "name": "runtime-reviewer",
        "description": "运行时审稿",
        "subagent_type": "runtime-reviewer",
        "source": "runtime-custom",
    }


def test_tool_end_emits_tool_returned() -> None:
    ev = _ev(
        "on_tool_end",
        name="search",
        run_id="tool-9",
        data={"input": {}, "output": "result-text"},
    )
    out = project(ev, SubagentAttribution(), KORO)
    assert [e.kind for e in out] == ["tool.returned"]
    assert out[0].run_id == KORO
    assert out[0].payload == {
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
    result = out[0].payload["result"]
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
    assert [e.kind for e in out] == ["tool.returned"]
    assert out[0].run_id == KORO
    assert out[0].payload["is_error"] is True
    assert out[0].payload["result"] == "boom"
    assert out[0].payload["rejected"] is False


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
    assert [e.kind for e in out] == ["subagent.finished"]
    assert out[0].run_id == KORO
    assert out[0].payload == {
        "segment_id": "sub-x",
        "subagent_id": "sub-x",
        "name": "researcher",
        "subagent_type": "researcher",
        "source": "built-in",
    }
    # attribution 已清：researcher 文本不再命中
    text_ev = _ev(
        "on_chat_model_stream",
        data={"chunk": AIMessageChunk(content="x")},
        metadata={"agent_name": "researcher"},
    )
    assert project(text_ev, attribution, KORO)[0].kind == "text.delta"


def test_unknown_event_returns_empty() -> None:
    assert project(_ev("on_chain_start"), SubagentAttribution(), KORO) == []


def test_envelope_run_id_separate_from_segment_and_tool_id() -> None:
    # I1: 信封 run_id 用 kokoro run_id；segment_id/tool_id 用 LC 每调用 run_id，二者必须分离。
    lc_id = "lc-runnable-id"
    text_out = project(
        _ev("on_chat_model_stream", run_id=lc_id, data={"chunk": AIMessageChunk(content="hi")}),
        SubagentAttribution(),
        KORO,
    )
    assert text_out[0].run_id == KORO
    assert text_out[0].payload["segment_id"] == lc_id
    tool_out = project(
        _ev("on_tool_start", name="search", run_id=lc_id, data={"input": {"q": "x"}}),
        SubagentAttribution(),
        KORO,
    )
    assert tool_out[0].run_id == KORO
    assert tool_out[0].payload["tool_id"] == lc_id
    assert tool_out[0].payload["segment_id"] == lc_id
