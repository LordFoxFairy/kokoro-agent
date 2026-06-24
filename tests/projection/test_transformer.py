from __future__ import annotations

from dataclasses import dataclass

from langchain_core.messages import AIMessage

from kokoro_agent.application.projection.transformer import (
    TOOL_RESULT_MAX_CHARS,
    custom_event,
    final_text_event,
    run_done_event,
    run_error_event,
    run_started_event,
    stream_text_event,
    subagent_finished_event,
    subagent_started_event,
    todo_event,
    tool_end_event,
    tool_start_event,
    usage_delta,
)

KORO = "kokoro-run"


@dataclass
class _FakeTool:
    tool_call_id: str
    tool_name: str
    input: dict[str, object] | None = None
    output: object = None
    error: str | None = None


@dataclass
class _FakeSub:
    name: str | None
    trigger_call_id: str | None
    task_input: str | None = None
    status: str = "completed"


def _delta(text: str) -> dict[str, object]:
    return {"event": "content-block-delta", "index": 0, "delta": {"type": "text-delta", "text": text}}


def test_stream_text_event_passes_delta_through() -> None:
    ev = stream_text_event(_delta("hi"), segment_id="seg-7", request_id=KORO, subagent_id=None)
    assert ev is not None
    assert ev.event == "text_chunk"
    assert ev.request_id == KORO
    assert ev.data == {
        "segment_id": "seg-7",
        "content": [{"type": "text-delta", "text": "hi"}],
        "final": False,
    }


def test_stream_text_event_tags_subagent() -> None:
    ev = stream_text_event(_delta("x"), segment_id="seg-3", request_id=KORO, subagent_id="sub-9")
    assert ev is not None
    assert ev.data["subagent_id"] == "sub-9"


def test_stream_text_event_skips_non_delta_blocks() -> None:
    assert stream_text_event(
        {"event": "message-start", "id": "m1"}, segment_id="s", request_id=KORO, subagent_id=None
    ) is None


def test_stream_text_event_skips_tool_call_delta() -> None:
    block = {"event": "content-block-delta", "delta": {"type": "tool_call_chunk", "args": "{}"}}
    assert stream_text_event(block, segment_id="s", request_id=KORO, subagent_id=None) is None


def test_final_text_event_from_content_blocks() -> None:
    msg = AIMessage(content="final")
    ev = final_text_event(msg, segment_id="seg-z", request_id=KORO, subagent_id=None)
    assert ev is not None
    assert ev.data == {
        "segment_id": "seg-z",
        "content": [{"type": "text", "text": "final"}],
        "final": True,
    }


def test_final_text_event_none_for_tool_only_message() -> None:
    msg = AIMessage(
        content="",
        tool_calls=[{"name": "now", "args": {}, "id": "t1", "type": "tool_call"}],
    )
    assert final_text_event(msg, segment_id="s", request_id=KORO, subagent_id=None) is None


def test_usage_delta_from_message() -> None:
    msg = AIMessage(
        content="x", usage_metadata={"input_tokens": 3, "output_tokens": 5, "total_tokens": 8}
    )
    assert usage_delta(msg) == {"input_tokens": 3, "output_tokens": 5, "total_tokens": 8}


def test_usage_delta_empty_without_metadata() -> None:
    assert usage_delta(AIMessage(content="x")) == {}
    assert usage_delta(None) == {}


def test_todo_event() -> None:
    tc = _FakeTool("call-todo", "write_todos", {"todos": [{"content": "a", "status": "pending"}]})
    ev = todo_event(tc, request_id=KORO)
    assert ev.event == "agent_status"
    assert ev.data == {
        "status": "todo_updated",
        "segment_id": "call-todo",
        "todos": [{"content": "a", "status": "pending"}],
    }


def test_tool_start_event_uses_canonical_id() -> None:
    tc = _FakeTool("call-5", "search", {"query": "kokoro", "limit": 3})
    ev = tool_start_event(tc, request_id=KORO)
    assert ev.event == "tool_call_start"
    assert ev.data == {
        "segment_id": "call-5",
        "tool_id": "call-5",
        "name": "search",
        "args": {"query": "kokoro", "limit": 3},
    }


def test_tool_end_event_success() -> None:
    tc = _FakeTool("call-9", "search", {}, output="result-text")
    ev = tool_end_event(tc, request_id=KORO)
    assert ev.event == "tool_call_end"
    assert ev.data == {
        "segment_id": "call-9",
        "tool_id": "call-9",
        "name": "search",
        "result": "result-text",
        "is_error": False,
        "rejected": False,
    }


def test_tool_end_event_error() -> None:
    tc = _FakeTool("t-err", "search", {}, error="boom")
    ev = tool_end_event(tc, request_id=KORO)
    assert ev.data["is_error"] is True
    assert ev.data["result"] == "boom"
    assert ev.data["rejected"] is False


def test_tool_end_result_truncated() -> None:
    huge = "x" * (TOOL_RESULT_MAX_CHARS + 100)
    ev = tool_end_event(_FakeTool("t", "search", {}, output=huge), request_id=KORO)
    result = ev.data["result"]
    assert isinstance(result, str)
    assert len(result) < len(huge)
    assert result.startswith("x" * TOOL_RESULT_MAX_CHARS)


def test_subagent_started_event_built_in() -> None:
    sub = _FakeSub(name="researcher", trigger_call_id="sub-x", task_input="查资料")
    ev = subagent_started_event(sub, request_id=KORO)
    assert ev.event == "agent_status"
    assert ev.data == {
        "status": "subagent_started",
        "segment_id": "sub-x",
        "subagent_id": "sub-x",
        "name": "researcher",
        "description": "查资料",
        "subagent_type": "researcher",
        "source": "built-in",
    }


def test_subagent_unknown_name_is_runtime_custom() -> None:
    sub = _FakeSub(name="totally-unknown", trigger_call_id="sa3", task_input="运行时审稿")
    ev = subagent_started_event(sub, request_id=KORO)
    assert ev.data["source"] == "runtime-custom"


def test_subagent_finished_event() -> None:
    sub = _FakeSub(name="researcher", trigger_call_id="sub-x")
    ev = subagent_finished_event(sub, request_id=KORO)
    assert ev.data == {
        "status": "subagent_finished",
        "segment_id": "sub-x",
        "subagent_id": "sub-x",
        "name": "researcher",
        "subagent_type": "researcher",
        "source": "built-in",
    }


def test_custom_event_passthrough() -> None:
    ev = custom_event({"kind": "billing", "amount": 7}, request_id=KORO)
    assert ev is not None
    assert ev.event == "agent_status"
    assert ev.data == {"status": "custom", "custom": {"kind": "billing", "amount": 7}}


def test_custom_event_skips_dirty_payload() -> None:
    assert custom_event({"blob": b"raw"}, request_id=KORO) is None


def test_tool_start_preserves_args_verbatim() -> None:
    # 模型入参原样透传：嵌套对象/数组/null 全保留，不做任何丢弃或转换。
    tc = _FakeTool("c-1", "query", {"filters": {"k": "v"}, "ids": [1, 2], "n": None})
    ev = tool_start_event(tc, request_id=KORO)
    assert ev.data["args"] == {"filters": {"k": "v"}, "ids": [1, 2], "n": None}


def test_run_started_event() -> None:
    ev = run_started_event(KORO)
    assert ev.event == "agent_status"
    assert ev.request_id == KORO
    assert ev.data == {"status": "started"}


def test_run_done_event() -> None:
    ev = run_done_event({"input_tokens": 3, "output_tokens": 5}, request_id=KORO)
    assert ev.event == "agent_done"
    assert ev.data == {"status": "completed", "usage": {"input_tokens": 3, "output_tokens": 5}}


def test_run_error_event() -> None:
    ev = run_error_event(ValueError("boom"), request_id=KORO)
    assert ev.event == "agent_error"
    assert ev.data == {"error_kind": "ValueError", "message": "boom"}
