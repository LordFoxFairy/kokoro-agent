import pytest
from langchain_core.messages import AIMessage, HumanMessage

from kokoro_agent.application.projection.awaiting import awaiting_approval_events


def _ai(tool_calls: list[dict[str, object]]) -> AIMessage:
    return AIMessage(content="", tool_calls=tool_calls)


def test_single_pending_aligns_tool_id() -> None:
    messages = [
        HumanMessage(content="go"),
        _ai([{"name": "danger", "args": {"x": 1}, "id": "call-A"}]),
    ]
    action_requests = [{"name": "danger", "args": {"x": 1}, "description": "do danger"}]
    events = awaiting_approval_events(
        messages, action_requests, frozenset({"danger"}), segment_id="seg-1", request_id="r1"
    )
    assert len(events) == 1
    ev = events[0]
    assert ev.event == "agent_status"
    assert ev.request_id == "r1"
    assert ev.data == {
        "status": "awaiting_approval",
        "segment_id": "seg-1",
        "pending": [{"tool_id": "call-A", "name": "danger", "args": {"x": 1}}],
    }


def test_filters_to_interrupt_subsequence_and_aligns_in_order() -> None:
    # AIMessage 含 3 个 tool_call，其中 safe 自动批准未进 interrupt_on_names；
    # action_requests 是被命中的同序子序列（danger1, danger2）。
    messages = [
        _ai(
            [
                {"name": "danger1", "args": {"a": 1}, "id": "call-1"},
                {"name": "safe", "args": {"b": 2}, "id": "call-2"},
                {"name": "danger2", "args": {"c": 3}, "id": "call-3"},
            ]
        )
    ]
    action_requests = [
        {"name": "danger1", "args": {"a": 1}, "description": ""},
        {"name": "danger2", "args": {"c": 3}, "description": ""},
    ]
    events = awaiting_approval_events(
        messages,
        action_requests,
        frozenset({"danger1", "danger2"}),
        segment_id="seg-x",
        request_id="r2",
    )
    assert len(events) == 1
    assert events[0].data == {
        "status": "awaiting_approval",
        "segment_id": "seg-x",
        "pending": [
            {"tool_id": "call-1", "name": "danger1", "args": {"a": 1}},
            {"tool_id": "call-3", "name": "danger2", "args": {"c": 3}},
        ],
    }


def test_no_pending_yields_empty() -> None:
    messages = [_ai([{"name": "safe", "args": {}, "id": "call-1"}])]
    events = awaiting_approval_events(
        messages, [], frozenset({"danger"}), segment_id="seg", request_id="r3"
    )
    assert events == []


def test_no_ai_message_yields_empty() -> None:
    messages = [HumanMessage(content="hi")]
    events = awaiting_approval_events(
        messages,
        [{"name": "danger", "args": {}, "description": ""}],
        frozenset({"danger"}),
        segment_id="seg",
        request_id="r4",
    )
    assert events == []


def test_args_narrowed_to_json_scalars() -> None:
    messages = [_ai([{"name": "danger", "args": {"ok": "v", "bad": object()}, "id": "c1"}])]
    action_requests = [{"name": "danger", "args": {"ok": "v"}, "description": ""}]
    events = awaiting_approval_events(
        messages, action_requests, frozenset({"danger"}), segment_id="s", request_id="r5"
    )
    assert events[0].data == {
        "status": "awaiting_approval",
        "segment_id": "s",
        "pending": [{"tool_id": "c1", "name": "danger", "args": {"ok": "v"}}],
    }


def test_length_mismatch_fails_loud() -> None:
    # pending(2) 与 action_requests(1) 不等长=wiring bug，须抛而非静默截断。
    messages = [
        _ai(
            [
                {"name": "danger", "args": {"a": 1}, "id": "c1"},
                {"name": "danger", "args": {"b": 2}, "id": "c2"},
            ]
        )
    ]
    action_requests = [{"name": "danger", "args": {"a": 1}, "description": ""}]
    with pytest.raises(ValueError, match="awaiting 对齐失配"):
        awaiting_approval_events(
            messages, action_requests, frozenset({"danger"}), segment_id="s", request_id="r6"
        )
