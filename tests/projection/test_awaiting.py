import pytest
from langchain.agents.middleware.human_in_the_loop import ActionRequest
from langchain_core.messages import AIMessage, HumanMessage

from kokoro_agent.application.projection.awaiting import awaiting_approval_events


def _ai(tool_calls: list[dict[str, object]], *, id: str | None = None) -> AIMessage:
    return AIMessage(content="", tool_calls=tool_calls, id=id)


def _ar(name: str, args: dict[str, object], description: str = "") -> ActionRequest:
    return {"name": name, "args": args, "description": description}


def test_single_pending_aligns_tool_id_and_segment() -> None:
    messages = [
        HumanMessage(content="go"),
        _ai([{"name": "danger", "args": {"x": 1}, "id": "call-A"}], id="seg-1"),
    ]
    action_requests = [_ar("danger", {"x": 1}, "do danger")]
    events = awaiting_approval_events(
        messages, action_requests, frozenset({"danger"}), request_id="r1"
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
            ],
            id="seg-x",
        )
    ]
    action_requests = [_ar("danger1", {"a": 1}), _ar("danger2", {"c": 3})]
    events = awaiting_approval_events(
        messages, action_requests, frozenset({"danger1", "danger2"}), request_id="r2"
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
    messages = [_ai([{"name": "safe", "args": {}, "id": "call-1"}], id="s")]
    events = awaiting_approval_events(messages, [], frozenset({"danger"}), request_id="r3")
    assert events == []


def test_no_ai_message_yields_empty() -> None:
    messages = [HumanMessage(content="hi")]
    events = awaiting_approval_events(
        messages, [_ar("danger", {})], frozenset({"danger"}), request_id="r4"
    )
    assert events == []


def test_args_drops_non_json_values() -> None:
    # wash_args：非 JSON 值（object()）在 wire 边界丢弃，标量保留。
    messages = [_ai([{"name": "danger", "args": {"ok": "v", "bad": object()}, "id": "c1"}], id="s")]
    action_requests = [_ar("danger", {"ok": "v", "bad": object()})]
    events = awaiting_approval_events(
        messages, action_requests, frozenset({"danger"}), request_id="r5"
    )
    assert events[0].data == {
        "status": "awaiting_approval",
        "segment_id": "s",
        "pending": [{"tool_id": "c1", "name": "danger", "args": {"ok": "v"}}],
    }


def test_args_preserve_nested_structures() -> None:
    # 锁定 awaiting 走 wash_args（非旧 _scalar_args）：嵌套对象/数组/null 全透传。
    nested: dict[str, object] = {"filters": {"k": "v"}, "ids": [1, 2], "n": None}
    messages = [_ai([{"name": "danger", "args": nested, "id": "c1"}], id="s")]
    events = awaiting_approval_events(
        messages, [_ar("danger", nested)], frozenset({"danger"}), request_id="r5b"
    )
    assert events[0].data["pending"] == [
        {"tool_id": "c1", "name": "danger", "args": {"filters": {"k": "v"}, "ids": [1, 2], "n": None}}
    ]


def test_length_mismatch_fails_loud() -> None:
    # pending(2) 与 action_requests(1) 不等长=wiring bug，须抛而非静默截断。
    messages = [
        _ai(
            [
                {"name": "danger", "args": {"a": 1}, "id": "c1"},
                {"name": "danger", "args": {"b": 2}, "id": "c2"},
            ],
            id="s",
        )
    ]
    action_requests = [_ar("danger", {"a": 1})]
    with pytest.raises(ValueError, match="awaiting 对齐失配"):
        awaiting_approval_events(
            messages, action_requests, frozenset({"danger"}), request_id="r6"
        )
