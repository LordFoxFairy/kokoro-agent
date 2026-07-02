import pytest
from langchain_core.messages import AIMessage, HumanMessage
from pydantic import ValidationError

from kokoro_agent.execution.approvals import (
    ApprovalRequest,
    tool_approval_events,
    tool_approval_requests,
)
from kokoro_agent.tools.names import ASK_USER_TOOL_NAME


def _ai(tool_calls: list[dict[str, object]], *, id: str | None = None) -> AIMessage:
    return AIMessage(content="", tool_calls=tool_calls, id=id)


def _ar(
    name: str,
    args: dict[str, object],
    *,
    description: str = "review this tool",
    allowed_decisions: list[str] | None = None,
) -> ApprovalRequest:
    return ApprovalRequest.model_validate(
        {
            "name": name,
            "args": args,
            "description": description,
            "allowed_decisions": allowed_decisions or ["approve", "edit", "reject"],
        }
    )


def _raw_action(name: str, args: dict[str, object], description: str = "") -> dict[str, object]:
    return {"name": name, "args": args, "description": description}


def _raw_payload(*actions: dict[str, object]) -> dict[str, object]:
    return {
        "action_requests": list(actions),
        "review_configs": [
            {
                "action_name": str(action["name"]),
                "allowed_decisions": ["approve", "edit", "reject", "respond"],
            }
            for action in actions
        ],
    }


def test_single_pending_aligns_tool_id_and_segment() -> None:
    messages = [
        HumanMessage(content="go"),
        _ai([{"name": "danger", "args": {"x": 1}, "id": "call-A"}], id="seg-1"),
    ]
    action_requests = [_ar("danger", {"x": 1})]
    events = tool_approval_events(
        messages, action_requests, frozenset({"danger"}), request_id="r1"
    )
    assert len(events) == 1
    ev = events[0]
    assert ev.event == "tool_call_awaiting"
    assert ev.request_id == "r1"
    assert ev.data == {
        "segment_id": "seg-1",
        "tool_id": "call-A",
        "name": "danger",
        "args": {"x": 1},
        "description": "review this tool",
        "allowed_decisions": ["approve", "edit", "reject"],
        "kind": "tool_approval",
        "editable": True,
    }


def test_filters_to_interrupt_subsequence_and_aligns_in_order() -> None:
    # AIMessage 含 3 个 tool_call，其中 safe 自动批准未进 approval_tool_names；
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
    events = tool_approval_events(
        messages, action_requests, frozenset({"danger1", "danger2"}), request_id="r2"
    )
    # 逐工具：2 个 pending → 2 条顶层 tool_call_awaiting，同序对齐。
    assert [e.event for e in events] == ["tool_call_awaiting", "tool_call_awaiting"]
    assert [e.data for e in events] == [
        {
            "segment_id": "seg-x",
            "tool_id": "call-1",
            "name": "danger1",
            "args": {"a": 1},
            "description": "review this tool",
            "allowed_decisions": ["approve", "edit", "reject"],
            "kind": "tool_approval",
            "editable": True,
        },
        {
            "segment_id": "seg-x",
            "tool_id": "call-3",
            "name": "danger2",
            "args": {"c": 3},
            "description": "review this tool",
            "allowed_decisions": ["approve", "edit", "reject"],
            "kind": "tool_approval",
            "editable": True,
        },
    ]


def test_no_pending_yields_empty() -> None:
    messages = [_ai([{"name": "safe", "args": {}, "id": "call-1"}], id="s")]
    events = tool_approval_events(messages, [], frozenset({"danger"}), request_id="r3")
    assert events == []


def test_no_ai_message_yields_empty() -> None:
    messages = [HumanMessage(content="hi")]
    events = tool_approval_events(
        messages, [_ar("danger", {})], frozenset({"danger"}), request_id="r4"
    )
    assert events == []


def test_args_preserve_nested_structures() -> None:
    # 审批入参原样透传：嵌套对象/数组/null 全保留，不丢弃不转换。
    nested: dict[str, object] = {"filters": {"k": "v"}, "ids": [1, 2], "n": None}
    messages = [_ai([{"name": "danger", "args": nested, "id": "c1"}], id="s")]
    events = tool_approval_events(
        messages, [_ar("danger", nested)], frozenset({"danger"}), request_id="r5b"
    )
    assert len(events) == 1
    assert events[0].event == "tool_call_awaiting"
    assert events[0].data == {
        "segment_id": "s",
        "tool_id": "c1",
        "name": "danger",
        "args": {"filters": {"k": "v"}, "ids": [1, 2], "n": None},
        "description": "review this tool",
        "allowed_decisions": ["approve", "edit", "reject"],
        "kind": "tool_approval",
        "editable": True,
    }


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
    with pytest.raises(ValueError, match="HITL 审批对齐失配"):
        tool_approval_events(
            messages, action_requests, frozenset({"danger"}), request_id="r6"
        )


def test_interrupt_payload_parses_to_application_dto() -> None:
    requests = tool_approval_requests(
        [_raw_payload(_raw_action("danger", {"x": 1}, "do danger"))]
    )
    assert requests == [
        ApprovalRequest(
            name="danger",
            args={"x": 1},
            description="do danger",
            allowed_decisions=["approve", "edit", "reject", "respond"],
        )
    ]


def test_ask_user_awaiting_event_is_respond_only() -> None:
    messages = [
        _ai(
            [{"name": ASK_USER_TOOL_NAME, "args": {"question": "Pick one"}, "id": "ask-1"}],
            id="seg-ask",
        )
    ]
    events = tool_approval_events(
        messages,
        [
            _ar(
                ASK_USER_TOOL_NAME,
                {"question": "Pick one"},
                description="Ask the user",
                allowed_decisions=["respond"],
            )
        ],
        frozenset({ASK_USER_TOOL_NAME}),
        request_id="r-ask",
    )
    assert events[0].data == {
        "segment_id": "seg-ask",
        "tool_id": "ask-1",
        "name": ASK_USER_TOOL_NAME,
        "args": {"question": "Pick one"},
        "description": "Ask the user",
        "allowed_decisions": ["respond"],
        "kind": "ask_user",
        "editable": False,
    }


def test_interrupt_payload_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        tool_approval_requests(
            [_raw_payload(_raw_action("danger", {"x": 1}) | {"unexpected": True})]
        )


def test_interrupt_payload_requires_review_config_for_each_action() -> None:
    with pytest.raises(ValueError, match="review_configs missing"):
        tool_approval_requests(
            [
                {
                    "action_requests": [_raw_action("danger", {"x": 1})],
                    "review_configs": [],
                }
            ]
        )
