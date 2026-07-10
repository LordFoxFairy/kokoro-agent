"""HITL 规格：pending 帧、awaiting 载荷、resume 对齐 fail-loud 矩阵、快照直发终态。"""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.types import Interrupt
from pydantic import JsonValue, TypeAdapter, ValidationError

from kokoro_agent.contract import ResumeDecision
from kokoro_agent.execution.approvals import (
    PendingFrame,
    align_decisions,
    approval_requests,
    awaiting_payloads,
    has_pending_interrupt,
    pending_frame,
    align_review_decisions,
    resolution_payloads,
    resume_command_decisions,
    review_entries,
    review_frame,
    review_resume_value,
)
from fakes import FakeState

_DECISION: TypeAdapter[ResumeDecision] = TypeAdapter(ResumeDecision)


def _decision(raw: dict[str, JsonValue]) -> ResumeDecision:
    return _DECISION.validate_python(raw)


def _interrupt(names: list[str]) -> Interrupt:
    return Interrupt(
        value={
            "action_requests": [
                {"name": name, "args": {"n": i}, "description": f"do {name}"}
                for i, name in enumerate(names)
            ],
            "review_configs": [
                {"action_name": name, "allowed_decisions": ["approve", "edit", "reject"]}
                for name in names
            ],
        }
    )


def _state(tool_calls: list[tuple[str, str]], names: list[str]) -> FakeState:
    ai = AIMessage(
        content="",
        id="seg-1",
        tool_calls=[{"name": name, "args": {}, "id": tool_id} for tool_id, name in tool_calls],
    )
    return FakeState(
        interrupts=(_interrupt(names),),
        values={"messages": [HumanMessage(content="go"), ai]},
    )


_TWO_TOOL_STATE = _state([("call-A", "danger"), ("call-B", "danger")], ["danger", "danger"])
_NAMES = frozenset({"danger"})


def test_has_pending_interrupt() -> None:
    assert has_pending_interrupt(_TWO_TOOL_STATE) is True
    assert has_pending_interrupt(FakeState()) is False


def test_pending_frame_filters_and_orders() -> None:
    state = _state(
        [("call-A", "danger"), ("call-x", "safe_tool"), ("call-B", "danger")],
        ["danger", "danger"],
    )
    frame = pending_frame(state, _NAMES)
    assert frame.segment_id == "seg-1"
    assert frame.tools == (("call-A", "danger"), ("call-B", "danger"))
    assert frame.tool_ids == ["call-A", "call-B"]


def test_pending_frame_no_messages_is_empty() -> None:
    frame = pending_frame(FakeState(values={}), _NAMES)
    assert frame == PendingFrame("", ())


def test_awaiting_description_is_tool_self_description_never_template() -> None:
    # wire 只带数据：description=工具自述（describe_tool 提供）；查不到发空串，
    # deepagents 的英文 interrupt 模板永不上 wire。
    state = _state([("call-A", "danger"), ("call-B", "harmless")], ["danger", "harmless"])
    names = frozenset({"danger", "harmless"})
    payloads = awaiting_payloads(
        state, names, describe_tool=lambda name: "危险操作，写真实文件" if name == "danger" else None
    )
    by_name = {p.name: p.description for p in payloads}
    assert by_name["danger"] == "危险操作，写真实文件"
    assert by_name["harmless"] == ""  # 无自述：空串，不是模板

    bare = awaiting_payloads(state, names)
    assert all(p.description == "" for p in bare)


def test_awaiting_payloads_carry_full_pending_set() -> None:
    payloads = awaiting_payloads(_TWO_TOOL_STATE, _NAMES)
    assert [p.tool_id for p in payloads] == ["call-A", "call-B"]
    # 每帧都携带同帧完整待批集合：web「凑齐才提交」读契约而非内嵌算法。
    assert all(p.pending_tool_ids == ["call-A", "call-B"] for p in payloads)
    assert all(p.segment_id == "seg-1" for p in payloads)
    assert all(p.kind == "tool_approval" for p in payloads)
    assert all(p.editable for p in payloads)


def test_awaiting_payloads_ask_user_kind() -> None:
    ai = AIMessage(
        content="", id="seg-2", tool_calls=[{"name": "ask_user_question", "args": {}, "id": "q1"}]
    )
    interrupt = Interrupt(
        value={
            "action_requests": [{"name": "ask_user_question", "args": {"question": "?"}, "description": "ask"}],
            "review_configs": [{"action_name": "ask_user_question", "allowed_decisions": ["respond"]}],
        }
    )
    state = FakeState(interrupts=(interrupt,), values={"messages": [ai]})
    payloads = awaiting_payloads(state, frozenset({"ask_user_question"}))
    assert payloads[0].kind == "ask_user_question"
    assert payloads[0].editable is False
    assert payloads[0].allowed_decisions == ["respond"]


def test_awaiting_alignment_mismatch_fails_loud() -> None:
    # interrupt 声称 2 个审批、消息里只有 1 个门控工具 → invariant 破裂。
    state = _state([("call-A", "danger")], ["danger", "danger"])
    with pytest.raises(ValueError, match="alignment mismatch"):
        awaiting_payloads(state, _NAMES)


def test_approval_requests_missing_review_config_fails_loud() -> None:
    interrupt = Interrupt(
        value={
            "action_requests": [{"name": "danger", "args": {}, "description": "d"}],
            "review_configs": [],
        }
    )
    with pytest.raises(ValueError, match="missing action names"):
        approval_requests((interrupt,))


@pytest.mark.parametrize(
    "value",
    [
        {},  # 缺两键
        {"action_requests": [], "review_configs": [], "extra": 1},  # 未知字段
        {"action_requests": [{"name": "x"}], "review_configs": []},  # action 缺字段
        "not a dict",
    ],
)
def test_approval_requests_malformed_interrupt_rejected(value: object) -> None:
    with pytest.raises(ValidationError):
        approval_requests((Interrupt(value=value),))


_FRAME = PendingFrame("seg-1", (("call-A", "danger"), ("call-B", "danger")))


def test_align_decisions_reorders_by_pending() -> None:
    ordered = align_decisions(
        [
            _decision({"type": "reject", "tool_id": "call-B", "reason": "no"}),
            _decision({"type": "approve", "tool_id": "call-A"}),
        ],
        _FRAME,
    )
    assert [d.tool_id for d in ordered] == ["call-A", "call-B"]


@pytest.mark.parametrize(
    "decisions",
    [
        [{"type": "approve", "tool_id": "call-A"}],  # 缺 call-B
        [
            {"type": "approve", "tool_id": "call-A"},
            {"type": "approve", "tool_id": "call-B"},
            {"type": "approve", "tool_id": "call-C"},
        ],  # 多余未知
        [
            {"type": "approve", "tool_id": "call-A"},
            {"type": "reject", "tool_id": "call-A", "reason": "dup"},
        ],  # 重复 tool_id
        [{"type": "approve", "tool_id": "ghost"}],  # 全未知
    ],
)
def test_align_decisions_fail_loud_matrix(decisions: list[dict[str, JsonValue]]) -> None:
    with pytest.raises(ValueError):
        align_decisions([_decision(d) for d in decisions], _FRAME)


def test_align_decisions_respond_only_for_ask_user() -> None:
    frame = PendingFrame("seg-1", (("call-A", "danger"), ("call-B", "ask_user_question")))
    # respond 用于普通审批工具即越界 fail-loud。
    with pytest.raises(ValueError, match="respond decision not allowed"):
        align_decisions(
            [
                _decision({"type": "respond", "tool_id": "call-A", "response": "x"}),
                _decision({"type": "respond", "tool_id": "call-B", "response": "y"}),
            ],
            frame,
        )
    # ask_user 只接 respond：approve 用于 ask_user 即越界 fail-loud。
    with pytest.raises(ValueError, match="only respond"):
        align_decisions(
            [
                _decision({"type": "approve", "tool_id": "call-A"}),
                _decision({"type": "approve", "tool_id": "call-B"}),
            ],
            frame,
        )


def test_resume_command_decision_shapes() -> None:
    ordered = [
        _decision({"type": "approve", "tool_id": "call-A"}),
        _decision({"type": "edit", "tool_id": "call-B", "args": {"x": 2}}),
    ]
    assert resume_command_decisions(ordered, _FRAME) == [
        {"type": "approve"},
        {"type": "edit", "edited_action": {"name": "danger", "args": {"x": 2}}},
    ]


def test_resume_command_approve_with_args_becomes_edit() -> None:
    ordered = [
        _decision({"type": "approve", "tool_id": "call-A", "args": {"safe": True}}),
        _decision({"type": "approve", "tool_id": "call-B"}),
    ]
    assert resume_command_decisions(ordered, _FRAME) == [
        {"type": "edit", "edited_action": {"name": "danger", "args": {"safe": True}}},
        {"type": "approve"},
    ]


def test_resume_command_reject_and_respond_messages() -> None:
    frame = PendingFrame("seg-1", (("call-A", "danger"), ("call-B", "ask_user_question")))
    ordered = [
        _decision({"type": "reject", "tool_id": "call-A"}),
        _decision({"type": "respond", "tool_id": "call-B", "response": "北京"}),
    ]
    assert resume_command_decisions(ordered, frame) == [
        {"type": "reject", "message": "rejected by user"},
        {"type": "respond", "message": "北京"},
    ]


def test_resolution_payloads_reject_and_respond_snapshot() -> None:
    frame = PendingFrame("seg-1", (("call-A", "danger"), ("call-B", "ask_user_question")))
    ordered = [
        _decision({"type": "reject", "tool_id": "call-A", "reason": "太危险"}),
        _decision({"type": "respond", "tool_id": "call-B", "response": "北京"}),
    ]
    rejected, responded = resolution_payloads(ordered, frame)
    assert rejected.rejected is True
    assert rejected.reject_reason == "太危险"
    assert rejected.result == "太危险"
    assert rejected.is_error is False
    assert responded.responded is True
    assert responded.result == "北京"
    assert responded.rejected is None


def test_resolution_payloads_skip_approve_and_edit() -> None:
    ordered = [
        _decision({"type": "approve", "tool_id": "call-A"}),
        _decision({"type": "edit", "tool_id": "call-B", "args": {}}),
    ]
    assert resolution_payloads(ordered, _FRAME) == []


# --- result_review 暂停帧 ---


def _review_interrupt(tool_id: str, name: str = "lookup", result: str = "raw") -> Interrupt:
    # review 预设的 HumanRequest 信封（request_id=tool_id）：与 ToolResultReviewMiddleware
    # 经 request_human(kind="review") 发出的 interrupt.value 同构。
    return Interrupt(
        value={
            "kokoro_human_request": {
                "request_id": tool_id,
                "kind": "review",
                "response_schema": None,
                "context": {
                    "name": name,
                    "args": {"q": "x"},
                    "result": result,
                    "is_error": False,
                },
            }
        }
    )


def _review_state(tool_id: str = "call-R") -> FakeState:
    ai = AIMessage(
        content="", id="seg-r", tool_calls=[{"name": "lookup", "args": {}, "id": tool_id}]
    )
    return FakeState(
        interrupts=(_review_interrupt(tool_id),),
        values={"messages": [HumanMessage(content="go"), ai]},
    )


def test_review_entries_none_for_approval_shape() -> None:
    assert review_entries(_TWO_TOOL_STATE.interrupts) is None


def test_review_entries_parse() -> None:
    entries = review_entries(_review_state().interrupts)
    assert entries is not None and entries[0].tool_id == "call-R"
    assert entries[0].result == "raw"


def test_review_entries_mixed_frame_fails_loud() -> None:
    with pytest.raises(ValueError, match="mixed"):
        review_entries((_interrupt(["danger"]), _review_interrupt("call-R")))


def test_review_entries_multiple_reviews_fail_loud() -> None:
    with pytest.raises(ValueError, match="multiple"):
        review_entries((_review_interrupt("a"), _review_interrupt("b")))


def test_review_awaiting_payload_carries_result() -> None:
    payloads = awaiting_payloads(_review_state(), frozenset())
    assert len(payloads) == 1
    payload = payloads[0]
    assert payload.kind == "result_review"
    assert payload.result == "raw"
    assert payload.segment_id == "seg-r"
    assert payload.allowed_decisions == ["approve", "respond", "reject"]
    assert payload.pending_tool_ids == ["call-R"]


def test_align_review_decisions_matrix() -> None:
    entries = review_entries(_review_state().interrupts)
    assert entries is not None
    frame = review_frame(_review_state(), entries)
    ordered = align_review_decisions([_decision({"type": "approve", "tool_id": "call-R"})], frame)
    assert ordered[0].tool_id == "call-R"
    with pytest.raises(ValueError):
        align_review_decisions([_decision({"type": "approve", "tool_id": "other"})], frame)
    with pytest.raises(ValueError, match="not allowed"):
        align_review_decisions(
            [_decision({"type": "edit", "tool_id": "call-R", "args": {}})], frame
        )


def test_review_resume_value_shapes() -> None:
    value = review_resume_value(
        [
            _decision({"type": "respond", "tool_id": "a", "response": "curated"}),
            _decision({"type": "reject", "tool_id": "b"}),
            _decision({"type": "approve", "tool_id": "c"}),
        ]
    )
    assert value == [
        {"tool_id": "a", "type": "respond", "response": "curated"},
        {"tool_id": "b", "type": "reject", "reason": "rejected by user"},
        {"tool_id": "c", "type": "approve"},
    ]
