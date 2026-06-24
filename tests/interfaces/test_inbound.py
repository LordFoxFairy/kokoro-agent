"""入站消息判别式联合解析测试：验证三种 kind 各自路由正确、畸形/未知 kind 返回 None。"""

from __future__ import annotations

from kokoro_agent.domain.json_payload import JsonObject
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.interfaces.inbound import (
    EditDecision,
    RejectDecision,
    RespondDecision,
    RunCancel,
    RunResume,
    parse_inbound,
)


def test_parse_run_request() -> None:
    raw: JsonObject = {
        "kind": "run.request",
        "run_id": "r1",
        "session_id": "s1",
        "conversation_id": "c1",
        "input": "hello",
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunRequest)
    assert result.run_id == "r1"


def test_parse_run_resume_approve() -> None:
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r2",
        "decision": {"type": "approve"},
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunResume)
    assert result.decision.type == "approve"


def test_parse_run_resume_edit() -> None:
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r3",
        "decision": {
            "type": "edit",
            "edited_action": {"name": "bash", "args": {"cmd": "ls"}},
        },
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunResume)
    assert isinstance(result.decision, EditDecision)
    assert result.decision.edited_action == {"name": "bash", "args": {"cmd": "ls"}}


def test_parse_run_resume_reject() -> None:
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r4",
        "decision": {"type": "reject", "message": "nope"},
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunResume)
    assert isinstance(result.decision, RejectDecision)
    assert result.decision.message == "nope"


def test_parse_run_cancel() -> None:
    raw: JsonObject = {"kind": "run.cancel", "run_id": "r5"}
    result = parse_inbound(raw)
    assert isinstance(result, RunCancel)
    assert result.run_id == "r5"


def test_unknown_kind_returns_none() -> None:
    raw: JsonObject = {"kind": "run.unknown", "run_id": "r6"}
    assert parse_inbound(raw) is None


def test_malformed_returns_none() -> None:
    # run.request 缺少必填 run_id
    raw: JsonObject = {"kind": "run.request", "session_id": "s1", "conversation_id": "c1", "input": "hi"}
    assert parse_inbound(raw) is None


def test_extra_fields_forbidden() -> None:
    raw: JsonObject = {
        "kind": "run.request",
        "run_id": "r7",
        "session_id": "s7",
        "conversation_id": "c7",
        "input": "hi",
        "unexpected_field": "boom",
    }
    assert parse_inbound(raw) is None


# ── ResumeDecision 跨字段强校验测试 ──────────────────────────────────────────


def test_edit_without_edited_action_returns_none() -> None:
    # edit 型必须携带 edited_action；缺失应被 parse_inbound 收为 None。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r10",
        "decision": {"type": "edit"},
    }
    assert parse_inbound(raw) is None


def test_reject_without_message_returns_none() -> None:
    # reject 型必须携带 message；缺失应被收为 None。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r11",
        "decision": {"type": "reject"},
    }
    assert parse_inbound(raw) is None


def test_respond_without_message_returns_none() -> None:
    # respond 型必须携带 message；缺失应被收为 None。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r12",
        "decision": {"type": "respond"},
    }
    assert parse_inbound(raw) is None


def test_approve_with_edited_action_returns_none() -> None:
    # approve 型不应携带 edited_action，多余字段视为非法。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r13",
        "decision": {"type": "approve", "edited_action": {"name": "bash"}},
    }
    assert parse_inbound(raw) is None


def test_approve_with_message_returns_none() -> None:
    # approve 型不应携带 message，多余字段视为非法。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r14",
        "decision": {"type": "approve", "message": "extra"},
    }
    assert parse_inbound(raw) is None


def test_edit_with_edited_action_ok() -> None:
    # edit 型携带 edited_action 时合法。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r15",
        "decision": {"type": "edit", "edited_action": {"name": "bash", "args": {}}},
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunResume)
    assert isinstance(result.decision, EditDecision)
    assert result.decision.edited_action == {"name": "bash", "args": {}}


def test_reject_with_message_ok() -> None:
    # reject 型携带 message 时合法。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r16",
        "decision": {"type": "reject", "message": "not approved"},
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunResume)
    assert isinstance(result.decision, RejectDecision)
    assert result.decision.message == "not approved"


def test_respond_with_message_ok() -> None:
    # respond 型携带 message 时合法。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r17",
        "decision": {"type": "respond", "message": "please clarify"},
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunResume)
    assert isinstance(result.decision, RespondDecision)
    assert result.decision.message == "please clarify"


def test_approve_clean_ok() -> None:
    # approve 型不携带任何可选字段时合法。
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r18",
        "decision": {"type": "approve"},
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunResume)
    assert result.decision.type == "approve"
