"""入站消息判别式联合解析测试：验证三种 kind 各自路由正确、畸形/未知 kind 返回 None。"""

from __future__ import annotations

from kokoro_agent.domain.json_payload import JsonObject
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.wire.run_request import RunCancel, RunResume, parse_inbound


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
    assert result.decision.edited_action == {"name": "bash", "args": {"cmd": "ls"}}


def test_parse_run_resume_reject() -> None:
    raw: JsonObject = {
        "kind": "run.resume",
        "run_id": "r4",
        "decision": {"type": "reject", "message": "nope"},
    }
    result = parse_inbound(raw)
    assert isinstance(result, RunResume)
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
