"""契约门禁：生成物 strict 模型的崩溃/空值/枚举边界矩阵——agent 侧词汇即 wire。"""

from __future__ import annotations

import pytest
from pydantic import JsonValue, ValidationError

from kokoro_agent.contract import (
    BLOCK_MS,
    CONSUMER_GROUP,
    LIVE_MAXLEN,
    REQUESTS_MAXLEN,
    REQUESTS_STREAM,
    RUN_EVENTS_MAXLEN,
    agent_event_adapter,
    event_id,
    inbound_adapter,
    live_stream,
    run_control_stream,
    run_events_stream,
)


def _envelope(kind: str, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
    return {"kind": kind, "run_id": "r1", "index": 0, "timestamp": 1, "payload": payload}


_AWAITING_PAYLOAD: dict[str, JsonValue] = {
    "segment_id": "seg",
    "tool_id": "t1",
    "name": "execute",
    "args": {"cmd": "ls"},
    "description": "run",
    "allowed_decisions": ["approve", "edit", "reject"],
    "kind": "tool_approval",
    "editable": True,
    "pending_tool_ids": ["t1", "t2"],
}

_ALL_KINDS: list[tuple[str, dict[str, JsonValue]]] = [
    ("run.started", {}),
    ("thinking.delta", {"segment_id": "seg", "delta": "hmm"}),
    ("message.delta", {"segment_id": "seg", "delta": "hi"}),
    ("message.completed", {"segment_id": "seg", "content": "hi there"}),
    ("tool.invoked", {"segment_id": "seg", "tool_id": "t1", "name": "execute", "args": {}}),
    ("tool.output.delta", {"segment_id": "seg", "tool_id": "t1", "name": "execute", "delta": "ln\n"}),
    ("tool.awaiting_approval", _AWAITING_PAYLOAD),
    (
        "tool.returned",
        {"segment_id": "seg", "tool_id": "t1", "name": "execute", "result": "ok", "is_error": False},
    ),
    ("todo.updated", {"todos": [{"content": "plan", "status": "pending"}]}),
    (
        "subagent.started",
        {
            "segment_id": "seg",
            "subagent_id": "sub1",
            "name": "researcher",
            "description": "dig",
            "subagent_type": "researcher",
            "source": "built-in",
        },
    ),
    (
        "subagent.finished",
        {
            "segment_id": "seg",
            "subagent_id": "sub1",
            "name": "researcher",
            "subagent_type": "researcher",
            "source": "runtime-custom",
        },
    ),
    ("subagent.text.delta", {"segment_id": "seg", "subagent_id": "sub1", "text": "x"}),
    ("subagent.text.completed", {"segment_id": "seg", "subagent_id": "sub1", "text": "xy"}),
    (
        "run.completed",
        {"status": "completed", "token_usage": {"input_tokens": 1, "output_tokens": 2}},
    ),
    ("run.failed", {"error_kind": "ValueError", "message": "boom"}),
]


@pytest.mark.parametrize(("kind", "payload"), _ALL_KINDS, ids=[k for k, _ in _ALL_KINDS])
def test_all_wire_kinds_round_trip(kind: str, payload: dict[str, JsonValue]) -> None:
    event = agent_event_adapter.validate_python(_envelope(kind, payload))
    assert event.kind == kind
    assert event.run_id == "r1"
    dumped = event.model_dump()
    assert agent_event_adapter.validate_python(dumped).model_dump() == dumped


@pytest.mark.parametrize(
    "envelope",
    [
        _envelope("agent_status", {"status": "started"}),  # 旧词汇彻底非法
        _envelope("text.delta", {"segment_id": "seg", "delta": "hi"}),  # 旧 kind 名作废
        _envelope("message.delta", {"segment_id": "seg", "delta": "hi", "role": "user"}),  # 未知字段
        _envelope("message.delta", {"segment_id": "seg"}),  # 缺必填
        _envelope("message.delta", {"segment_id": "", "delta": "hi"}),  # 空 segment_id
        {**_envelope("message.delta", {"segment_id": "s", "delta": "x"}), "extra": 1},  # 信封污染
        {**_envelope("message.delta", {"segment_id": "s", "delta": "x"}), "index": -1},  # 负 index
        {**_envelope("message.delta", {"segment_id": "s", "delta": "x"}), "run_id": ""},  # 空 run_id
        _envelope("run.completed", {"status": "done"}),  # 非法终态枚举
        _envelope("run.failed", {"error_kind": "E", "message": ""}),  # 空错误消息
        _envelope("todo.updated", {"todos": [{"content": "x", "status": "later"}]}),  # 非法 todo 态
        _envelope(
            "subagent.started",
            {
                "segment_id": "s",
                "subagent_id": "b",
                "name": "n",
                "description": "",
                "subagent_type": "n",
                "source": "unknown",
            },
        ),  # 非法 source
    ],
)
def test_malformed_events_rejected(envelope: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        agent_event_adapter.validate_python(envelope)


def test_missing_kind_rejected() -> None:
    with pytest.raises(ValidationError):
        agent_event_adapter.validate_python({"run_id": "r1", "index": 0, "timestamp": 1, "payload": {}})


@pytest.mark.parametrize("status", ["completed", "cancelled"])
def test_run_completed_terminal_statuses(status: str) -> None:
    event = agent_event_adapter.validate_python(
        _envelope("run.completed", {"status": status, "token_usage": None})
    )
    assert event.kind == "run.completed"


# --- 控制面（v2.1 形状：thread_id + runtime + context，禁 conversation_id/session_id/execution_style） ---

_MODEL: dict[str, JsonValue] = {"provider": "anthropic", "name": "claude"}
_PERMISSIONS: dict[str, JsonValue] = {
    "approval_tools": ["execute"],
    "review_tools": [],
    "subagent_create": "deny",
    "filesystem": "read_only",
}
_RUNTIME: dict[str, JsonValue] = {
    "model": _MODEL,
    "tools": [],
    "skills": [],
    "mcp": [],
    "subagents": [],
    "backend": "state",
    "permissions": _PERMISSIONS,
}
_CONTEXT: dict[str, JsonValue] = {"namespace": "local:s1", "session_id": "s1"}


def _request(**extra: JsonValue) -> dict[str, JsonValue]:
    return {
        "kind": "run.request",
        "run_id": "r1",
        "thread_id": "c1",
        "input": {"message_id": "m1", "content": "hi"},
        "runtime": _RUNTIME,
        "context": _CONTEXT,
        **extra,
    }


def _control(kind: str, **extra: JsonValue) -> dict[str, JsonValue]:
    return {"kind": kind, "run_id": "r1", "thread_id": "c1", **extra}


def test_run_request_parses() -> None:
    msg = inbound_adapter.validate_python(_request())
    assert msg.kind == "run.request"


@pytest.mark.parametrize(
    "decision",
    [
        {"type": "approve", "tool_id": "t1"},
        {"type": "approve", "tool_id": "t1", "args": {"x": 1}},
        {"type": "edit", "tool_id": "t1", "args": {"x": 2}},
        {"type": "reject", "tool_id": "t1"},
        {"type": "reject", "tool_id": "t1", "reason": "no"},
        {"type": "respond", "tool_id": "t1", "response": "42"},
    ],
)
def test_resume_decisions_accepted(decision: dict[str, JsonValue]) -> None:
    msg = inbound_adapter.validate_python(_control("run.resume", decisions=[decision]))
    assert msg.kind == "run.resume"


@pytest.mark.parametrize(
    "raw",
    [
        _control("run.resume", decisions=[]),  # 空决策列表
        _control("run.resume", decisions=[{"type": "veto", "tool_id": "t1"}]),  # 未知判别
        _control("run.resume", decisions=[{"type": "edit", "tool_id": "t1"}]),  # edit 缺 args
        _control("run.resume", decisions=[{"type": "respond", "tool_id": "t1", "response": ""}]),
        _request(conversation_id="c1"),  # 旧字段污染
        _request(execution_style="fast"),  # 旧字段污染
        {**_request(), "input": {"content": "hi"}},  # 缺 message_id
        {**_request(), "runtime": {**_RUNTIME, "backend": "docker"}},  # 非法 backend 枚举
        _control("run.cancel", extra_field=1),  # 信封污染
        {"kind": "run.pause", "run_id": "r1", "thread_id": "c1"},  # 未知 kind
    ],
)
def test_malformed_control_rejected(raw: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        inbound_adapter.validate_python(raw)


def test_run_cancel_parses() -> None:
    msg = inbound_adapter.validate_python(_control("run.cancel"))
    assert msg.kind == "run.cancel"


# --- 流常量：与 contract/README 立法一致 ---


def test_stream_constants() -> None:
    assert REQUESTS_STREAM == "kokoro:runs:requests"
    assert CONSUMER_GROUP == "kokoro-agent"
    assert REQUESTS_MAXLEN == 10000
    assert RUN_EVENTS_MAXLEN == 10000
    assert LIVE_MAXLEN == 512
    assert BLOCK_MS == 1000
    assert run_events_stream("abc") == "kokoro:run:abc:events"
    assert run_control_stream("abc") == "kokoro:run:abc:control"
    assert live_stream("s1") == "kokoro:session:s1:live"
    assert event_id("r1", 7) == "r1:7"
