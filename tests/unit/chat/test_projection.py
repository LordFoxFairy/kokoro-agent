"""Safe AgentEvent -> GA chat fact projection."""

from __future__ import annotations

import json

import pytest

from kokoro_agent.application.chat.mappers import wire_epoch_millis_to_utc
from kokoro_agent.domain.chat.models import assistant_message_id
from kokoro_agent.domain.chat.projection import project_chat_fact
from kokoro_agent.clients.system import ModelResolutionError
from kokoro_agent.execution.failures import run_failed_payload
from kokoro_agent.protocol import (
    MessageCompletedPayload,
    MessageDeltaPayload,
    ThinkingDeltaPayload,
    ToolInvokedPayload,
    ToolOutputDeltaPayload,
)


def test_assistant_message_id_is_stable_and_not_native_segment_id() -> None:
    first = assistant_message_id("ns", "run-1", "native-segment")

    assert first == assistant_message_id("ns", "run-1", "native-segment")
    assert first != "native-segment"
    assert first.startswith("msg_")


def test_message_delta_projects_to_safe_chat_event() -> None:
    projection = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        created_at=wire_epoch_millis_to_utc(10),
        payload=MessageDeltaPayload(segment_id="native-segment", delta="hello"),
    )

    assert projection is not None
    assert projection.message is None
    assert projection.event.event_type == "assistant.delta"
    assert projection.event.chat_message_id == assistant_message_id(
        "ns", "run-1", "native-segment"
    )
    assert json.loads(projection.event.payload_json) == {"delta": "hello"}


def test_message_completed_projects_final_chat_message() -> None:
    projection = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        created_at=wire_epoch_millis_to_utc(10),
        payload=MessageCompletedPayload(
            segment_id="native-segment", content="final answer"
        ),
    )

    assert projection is not None
    assert projection.event.event_type == "assistant.completed"
    assert projection.message is not None
    assert projection.message.role == "assistant"
    assert projection.message.status == "completed"
    assert projection.message.content == "final answer"


def test_tool_activity_never_persists_args() -> None:
    projection = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        created_at=wire_epoch_millis_to_utc(10),
        payload=ToolInvokedPayload(
            segment_id="segment",
            tool_id="tool-1",
            name="send_email",
            args={"token": "secret", "body": "private"},
        ),
    )

    assert projection is not None
    decoded = json.loads(projection.event.payload_json)
    assert set(decoded) == {
        "activity",
        "activity_id",
        "segment_id",
        "status",
        "display_code",
    }
    assert decoded["activity"] == "tool"
    assert decoded["display_code"] == "tool.execution"
    assert decoded["status"] == "running"
    assert (
        decoded["activity_id"].startswith("act_") and len(decoded["activity_id"]) == 68
    )
    assert decoded["segment_id"].startswith("seg_") and len(decoded["segment_id"]) == 68
    assert "secret" not in projection.event.payload_json


def test_private_execution_payloads_are_not_chat_facts() -> None:
    assert (
        project_chat_fact(
            tenant_id="tenant",
            namespace="ns",
            session_id="session-1",
            run_id="run-1",
            source_index=1,
            created_at=wire_epoch_millis_to_utc(10),
            payload=ThinkingDeltaPayload(segment_id="segment", delta="private thought"),
        )
        is None
    )
    assert (
        project_chat_fact(
            tenant_id="tenant",
            namespace="ns",
            session_id="session-1",
            run_id="run-1",
            source_index=1,
            created_at=wire_epoch_millis_to_utc(10),
            payload=ToolOutputDeltaPayload(
                segment_id="segment",
                tool_id="tool-1",
                name="execute",
                delta="private output",
            ),
        )
        is None
    )


def test_run_failure_exposes_stable_code_not_internal_error_text() -> None:
    projection = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        created_at=wire_epoch_millis_to_utc(10),
        payload=run_failed_payload(ValueError("token sk-secret failed")),
    )

    assert projection is not None
    assert json.loads(projection.event.payload_json) == {
        "status": "failed",
        "code": "internal_error",
        "retryable": False,
    }
    assert "sk-secret" not in projection.event.payload_json


@pytest.mark.parametrize("retryable", [True, False])
def test_failure_contract_chat_keeps_verified_retryable_without_raw_diagnostics(
    retryable: bool,
) -> None:
    error = ModelResolutionError("MODEL_UNAVAILABLE", retryable=retryable)
    payload = run_failed_payload(error)
    projection = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        created_at=wire_epoch_millis_to_utc(10),
        payload=payload,
    )
    assert projection is not None
    assert projection.event.event_type == "run.failed"
    assert json.loads(projection.event.payload_json) == {
        "status": "failed",
        "code": "model_unavailable",
        "retryable": retryable,
    }
    assert payload.model_dump() == {"code": "model_unavailable", "retryable": retryable}


def test_failure_contract_dynamic_exception_name_and_message_never_enter_wire() -> None:
    error_type = type("SENTINEL_PRIVATE_EXCEPTION", (Exception,), {})
    payload = run_failed_payload(error_type("SENTINEL_PASSWORD_TOKEN"))
    assert payload.model_dump() == {"code": "internal_error", "retryable": False}
    assert "SENTINEL" not in payload.model_dump_json()


def test_partial_native_approval_cannot_impersonate_complete_interaction_source() -> (
    None
):
    from kokoro_agent.protocol import ToolAwaitingApprovalPayload

    payload = ToolAwaitingApprovalPayload(
        segment_id="segment",
        tool_id="tool",
        name="lookup",
        kind="tool_approval",
        description="Review",
        allowed_decisions=["approve"],
        args={},
        editable=False,
        pending_tool_ids=["tool"],
    )
    with pytest.raises(ValueError, match="complete durable pause"):
        project_chat_fact(
            tenant_id="tenant",
            namespace="ns",
            session_id="session",
            run_id="run",
            source_index=1,
            created_at=wire_epoch_millis_to_utc(10),
            payload=payload,
        )


def test_committed_interaction_row_decoder_rejects_legacy_event_type() -> None:
    from kokoro_agent.infrastructure.chat_mappers import chat_event_from_row

    row = dict(
        chat_event_id="event",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        run_id="run",
        source_index=0,
        chat_message_id=None,
        event_type="interaction.state",
        payload_json="{}",
        created_at=wire_epoch_millis_to_utc(10),
        seq=1,
    )
    assert chat_event_from_row(row).event_type == "interaction.state"
    with pytest.raises(TypeError, match="unsupported"):
        chat_event_from_row({**row, "event_type": "interaction"})


@pytest.mark.parametrize(
    "todos", [[], [{"content": "用户可见计划", "status": "pending"}]]
)
def test_r91_complete_todo_table_is_a_chat_fact(todos: list[dict[str, str]]) -> None:
    from kokoro_agent.protocol import TodoUpdatedPayload

    projection = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=7,
        created_at=wire_epoch_millis_to_utc(10),
        payload=TodoUpdatedPayload.model_validate({"todos": todos}),
    )
    assert projection is not None, "a valid complete Todo table must be durable Chat"
    assert projection.event.event_type == "todo.updated"
    assert json.loads(projection.event.payload_json) == {"todos": todos}
    assert projection.message is None


@pytest.mark.parametrize(
    "branch", ["tool_start", "tool_end", "subagent_start", "subagent_end"]
)
def test_r91_activity_contains_only_safe_closed_fields(branch: str) -> None:
    import hashlib
    from kokoro_agent.protocol import (
        SubagentFinishedPayload,
        SubagentStartedPayload,
        ToolReturnedPayload,
    )

    raw_segment = "SENTINEL_INTERNAL_SEGMENT"
    raw_call = "SENTINEL_INTERNAL_CALL"
    if branch == "tool_start":
        payload = ToolInvokedPayload(
            segment_id=raw_segment,
            tool_id=raw_call,
            name="SENTINEL_TOOL_NAME",
            args={"token": "SENTINEL_CREDENTIAL"},
        )
    elif branch == "tool_end":
        payload = ToolReturnedPayload(
            segment_id=raw_segment,
            tool_id=raw_call,
            name="SENTINEL_TOOL_NAME",
            result="SENTINEL_RESULT /private/package/file",
            is_error=True,
            truncated=True,
        )
    elif branch == "subagent_start":
        payload = SubagentStartedPayload(
            segment_id=raw_segment,
            subagent_id=raw_call,
            name="SENTINEL_AGENT_NAME",
            description="SENTINEL_TASK_INPUT",
            subagent_type="SENTINEL_INTERNAL_TYPE",
            source="runtime-custom",
        )
    else:
        payload = SubagentFinishedPayload(
            segment_id=raw_segment,
            subagent_id=raw_call,
            name="SENTINEL_AGENT_NAME",
            subagent_type="SENTINEL_INTERNAL_TYPE",
            source="runtime-custom",
            failed=True,
            error="SENTINEL_STACK token=private",
        )
    projection = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=7,
        created_at=wire_epoch_millis_to_utc(10),
        payload=payload,
    )
    assert projection is not None
    wire = projection.event.payload_json
    assert "SENTINEL" not in wire, "raw identity/name/result/error must not enter Chat"
    activity = "tool" if branch.startswith("tool") else "subagent"

    # R90 D0 reference digest, not a substitute production projector.
    def digest(tag: str, parts: list[str]) -> str:
        material = [
            "kokoro-agent.safe-progress.v1",
            tag,
            "tenant",
            "ns",
            "session-1",
            "run-1",
            *parts,
        ]
        return hashlib.sha256(
            json.dumps(
                material,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8", errors="strict")
        ).hexdigest()

    assert json.loads(wire) == {
        "activity": activity,
        "activity_id": "act_" + digest(f"{activity}-activity", [raw_segment, raw_call]),
        "segment_id": "seg_" + digest("segment", [raw_segment]),
        "status": "running" if branch.endswith("start") else "failed",
        "display_code": f"{activity}.execution",
    }
    assert projection.message is None


def test_r91_subagent_private_thinking_remains_unprojected() -> None:
    from kokoro_agent.protocol import SubagentThinkingDeltaPayload

    assert (
        project_chat_fact(
            tenant_id="tenant",
            namespace="ns",
            session_id="session-1",
            run_id="run-1",
            source_index=8,
            created_at=wire_epoch_millis_to_utc(10),
            payload=SubagentThinkingDeltaPayload(
                segment_id="private-segment",
                subagent_id="private-call",
                delta="SENTINEL_PRIVATE_THOUGHT",
            ),
        )
        is None
    )


@pytest.mark.parametrize("content", ["a" * 1025, "😀" * 1025, "\ud800", "\udfff"])
def test_r93_todo_rejects_overlong_or_surrogate_content(content: str) -> None:
    from kokoro_agent.protocol import TodoUpdatedPayload

    with pytest.raises(ValueError):
        TodoUpdatedPayload.model_validate(
            {"todos": [{"content": content, "status": "pending"}]}
        )


@pytest.mark.parametrize("count", [0, 100, 101])
def test_r93_todo_item_count_boundary(count: int) -> None:
    from kokoro_agent.protocol import TodoUpdatedPayload

    value = {"todos": [{"content": "a", "status": "pending"}] * count}
    if count > 100:
        with pytest.raises(ValueError):
            TodoUpdatedPayload.model_validate(value)
    else:
        assert len(TodoUpdatedPayload.model_validate(value).todos) == count


@pytest.mark.parametrize("extra_byte", [False, True])
def test_r93_todo_complete_payload_utf8_budget(extra_byte: bool) -> None:
    from kokoro_agent.protocol import TodoUpdatedPayload

    value = {
        "todos": [{"content": "😀" * 1024, "status": "pending"} for _ in range(16)]
    }

    def encode() -> bytes:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")

    excess = len(encode()) - 65536
    ascii_count, residue = divmod(excess, 3)
    tail = "" if residue == 0 else "汉" if residue == 1 else "é"
    value["todos"][0]["content"] = (
        "a" * ascii_count + tail + "😀" * (1024 - ascii_count - bool(residue))
    )
    if extra_byte:
        value["todos"][0]["content"] = "é" + value["todos"][0]["content"][1:]
    assert len(encode()) == 65536 + int(extra_byte)
    assert all(len(item["content"]) == 1024 for item in value["todos"])
    if extra_byte:
        with pytest.raises(ValueError):
            TodoUpdatedPayload.model_validate(value)
    else:
        assert TodoUpdatedPayload.model_validate(value).model_dump() == value


@pytest.mark.parametrize("phase", ["resolving", "loading", "ready"])
@pytest.mark.parametrize("code", [None, "skill_resolve_failed", "skill_load_failed"])
def test_r93_skill_nonfailure_forbids_error_code_presence(
    phase: str, code: str | None
) -> None:
    from kokoro_agent.protocol import SkillPhase

    with pytest.raises(ValueError):
        SkillPhase.model_validate({"phase": phase, "error_code": code})


@pytest.mark.parametrize(
    "value",
    [
        {"phase": "failed"},
        {"phase": "failed", "error_code": None},
        {"phase": "failed", "error_code": "mcp_failed"},
        {"phase": "ready", "truncated": True},
    ],
)
def test_r93_skill_failure_requires_exact_code_and_forbids_unknown_fields(
    value: dict[str, object],
) -> None:
    from kokoro_agent.protocol import SkillPhase

    with pytest.raises(ValueError):
        SkillPhase.model_validate(value)


@pytest.mark.parametrize("code", ["skill_resolve_failed", "skill_load_failed"])
def test_r93_skill_failure_code_positive_control(code: str) -> None:
    from kokoro_agent.protocol import SkillPhase

    assert (
        SkillPhase.model_validate({"phase": "failed", "error_code": code}).error_code
        == code
    )


@pytest.mark.parametrize(
    "todos",
    [
        [],
        [
            {"content": "待办😀", "status": "pending"},
            {"content": "进行中", "status": "in_progress"},
            {"content": "已完成", "status": "completed"},
        ],
    ],
)
def test_r94_todo_projection_round_trips_through_postgres_row_decoder(
    todos: list[dict[str, str]],
) -> None:
    from kokoro_agent.domain.chat.models import chat_event_id
    from kokoro_agent.infrastructure.chat_mappers import chat_event_from_row
    from kokoro_agent.protocol import TodoUpdatedPayload

    projection = project_chat_fact(
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        run_id="run",
        source_index=7,
        created_at=wire_epoch_millis_to_utc(10),
        payload=TodoUpdatedPayload.model_validate({"todos": todos}),
    )
    assert projection is not None
    expected_bytes = json.dumps(
        {"todos": todos},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert projection.event.payload_json.encode("utf-8") == expected_bytes
    row = {
        **projection.event.model_dump(mode="python"),
        "chat_event_id": chat_event_id("ns", "run", 7),
        "seq": 23,
    }
    record = chat_event_from_row(row)
    assert record.event_type == "todo.updated"
    assert record.payload_json.encode("utf-8") == expected_bytes
    assert json.loads(record.payload_json) == {"todos": todos}
    assert (
        record.model_dump(exclude={"chat_event_id", "seq"})
        == projection.event.model_dump()
    )
    assert record.chat_event_id == row["chat_event_id"]
    assert record.seq == 23
    assert record.chat_message_id is None


@pytest.mark.parametrize(
    "event_type", ["unknown", "todo", "todos.updated", "interaction"]
)
def test_r94_todo_row_decoder_does_not_admit_unknown_or_alias_events(
    event_type: str,
) -> None:
    from kokoro_agent.infrastructure.chat_mappers import chat_event_from_row

    row = {
        "tenant_id": "tenant",
        "namespace": "ns",
        "session_id": "session",
        "run_id": "run",
        "source_index": 7,
        "chat_message_id": None,
        "event_type": event_type,
        "payload_json": '{"todos":[]}',
        "created_at": wire_epoch_millis_to_utc(10),
        "chat_event_id": "event",
        "seq": 23,
    }
    with pytest.raises(TypeError, match="event_type.*unsupported value"):
        chat_event_from_row(row)
