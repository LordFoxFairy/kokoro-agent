"""Safe AgentEvent -> GA chat fact projection."""

from __future__ import annotations

import json

from kokoro_agent.chat.models import assistant_message_id
from kokoro_agent.chat.projection import project_chat_fact
from kokoro_agent.contract import (
    MessageCompletedPayload,
    MessageDeltaPayload,
    RunFailedPayload,
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
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        timestamp=10,
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
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        timestamp=10,
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
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        timestamp=10,
        payload=ToolInvokedPayload(
            segment_id="segment",
            tool_id="tool-1",
            name="send_email",
            args={"token": "secret", "body": "private"},
        ),
    )

    assert projection is not None
    assert json.loads(projection.event.payload_json) == {
        "activity": "tool",
        "name": "send_email",
        "status": "started",
    }
    assert "secret" not in projection.event.payload_json


def test_private_execution_payloads_are_not_chat_facts() -> None:
    assert (
        project_chat_fact(
            namespace="ns",
            session_id="session-1",
            run_id="run-1",
            source_index=1,
            timestamp=10,
            payload=ThinkingDeltaPayload(segment_id="segment", delta="private thought"),
        )
        is None
    )
    assert (
        project_chat_fact(
            namespace="ns",
            session_id="session-1",
            run_id="run-1",
            source_index=1,
            timestamp=10,
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
        namespace="ns",
        session_id="session-1",
        run_id="run-1",
        source_index=1,
        timestamp=10,
        payload=RunFailedPayload(
            code="internal_error",
            error_kind="SecretProviderError",
            message="token sk-secret failed",
        ),
    )

    assert projection is not None
    assert json.loads(projection.event.payload_json) == {
        "status": "failed",
        "code": "internal_error",
    }
    assert "sk-secret" not in projection.event.payload_json
