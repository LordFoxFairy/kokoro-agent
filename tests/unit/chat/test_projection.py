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
    assert json.loads(projection.event.payload_json) == {
        "activity": "tool",
        "name": "send_email",
        "segment_id": "segment",
        "status": "started",
        "tool_id": "tool-1",
    }
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
