"""Agent-owned launch and replay responses have concrete machine types."""

from __future__ import annotations

import copy
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from jsonschema import validate
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import SecretStr

from kokoro_agent import contract_check
from kokoro_agent.domain.chat.models import ChatEventRecord
from kokoro_agent.domain.run.scope import runtime_namespace
from kokoro_agent.infrastructure.postgres_run_repository import RunRepositorySettings
from kokoro_agent.interfaces.http import server
from kokoro_agent.protocol.control import ExecutionIdentity, IdentityRef
from kokoro_agent.streams.factory import StreamSettings
from support.chat import FakeChatRepository
from support.fakes import FakeBus, FakeRunRepository


OPENAPI = Path(__file__).resolve().parents[2] / "contract/openapi/v1/openapi.json"


def _document() -> dict[str, Any]:
    return json.loads(OPENAPI.read_text(encoding="utf-8"))


def _response_ref(document: dict[str, Any], path: str, method: str, status: str) -> str:
    return document["paths"][path][method]["responses"][status]["content"][
        "application/json"
    ]["schema"]["$ref"]


def _validate_response(document: dict[str, Any], ref: str, payload: object) -> None:
    validate(payload, {"$ref": ref, "components": document["components"]})


def test_launch_and_replay_success_responses_bind_existing_typed_data() -> None:
    document = _document()
    schemas = document["components"]["schemas"]
    for path, method, status, envelope, data in (
        ("/v1/runs", "post", "202", "LaunchReceiptEnvelope", "LaunchReceipt"),
        (
            "/v1/sessions/{session_id}/events",
            "get",
            "200",
            "ReplayPageEnvelope",
            "ReplayPage",
        ),
    ):
        assert _response_ref(document, path, method, status) == (
            f"#/components/schemas/{envelope}"
        )
        assert schemas[envelope] == {
            "type": "object",
            "required": ["data", "meta"],
            "properties": {
                "data": {"$ref": f"#/components/schemas/{data}"},
                "meta": {"$ref": "#/components/schemas/Meta"},
            },
            "additionalProperties": False,
        }


@pytest.mark.parametrize("operation", ["launch", "replay"])
def test_checker_rejects_generic_data_fallback(operation: str) -> None:
    document = _document()
    changed = copy.deepcopy(document)
    if operation == "launch":
        path, method, status = "/v1/runs", "post", "202"
    else:
        path, method, status = "/v1/sessions/{session_id}/events", "get", "200"
    changed["paths"][path][method]["responses"][status]["content"]["application/json"][
        "schema"
    ] = {"$ref": "#/components/schemas/DataEnvelope"}
    with pytest.raises(ValueError, match="typed response"):
        contract_check.validate_openapi_document(changed)


@pytest.mark.parametrize("schema", ["Meta", "LaunchReceipt", "ReplayPage", "ChatEvent"])
def test_checker_rejects_weakened_referenced_chat_schema(schema: str) -> None:
    changed = _document()
    changed["components"]["schemas"][schema] = {
        "type": "object",
        "additionalProperties": True,
    }
    with pytest.raises(ValueError, match="typed response"):
        contract_check.validate_openapi_document(changed)


@pytest.mark.asyncio
async def test_actual_http_dispatch_serializes_typed_launch_and_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = FakeBus()
    run_repository = FakeRunRepository()
    chat_repository = FakeChatRepository()

    @asynccontextmanager
    async def run_context(_settings: object) -> AsyncGenerator[FakeRunRepository]:
        yield run_repository

    @asynccontextmanager
    async def chat_context(_settings: object) -> AsyncGenerator[FakeChatRepository]:
        yield chat_repository

    def stream_factory(_settings: object) -> FakeBus:
        return bus

    monkeypatch.setattr(server, "make_stream", stream_factory)
    monkeypatch.setattr(server, "make_run_repository", run_context)
    monkeypatch.setattr(server, "make_chat_repository", chat_context)

    class Config:
        stream = StreamSettings(redis_url="redis://unused")
        run_repository = RunRepositorySettings(
            database_url="postgres://unused", schema_name="unused", lease_ttl_ms=1
        )
        database_url = "postgres://unused"
        database_schema = "unused"
        internal_secret_agent = SecretStr("secret")

    headers = {
        "authorization": "Bearer secret",
        "x-kokoro-tenant-ref": "tenant",
        "x-kokoro-subject-ref": "subject",
        "x-kokoro-actor-ref": "actor",
        "x-kokoro-identity-assertion-ref": "assertion",
        "x-request-id": "request-typed-response",
    }
    status, launch = await server.dispatch_request(
        Config(),
        "POST",
        "/v1/runs",
        {},
        headers,
        {
            "request_id": "request-1",
            "run_id": "run-1",
            "session_id": "session-1",
            "feature_key": "chat",
            "selected_skill_source_refs": [],
            "message_id": "message-1",
            "content": "hello",
        },
    )
    assert status == 202
    assert launch == {
        "data": {"run_id": "run-1", "session_id": "session-1", "replayed": False},
        "meta": {"request_id": "request-typed-response"},
    }
    document = _document()
    _validate_response(
        document, _response_ref(document, "/v1/runs", "post", "202"), launch
    )

    chat_repository.records.append(
        ChatEventRecord(
            tenant_id="tenant",
            namespace=runtime_namespace(
                ExecutionIdentity(
                    tenant_ref="tenant",
                    actor=IdentityRef(kind="user", opaque_ref="actor"),
                    subject=IdentityRef(kind="user", opaque_ref="subject"),
                    identity_assertion_ref="assertion",
                )
            ),
            session_id="session-1",
            run_id="run-1",
            source_index=0,
            chat_message_id=None,
            event_type="run.started",
            payload_json="{}",
            created_at=datetime.now(UTC),
            chat_event_id="event-1",
            seq=1,
        )
    )

    status, replay = await server.dispatch_request(
        Config(), "GET", "/v1/sessions/session-1/events", {}, headers, None
    )
    assert status == 200
    event = cast(dict[str, Any], cast(dict[str, Any], replay)["data"])["events"][0]
    assert isinstance(event["created_at"], int)
    assert replay == {
        "data": {
            "events": [
                {
                    "chat_event_id": "event-1",
                    "session_id": "session-1",
                    "run_id": "run-1",
                    "source_index": 0,
                    "chat_message_id": None,
                    "event_type": "run.started",
                    "payload_json": "{}",
                    "seq": 1,
                    "created_at": event["created_at"],
                }
            ],
            "next_seq": 1,
            "watermark": 1,
        },
        "meta": {"request_id": "request-typed-response"},
    }
    _validate_response(
        document,
        _response_ref(document, "/v1/sessions/{session_id}/events", "get", "200"),
        replay,
    )


# R31 full replacement source: payload_json has a real owner-decoded schema.
def _hitl4_state(phase: str = "waiting") -> dict[str, Any]:
    groups: list[dict[str, Any]] = [
        {
            "group_id": "group-1",
            "items": [
                {
                    "item_id": "item-1",
                    "request_id": "call-1",
                    "kind": "tool_approval",
                    "allowed_decisions": ["approve", "reject"],
                    "display": {
                        "name": "lookup",
                        "description": "Review lookup",
                        "editable": False,
                        "input_schema": {"type": "object", "properties": {}},
                    },
                    "validation": {
                        "code": "json_schema_invalid",
                        "instance_path": ["query"],
                    },
                }
            ],
        }
    ]
    return {
        "interaction_revision": 2,
        "pause_revision": 1,
        "pause_ref": "pause-opaque-1",
        "phase": phase,
        "groups": groups if phase in {"waiting", "resuming"} else [],
        "action_result": (
            {"command_id": "command-1", "pause_revision": 1, "kind": "accepted"}
            if phase == "resuming"
            else None
        ),
    }


def _hitl4_state_schema() -> dict[str, Any]:
    document = _document()
    schema = document["components"]["schemas"]["ChatEvent"]
    mapping = schema["x-kokoro-decoded-payloads"]["mapping"]
    assert mapping.get("interaction.state") == (
        "#/components/schemas/ChatInteractionState"
    ), "HITL4 needs a strict full-state decoded payload, not untyped payload_json"
    return {"$ref": mapping["interaction.state"], "components": document["components"]}


def test_hitl4_chat_source_replaces_old_interaction_type() -> None:
    event = _document()["components"]["schemas"]["ChatEvent"]
    values = event["properties"]["event_type"]["enum"]
    assert "interaction.state" in values
    assert "interaction" not in values, "No old source/alias alongside HITL4"


@pytest.mark.parametrize("phase", ["waiting", "resuming", "active", "terminal"])
def test_hitl4_source_validates_complete_phase_and_collection(phase: str) -> None:
    validate(_hitl4_state(phase), _hitl4_state_schema())


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_groups",
        "empty_waiting",
        "active_with_items",
        "missing_ref",
        "zero_revision",
        "raw_args",
        "raw_locator",
        "validation_value",
        "unknown_field",
    ],
)
def test_hitl4_source_rejects_partial_or_private_payloads(mutation: str) -> None:
    schema = _hitl4_state_schema()
    valid = _hitl4_state()
    validate(valid, schema)
    invalid = copy.deepcopy(valid)
    if mutation == "missing_groups":
        del invalid["groups"]
    elif mutation == "empty_waiting":
        invalid["groups"] = []
    elif mutation == "active_with_items":
        invalid["phase"] = "active"
    elif mutation == "missing_ref":
        del invalid["pause_ref"]
    elif mutation == "zero_revision":
        invalid["interaction_revision"] = 0
    elif mutation == "raw_args":
        invalid["groups"][0]["items"][0]["args"] = {"token": "private-marker"}
    elif mutation == "raw_locator":
        invalid["checkpoint_id"] = "private-checkpoint"
    elif mutation == "validation_value":
        invalid["groups"][0]["items"][0]["validation"]["value"] = "private-marker"
    else:
        invalid["unregistered"] = True
    with pytest.raises(SchemaValidationError):
        validate(invalid, schema)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_action",
        "resuming_null",
        "resuming_consumed",
        "preview_without_source",
        "duplicate_allowed",
        "empty_input_schema_missing",
        "null_waiting_ref",
    ],
)
def test_hitl4_machine_and_runtime_reject_invalid_action_or_display(
    mutation: str,
) -> None:
    from pydantic import ValidationError
    from kokoro_agent.protocol.events import ChatInteractionState

    value = _hitl4_state("resuming")
    schema = _hitl4_state_schema()
    validate(value, schema)
    ChatInteractionState.model_validate(value)
    if mutation == "missing_action":
        del value["action_result"]
    elif mutation == "resuming_null":
        value["action_result"] = None
    elif mutation == "resuming_consumed":
        value["action_result"]["kind"] = "native_consumed"
    elif mutation == "preview_without_source":
        value["groups"][0]["items"][0]["display"]["result_preview"] = "safe result"
    elif mutation == "duplicate_allowed":
        value["groups"][0]["items"][0]["allowed_decisions"] = ["approve", "approve"]
    elif mutation == "empty_input_schema_missing":
        del value["groups"][0]["items"][0]["display"]["input_schema"]
    else:
        value["pause_ref"] = None
    with pytest.raises(SchemaValidationError):
        validate(value, schema)
    with pytest.raises(ValidationError):
        ChatInteractionState.model_validate(value)
