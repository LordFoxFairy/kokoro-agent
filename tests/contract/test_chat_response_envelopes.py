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


def _r95_declared_decoded_payload(
    document: dict[str, Any], kind: str, value: object
) -> None:
    # HTTP5 requires an owner binding before any decoded validation. Keep this
    # independent JSON Schema check alongside the raw owner-checker coverage.
    mapping = document["components"]["schemas"]["ChatEvent"][
        "x-kokoro-decoded-payloads"
    ]["mapping"]
    assert kind in mapping, "HTTP5 decoded profile binding is required"
    _validate_response(document, mapping[kind], value)


def _r95_execution_payload(kind: str, state: str, code: str | None = None):
    from kokoro_agent import protocol

    if kind == "todo":
        return protocol.TodoUpdatedPayload.model_validate(
            {
                "todos": []
                if state == "empty"
                else [
                    {"content": "计划😀", "status": "pending"},
                    {"content": "执行", "status": "in_progress"},
                    {"content": "完成", "status": "completed"},
                ]
            }
        )
    if kind == "skill":
        value: dict[str, object] = {
            "anchor_source_index": 7,
            "source_refs": ["skill:one", "skill:two"],
            "phase": state,
        }
        if code is not None:
            value["error_code"] = code
        return protocol.SkillProgressPayload.model_validate(value)
    if kind == "tool":
        if state == "running":
            return protocol.ToolInvokedPayload(
                segment_id="private-segment",
                tool_id="private-tool",
                name="SENTINEL_PRIVATE_NAME",
                args={"secret": "SENTINEL_PRIVATE_ARGS"},
            )
        return protocol.ToolReturnedPayload(
            segment_id="private-segment",
            tool_id="private-tool",
            name="SENTINEL_PRIVATE_NAME",
            result="SENTINEL_PRIVATE_RESULT",
            is_error=state == "failed",
        )
    if state == "running":
        return protocol.SubagentStartedPayload(
            segment_id="private-segment",
            subagent_id="private-subagent",
            name="SENTINEL_PRIVATE_NAME",
            description="SENTINEL_PRIVATE_DESCRIPTION",
            subagent_type="private-type",
            source="runtime-custom",
        )
    return protocol.SubagentFinishedPayload(
        segment_id="private-segment",
        subagent_id="private-subagent",
        name="SENTINEL_PRIVATE_NAME",
        subagent_type="private-type",
        source="runtime-custom",
        failed=state == "failed",
        error="SENTINEL_PRIVATE_ERROR" if state == "failed" else None,
    )


def _r95_projection(kind: str, state: str, code: str | None = None):
    from kokoro_agent.domain.chat.projection import project_chat_fact

    identity = ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref="subject"),
        identity_assertion_ref="assertion",
    )
    result = project_chat_fact(
        tenant_id="tenant",
        namespace=runtime_namespace(identity),
        session_id="session-1",
        run_id="run-1",
        source_index=7,
        created_at=datetime(2026, 10, 2, tzinfo=UTC),
        payload=_r95_execution_payload(kind, state, code),
    )
    assert result is not None
    return result


@pytest.mark.parametrize(
    "kind,state,code",
    [
        ("todo", "empty", None),
        ("todo", "full", None),
        ("tool", "running", None),
        ("tool", "completed", None),
        ("tool", "failed", None),
        ("subagent", "running", None),
        ("subagent", "completed", None),
        ("subagent", "failed", None),
        ("skill", "resolving", None),
        ("skill", "loading", None),
        ("skill", "ready", None),
        ("skill", "failed", "skill_resolve_failed"),
        ("skill", "failed", "skill_load_failed"),
    ],
)
async def test_r95_actual_replay_response_obeys_http5_progress_machine(
    monkeypatch: pytest.MonkeyPatch, kind: str, state: str, code: str | None
) -> None:
    import psycopg
    from typing import NoReturn

    unexpected_connections: list[str] = []

    async def reject_real_database(*_args: object, **_kwargs: object) -> NoReturn:
        unexpected_connections.append("postgres")
        raise AssertionError("R95 contract test attempted a real database connection")

    # Lowest driver entry is patched before dispatch: an omitted repository
    # factory must fail deterministically without DNS/TCP or resource access.
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", reject_real_database)
    repository = FakeChatRepository()
    projection = _r95_projection(kind, state, code)
    await repository.append(projection)

    @asynccontextmanager
    async def chat_context(_settings: object) -> AsyncGenerator[FakeChatRepository]:
        yield repository

    @asynccontextmanager
    async def run_context(_settings: object) -> AsyncGenerator[FakeRunRepository]:
        yield FakeRunRepository()

    def stream_factory(_settings: object) -> FakeBus:
        return FakeBus()

    monkeypatch.setattr(server, "make_chat_repository", chat_context)
    monkeypatch.setattr(server, "make_run_repository", run_context)
    monkeypatch.setattr(server, "make_stream", stream_factory)

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
        "x-request-id": "r95-request",
    }
    status, replay = await server.dispatch_request(
        Config(), "GET", "/v1/sessions/session-1/events", {}, headers, None
    )
    assert unexpected_connections == [], "R95 repository factory isolation was bypassed"
    assert status == 200
    response = cast(dict[str, Any], replay)
    assert len(response["data"]["events"]) == 1
    event = response["data"]["events"][0]
    assert event["payload_json"] == projection.event.payload_json
    assert "SENTINEL_PRIVATE" not in event["payload_json"]
    document = _document()
    _validate_response(
        document,
        _response_ref(document, "/v1/sessions/{session_id}/events", "get", "200"),
        replay,
    )
    _r95_declared_decoded_payload(
        document, event["event_type"], json.loads(event["payload_json"])
    )
    from kokoro_agent.chat_contract_check import validate_decoded_chat_payload

    validate_decoded_chat_payload(
        document, event_type=event["event_type"], payload_json=event["payload_json"]
    )


@pytest.mark.parametrize("kind", ["tool", "subagent", "skill"])
@pytest.mark.parametrize(
    "field",
    [
        "truncated",
        "name",
        "args",
        "result",
        "error",
        "description",
        "source",
        "subagent_type",
        "tool_id",
        "subagent_id",
        "task_input",
        "unknown",
    ],
)
def test_r95_activity_machine_rejects_every_private_or_unknown_field(
    kind: str, field: str
) -> None:
    document = _document()
    value = json.loads(
        _r95_projection(
            kind, "resolving" if kind == "skill" else "running"
        ).event.payload_json
    )
    _r95_declared_decoded_payload(document, "activity", value)
    with pytest.raises(SchemaValidationError):
        _r95_declared_decoded_payload(
            document, "activity", {**value, field: "SENTINEL_PRIVATE"}
        )


@pytest.mark.parametrize("kind", ["tool", "subagent", "skill"])
@pytest.mark.parametrize(
    "mutation",
    [
        "empty",
        "missing_identity",
        "short_identity",
        "upper_identity",
        "wrong_prefix",
        "identity_newline",
        "null_identity",
        "unknown_branch",
        "wrong_branch_field",
        "invalid_state",
        "missing_state",
        "wrong_display",
    ],
)
def test_r95_activity_machine_rejects_shape_identity_and_branch_drift(
    kind: str, mutation: str
) -> None:
    document = _document()
    value = json.loads(
        _r95_projection(
            kind, "resolving" if kind == "skill" else "running"
        ).event.payload_json
    )
    _r95_declared_decoded_payload(document, "activity", value)
    if mutation == "empty":
        value = dict[str, Any]()
    elif mutation == "missing_identity":
        del value["activity_id"]
    elif mutation == "short_identity":
        value["activity_id"] = "act_" + "a" * 63
    elif mutation == "upper_identity":
        value["activity_id"] = "act_" + "A" * 64
    elif mutation == "wrong_prefix":
        value["activity_id"] = "seg_" + "a" * 64
    elif mutation == "identity_newline":
        value["activity_id"] += "\n"
    elif mutation == "null_identity":
        value["activity_id"] = None
    elif mutation == "unknown_branch":
        value["activity"] = "other"
    elif mutation == "wrong_branch_field":
        value["segment_id" if kind == "skill" else "preflight_id"] = "seg_" + "a" * 64
    elif mutation == "invalid_state":
        value["phase" if kind == "skill" else "status"] = "unknown"
    elif mutation == "missing_state":
        del value["phase" if kind == "skill" else "status"]
    else:
        value["display_code"] = "raw.private-tool"
    with pytest.raises(SchemaValidationError):
        _r95_declared_decoded_payload(document, "activity", value)


@pytest.mark.parametrize("phase", ["resolving", "loading", "ready", "failed"])
@pytest.mark.parametrize("mutation", ["missing_code", "null_code", "unknown_code"])
def test_r95_skill_machine_error_code_presence(phase: str, mutation: str) -> None:
    document = _document()
    value = json.loads(
        _r95_projection(
            "skill", phase, "skill_load_failed" if phase == "failed" else None
        ).event.payload_json
    )
    _r95_declared_decoded_payload(document, "activity", value)
    if mutation == "missing_code":
        if phase != "failed":
            value["error_code"] = "skill_resolve_failed"
        else:
            del value["error_code"]
    else:
        value["error_code"] = None if mutation == "null_code" else "other_error"
    with pytest.raises(SchemaValidationError):
        _r95_declared_decoded_payload(document, "activity", value)


@pytest.mark.parametrize(
    "valid",
    [
        {"todos": []},
        {"todos": [{"content": "😀" * 1024, "status": "completed"}]},
        {"todos": [{"content": "a", "status": "pending"}] * 100},
        {
            "todos": [
                {"content": '"\\\b\t\n\f\r\x00/\u2028\u2029😀', "status": "in_progress"}
            ]
        },
    ],
)
def test_r95_todo_machine_and_runtime_accept_unicode_count_and_escape_boundaries(
    valid: dict[str, Any],
) -> None:
    from kokoro_agent.protocol import TodoUpdatedPayload

    parsed = TodoUpdatedPayload.model_validate(valid)
    expected = json.dumps(
        valid,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8", errors="strict")
    assert parsed.canonical_bytes() == expected
    _r95_declared_decoded_payload(_document(), "todo.updated", valid)


@pytest.mark.parametrize(
    "invalid",
    [
        {},
        {"todos": None},
        {"todos": "[]"},
        {"todos": [], "extra": True},
        {"todos": [{"content": "", "status": "pending"}]},
        {"todos": [{"content": "a" * 1025, "status": "pending"}]},
        {"todos": [{"content": "😀" * 1025, "status": "pending"}]},
        {"todos": [{"content": "\ud800", "status": "pending"}]},
        {"todos": [{"content": "\udfff", "status": "pending"}]},
        {"todos": [{"content": "a", "status": "pending"}] * 101},
        {"todos": [{"content": "a", "status": "unknown"}]},
        {"todos": [{"content": 1, "status": "pending"}]},
        {"todos": [{"content": "a", "status": "pending", "raw": "private"}]},
    ],
)
def test_r95_todo_machine_rejects_the_same_invalid_tables_as_runtime(
    invalid: dict[str, Any],
) -> None:
    from pydantic import ValidationError
    from kokoro_agent.protocol import TodoUpdatedPayload

    with pytest.raises(ValidationError):
        TodoUpdatedPayload.model_validate(invalid)
    with pytest.raises(SchemaValidationError):
        _r95_declared_decoded_payload(_document(), "todo.updated", invalid)


@pytest.mark.parametrize(
    "kind", ["unknown", "todo", "todos.updated", "skill.progress", "interaction"]
)
def test_r95_chat_event_machine_rejects_unknown_and_legacy_discriminators(
    kind: str,
) -> None:
    from kokoro_agent.application.chat.mappers import chat_event_to_view

    projection = _r95_projection("tool", "running")
    record = ChatEventRecord(
        **projection.event.model_dump(), chat_event_id="event", seq=1
    )
    event = chat_event_to_view(record).model_dump(mode="json")
    document = _document()
    _validate_response(document, "#/components/schemas/ChatEvent", event)
    event["event_type"] = kind
    with pytest.raises(SchemaValidationError):
        _validate_response(document, "#/components/schemas/ChatEvent", event)


@pytest.mark.parametrize(
    "kind,field",
    [
        ("tool", "segment_id"),
        ("tool", "display_code"),
        ("subagent", "segment_id"),
        ("subagent", "display_code"),
        ("skill", "preflight_id"),
        ("skill", "source_refs"),
    ],
)
def test_r95_activity_machine_requires_every_branch_member(
    kind: str, field: str
) -> None:
    document = _document()
    value = json.loads(
        _r95_projection(
            kind, "resolving" if kind == "skill" else "running"
        ).event.payload_json
    )
    _r95_declared_decoded_payload(document, "activity", value)
    del value[field]
    with pytest.raises(SchemaValidationError):
        _r95_declared_decoded_payload(document, "activity", value)


@pytest.mark.parametrize(
    "kind,field,prefix",
    [
        ("tool", "segment_id", "seg_"),
        ("subagent", "segment_id", "seg_"),
        ("skill", "preflight_id", "spf_"),
    ],
)
@pytest.mark.parametrize("mutation", ["short", "uppercase", "newline", "null"])
def test_r95_activity_machine_requires_strict_secondary_opaque_ids(
    kind: str, field: str, prefix: str, mutation: str
) -> None:
    value = json.loads(
        _r95_projection(
            kind, "resolving" if kind == "skill" else "running"
        ).event.payload_json
    )
    document = _document()
    _r95_declared_decoded_payload(document, "activity", value)
    value[field] = {
        "short": prefix + "a" * 63,
        "uppercase": prefix + "A" * 64,
        "newline": prefix + "a" * 64 + "\n",
        "null": None,
    }[mutation]
    with pytest.raises(SchemaValidationError):
        _r95_declared_decoded_payload(document, "activity", value)


@pytest.mark.parametrize(
    "refs",
    [
        [],
        None,
        "skill:one",
        [""],
        ["raw/path"],
        ["skill:one\n"],
        ["skill:one", "skill:one"],
    ],
)
def test_r95_skill_machine_retains_canonical_selected_source_refs(refs: object) -> None:
    document = _document()
    value = json.loads(_r95_projection("skill", "ready").event.payload_json)
    _r95_declared_decoded_payload(document, "activity", value)
    value["source_refs"] = refs
    with pytest.raises(SchemaValidationError):
        _r95_declared_decoded_payload(document, "activity", value)


@pytest.mark.parametrize("kind", ["tool", "subagent", "skill"])
def test_r95_real_progress_identity_uses_approved_scope_and_full_digest(
    kind: str,
) -> None:
    import hashlib

    projection = _r95_projection(kind, "resolving" if kind == "skill" else "running")
    event = projection.event
    value = json.loads(event.payload_json)

    def identity(prefix: str, tag: str, *parts: str) -> str:
        encoded = json.dumps(
            [
                "kokoro-agent.safe-progress.v1",
                tag,
                event.tenant_id,
                event.namespace,
                event.session_id,
                event.run_id,
                *parts,
            ],
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return prefix + hashlib.sha256(encoded).hexdigest()

    if kind == "skill":
        assert value["activity_id"] == identity("act_", "skill-activity")
        assert value["preflight_id"] == identity("spf_", "skill-preflight", "7")
        assert value["source_refs"] == ["skill:one", "skill:two"]
    else:
        assert value["activity_id"] == identity(
            "act_", f"{kind}-activity", "private-segment", f"private-{kind}"
        )
        assert value["segment_id"] == identity("seg_", "segment", "private-segment")
        assert value["display_code"] == f"{kind}.execution"
    _r95_declared_decoded_payload(_document(), "activity", value)


def _r97_budget_payload(extra_byte: bool) -> dict[str, Any]:
    value = {
        "todos": [{"content": "😀" * 1024, "status": "pending"} for _ in range(16)]
    }
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    count, remainder = divmod(len(encoded) - 65536, 3)
    tail = "" if remainder == 0 else "汉" if remainder == 1 else "é"
    value["todos"][0]["content"] = (
        "a" * count + tail + "😀" * (1024 - count - bool(remainder))
    )
    if extra_byte:
        value["todos"][0]["content"] = "é" + value["todos"][0]["content"][1:]
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert len(canonical) == 65536 + int(extra_byte)
    return value


@pytest.mark.parametrize("extra_byte", [False, True])
@pytest.mark.parametrize(
    "representation", ["canonical", "whitespace", "key_order", "escaped_unicode"]
)
def test_r97_owner_raw_checker_measures_canonical_whole_todo_payload(
    extra_byte: bool, representation: str
) -> None:
    from kokoro_agent.chat_contract_check import validate_decoded_chat_payload

    value = _r97_budget_payload(extra_byte)
    if representation == "key_order":
        value = {
            "todos": [
                {"status": item["status"], "content": item["content"]}
                for item in value["todos"]
            ]
        }
    raw = json.dumps(
        value,
        ensure_ascii=representation == "escaped_unicode",
        sort_keys=representation == "canonical",
        indent=2 if representation == "whitespace" else None,
        separators=None if representation == "whitespace" else (",", ":"),
    )
    if representation in {"whitespace", "escaped_unicode"}:
        assert len(raw.encode("utf-8")) > 65536  # raw length is NOT the budget
    document = _document()
    if extra_byte:
        with pytest.raises(ValueError, match="canonical UTF-8 budget"):
            validate_decoded_chat_payload(
                document, event_type="todo.updated", payload_json=raw
            )
    else:
        validate_decoded_chat_payload(
            document, event_type="todo.updated", payload_json=raw
        )


@pytest.mark.parametrize(
    "raw",
    [
        '{"todos":[],"todos":[]}',
        '{"todos":[],"todos":[{"content":"a","status":"pending"}]}',
        '{"todos":[{"content":"a","status":"pending"}],"todos":[]}',
        r'{"todos":[],"\u0074odos":[]}',
        '{"todos":[{"content":"a","content":"a","status":"pending"}]}',
        '{"todos":[{"content":"a","content":"b","status":"pending"}]}',
        r'{"todos":[{"content":"a","\u0063ontent":"a","status":"pending"}]}',
        '{"todos":[{"content":"a","status":"pending","status":"pending"}]}',
        r'{"todos":[{"content":"a","status":"pending","\u0073tatus":"completed"}]}',
    ],
)
def test_r97_owner_raw_checker_rejects_duplicate_members_before_decoding_loss(
    raw: str,
) -> None:
    from kokoro_agent.chat_contract_check import validate_decoded_chat_payload

    # Pass the original text, never json.loads before the owner boundary.
    with pytest.raises(ValueError, match="duplicate JSON member"):
        validate_decoded_chat_payload(
            _document(), event_type="todo.updated", payload_json=raw
        )


@pytest.mark.parametrize(
    "raw,reason",
    [
        ("", "malformed"),
        ("{", "malformed"),
        ('{"todos":[],}', "malformed"),
        ('{"todos":[]}{}', "malformed"),
        ('{"todos":[]} trailing', "malformed"),
        ('{"todos":NaN}', "non-finite"),
        ('{"todos":Infinity}', "non-finite"),
        ('{"todos":-Infinity}', "non-finite"),
        ('{"todos":1e999}', "non-finite"),
        ("[]", "object"),
        ("null", "object"),
        ('"text"', "object"),
        ("1", "object"),
        (r'{"todos":[{"content":"\ud800","status":"pending"}]}', "surrogate"),
        (r'{"todos":[{"content":"\udfff","status":"pending"}]}', "surrogate"),
        (r'{"todos":[{"content":"\ud83dX","status":"pending"}]}', "surrogate"),
    ],
)
def test_r97_owner_raw_checker_rejects_malformed_nonfinite_nonobject_and_surrogate(
    raw: str, reason: str
) -> None:
    from kokoro_agent.chat_contract_check import validate_decoded_chat_payload

    with pytest.raises(ValueError, match=reason):
        validate_decoded_chat_payload(
            _document(), event_type="todo.updated", payload_json=raw
        )


@pytest.mark.parametrize(
    "raw",
    [
        '{"todos":[]}',
        r'{"todos":[{"content":"\ud83d\ude00","status":"pending"}]}',
        '{"todos":[{"content":"😀","status":"pending"}]}',
        '{"todos":[{"content":"same","status":"pending"},{"content":"same","status":"completed"}]}',
        json.dumps(
            {
                "todos": [
                    {"content": '"\\\b\t\n\f\r\x00/\u2028\u2029😀', "status": "pending"}
                ]
            },
            ensure_ascii=True,
        ),
    ],
)
def test_r97_owner_raw_checker_accepts_legitimate_unicode_and_repeated_values(
    raw: str,
) -> None:
    from kokoro_agent.chat_contract_check import validate_decoded_chat_payload

    validate_decoded_chat_payload(
        _document(), event_type="todo.updated", payload_json=raw
    )


@pytest.mark.parametrize(
    "event_type,payload",
    [
        (
            "run.failed",
            {"code": "internal_error", "retryable": False, "status": "failed"},
        ),
        ("interaction.state", _hitl4_state()),
    ],
)
def test_r97_owner_raw_checker_preserves_failure_and_hitl_profiles(
    event_type: str, payload: dict[str, Any]
) -> None:
    from kokoro_agent.chat_contract_check import validate_decoded_chat_payload

    document = _document()
    validate_decoded_chat_payload(
        document, event_type=event_type, payload_json=json.dumps(payload)
    )
    with pytest.raises(ValueError, match="owner schema"):
        validate_decoded_chat_payload(
            document,
            event_type=event_type,
            payload_json=json.dumps({**payload, "private": "raw"}),
        )
    with pytest.raises(ValueError, match="owner schema"):
        validate_decoded_chat_payload(
            document, event_type="todo.updated", payload_json=json.dumps(payload)
        )
    if event_type == "run.failed":
        with pytest.raises(ValueError, match="owner schema"):
            validate_decoded_chat_payload(
                document,
                event_type=event_type,
                payload_json=json.dumps({**payload, "retryable": True}),
            )


@pytest.mark.parametrize(
    "event_type",
    [
        "unknown",
        "todo",
        "todos.updated",
        "skill.progress",
        "assistant.delta",
        "delivery",
    ],
)
def test_r97_owner_raw_checker_does_not_invent_unregistered_profiles(
    event_type: str,
) -> None:
    from kokoro_agent.chat_contract_check import validate_decoded_chat_payload

    with pytest.raises(ValueError, match="unsupported decoded Chat event type"):
        validate_decoded_chat_payload(
            _document(), event_type=event_type, payload_json="{}"
        )


@pytest.mark.parametrize(
    "mutation", ["missing", "wrong_profile", "remote_ref", "nested_remote_ref"]
)
def test_r97_owner_raw_checker_rejects_missing_wrong_or_remote_schema_binding(
    mutation: str,
) -> None:
    from kokoro_agent.chat_contract_check import validate_decoded_chat_payload

    document = _document()
    validate_decoded_chat_payload(
        document, event_type="todo.updated", payload_json='{"todos":[]}'
    )
    schemas = document["components"]["schemas"]
    mapping = schemas["ChatEvent"]["x-kokoro-decoded-payloads"]["mapping"]
    if mutation == "missing":
        del mapping["todo.updated"]
    elif mutation == "wrong_profile":
        mapping["todo.updated"] = mapping["run.failed"]
    elif mutation == "remote_ref":
        mapping["todo.updated"] = "https://unused.invalid/schema.json"
    else:
        schemas["ChatTodo"]["properties"]["todos"] = {
            "$ref": "https://unused.invalid/schema.json"
        }
    with pytest.raises(ValueError, match="mapping|local component"):
        validate_decoded_chat_payload(
            document, event_type="todo.updated", payload_json='{"todos":[]}'
        )
