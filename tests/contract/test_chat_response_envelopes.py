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
