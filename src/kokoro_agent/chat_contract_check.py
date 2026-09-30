"""Validate typed Agent-owned launch and replay HTTP response bindings."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES

from pydantic import TypeAdapter, ValidationError

_OBJECT = TypeAdapter(dict[str, object])
_IDENTIFIER = {"type": "string", "minLength": 1}
_NONNEGATIVE_INT64 = {"type": "integer", "format": "int64", "minimum": 0}
_TYPED_SCHEMAS: dict[str, object] = {
    "Meta": {
        "type": "object",
        "required": ["request_id"],
        "properties": {"request_id": _IDENTIFIER},
        "additionalProperties": False,
    },
    "LaunchReceipt": {
        "type": "object",
        "required": ["run_id", "session_id", "replayed"],
        "properties": {
            "run_id": _IDENTIFIER,
            "session_id": _IDENTIFIER,
            "replayed": {"type": "boolean"},
        },
        "additionalProperties": False,
    },
    "ReplayPage": {
        "type": "object",
        "required": ["events", "next_seq", "watermark"],
        "properties": {
            "events": {
                "type": "array",
                "items": {"$ref": "#/components/schemas/ChatEvent"},
            },
            "next_seq": _NONNEGATIVE_INT64,
            "watermark": _NONNEGATIVE_INT64,
        },
        "additionalProperties": False,
    },
    "ChatEvent": {
        "x-kokoro-decoded-payloads": {
            "discriminator": "event_type",
            "property": "payload_json",
            "mapping": {"run.failed": "#/components/schemas/ChatFailure"},
        },
        "type": "object",
        "required": [
            "chat_event_id",
            "session_id",
            "run_id",
            "source_index",
            "event_type",
            "payload_json",
            "seq",
            "created_at",
        ],
        "properties": {
            "chat_event_id": _IDENTIFIER,
            "session_id": _IDENTIFIER,
            "run_id": _IDENTIFIER,
            "source_index": _NONNEGATIVE_INT64,
            "chat_message_id": {"type": ["string", "null"], "minLength": 1},
            "event_type": {
                "type": "string",
                "enum": [
                    "run.started",
                    "assistant.delta",
                    "assistant.completed",
                    "activity",
                    "interaction",
                    "delivery",
                    "run.completed",
                    "run.failed",
                ],
            },
            "payload_json": {"type": "string"},
            "seq": {"type": "integer", "format": "int64", "minimum": 1},
            "created_at": {
                **_NONNEGATIVE_INT64,
                "description": "Internal Agent UTC epoch milliseconds.",
            },
        },
        "additionalProperties": False,
    },
}


def _object(value: object, *, source: str) -> dict[str, object]:
    try:
        return _OBJECT.validate_python(value)
    except ValidationError as error:
        raise ValueError(f"{source} must be a JSON object") from error


def validate_chat_response_contract(
    document: dict[str, object], paths: dict[str, object]
) -> None:
    """Keep the two BFF-consumed response types out of the generic envelope."""

    components = _object(document.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    for name, expected in _TYPED_SCHEMAS.items():
        if schemas.get(name) != expected:
            raise ValueError(f"{name} typed response payload schema is invalid")
    for path, method, status, envelope, payload in (
        ("/v1/runs", "post", "202", "LaunchReceiptEnvelope", "LaunchReceipt"),
        (
            "/v1/sessions/{session_id}/events",
            "get",
            "200",
            "ReplayPageEnvelope",
            "ReplayPage",
        ),
    ):
        expected = {
            "type": "object",
            "required": ["data", "meta"],
            "properties": {
                "data": {"$ref": f"#/components/schemas/{payload}"},
                "meta": {"$ref": "#/components/schemas/Meta"},
            },
            "additionalProperties": False,
        }
        actual = _object(schemas.get(envelope), source=envelope)
        if actual != expected:
            raise ValueError(f"{envelope} typed response envelope is invalid")
        item = _object(paths.get(path), source=path)
        operation = _object(item.get(method), source=f"{method} {path}")
        responses = _object(operation.get("responses"), source="responses")
        response = _object(responses.get(status), source=f"{status} response")
        content = _object(response.get("content"), source="response content")
        media = _object(content.get("application/json"), source="JSON response")
        if media.get("schema") != {"$ref": f"#/components/schemas/{envelope}"}:
            raise ValueError(f"{method} {path} typed response reference is invalid")


OPENAPI_RELATIVE = "contract/openapi/v1/openapi.json"


def read_contract_document(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise ValueError(f"contract source is missing: {path}")
    try:
        return _OBJECT.validate_json(path.read_bytes())
    except ValidationError as error:
        raise ValueError(f"{path} must contain a JSON object") from error


GENERATED_FAILURE_RELATIVE = "src/kokoro_agent/protocol/run_failure_generated.py"


def failure_model_bytes(
    document: dict[str, object], *, source_sha256: str = ""
) -> bytes:
    """Compile only the approved two failure profiles; reject unsupported shapes."""
    if document.get("openapi") != "3.1.0":
        raise ValueError("failure profiles require OpenAPI 3.1.0")
    if _object(document.get("info"), source="info").get("version") != "3.0.0":
        raise ValueError("failure profiles require HTTP 3.0.0")
    schemas = _object(
        _object(document.get("components"), source="components").get("schemas"),
        source="schemas",
    )
    failure = _object(schemas.get("Failure"), source="Failure")
    properties = _object(failure.get("properties"), source="Failure properties")
    code = _object(properties.get("code"), source="Failure code")
    codes = TypeAdapter(list[str]).validate_python(code.get("enum"))
    if (
        not codes
        or len(codes) != len(set(codes))
        or any(
            not value
            or not value.isascii()
            or not all(char.islower() or char == "_" for char in value)
            for value in codes
        )
    ):
        raise ValueError("failure codes must be unique nonempty snake_case strings")
    retryable_codes = ["model_unavailable", "dependency_unavailable"]
    if not set(retryable_codes) <= set(codes):
        raise ValueError("failure availability codes are missing")
    expected = {
        "type": "object",
        "required": ["code", "retryable"],
        "properties": {
            "code": {"type": "string", "enum": codes},
            "retryable": {"type": "boolean"},
        },
        "if": {"properties": {"retryable": {"const": True}}},
        "then": {"properties": {"code": {"enum": retryable_codes}}},
    }
    run = {"$ref": "#/components/schemas/Failure", "unevaluatedProperties": False}
    chat = {
        "allOf": [
            {"$ref": "#/components/schemas/Failure"},
            {
                "type": "object",
                "required": ["status"],
                "properties": {"status": {"type": "string", "const": "failed"}},
            },
        ],
        "unevaluatedProperties": False,
    }
    decoded = {
        "discriminator": "event_type",
        "property": "payload_json",
        "mapping": {"run.failed": "#/components/schemas/ChatFailure"},
    }
    # JSON comparison distinguishes true/1 and false/0, unlike Python equality.
    for actual, required in (
        (failure, expected),
        (schemas.get("RunFailure"), run),
        (schemas.get("ChatFailure"), chat),
    ):
        if json.dumps(actual, sort_keys=True) != json.dumps(required, sort_keys=True):
            raise ValueError("failure canonical profile or tuple is invalid")
    event = _object(schemas.get("ChatEvent"), source="ChatEvent")
    if event.get("x-kokoro-decoded-payloads") != decoded:
        raise ValueError("failure decoded payload mapping is invalid")
    payload = _object(
        _object(event.get("properties"), source="ChatEvent properties").get(
            "payload_json"
        ),
        source="payload_json",
    )
    if payload != {"type": "string"}:
        raise ValueError("failure payload_json must remain an unrestricted string")
    literals = "".join(f'    "{value}",\n' for value in codes)
    retry_tuple = "".join(f'            "{value}",\n' for value in retryable_codes)
    return (
        "# Generated by scripts/generate_failure_models.py. DO NOT EDIT.\n"
        f"# Source: {OPENAPI_RELATIVE} sha256={source_sha256}\n"
        "from __future__ import annotations\n\n"
        "from typing import Literal, Self\n\n"
        "from pydantic import BaseModel, ConfigDict, model_validator\n\n"
        f"RunErrorCode = Literal[\n{literals}]\n\n\n"
        "class RunFailedPayload(BaseModel):\n"
        '    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)\n\n'
        "    code: RunErrorCode\n"
        "    retryable: bool\n\n"
        '    @model_validator(mode="after")\n'
        "    def validate_failure_tuple(self) -> Self:\n"
        f"        if self.retryable and self.code not in (\n{retry_tuple}        ):\n"
        '            raise ValueError("invalid failure code/retryable tuple")\n'
        "        return self\n\n\n"
        "class ChatFailure(RunFailedPayload):\n"
        '    status: Literal["failed"]\n'
    ).encode("utf-8")


def generate_failure_models(root: Path, *, check: bool) -> None:
    """Generate/check all failure bytes and their direct provenance without writing in check mode."""
    source = root / OPENAPI_RELATIVE
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    expected = failure_model_bytes(
        read_contract_document(source), source_sha256=source_hash
    )
    output = root / GENERATED_FAILURE_RELATIVE
    provenance_path = root / "contract/provenance.json"
    provenance = read_contract_document(provenance_path)
    artifact = {
        "path": GENERATED_FAILURE_RELATIVE,
        "sha256": hashlib.sha256(expected).hexdigest(),
        "source": OPENAPI_RELATIVE,
        "source_sha256": source_hash,
        "generator": "scripts/generate_failure_models.py",
    }
    if check:
        if not output.is_file() or output.read_bytes() != expected:
            raise ValueError("failure generated bytes are stale")
        if provenance.get("generated_artifacts") != [artifact]:
            raise ValueError("failure generated provenance is stale")
        return
    output.write_bytes(expected)
    provenance["generated_artifacts"] = [artifact]
    provenance["http_contract"] = {
        "version": "3.0.0",
        "path": OPENAPI_RELATIVE,
        "sha256": source_hash,
    }
    provenance["source_files"] = list(OWNER_SOURCE_FILES)
    digest = hashlib.sha256()
    for relative in OWNER_SOURCE_FILES:
        digest.update((root / relative).read_bytes())
    provenance["combined_sha256"] = digest.hexdigest()
    provenance_path.write_text(
        json.dumps(provenance, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
