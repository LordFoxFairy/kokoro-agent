"""Validate typed Agent-owned launch and replay HTTP response bindings."""

from __future__ import annotations

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
