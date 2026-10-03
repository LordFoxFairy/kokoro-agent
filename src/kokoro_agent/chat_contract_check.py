"""Validate typed Agent-owned launch and replay HTTP response bindings."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError as SchemaValidationError
from jsonschema import validate as validate_schema_instance

from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES

from pydantic import TypeAdapter, ValidationError

_OBJECT = TypeAdapter(dict[str, object])
_IDENTIFIER = {"type": "string", "minLength": 1}
_NONNEGATIVE_INT64 = {"type": "integer", "format": "int64", "minimum": 0}
_DECODED_PROFILES = {
    "run.failed": "#/components/schemas/ChatFailure",
    "interaction.state": "#/components/schemas/ChatInteractionState",
    "activity": "#/components/schemas/ChatActivity",
    "todo.updated": "#/components/schemas/ChatTodo",
}

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
            "mapping": _DECODED_PROFILES,
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
                    "todo.updated",
                    "interaction.state",
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


_HITL_REQUIRED: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "ResumeControl": (
        frozenset(
            {"kind", "session_id", "expected_pause_revision", "pause_ref", "decisions"}
        ),
        frozenset(),
    ),
    "ChatInteractionState": (
        frozenset(
            {
                "interaction_revision",
                "pause_revision",
                "pause_ref",
                "phase",
                "groups",
                "action_result",
            }
        ),
        frozenset(),
    ),
    "InteractionGroup": (frozenset({"group_id", "items"}), frozenset()),
    "InteractionItem": (
        frozenset({"item_id", "request_id", "kind", "allowed_decisions", "display"}),
        frozenset({"validation"}),
    ),
    "InteractionDisplay": (
        frozenset({"name", "description", "editable", "input_schema"}),
        frozenset({"result_preview", "truncated", "source"}),
    ),
    "InteractionValidation": (frozenset({"code", "instance_path"}), frozenset()),
    "InteractionActionResult": (
        frozenset({"command_id", "pause_revision", "kind"}),
        frozenset(),
    ),
}


def _validate_hitl_shapes(schemas: dict[str, object]) -> None:
    """Guard the owner-only identity and safe projection boundaries retained in HTTP5."""
    for name, (required, optional) in _HITL_REQUIRED.items():
        schema = _object(schemas.get(name), source=f"HITL {name}")
        fields = TypeAdapter(list[str]).validate_python(schema.get("required"))
        properties = _object(schema.get("properties"), source=f"HITL {name} properties")
        if (
            schema.get("type") != "object"
            or schema.get("additionalProperties") is not False
            or len(fields) != len(required)
            or frozenset(fields) != required
            or frozenset(properties) != required | optional
        ):
            raise ValueError(f"HITL {name} strict fields are invalid")
    resume = _object(schemas["ResumeControl"], source="HITL resume")
    properties = _object(resume["properties"], source="HITL resume properties")
    if (
        properties.get("expected_pause_revision") != {"type": "integer", "minimum": 1}
        or properties.get("pause_ref") != _IDENTIFIER
    ):
        raise ValueError("HITL required pause identity is invalid")
    decision = _object(schemas.get("ResumeDecision"), source="HITL decision")
    branches = TypeAdapter(list[dict[str, object]]).validate_python(
        decision.get("oneOf")
    )
    expected_fields: dict[str, tuple[set[str], set[str]]] = {
        "approve": ({"type", "item_id"}, {"args"}),
        "edit": ({"type", "item_id", "args"}, set()),
        "reject": ({"type", "item_id"}, {"reason"}),
        "respond": ({"type", "item_id", "response"}, set()),
        "submit": ({"type", "item_id", "value"}, set()),
    }
    seen: set[str] = set()
    for branch in branches:
        properties = _object(
            branch.get("properties"), source="HITL decision properties"
        )
        kind = _object(properties.get("type"), source="HITL decision type").get("const")
        if not isinstance(kind, str) or kind not in expected_fields or kind in seen:
            raise ValueError("HITL decision kinds are invalid")
        seen.add(kind)
        decision_required, decision_optional = expected_fields[kind]
        fields = TypeAdapter(list[str]).validate_python(branch.get("required"))
        if (
            branch.get("additionalProperties") is not False
            or set(properties) != decision_required | decision_optional
            or set(fields) != decision_required
            or len(fields) != len(decision_required)
            or properties.get("item_id") != _IDENTIFIER
        ):
            raise ValueError("HITL decision identity or strict fields are invalid")
    if seen != set(expected_fields):
        raise ValueError("HITL decision collection is incomplete")


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
    _validate_hitl_shapes(schemas)
    for name, expected in _TYPED_SCHEMAS.items():
        if schemas.get(name) != expected:
            raise ValueError(f"{name} typed response payload schema is invalid")
    _validate_progress_shapes(document, schemas)
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
    if _object(document.get("info"), source="info").get("version") != "5.0.0":
        raise ValueError("failure profiles require HTTP 5.0.0")
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
        "mapping": _DECODED_PROFILES,
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
        "version": "5.0.0",
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


def _unique_json_members(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member in decoded payload")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> object:
    raise ValueError("non-finite JSON value in decoded payload")


def _canonical_payload_bytes(value: dict[str, object]) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise ValueError(
            "decoded payload contains invalid Unicode surrogate"
        ) from error
    except ValueError as error:
        raise ValueError("non-finite JSON value in decoded payload") from error


def _local_schema_references(value: object, schemas: dict[str, object]) -> None:
    """Reject remote/dynamic resolution before invoking the JSON Schema engine."""
    original: object = value
    if isinstance(value, dict):
        shape = _object(original, source="schema")
        if any(key in shape for key in ("$id", "$dynamicRef", "$recursiveRef")):
            raise ValueError("decoded schema must use local component references")
        if "$ref" in shape:
            reference = shape["$ref"]
            prefix = "#/components/schemas/"
            if (
                not isinstance(reference, str)
                or not reference.startswith(prefix)
                or reference[len(prefix) :] not in schemas
            ):
                raise ValueError("decoded schema reference must name a local component")
        for child in shape.values():
            _local_schema_references(child, schemas)
    elif isinstance(value, list):
        for child in TypeAdapter(list[object]).validate_python(value):
            _local_schema_references(child, schemas)


def validate_decoded_chat_payload(
    document: dict[str, object], *, event_type: str, payload_json: str
) -> None:
    """Validate the four mapped raw Chat profiles without I/O or normalization.

    Ordinary Message/Delivery profiles are outside this entry's scope. Callers
    dispatch only declared profiles; unknown/missing bindings fail closed.
    """
    if event_type not in _DECODED_PROFILES:
        raise ValueError("unsupported decoded Chat event type")
    components = _object(document.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    event = _object(schemas.get("ChatEvent"), source="ChatEvent")
    binding = _object(event.get("x-kokoro-decoded-payloads"), source="decoded binding")
    if binding != {
        "discriminator": "event_type",
        "property": "payload_json",
        "mapping": _DECODED_PROFILES,
    }:
        raise ValueError("decoded Chat profile mapping is invalid")
    _local_schema_references(schemas, schemas)
    try:
        decoded: object = json.loads(
            payload_json,
            object_pairs_hook=_unique_json_members,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError as error:
        raise ValueError("malformed decoded payload JSON") from error
    value = _object(decoded, source="decoded payload")
    canonical = _canonical_payload_bytes(value)
    reference = _DECODED_PROFILES[event_type]
    try:
        validate_schema_instance(
            value,
            {"$ref": reference, "components": components},
            cls=Draft202012Validator,
        )
    except SchemaValidationError as error:
        raise ValueError("decoded Chat payload violates owner schema") from error
    if event_type == "todo.updated":
        shape = _object(schemas.get("ChatTodo"), source="ChatTodo")
        limit = shape.get("x-kokoro-json-byte-limit")
        if type(limit) is not int or limit != 65536:
            raise ValueError("Todo canonical UTF-8 byte limit is invalid")
        if len(canonical) > limit:
            raise ValueError("complete Todo payload exceeds canonical UTF-8 budget")


def _validate_progress_shapes(
    document: dict[str, object], schemas: dict[str, object]
) -> None:
    """Pin safety-critical constraints, not a second editable progress schema."""
    _local_schema_references(schemas, schemas)
    todo = _object(schemas.get("ChatTodo"), source="Todo schema")
    limit = todo.get("x-kokoro-json-byte-limit")
    if type(limit) is not int or limit != 65536:
        raise ValueError("Todo canonical UTF-8 byte limit is invalid")
    if (
        todo.get("type") != "object"
        or todo.get("additionalProperties") is not False
        or todo.get("required") != ["todos"]
    ):
        raise ValueError("Todo strict object is invalid")
    properties = _object(todo.get("properties"), source="Todo properties")
    if set(properties) != {"todos"}:
        raise ValueError("Todo closed fields are invalid")
    items = _object(properties["todos"], source="Todo array")
    if (
        items.get("type") != "array"
        or type(items.get("maxItems")) is not int
        or items.get("maxItems") != 100
    ):
        raise ValueError("Todo item count is invalid")
    item = _object(items.get("items"), source="Todo item")
    if item.get("additionalProperties") is not False or item.get("required") != [
        "content",
        "status",
    ]:
        raise ValueError("Todo item fields are invalid")
    fields = _object(item.get("properties"), source="Todo item properties")
    content = _object(fields.get("content"), source="Todo content")
    if (
        set(fields) != {"content", "status"}
        or content.get("minLength") != 1
        or content.get("maxLength") != 1024
    ):
        raise ValueError("Todo content codepoint budget is invalid")
    activity = _object(schemas.get("ChatActivity"), source="activity schema")
    branches = TypeAdapter(list[dict[str, object]]).validate_python(
        activity.get("oneOf")
    )
    required_fields = {
        "tool": {"activity", "activity_id", "segment_id", "status", "display_code"},
        "subagent": {"activity", "activity_id", "segment_id", "status", "display_code"},
        "skill": {"activity", "activity_id", "preflight_id", "source_refs", "phase"},
    }
    seen: set[str] = set()
    for branch in branches:
        fields = _object(branch.get("properties"), source="activity properties")
        kind = _object(fields.get("activity"), source="activity kind").get("const")
        if not isinstance(kind, str) or kind not in required_fields or kind in seen:
            raise ValueError("activity branch kinds are invalid")
        seen.add(kind)
        required = TypeAdapter(list[str]).validate_python(branch.get("required"))
        optional: set[str] = {"error_code"} if kind == "skill" else set()
        if (
            branch.get("additionalProperties") is not False
            or set(required) != required_fields[kind]
            or len(required) != len(set(required))
            or set(fields) != required_fields[kind] | optional
        ):
            raise ValueError("activity closed fields are invalid")
    if seen != set(required_fields):
        raise ValueError("activity branch collection is incomplete")
    # Exercise the registered schema through the same pure raw entry as replay
    # conformance tests, rather than trusting decorative extension keywords.
    validate_decoded_chat_payload(
        document, event_type="todo.updated", payload_json='{"todos":[]}'
    )
