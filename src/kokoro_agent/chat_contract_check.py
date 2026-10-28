"""Validate typed Agent-owned launch and replay HTTP response bindings."""

from __future__ import annotations

import hashlib
import copy
import json
from pathlib import Path
from typing import get_args

from jsonschema import Draft202012Validator, ValidationError as SchemaValidationError
from jsonschema import validate as validate_schema_instance

from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES
from kokoro_agent.bff_admission_contract import GENERATED as BFF_BUDGET_GENERATED
from kokoro_agent.domain.chat.projection import ToolDisplayCode, ToolActivity

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

    document = _display_parent(document)
    paths = _object(document.get("paths"), source="paths")
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


_HTTP8_TOOL_DISPLAY_CODES = (
    "tool.execution",
    "tool.file.read",
    "tool.mcp.discover",
    "tool.mcp.describe",
    "tool.mcp.call",
    "tool.web.search",
    "tool.web.fetch",
)


_TOOL_DETAIL_SCHEMA: dict[str, object] = {
    "type": "object",
    "required": ["kind", "name"],
    "properties": {
        "kind": {"type": "string", "enum": ["file", "mcp_tool"]},
        "name": {
            "type": "string",
            "minLength": 1,
            "maxLength": 120,
            "x-kokoro-utf8-byte-limit": 480,
            "x-kokoro-unicode-domain": "NFC Unicode scalars; no Cc, Cf or Cs",
        },
    },
    "additionalProperties": False,
}
_TOOL_DETAIL_CONDITIONS: list[dict[str, object]] = [
    {
        "if": {
            "required": ["display_detail"],
            "properties": {
                "display_detail": {"properties": {"kind": {"const": "file"}}}
            },
        },
        "then": {
            "properties": {
                "display_code": {
                    "enum": ["tool.file.read", "tool.file.write", "tool.file.deliver"]
                }
            }
        },
    },
    {
        "if": {
            "required": ["display_detail"],
            "properties": {
                "display_detail": {"properties": {"kind": {"const": "mcp_tool"}}}
            },
        },
        "then": {
            "properties": {
                "display_code": {"enum": ["tool.mcp.describe", "tool.mcp.call"]}
            }
        },
    },
]


def http82_basename_parent_bytes(document: dict[str, object]) -> bytes:
    """Undo only file-name semantics, preserving the complete first HTTP8.2 graph."""
    parent = copy.deepcopy(_display_parent(document))
    if _object(parent.get("info"), source="info").get("version") != "8.2.0":
        raise ValueError("file basename correction requires HTTP 8.2.0")
    components = _object(parent.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    activity = _object(schemas.get("ChatActivity"), source="ChatActivity")
    branches = TypeAdapter(list[dict[str, object]]).validate_python(
        activity.get("oneOf")
    )
    if len(branches) != 3:
        raise ValueError("file basename activity branches differ")
    conditions = TypeAdapter(list[dict[str, object]]).validate_python(
        branches[0].get("allOf")
    )
    if len(conditions) != 2:
        raise ValueError("file basename conditions differ")
    then = _object(conditions[0].get("then"), source="file condition")
    properties = _object(then.get("properties"), source="file condition properties")
    if properties.pop("display_detail", None) != {
        "properties": {"name": {"pattern": r"^(?!\.{1,2}$)[^/\\:]+$"}}
    }:
        raise ValueError("file basename correction differs")
    then["properties"] = properties
    conditions[0]["then"] = then
    branches[0]["allOf"] = conditions
    activity["oneOf"] = branches
    schemas["ChatActivity"] = activity
    components["schemas"] = schemas
    parent["components"] = components
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "d16786d4b8214ba8ba22950c9b4ab9b297f0a2982724a89720bc268d2445cb71"
    ):
        raise ValueError("complete first HTTP 8.2 parent differs")
    return raw


def http81_parent_bytes(document: dict[str, object]) -> bytes:
    """Undo the one optional bounded detail, preserving every HTTP8.1 byte."""
    parent = _object(
        json.loads(http82_basename_parent_bytes(document)),
        source="first HTTP8.2 parent",
    )
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "8.2.0":
        raise ValueError("tool detail extension requires HTTP 8.2.0")
    components = _object(parent.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    activity = _object(schemas.get("ChatActivity"), source="ChatActivity")
    branches = TypeAdapter(list[dict[str, object]]).validate_python(
        activity.get("oneOf")
    )
    properties = _object(branches[0].get("properties"), source="tool properties")
    if (
        properties.pop("display_detail", None) != _TOOL_DETAIL_SCHEMA
        or branches[0].pop("allOf", None) != _TOOL_DETAIL_CONDITIONS
    ):
        raise ValueError("tool detail safety shape differs")
    branches[0]["properties"] = properties
    activity["oneOf"] = branches
    schemas["ChatActivity"] = activity
    components["schemas"] = schemas
    parent["components"] = components
    info["version"] = "8.1.0"
    parent["info"] = info
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "1642829ccf5364692051e836938ef33b36cffeca25f2b7766ad95cd44a1542c9"
    ):
        raise ValueError("complete HTTP 8.1 parent differs")
    return raw


def http80_parent_bytes(document: dict[str, object]) -> bytes:
    """Undo only the two file categories and prove the complete HTTP8 parent."""
    parent = _object(json.loads(http81_parent_bytes(document)), source="HTTP8.1 parent")
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "8.1.0":
        raise ValueError("file display extension requires HTTP 8.1.0")
    components = _object(parent.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    branches = TypeAdapter(list[dict[str, object]]).validate_python(
        _object(schemas.get("ChatActivity"), source="ChatActivity").get("oneOf")
    )
    properties = _object(branches[0].get("properties"), source="tool properties")
    current_codes = [*_HTTP8_TOOL_DISPLAY_CODES, "tool.file.write", "tool.file.deliver"]
    if list(get_args(ToolDisplayCode)) != current_codes or properties.get(
        "display_code"
    ) != {
        "type": "string",
        "enum": current_codes,
    }:
        raise ValueError("file display code extension is not exact")
    properties["display_code"] = {
        "type": "string",
        "enum": list(_HTTP8_TOOL_DISPLAY_CODES),
    }
    branches[0]["properties"] = properties
    activity = _object(schemas["ChatActivity"], source="ChatActivity")
    activity["oneOf"] = branches
    schemas["ChatActivity"] = activity
    components["schemas"] = schemas
    parent["components"] = components
    info["version"] = "8.0.0"
    parent["info"] = info
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "93103b5f1df90ad4524e7fc08274211ed2f0805760d70923931e9449a59e37ff"
    ):
        raise ValueError("complete HTTP 8.0 parent differs")
    return raw


def http74_parent_bytes(document: dict[str, object]) -> bytes:
    """Undo only Agent8's closed display output extension, preserving the full parent."""
    parent = _object(
        json.loads(http80_parent_bytes(document)), source="HTTP 8.0 parent"
    )
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "8.0.0":
        raise ValueError("tool display extension requires HTTP 8.0.0")
    info["version"] = "7.4.0"
    schemas = _object(
        _object(parent["components"], source="components")["schemas"], source="schemas"
    )
    branches = TypeAdapter(list[dict[str, object]]).validate_python(
        _object(schemas["ChatActivity"], source="ChatActivity")["oneOf"]
    )
    properties = _object(branches[0]["properties"], source="tool properties")
    if properties["display_code"] != {
        "type": "string",
        "enum": list(_HTTP8_TOOL_DISPLAY_CODES),
    }:
        raise ValueError("tool display code extension is not exact")
    properties["display_code"] = {"type": "string", "const": "tool.execution"}
    branches[0]["properties"] = properties
    activity = _object(schemas["ChatActivity"], source="ChatActivity")
    activity["oneOf"] = branches
    schemas["ChatActivity"] = activity
    components = _object(parent["components"], source="components")
    components["schemas"] = schemas
    parent["components"] = components
    parent["info"] = info
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "7637757f2f8b1fc1556e62c6566db5de5eb2ae883cf6e24e918b53081cc664ad"
    ):
        raise ValueError("complete HTTP 7.4 parent differs")
    return raw


def http72_parent_bytes(document: dict[str, object]) -> bytes:
    """Prove the complete published parent; remove only this release's named extension."""
    parent = copy.deepcopy(document)
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "7.3.0":
        raise ValueError("pending settlement failure requires HTTP 7.3.0")
    info["version"] = "7.2.0"
    parent["info"] = info
    schemas = _object(
        _object(parent.get("components"), source="components").get("schemas"),
        source="schemas",
    )
    failure = _object(schemas.get("Failure"), source="Failure")
    props = _object(failure.get("properties"), source="properties")
    code = _object(props.get("code"), source="code")
    codes = TypeAdapter(list[str]).validate_python(code.get("enum"), strict=True)
    if (
        not codes
        or codes[-1] != "model_settlement_pending"
        or codes.count("model_settlement_pending") != 1
    ):
        raise ValueError("pending settlement failure extension is not exact")
    code["enum"] = codes[:-1]
    props["code"] = code
    failure["properties"] = props
    schemas["Failure"] = failure
    components = _object(parent.get("components"), source="components")
    components["schemas"] = schemas
    parent["components"] = components
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "4e75244c676e02736867d2f461d18b4d67d93151ff03ffaf6e632685b77ca9db"
    ):
        raise ValueError("complete HTTP 7.2 parent differs")
    return raw


GENERATED_FAILURE_RELATIVE = "src/kokoro_agent/protocol/run_failure_generated.py"


def failure_model_bytes(
    document: dict[str, object], *, source_sha256: str = ""
) -> bytes:
    """Compile only the approved two failure profiles; reject unsupported shapes."""
    document = _display_parent(document)
    if document.get("openapi") != "3.1.0":
        raise ValueError("failure profiles require OpenAPI 3.1.0")
    if _object(document.get("info"), source="info").get("version") != "8.2.0":
        raise ValueError("failure profiles require HTTP 8.2.0")
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
    document = read_contract_document(source)
    # Current generation must contain the complete exact child, not merely a
    # compatible historical failure tuple with an invented new provenance tag.
    http86_action_title_parent_bytes(document)
    expected = failure_model_bytes(document, source_sha256=source_hash)
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
    records = TypeAdapter(list[dict[str, object]]).validate_python(
        provenance.get("generated_artifacts"), strict=True
    )
    if [record.get("path") for record in records] != [
        GENERATED_FAILURE_RELATIVE,
        BFF_BUDGET_GENERATED,
    ]:
        raise ValueError(
            "generated artifact inventory differs from current owner closure"
        )
    if check:
        if not output.is_file() or output.read_bytes() != expected:
            raise ValueError("failure generated bytes are stale")
        if records[0] != artifact:
            raise ValueError("failure generated provenance is stale")
        return
    output.write_bytes(expected)
    provenance["generated_artifacts"] = [artifact, records[1]]
    provenance["http_contract"] = {
        "version": "8.7.0",
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
    if event_type == "activity" and value.get("activity") == "tool":
        ToolActivity.model_validate(value, strict=True)
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
        optional: set[str] = (
            {"error_code"}
            if kind == "skill"
            else {"display_detail"}
            if kind == "tool"
            else set()
        )
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


# HTTP8.3 exact named display delta; old parent guards remain unmodified.
_DISPLAY_PATH = {
    "type": "string",
    "minLength": 1,
    "maxLength": 4096,
    "x-kokoro-utf8-byte-limit": 4096,
    "x-kokoro-unicode-domain": "NFC Unicode scalars; no Cc, Cf or Cs",
    "description": "Validated absolute virtual backend path; never a host path, URL, "
    "secret or unproven raw argument.",
}

_DISPLAY_SKILLS = {
    "type": "array",
    "maxItems": 16,
    "items": {"$ref": "#/components/schemas/SkillDisplay"},
    "x-kokoro-unique-by": "source_ref",
    "x-kokoro-binding": "unique source_ref subset of original ready source_refs; no "
    "current catalog lookup",
}

_SKILL_DISPLAY_SCHEMA = {
    "type": "object",
    "required": ["source_ref", "name", "virtual_path"],
    "properties": {
        "source_ref": {
            "type": "string",
            "minLength": 7,
            "maxLength": 197,
            "pattern": "^skill:(?!skill:)[A-Za-z0-9][A-Za-z0-9._:-]{0,190}(?![\\s\\S])",
        },
        "name": {
            "type": "string",
            "minLength": 1,
            "maxLength": 120,
            "x-kokoro-utf8-byte-limit": 480,
            "x-kokoro-unicode-domain": "NFC Unicode scalars; no Cc, Cf or Cs",
        },
        "virtual_path": {
            "type": "string",
            "minLength": 1,
            "maxLength": 4096,
            "x-kokoro-utf8-byte-limit": 4096,
            "x-kokoro-unicode-domain": "NFC Unicode scalars; no Cc, Cf or Cs",
            "description": "Validated absolute virtual backend "
            "path; never a host path, URL, secret "
            "or unproven raw argument.",
        },
        "revision": {
            "type": "string",
            "pattern": "^[1-9][0-9]{0,18}(?![\\s\\S])",
            "x-kokoro-int64-decimal": True,
        },
    },
    "additionalProperties": False,
    "x-kokoro-binding": "virtual_path must equal /.skills/{base64url(ASCII source_ref "
    "suffix)}/SKILL.md; revision only from this Run exact "
    "ResolvedSkill, absent if unproven",
}

_RUN_DISPLAY_SCHEMA = {
    "type": "object",
    "required": ["run_id", "display_skills", "tool_paths"],
    "properties": {
        "run_id": {"type": "string", "minLength": 1},
        "display_skills": {
            "type": "array",
            "maxItems": 16,
            "items": {"$ref": "#/components/schemas/SkillDisplay"},
            "x-kokoro-unique-by": "source_ref",
            "x-kokoro-binding": "unique source_ref subset of "
            "original ready source_refs; no "
            "current catalog lookup",
        },
        "tool_paths": {
            "type": "array",
            "maxItems": 128,
            "x-kokoro-unique-by": "activity_id",
            "items": {
                "type": "object",
                "required": ["activity_id", "virtual_path"],
                "properties": {
                    "activity_id": {
                        "type": "string",
                        "minLength": 68,
                        "maxLength": 68,
                        "pattern": "^act_[0-9a-f]{64}(?![\\s\\S])",
                    },
                    "virtual_path": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 4096,
                        "x-kokoro-utf8-byte-limit": 4096,
                        "x-kokoro-unicode-domain": "NFC "
                        "Unicode "
                        "scalars; "
                        "no "
                        "Cc, "
                        "Cf "
                        "or "
                        "Cs",
                        "description": "Validated "
                        "absolute "
                        "virtual "
                        "backend "
                        "path; "
                        "never "
                        "a "
                        "host "
                        "path, "
                        "URL, "
                        "secret "
                        "or "
                        "unproven "
                        "raw "
                        "argument.",
                    },
                },
                "additionalProperties": False,
            },
        },
    },
    "additionalProperties": False,
    "description": "Recovered safe display facts only. Missing facts remain unknown; "
    "source events and execution authority are unchanged. No completeness "
    "assertion.",
}

_RUN_DISPLAY_ENVELOPE = {
    "type": "object",
    "required": ["data", "meta"],
    "properties": {
        "data": {"$ref": "#/components/schemas/RunDisplay"},
        "meta": {"$ref": "#/components/schemas/Meta"},
    },
    "additionalProperties": False,
}

_RUN_DISPLAY_ROUTE: dict[str, object] = {
    "get": {
        "tags": ["runs"],
        "summary": "Read bounded identity-scoped Run display facts",
        "operationId": "getRunDisplay",
        "security": [{"AgentServiceBearer": []}],
        "parameters": [
            {
                "name": "run_id",
                "in": "path",
                "required": True,
                "schema": {"type": "string", "minLength": 1},
            },
            {"$ref": "#/components/parameters/RequestId"},
            {"$ref": "#/components/parameters/TenantRef"},
            {"$ref": "#/components/parameters/SubjectRef"},
            {"$ref": "#/components/parameters/SubjectKind"},
            {"$ref": "#/components/parameters/ActorRef"},
            {"$ref": "#/components/parameters/ActorKind"},
            {"$ref": "#/components/parameters/AssertionRef"},
        ],
        "x-kokoro-owner": "kokoro-agent",
        "x-kokoro-visibility": "internal-owner",
        "x-kokoro-stability": "stable",
        "x-kokoro-idempotency": "read-only",
        "x-kokoro-permission": "agent-service",
        "responses": {
            "200": {
                "description": "Safe recovered display facts, at most 1MiB.",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/RunDisplayEnvelope"}
                    }
                },
            },
            "401": {"$ref": "#/components/responses/Unauthorized"},
            "404": {"$ref": "#/components/responses/NotFound"},
            "503": {"$ref": "#/components/responses/Unavailable"},
            "400": {"$ref": "#/components/responses/BadRequest"},
        },
        "x-kokoro-read-budget": {
            "total_timeout_seconds": 5,
            "checkpoint_locators": 32,
            "native_input_bytes": 8388608,
            "query_body": "forbidden; Content-Length, "
            "Transfer-Encoding and Idempotency-Key "
            "forbidden",
        },
    }
}

_DISPLAY_READY_CONDITION = {
    "if": {"required": ["display_skills"]},
    "then": {"properties": {"phase": {"const": "ready"}}},
}

_DISPLAY_MCP_CONDITION = {
    "properties": {"display_detail": {"not": {"required": ["virtual_path"]}}}
}


_TOOL_FACTS = {
    "type": "array",
    "maxItems": 128,
    "x-kokoro-unique-by": "activity_id",
    "items": {
        "type": "object",
        "required": ["activity_id"],
        "properties": {
            "activity_id": {
                "type": "string",
                "minLength": 68,
                "maxLength": 68,
                "pattern": "^act_[0-9a-f]{64}(?![\\s\\S])",
            },
            "display_detail": {
                "type": "object",
                "required": ["kind", "name"],
                "properties": {
                    "kind": {"type": "string", "enum": ["mcp_tool"]},
                    "name": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 120,
                        "x-kokoro-utf8-byte-limit": 480,
                        "x-kokoro-unicode-domain": "NFC Unicode scalars; no Cc, Cf or Cs",
                    },
                },
                "additionalProperties": False,
            },
            "status": {"type": "string", "enum": ["failed"]},
        },
        "anyOf": [{"required": ["display_detail"]}, {"required": ["status"]}],
        "additionalProperties": False,
    },
    "description": "Read-only exact original MCP activity facts: safe public names witnessed by successful same-Run calls, or independently proved failed journal outcomes. No source event or Run status mutation.",
}


_ACTIVITY_DETAILS = {
    "type": "array",
    "maxItems": 128,
    "x-kokoro-unique-by": "activity_id",
    "items": {
        "type": "object",
        "required": ["activity_id", "tool_name"],
        "properties": {
            "activity_id": {
                "type": "string",
                "minLength": 68,
                "maxLength": 68,
                "pattern": "^act_[0-9a-f]{64}(?![\\s\\S])",
            },
            "tool_name": {
                "type": "string",
                "minLength": 1,
                "maxLength": 120,
                "x-kokoro-utf8-byte-limit": 480,
                "x-kokoro-unicode-domain": "NFC Unicode scalars; no Cc, Cf or Cs",
            },
            "target": {
                "type": "object",
                "required": ["kind", "name"],
                "properties": {
                    "kind": {"type": "string", "enum": ["file", "mcp_tool"]},
                    "name": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 120,
                        "x-kokoro-utf8-byte-limit": 480,
                        "x-kokoro-unicode-domain": "NFC "
                        "Unicode "
                        "scalars; "
                        "no Cc, Cf "
                        "or Cs",
                    },
                    "virtual_path": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 4096,
                        "x-kokoro-utf8-byte-limit": 4096,
                        "x-kokoro-unicode-domain": "NFC "
                        "Unicode "
                        "scalars; "
                        "no "
                        "Cc, "
                        "Cf "
                        "or "
                        "Cs",
                        "description": "Validated "
                        "absolute "
                        "virtual "
                        "backend path; "
                        "never a host "
                        "path, URL, "
                        "secret or "
                        "unproven raw "
                        "argument.",
                    },
                },
                "additionalProperties": False,
                "allOf": [
                    {
                        "if": {"properties": {"kind": {"const": "file"}}},
                        "then": {
                            "properties": {
                                "name": {"pattern": "^(?!\\.{1,2}$)[^/\\\\:]+$"}
                            }
                        },
                    },
                    {
                        "if": {"properties": {"kind": {"const": "mcp_tool"}}},
                        "then": {"not": {"required": ["virtual_path"]}},
                    },
                ],
            },
            "input_preview": {
                "type": "object",
                "required": ["text", "truncated", "redacted"],
                "properties": {
                    "text": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 2048,
                        "x-kokoro-utf8-byte-limit": 2048,
                    },
                    "truncated": {"type": "boolean"},
                    "redacted": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
            "output_preview": {
                "type": "object",
                "required": ["text", "truncated", "redacted"],
                "properties": {
                    "text": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 4096,
                        "x-kokoro-utf8-byte-limit": 4096,
                    },
                    "truncated": {"type": "boolean"},
                    "redacted": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
        },
        "additionalProperties": False,
    },
    "description": "Private same-Run evidence-bound tool details only; missing fields remain unknown. "
    "Filtered bounded text is not a universal secret-free guarantee. Never Share, "
    "execution authority, HTML, autolinks or instructions.",
}


_TOOL_DESCRIPTION = {
    "type": "object",
    "required": ["text", "truncated", "redacted"],
    "properties": {
        "text": {
            "type": "string",
            "minLength": 1,
            "maxLength": 1024,
            "x-kokoro-utf8-byte-limit": 1024,
        },
        "truncated": {"type": "boolean"},
        "redacted": {"type": "boolean"},
    },
    "additionalProperties": False,
    "description": "Provider capability documentation observed in this Run, "
    "matched by connection and selector. Not a per-call purpose, "
    "execution result, title or authorization. Missing or "
    "conflicting evidence is omitted; private display only.",
}

_TOOL_DESCRIPTION_CONDITION = [
    {
        "if": {"required": ["tool_description"]},
        "then": {
            "required": ["target"],
            "properties": {
                "tool_name": {"enum": ["mcp_call", "mcp_describe_tool"]},
                "target": {"properties": {"kind": {"const": "mcp_tool"}}},
            },
        },
    }
]


_ACTION_TITLE = {
    "type": "string",
    "minLength": 1,
    "maxLength": 256,
    "x-kokoro-utf8-byte-limit": 256,
    "description": "Public intent of this exact model tool call, bound before approval edits. Not reasoning, capability documentation, execution status or authority. No surrounding whitespace, control/format/surrogate characters or private text. Omitted when absent or mismatched; never inferred from adjacent prose.",
}


def http86_action_title_parent_bytes(document: dict[str, object]) -> bytes:
    """Remove only the exact per-call intent field; prove all issued HTTP8.6 bytes."""
    parent = copy.deepcopy(document)
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "8.7.0":
        raise ValueError("public action titles require HTTP 8.7.0")
    components = _object(parent.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    activity = _object(schemas.get("ChatActivity"), source="ChatActivity")
    branches = TypeAdapter(list[dict[str, object]]).validate_python(
        activity.get("oneOf")
    )
    fields = _object(branches[0].get("properties"), source="tool activity fields")
    if fields.pop("action_title", None) != _ACTION_TITLE:
        raise ValueError("public action title delta differs")
    branches[0]["properties"] = fields
    activity["oneOf"] = branches
    schemas["ChatActivity"] = activity
    components["schemas"] = schemas
    parent["components"] = components
    info["version"] = "8.6.0"
    parent["info"] = info
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "de73c229a962e37afbf3f69aba160a8b6b9a56cd2603952a3d3d97f5a610b64b"
    ):
        raise ValueError("complete HTTP8.6 action title parent differs")
    return raw


def http85_tool_description_parent_bytes(document: dict[str, object]) -> bytes:
    """Remove only the bounded capability description; prove issued HTTP8.5."""
    if _object(document.get("info"), source="info").get("version") == "8.7.0":
        document = _object(
            json.loads(http86_action_title_parent_bytes(document)),
            source="HTTP8.6 parent",
        )
    parent = copy.deepcopy(document)
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "8.6.0":
        raise ValueError("tool descriptions require HTTP 8.6.0")
    schemas = _object(
        _object(parent.get("components"), source="components").get("schemas"),
        source="schemas",
    )
    display = _object(schemas.get("RunDisplay"), source="RunDisplay")
    properties = _object(display.get("properties"), source="RunDisplay properties")
    details = _object(properties.get("activity_details"), source="activity details")
    item = _object(details.get("items"), source="activity detail")
    fields = _object(item.get("properties"), source="activity detail fields")
    if (
        fields.pop("tool_description", None) != _TOOL_DESCRIPTION
        or item.pop("allOf", None) != _TOOL_DESCRIPTION_CONDITION
    ):
        raise ValueError("tool description delta differs")
    item["properties"] = fields
    details["items"] = item
    properties["activity_details"] = details
    display["properties"] = properties
    schemas["RunDisplay"] = display
    components = _object(parent["components"], source="components")
    components["schemas"] = schemas
    parent["components"] = components
    info["version"] = "8.5.0"
    parent["info"] = info
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "b000f24e88bf5e4f20718ae7f6b29c94ef96b2c6fe42f73ef7231695f438e897"
    ):
        raise ValueError("complete HTTP8.5 tool description parent differs")
    return raw


def http84_activity_details_parent_bytes(document: dict[str, object]) -> bytes:
    """Remove only private details and prove every byte of the issued HTTP8.4."""
    if _object(document.get("info"), source="info").get("version") in {
        "8.6.0",
        "8.7.0",
    }:
        document = _object(
            json.loads(http85_tool_description_parent_bytes(document)),
            source="HTTP8.5 parent",
        )
    parent = copy.deepcopy(document)
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "8.5.0":
        raise ValueError("private details require HTTP 8.5.0")
    components = _object(parent.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    display = _object(schemas.get("RunDisplay"), source="RunDisplay")
    properties = _object(display.get("properties"), source="RunDisplay properties")
    if properties.pop("activity_details", None) != _ACTIVITY_DETAILS or display.get(
        "required"
    ) != ["run_id", "display_skills", "tool_paths", "tool_facts", "activity_details"]:
        raise ValueError("private details delta differs")
    display["properties"] = properties
    display["required"] = ["run_id", "display_skills", "tool_paths", "tool_facts"]
    schemas["RunDisplay"] = display
    components["schemas"] = schemas
    parent["components"] = components
    info["version"] = "8.4.0"
    parent["info"] = info
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "239219e5a80427a74f8d89363c5a7a4fe2753f99c6b469b02bab7fba9ba44f52"
    ):
        raise ValueError("complete HTTP8.4 private details parent differs")
    return raw


def http83_tool_facts_parent_bytes(document: dict[str, object]) -> bytes:
    """Remove only the exact historical MCP display delta; prove all HTTP8.3 bytes."""
    if _object(document.get("info"), source="info").get("version") in {
        "8.5.0",
        "8.6.0",
        "8.7.0",
    }:
        document = _object(
            json.loads(http84_activity_details_parent_bytes(document)),
            source="HTTP8.4 parent",
        )
    parent = copy.deepcopy(document)
    info = _object(parent.get("info"), source="info")
    if info.get("version") != "8.4.0":
        raise ValueError("tool facts require HTTP 8.4.0")
    components = _object(parent.get("components"), source="components")
    schemas = _object(components.get("schemas"), source="schemas")
    display = _object(schemas.get("RunDisplay"), source="RunDisplay")
    properties = _object(display.get("properties"), source="RunDisplay properties")
    if properties.pop("tool_facts", None) != _TOOL_FACTS or display.get("required") != [
        "run_id",
        "display_skills",
        "tool_paths",
        "tool_facts",
    ]:
        raise ValueError("exact historical tool facts delta differs")
    display["properties"] = properties
    display["required"] = ["run_id", "display_skills", "tool_paths"]
    schemas["RunDisplay"] = display
    components["schemas"] = schemas
    parent["components"] = components
    info["version"] = "8.3.0"
    parent["info"] = info
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "be642c2313fbaf447d4e37bda2a7920f867c3a06d39f7e0e82efaf212e7080d6"
    ):
        raise ValueError("complete HTTP8.3 tool facts parent differs")
    return raw


def http82_display_parent_bytes(document: dict[str, object]) -> bytes:
    """Remove only the issued display nodes, then prove every HTTP8.2 byte."""
    if _object(document.get("info"), source="info").get("version") in {
        "8.4.0",
        "8.5.0",
        "8.6.0",
        "8.7.0",
    }:
        document = _object(
            json.loads(http83_tool_facts_parent_bytes(document)),
            source="HTTP8.3 tool facts parent",
        )
    parent = copy.deepcopy(document)
    if _object(parent.get("info"), source="info").get("version") != "8.3.0":
        raise ValueError("display delta requires HTTP 8.3.0")
    paths = _object(parent.get("paths"), source="paths")
    if paths.pop("/v1/runs/{run_id}/display", None) != _RUN_DISPLAY_ROUTE:
        raise ValueError("Run display resource differs")
    schemas = _object(
        _object(parent.get("components"), source="components").get("schemas"),
        source="schemas",
    )
    for name, expected in (
        ("SkillDisplay", _SKILL_DISPLAY_SCHEMA),
        ("RunDisplay", _RUN_DISPLAY_SCHEMA),
        ("RunDisplayEnvelope", _RUN_DISPLAY_ENVELOPE),
    ):
        if schemas.pop(name, None) != expected:
            raise ValueError("Run display schema differs")
    branches = TypeAdapter(list[dict[str, object]]).validate_python(
        _object(schemas.get("ChatActivity"), source="ChatActivity").get("oneOf")
    )
    tool = _object(branches[0].get("properties"), source="tool")
    detail = _object(
        _object(tool.get("display_detail"), source="detail").get("properties"),
        source="detail properties",
    )
    if detail.pop("virtual_path", None) != _DISPLAY_PATH:
        raise ValueError("file virtual display path differs")
    conditions = TypeAdapter(list[dict[str, object]]).validate_python(
        branches[0].get("allOf")
    )
    then = _object(conditions[1].get("then"), source="MCP condition")
    mcp_properties = _object(then.get("properties"), source="MCP properties")
    if (
        mcp_properties.pop("display_detail", None)
        != _DISPLAY_MCP_CONDITION["properties"]["display_detail"]
    ):
        raise ValueError("MCP virtual path exclusion differs")
    skill_properties = _object(branches[2].get("properties"), source="Skill")
    if skill_properties.pop("display_skills", None) != _DISPLAY_SKILLS or branches[
        2
    ].pop("allOf", None) != [_DISPLAY_READY_CONDITION]:
        raise ValueError("Skill display delta differs")
    # TypeAdapter may copy nested containers; assign changed exact nodes back.
    detail_schema = _object(tool.get("display_detail"), source="detail")
    detail_schema["properties"] = detail
    tool["display_detail"] = detail_schema
    branches[0]["properties"] = tool
    then["properties"] = mcp_properties
    conditions[1]["then"] = then
    branches[0]["allOf"] = conditions
    branches[2]["properties"] = skill_properties
    schemas["ChatActivity"] = {"oneOf": branches}
    components = _object(parent.get("components"), source="components")
    components["schemas"] = schemas
    parent["components"] = components
    parent["paths"] = paths
    info = _object(parent.get("info"), source="info")
    info["version"] = "8.2.0"
    parent["info"] = info
    raw = (json.dumps(parent, indent=2, ensure_ascii=False) + "\n").encode()
    if (
        hashlib.sha256(raw).hexdigest()
        != "8a0b9973661205b8e29020934c39e5c7ab629b4c42db4e79c845a410d96efa2a"
    ):
        raise ValueError("complete HTTP8.2 display parent differs")
    return raw


def _display_parent(document: dict[str, object]) -> dict[str, object]:
    if _object(document.get("info"), source="info").get("version") in {
        "8.3.0",
        "8.4.0",
        "8.5.0",
        "8.6.0",
        "8.7.0",
    }:
        return _object(
            json.loads(http82_display_parent_bytes(document)),
            source="HTTP8.2 display parent",
        )
    return document
