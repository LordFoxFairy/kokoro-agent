"""Compile the finite Agent usage schema into strict, immutable wire models."""

from __future__ import annotations

import hashlib
import json
import keyword
import re
from collections.abc import Callable
from pathlib import Path

import rfc8785
from jsonschema import Draft202012Validator
from pydantic import BaseModel, TypeAdapter

from kokoro_agent.model.usage_evidence import parse_evidence, validate_successor

VECTORS_RELATIVE = "contract/usage/v1/vectors.json"
MANIFEST_RELATIVE = "contract/usage/v1/manifest.json"
USAGE_SOURCE_FILES = (
    "contract/usage/v1/schema.json",
    VECTORS_RELATIVE,
    "scripts/generate_model_usage_models.py",
    "src/kokoro_agent/model_usage_contract.py",
    "src/kokoro_agent/model/usage_evidence.py",
    "src/kokoro_agent/protocol/model_usage_generated.py",
)
USAGE_AUDIT_FILES = (*USAGE_SOURCE_FILES, MANIFEST_RELATIVE)
SCHEMA_RELATIVE = "contract/usage/v1/schema.json"
GENERATED_RELATIVE = "src/kokoro_agent/protocol/model_usage_generated.py"
_OBJECT = TypeAdapter(dict[str, object])
_STRINGS = TypeAdapter(list[str])
_NODES = TypeAdapter(list[dict[str, object]])
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def _object(value: object) -> dict[str, object]:
    return _OBJECT.validate_python(value, strict=True)


def _keys(node: dict[str, object], allowed: set[str]) -> None:
    if set(node) - allowed:
        raise ValueError("unsupported usage schema keywords")


def _annotation(node: dict[str, object], defined: set[str]) -> str:
    if "$ref" in node:
        _keys(node, {"$ref"})
        reference = node["$ref"]
        if not isinstance(reference, str) or not reference.startswith("#/$defs/"):
            raise ValueError("usage schema references must be local")
        name = reference.removeprefix("#/$defs/")
        if name not in defined:
            raise ValueError("usage schema reference must precede its consumer")
        return name
    if "anyOf" in node:
        _keys(node, {"anyOf"})
        branches = _NODES.validate_python(node["anyOf"], strict=True)
        if len(branches) < 2:
            raise ValueError("usage union requires at least two branches")
        return " | ".join(_annotation(branch, defined) for branch in branches)
    kind = node.get("type")
    if kind == "null":
        _keys(node, {"type"})
        return "None"
    if kind == "boolean":
        _keys(node, {"type"})
        return "bool"
    if kind == "array":
        _keys(node, {"type", "items", "maxItems"})
        maximum = node.get("maxItems")
        if type(maximum) is not int or maximum < 1:
            raise ValueError("usage arrays must have a positive item bound")
        item = _annotation(_object(node["items"]), defined)
        return f"Annotated[list[{item}], Field(max_length={maximum})]"
    if kind != "string":
        raise ValueError("unsupported usage schema type")
    _keys(
        node,
        {
            "type",
            "const",
            "enum",
            "pattern",
            "minLength",
            "maxLength",
            "x-utf8-max-bytes",
            "x-max-decimal",
            "x-calendar-valid",
        },
    )
    if "const" in node or "enum" in node:
        _keys(node, {"type", "const"} if "const" in node else {"type", "enum"})
        values = [node["const"]] if "const" in node else node["enum"]
        strings = _STRINGS.validate_python(values, strict=True)
        if not strings or len(set(strings)) != len(strings):
            raise ValueError("usage literals must be nonempty and unique")
        return "Literal[" + ", ".join(json.dumps(value) for value in strings) + "]"
    constraints: list[str] = []
    for source, target in (
        ("pattern", "pattern"),
        ("minLength", "min_length"),
        ("maxLength", "max_length"),
    ):
        if source in node:
            value = node[source]
            if source == "pattern":
                if not isinstance(value, str):
                    raise ValueError("usage pattern must be text")
                re.compile(value)
            elif type(value) is not int or value < 0:
                raise ValueError("usage length must be nonnegative")
            constraints.append(f"{target}={json.dumps(value)}")
    validators: list[str] = []
    if "x-utf8-max-bytes" in node:
        limit = node["x-utf8-max-bytes"]
        if type(limit) is not int or limit not in {128, 512}:
            raise ValueError("unsupported usage UTF-8 bound")
        validators.append(f"AfterValidator(_utf8_limit({limit}))")
    if "x-max-decimal" in node:
        if node["x-max-decimal"] != "9223372036854775807":
            raise ValueError("unsupported usage decimal bound")
        validators.append("AfterValidator(_decimal_limit)")
    if "x-calendar-valid" in node:
        if node["x-calendar-valid"] is not True:
            raise ValueError("usage instants must validate their calendar")
        validators.append("AfterValidator(_calendar)")
    metadata = (
        [f"Field({', '.join(constraints)})"] if constraints else []
    ) + validators
    return f"Annotated[str, {', '.join(metadata)}]" if metadata else "str"


def _arguments(value: str) -> list[str]:
    """Split only top-level commas in a generated annotation, never user code."""
    result: list[str] = []
    depth = 0
    quoted = False
    escaped = False
    start = 0
    for position, character in enumerate(value):
        if quoted:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                quoted = False
        elif character == '"':
            quoted = True
        elif character in "[(":
            depth += 1
        elif character in "])":
            depth -= 1
        elif character == "," and depth == 0:
            result.append(value[start:position].strip())
            start = position + 1
    result.append(value[start:].strip())
    return result


def _layout(prefix: str, expression: str, suffix: str = "") -> list[str]:
    """Render the finite annotation dialect deterministically, without formatter I/O."""
    if len(prefix + expression + suffix) <= 88:
        return [prefix + expression + suffix]
    indentation = " " * (len(prefix) - len(prefix.lstrip()))
    inner_indent = indentation + "    "
    openings = [index for index, char in enumerate(expression) if char in "[("]
    if not openings:
        return [prefix + "(", inner_indent + expression, indentation + ")" + suffix]
    position = openings[0]
    opener = expression[position]
    closer = "]" if opener == "[" else ")"
    if not expression.endswith(closer):
        raise ValueError("unsupported generated annotation layout")
    body = expression[position + 1 : -1]
    result = [prefix + expression[: position + 1]]
    if len(inner_indent + body) <= 88:
        result.append(inner_indent + body)
    else:
        arguments = _arguments(body)
        for argument in arguments:
            result.extend(
                _layout(inner_indent, argument, "," if len(arguments) > 1 else "")
            )
    result.append(indentation + closer + suffix)
    return result


def model_usage_model_bytes(schema_bytes: bytes) -> bytes:
    """Only compile the approved finite shape dialect; never generate arbitrary code."""
    document = _strict_document(schema_bytes)
    _keys(
        document,
        {
            "$schema",
            "$id",
            "title",
            "x-artifact-version",
            "x-max-json-bytes",
            "x-max-json-depth",
            "$defs",
            "$ref",
        },
    )
    if document.get("x-artifact-version") != "1.0.0":
        raise ValueError("usage artifact version must be 1.0.0")
    if document.get("$ref") != "#/$defs/ModelUsageEvidence":
        raise ValueError("usage root must be ModelUsageEvidence")
    if (
        document.get("$schema") != "https://json-schema.org/draft/2020-12/schema"
        or document.get("$id") != "urn:kokoro-agent:model-usage-evidence:v1"
        or document.get("x-max-json-bytes") != 65536
        or document.get("x-max-json-depth") != 16
    ):
        raise ValueError("usage artifact has unsupported identity or resource limits")
    Draft202012Validator.check_schema(document)
    definitions = _object(document["$defs"])
    for name, extension, expected in (
        ("Count", "x-max-decimal", "9223372036854775807"),
        ("Ordinal", "x-max-decimal", "9223372036854775807"),
        ("Opaque", "x-utf8-max-bytes", 512),
        ("UtcInstant", "x-calendar-valid", True),
    ):
        if _object(definitions[name]).get(extension) != expected:
            raise ValueError("usage semantic bound is missing or changed")
    usage_properties = _object(_object(definitions["TokenUsageV1"])["properties"])
    for name, expected in (
        ("profile_id", {"type": "string", "const": "token_usage_v1"}),
        ("unit", {"type": "string", "const": "token"}),
        ("aggregation_mode", {"type": "string", "const": "final_cumulative"}),
        (
            "source_protocol",
            {
                "type": "string",
                "enum": ["openai_chat_completions", "ollama_openai_chat_completions"],
            },
        ),
    ):
        if usage_properties.get(name) != expected:
            raise ValueError("usage source interpretation is unsupported")
    identity_properties = _object(
        _object(definitions["EvidenceIdentity"])["properties"]
    )
    path = _object(identity_properties["peer_path"])
    if (
        path.get("maxItems") != 32
        or _object(path["items"]).get("x-utf8-max-bytes") != 128
    ):
        raise ValueError("usage peer path limits are unsupported")
    lines = [
        '"""Generated from contract/usage/v1/schema.json. Do not edit."""',
        "",
        "from __future__ import annotations",
        "",
        "from collections.abc import Callable",
        "from datetime import datetime",
        "import re",
        "from typing import Annotated, Literal, TypeAlias",
        "",
        "from pydantic import AfterValidator, BaseModel, ConfigDict, Field",
        "",
        f'SOURCE_SHA256 = "{hashlib.sha256(schema_bytes).hexdigest()}"',
        "",
        f"MAX_JSON_BYTES = {document['x-max-json-bytes']}",
        f"MAX_JSON_DEPTH = {document['x-max-json-depth']}",
        "",
        "",
        "def _utf8_limit(limit: int) -> Callable[[str], str]:",
        "    def validate(value: str) -> str:",
        '        if len(value.encode("utf-8", errors="strict")) > limit:',
        '            raise ValueError("usage string exceeds its UTF-8 bound")',
        "        return value",
        "",
        "    return validate",
        "",
        "",
        "def _decimal_limit(value: str) -> str:",
        "    if int(value) > 9223372036854775807:",
        '        raise ValueError("usage decimal exceeds int63")',
        "    return value",
        "",
        "",
        "def _calendar(value: str) -> str:",
        "    if not re.fullmatch(",
        '        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z", value',
        "    ):",
        '        raise ValueError("usage instant must be UTC milliseconds")',
        '    datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")',
        "    return value",
        "",
        "",
        "class UsageWireModel(BaseModel):",
        '    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)',
    ]
    defined: set[str] = set()
    for name, raw in definitions.items():
        if (
            not _NAME.fullmatch(name)
            or keyword.iskeyword(name)
            or name
            in {"UsageWireModel", "SOURCE_SHA256", "MAX_JSON_BYTES", "MAX_JSON_DEPTH"}
        ):
            raise ValueError("invalid usage definition name")
        node = _object(raw)
        lines.extend(["", ""])
        if node.get("type") == "object":
            _keys(node, {"type", "additionalProperties", "properties", "required"})
            properties = _object(node["properties"])
            required = _STRINGS.validate_python(node["required"], strict=True)
            if node.get("additionalProperties") is not False or required != list(
                properties
            ):
                raise ValueError(
                    "usage objects must be closed with all members required"
                )
            if not properties:
                raise ValueError("empty usage objects are unsupported")
            lines.append(f"class {name}(UsageWireModel):")
            for field, shape in properties.items():
                if (
                    not _NAME.fullmatch(field)
                    or keyword.iskeyword(field)
                    or hasattr(BaseModel, field)
                    or field in {"UsageWireModel", "SOURCE_SHA256"}
                ):
                    raise ValueError("invalid usage field name")
                annotation = _annotation(_object(shape), defined)
                lines.extend(_layout(f"    {field}: ", annotation))
        else:
            lines.extend(_layout(f"{name}: TypeAlias = ", _annotation(node, defined)))
        defined.add(name)
    if "ModelUsageEvidence" not in defined:
        raise ValueError("usage root definition is missing")
    return ("\n".join(lines) + "\n").encode()


def generate_model_usage_models(root: Path, *, check: bool) -> None:
    """Generation writes only the model; check mode never writes, including on error."""
    expected = model_usage_model_bytes((root / SCHEMA_RELATIVE).read_bytes())
    output = root / GENERATED_RELATIVE
    if check:
        if not output.is_file() or output.read_bytes() != expected:
            raise ValueError("generated usage model is stale")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(expected)


def _manifest_bytes(root: Path, generated: bytes) -> bytes:
    records = [
        {
            "path": relative,
            "sha256": hashlib.sha256(
                generated
                if relative == GENERATED_RELATIVE
                else (root / relative).read_bytes()
            ).hexdigest(),
        }
        for relative in sorted(USAGE_SOURCE_FILES)
    ]
    aggregate = hashlib.sha256(
        b"kokoro-agent:model-usage-artifact:v1\n" + rfc8785.dumps(records)
    ).hexdigest()
    manifest = {
        "artifact_version": "1.0.0",
        "owner": "kokoro-agent",
        "files": records,
        "aggregate_sha256": aggregate,
    }
    return (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode()


def generate_model_usage_artifact(root: Path, *, check: bool) -> None:
    """Regenerate or check the independent usage inventory; no provenance self-cycle."""
    model = model_usage_model_bytes((root / SCHEMA_RELATIVE).read_bytes())
    manifest = _manifest_bytes(root, model)
    outputs = {
        GENERATED_RELATIVE: model,
        MANIFEST_RELATIVE: manifest,
    }
    if check:
        for relative, expected in outputs.items():
            path = root / relative
            if not path.is_file() or path.read_bytes() != expected:
                raise ValueError("usage artifact is stale")
        return
    for relative, expected in outputs.items():
        output = root / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(expected)


def validate_model_usage_contract(root: Path) -> None:
    """Check packaged source bindings and execute the immutable conformance vectors."""
    generate_model_usage_artifact(root, check=True)
    vectors = _strict_document((root / VECTORS_RELATIVE).read_bytes())
    _keys(vectors, {"artifact_version", "positive", "negative", "pairs"})
    if vectors.get("artifact_version") != "1.0.0":
        raise ValueError("usage vector version is invalid")
    ids: set[str] = set()
    for category in ("positive", "negative", "pairs"):
        rows = _NODES.validate_python(vectors[category], strict=True)
        if not rows:
            raise ValueError("usage conformance category cannot be empty")
        for row in rows:
            identity = row.get("id")
            if not isinstance(identity, str) or not identity or identity in ids:
                raise ValueError("usage vector identity must be nonempty and unique")
            ids.add(identity)
            if category == "pairs":
                _keys(
                    row, {"id", "previous_json_utf8", "incoming_json_utf8", "accepted"}
                )
                accepted = row["accepted"]
                if type(accepted) is not bool:
                    raise ValueError("usage pair expectation must be boolean")
                previous = parse_evidence(_vector_bytes(row, "previous_json_utf8"))
                incoming = parse_evidence(_vector_bytes(row, "incoming_json_utf8"))
                if accepted:
                    validate_successor(previous, incoming)
                else:
                    _expect_rejection(lambda: validate_successor(previous, incoming))
            else:
                _keys(row, {"id", "json_utf8", "digest"})
                raw = _vector_bytes(row, "json_utf8")
                if category == "positive":
                    evidence = parse_evidence(raw)
                    if evidence.digest != row["digest"]:
                        raise ValueError("positive vector digest is not exact")
                    if rfc8785.dumps(json.loads(raw)) != raw:
                        raise ValueError("positive vector bytes are not canonical")
                else:
                    _expect_rejection(lambda: parse_evidence(raw))


def _vector_bytes(row: dict[str, object], key: str) -> bytes:
    value = row[key]
    if not isinstance(value, str):
        raise ValueError("usage vector JSON must be text")
    return value.encode("utf-8", errors="strict")


def _expect_rejection(operation: Callable[[], object]) -> None:
    try:
        operation()
    except ValueError:
        return
    raise ValueError("invalid usage vector was accepted")


def _strict_document(raw: bytes) -> dict[str, object]:
    def members(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate usage artifact member")
            result[key] = value
        return result

    def constant(value: str) -> object:
        raise ValueError("nonfinite usage artifact value")

    return _OBJECT.validate_python(
        json.loads(
            raw.decode("utf-8"), object_pairs_hook=members, parse_constant=constant
        ),
        strict=True,
    )
