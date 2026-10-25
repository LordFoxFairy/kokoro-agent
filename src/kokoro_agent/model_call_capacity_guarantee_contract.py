"""Generate and verify the one finite Agent-owned capacity guarantee."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import rfc8785
from jsonschema import Draft202012Validator, ValidationError as SchemaValidationError
from jsonschema import validate as validate_schema_instance
from pydantic import JsonValue, TypeAdapter

BASE = "contract/model-call-capacity-guarantee/v1"
GENERATED = "src/kokoro_agent/protocol/model_call_capacity_guarantee_generated.py"
NORMALIZATION = "contract/usage/v1/manifest.json"
NORMALIZATION_SHA = "ecd30d1908f410eed29181c9f52669f824312bf3a2e3874153d990b95fe58b91"
SOURCES = (
    f"{BASE}/schema.json",
    f"{BASE}/guarantee.json",
    f"{BASE}/evidence.json",
    f"{BASE}/vectors.json",
    NORMALIZATION,
    "scripts/generate_model_call_capacity_guarantee.py",
    "src/kokoro_agent/model_call_capacity_guarantee_contract.py",
    "src/kokoro_agent/model/capacity_guarantee.py",
    "src/kokoro_agent/model/factory.py",
    "src/kokoro_agent/distribution_assets.py",
    GENERATED,
)
AUDIT_FILES = (*SOURCES, f"{BASE}/manifest.json")
_OBJECT = TypeAdapter(dict[str, JsonValue])
_NODES = TypeAdapter(dict[str, dict[str, JsonValue]])
_STRINGS = TypeAdapter(list[str])
_VECTORS = TypeAdapter(dict[str, list[dict[str, JsonValue]]])


def _finite_fields(
    schema: dict[str, JsonValue], *, root: bool = False
) -> dict[str, dict[str, JsonValue]]:
    allowed = {"type", "additionalProperties", "required", "properties"}
    if root:
        allowed |= {"$schema", "$id", "title", "$defs"}
    if set(schema) != allowed:
        raise ValueError("capacity schema has unknown or missing keywords")
    properties = _NODES.validate_python(schema["properties"], strict=True)
    required = _STRINGS.validate_python(schema["required"], strict=True)
    if (
        schema.get("type") != "object"
        or schema.get("additionalProperties") is not False
        or set(properties) != set(required)
        or len(required) != len(properties)
    ):
        raise ValueError("capacity fields must be closed and required")
    for node in properties.values():
        kind = node.get("type")
        keys = {"type", "const"}
        if kind == "string":
            keys |= {"minLength", "maxLength"}
        elif kind == "array":
            keys |= {"items", "minItems", "maxItems"}
            if node.get("items") != {"type": "string"}:
                raise ValueError("capacity tuple must contain only strings")
        elif kind not in ("integer", "boolean"):
            raise ValueError("capacity schema type is unsupported")
        if set(node) != keys:
            raise ValueError("capacity field has unknown or missing keywords")
    return properties


def _expected(fields: dict[str, dict[str, JsonValue]]) -> bytes:
    return rfc8785.dumps({key: node["const"] for key, node in fields.items()})


def _model(name: str, fields: dict[str, dict[str, JsonValue]]) -> list[str]:
    lines = [
        "",
        f"class {name}(BaseModel):",
        '    model_config = ConfigDict(strict=True, frozen=True, extra="forbid", hide_input_in_errors=True)',
    ]
    for key, node in fields.items():
        value = node["const"]
        if isinstance(value, list):
            values = _STRINGS.validate_python(value, strict=True)
            annotation = (
                "tuple[" + ", ".join(f"Literal[{item!r}]" for item in values) + "]"
            )
        elif type(value) in (str, int, bool):
            annotation = f"Literal[{value!r}]"
        else:
            raise ValueError("capacity schema requires finite scalar or string tuple")
        lines.append(f"    {key}: {annotation}")
    return lines


def generate(root: Path, *, check: bool = False) -> None:
    """Only this guarantee's models/manifest are derived; old codecs are untouched."""
    schema = _OBJECT.validate_json(
        (root / BASE / "schema.json").read_bytes(), strict=True
    )
    guarantee = _OBJECT.validate_json(
        (root / BASE / "guarantee.json").read_bytes(), strict=True
    )
    evidence = _OBJECT.validate_json(
        (root / BASE / "evidence.json").read_bytes(), strict=True
    )
    Draft202012Validator.check_schema(schema)
    properties = _finite_fields(schema, root=True)
    definitions = _NODES.validate_python(schema["$defs"], strict=True)
    if set(definitions) != {"ReviewedEvidence"}:
        raise ValueError("capacity evidence definition is invalid")
    evidence_fields = _finite_fields(definitions["ReviewedEvidence"])
    validate_schema_instance(
        instance=guarantee, schema=schema, cls=Draft202012Validator
    )
    validate_schema_instance(
        instance=evidence,
        schema=definitions["ReviewedEvidence"],
        cls=Draft202012Validator,
    )
    expected_bytes = _expected(properties)
    evidence_bytes = _expected(evidence_fields)
    if (
        rfc8785.dumps(guarantee) != expected_bytes
        or rfc8785.dumps(evidence) != evidence_bytes
    ):
        raise ValueError("capacity artifact does not match its finite schema")
    if (
        evidence.get("owner") != "kokoro-agent"
        or evidence.get("artifact_version") != "1.0.0"
        or evidence.get("checked_on") != guarantee.get("evidence_checked_on")
        or evidence.get("profile_id") != guarantee.get("profile_id")
        or guarantee.get("evidence_version") != evidence.get("artifact_version")
        or guarantee.get("evidence_sha256")
        != hashlib.sha256(evidence_bytes).hexdigest()
    ):
        raise ValueError("capacity evidence identity is invalid")
    normalization = _OBJECT.validate_json(
        (root / NORMALIZATION).read_bytes(), strict=True
    )
    if (
        normalization.get("owner") != "kokoro-agent"
        or normalization.get("artifact_version") != "1.0.0"
        or normalization.get("aggregate_sha256") != NORMALIZATION_SHA
        or guarantee.get("normalization_owner") != normalization.get("owner")
        or guarantee.get("normalization_version")
        != normalization.get("artifact_version")
        or guarantee.get("normalization_manifest_aggregate_sha256") != NORMALIZATION_SHA
    ):
        raise ValueError("capacity normalization identity is invalid")
    vectors = _VECTORS.validate_json(
        (root / BASE / "vectors.json").read_bytes(), strict=True
    )
    if (
        set(vectors) != {"valid", "invalid"}
        or not vectors["valid"]
        or not vectors["invalid"]
    ):
        raise ValueError("capacity vectors are incomplete")
    for vector in vectors["valid"]:
        validate_schema_instance(
            instance=vector["value"], schema=schema, cls=Draft202012Validator
        )
        if rfc8785.dumps(vector["value"]) != expected_bytes:
            raise ValueError("capacity positive vector is rejected")
    for vector in vectors["invalid"]:
        mutation = _OBJECT.validate_python(vector["mutation"], strict=True)
        candidate = {**guarantee, **mutation}
        try:
            validate_schema_instance(
                instance=candidate, schema=schema, cls=Draft202012Validator
            )
        except SchemaValidationError:
            pass
        else:
            raise ValueError("capacity negative vector is accepted")
        if rfc8785.dumps(candidate) == expected_bytes:
            raise ValueError("capacity negative vector contradicts finite schema")
    lines = [
        "# fmt: off",
        '"""Generated from the finite capacity guarantee schema; do not edit."""',
        "from typing import Literal",
        "from pydantic import BaseModel, ConfigDict",
        "",
        f'GUARANTEE_SHA256 = "{hashlib.sha256(expected_bytes).hexdigest()}"',
        *_model("ModelCallCapacityEvidence", evidence_fields),
        *_model("ModelCallCapacityGuarantee", properties),
    ]
    content = ("\n".join(lines) + "\n").encode()
    path = root / GENERATED
    if check:
        if path.read_bytes() != content:
            raise ValueError("capacity generated model drift")
    else:
        path.write_bytes(content)
    hashes = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in SOURCES
    }
    manifest = {
        "owner": "kokoro-agent",
        "artifact_version": "1.0.0",
        "guarantee_sha256": hashlib.sha256(expected_bytes).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
        "source_sha256": hashes,
        "aggregate_sha256": hashlib.sha256(rfc8785.dumps(hashes)).hexdigest(),
    }
    data = (json.dumps(manifest, indent=2) + "\n").encode()
    manifest_path = root / BASE / "manifest.json"
    if check:
        if manifest_path.read_bytes() != data:
            raise ValueError("capacity manifest drift")
    else:
        manifest_path.write_bytes(data)
