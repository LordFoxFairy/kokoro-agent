"""Compile the finite immutable execution-bound contract, not usage history."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError
from jsonschema import validate as validate_schema_instance
from pydantic import JsonValue, TypeAdapter
from kokoro_agent.protocol.model_call_capacity_guarantee_generated import (
    GUARANTEE_SHA256,
)
from kokoro_agent.protocol.model_usage_v2_generated import SOURCE_SHA256

BASE = "contract/model-call-bound/v1"
GENERATED = "src/kokoro_agent/protocol/model_call_bound_generated.py"
SOURCES = (
    f"{BASE}/schema.json",
    f"{BASE}/vectors.json",
    "scripts/generate_model_call_bound.py",
    "src/kokoro_agent/model_call_bound_contract.py",
    "src/kokoro_agent/model/call_bound.py",
    GENERATED,
)
AUDIT_FILES = (*SOURCES, f"{BASE}/manifest.json")
_OBJECT = TypeAdapter(dict[str, JsonValue])
_NODES = TypeAdapter(dict[str, dict[str, JsonValue]])
_VECTORS = TypeAdapter(dict[str, list[dict[str, JsonValue]]])


def generate(root: Path, *, check: bool = False) -> None:
    schema_raw = (root / BASE / "schema.json").read_bytes()
    schema = _OBJECT.validate_json(schema_raw, strict=True)
    Draft202012Validator.check_schema(schema)
    if (
        set(schema)
        != {
            "$schema",
            "$id",
            "title",
            "x-artifact-version",
            "x-max-json-bytes",
            "x-max-json-depth",
            "type",
            "additionalProperties",
            "properties",
            "required",
            "$defs",
        }
        or schema["type"] != "object"
        or schema["additionalProperties"] is not False
    ):
        raise ValueError("bound schema dialect differs")
    if (
        schema["$id"] != "urn:kokoro-agent:model-call-bound:v1"
        or schema["x-artifact-version"] != "1.0.0"
        or schema["x-max-json-bytes"] != 65536
        or schema["x-max-json-depth"] != 16
    ):
        raise ValueError("bound schema identity differs")
    fields = _NODES.validate_python(schema["properties"], strict=True)
    if list(fields) != [
        "artifact_version",
        "producer",
        "profile_id",
        "revision",
        "sealed",
        "execution_bound_ref",
        "planned_binding_ref",
        "planned_binding_digest",
        "tenant_ref",
        "run_id",
        "call_id",
        "attempt_id",
        "attempt_ordinal",
        "lease_owner",
        "lease_generation",
        "final_request_binding_version",
        "final_request_digest",
        "request_profile",
        "capacity_guarantee_owner",
        "capacity_guarantee_id",
        "capacity_guarantee_version",
        "capacity_guarantee_digest",
        "unit",
        "prompt_tokens_max",
        "output_tokens_max",
        "total_tokens_max",
        "joint_constraints",
        "system_provenance",
        "sealed_at",
        "valid_until",
    ]:
        raise ValueError("bound field inventory differs")
    if schema["required"] != list(fields):
        raise ValueError("bound fields must be closed and required")
    provenance = _OBJECT.validate_json(
        (root / "contract/provenance.json").read_bytes(), strict=True
    )
    consumers = TypeAdapter(list[dict[str, JsonValue]]).validate_python(
        provenance["consumer_dependencies"], strict=True
    )
    system = next(item for item in consumers if item.get("owner") == "kokoro-system")
    if fields["capacity_guarantee_digest"] != {
        "type": "string",
        "const": GUARANTEE_SHA256,
    } or fields["system_provenance"] != {"type": "object", "const": system}:
        raise ValueError("bound formal owner identity differs")
    definitions = _NODES.validate_python(schema["$defs"], strict=True)
    usage_raw = (root / "contract/usage/v2/schema.json").read_bytes()
    if hashlib.sha256(usage_raw).hexdigest() != SOURCE_SHA256:
        raise ValueError("bound imported usage scalar schema differs")
    usage = _OBJECT.validate_json(usage_raw, strict=True)
    usage_defs = _NODES.validate_python(usage["$defs"], strict=True)
    for name in ("Opaque", "Digest", "Uuid", "Ordinal", "Count", "UtcInstant"):
        if definitions[name] != usage_defs[name]:
            raise ValueError("bound scalar schema differs")
    expected_refs = {
        "BoundRef",
        "PlannedRef",
        "OutputCap",
        "Opaque",
        "Digest",
        "Uuid",
        "Ordinal",
        "Count",
        "UtcInstant",
    }
    if set(definitions) != expected_refs:
        raise ValueError("bound scalar inventory differs")
    if definitions["OutputCap"] != {
        "type": "string",
        "pattern": "^[1-9][0-9]*$",
        "maxLength": 5,
        "x-max-decimal": "16384",
    }:
        raise ValueError("bound output limit differs")
    lines = [
        "# fmt: off",
        '"""Generated from contract/model-call-bound/v1/schema.json. Do not edit."""',
        "from __future__ import annotations",
        "from typing import Annotated, Literal",
        "from pydantic import AfterValidator, BaseModel, ConfigDict, Field, JsonValue",
        "from kokoro_agent.protocol.model_usage_v2_generated import Opaque, Digest, Uuid, Ordinal, Count, UtcInstant",
        f"SOURCE_SHA256 = {hashlib.sha256(schema_raw).hexdigest()!r}",
        "def _output(value: str) -> str:",
        "    if int(value) > 16384:",
        '        raise ValueError("bound output cap exceeds profile")',
        "    return value",
        'OutputCap = Annotated[str, Field(pattern="^[1-9][0-9]*$", max_length=5), AfterValidator(_output)]',
    ]
    for name in ("BoundRef", "PlannedRef"):
        node = definitions[name]
        if (
            set(node) != {"type", "pattern"}
            or node["type"] != "string"
            or not isinstance(node["pattern"], str)
        ):
            raise ValueError("bound reference dialect differs")
        lines.append(f"{name} = Annotated[str, Field(pattern={node['pattern']!r})]")
    for name, node in fields.items():
        if "const" in node and isinstance(node["const"], dict):
            if set(node) != {"type", "const"} or node["type"] != "object":
                raise ValueError("bound constant dialect differs")
            lines += [
                f"def _{name}(value: dict[str, JsonValue]) -> dict[str, JsonValue]:",
                f"    if value != {node['const']!r}:",
                '        raise ValueError("bound provenance differs")',
                "    return value",
            ]
    lines += [
        "class ModelCallBound(BaseModel):",
        '    model_config = ConfigDict(strict=True, frozen=True, extra="forbid", hide_input_in_errors=True)',
    ]
    for name, node in fields.items():
        if set(node) == {"$ref"}:
            reference = str(node["$ref"]).removeprefix("#/$defs/")
            if reference not in expected_refs:
                raise ValueError("bound reference differs")
            annotation = reference
        elif set(node) == {"type", "const"}:
            value = node["const"]
            if isinstance(value, dict):
                annotation = f"Annotated[dict[str, JsonValue], AfterValidator(_{name})]"
            elif isinstance(value, list):
                if node["type"] != "array" or not all(
                    isinstance(v, str) for v in value
                ):
                    raise ValueError("bound tuple differs")
                annotation = (
                    "tuple[" + ", ".join(f"Literal[{v!r}]" for v in value) + "]"
                )
            elif type(value) in (str, bool, int):
                annotation = f"Literal[{value!r}]"
            else:
                raise ValueError("bound constant differs")
        else:
            raise ValueError("bound field dialect differs")
        lines.append(f"    {name}: {annotation}")
    generated = ("\n".join(lines) + "\n").encode()
    vectors = _VECTORS.validate_json(
        (root / BASE / "vectors.json").read_bytes(), strict=True
    )
    if set(vectors) != {"valid", "invalid"} or not all(vectors.values()):
        raise ValueError("bound vectors are incomplete")
    for value in vectors["valid"]:
        validate_schema_instance(
            instance=value, schema=schema, cls=Draft202012Validator
        )
    for value in vectors["invalid"]:
        try:
            validate_schema_instance(
                instance=value, schema=schema, cls=Draft202012Validator
            )
        except ValidationError:
            pass
        else:
            raise ValueError("bound invalid vector was accepted")
    if check:
        if (root / GENERATED).read_bytes() != generated:
            raise ValueError("bound generated model drift")
    else:
        (root / GENERATED).write_bytes(generated)
    sources = {p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in SOURCES}
    manifest = (
        json.dumps(
            {
                "owner": "kokoro-agent",
                "artifact_version": "1.0.0",
                "schema_sha256": hashlib.sha256(schema_raw).hexdigest(),
                "source_sha256": sources,
                "aggregate_sha256": hashlib.sha256(
                    "".join(f"{p}:{h}\n" for p, h in sorted(sources.items())).encode()
                ).hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )
    target = root / BASE / "manifest.json"
    if check:
        if target.read_text() != manifest:
            raise ValueError("bound manifest drift")
    else:
        target.write_text(manifest)
