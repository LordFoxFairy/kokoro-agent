"""Build-only compiler for the single fixed BFF budget operation, never runtime I/O."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from pydantic import TypeAdapter
import yaml

OWNER_COMMIT = "0082b5cee4437367a608b5bd37732674a1622664"
OWNER_SHA256 = "695901b23f1f0322fd18ec2a11be27c5b6efcce07b04f57839498eab402d17c6"
SOURCE = "contract/bff/internal-v1/openapi.yaml"
GENERATED = "src/kokoro_agent/clients/bff_admission_generated.py"
BFF_ADMISSION_AUDIT_FILES = (
    SOURCE,
    GENERATED,
    "src/kokoro_agent/bff_admission_contract.py",
    "scripts/generate_bff_admission_models.py",
)
_OBJECT = TypeAdapter(dict[str, object])
_OBJECTS = TypeAdapter(list[dict[str, object]])
_STRINGS = TypeAdapter(list[str])

HEADER = '''"""Generated from the fixed BFF internal1.1 publication. Do not edit."""

from __future__ import annotations

from collections.abc import Callable
import re
from typing import Annotated, Literal, TypeAlias

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

SOURCE_SHA256 = "OWNER_SHA256"


def _patterns(*patterns: str) -> Callable[[str], str]:
    def validate(value: str) -> str:
        if not all(re.fullmatch(pattern, value, re.ASCII) for pattern in patterns):
            raise ValueError("invalid BFF budget field")
        return value

    return validate


'''


class _ModelCompiler:
    """Only this fixed finite dialect; local mutable emission state, no runtime registry."""

    def __init__(self) -> None:
        self.aliases: list[str] = []
        self.classes: list[str] = []
        self.counter = 0

    def annotation(self, node: dict[str, object], name: str) -> str:
        if node.get("type") == "object":
            self.emit_class(node, name)
            return name
        if node == {"type": "boolean"}:
            return "bool"
        if node == {"type": "string"}:
            return "str"
        if node.get("type") != "string" or set(node) - {
            "type",
            "minLength",
            "maxLength",
            "pattern",
            "allOf",
            "enum",
        }:
            raise ValueError("unsupported fixed BFF leaf")
        alias = f"Field{self.counter}"
        self.counter += 1
        if "enum" in node:
            values = _STRINGS.validate_python(node["enum"], strict=True)
            if set(node) != {"type", "enum"} or not values:
                raise ValueError("unsupported BFF enum")
            self.aliases.extend(
                [
                    f"{alias}: TypeAlias = Literal[",
                    *[f"    {json.dumps(v)}," for v in values],
                    "]",
                    "",
                    "",
                ]
            )
            return alias
        self.aliases.extend([f"{alias}: TypeAlias = Annotated[", "    str,"])
        limits: list[str] = []
        for key, option in (("minLength", "min_length"), ("maxLength", "max_length")):
            if key in node:
                bound = node[key]
                if type(bound) is not int or bound < 0:
                    raise ValueError("invalid BFF text bound")
                limits.append(f"        {option}={bound},")
        if limits:
            self.aliases.extend(["    Field(", *limits, "    ),"])
        patterns: list[str] = []
        if "pattern" in node:
            patterns = _STRINGS.validate_python([node["pattern"]], strict=True)
        if "allOf" in node:
            for part in _OBJECTS.validate_python(node["allOf"], strict=True):
                if set(part) != {"pattern"}:
                    raise ValueError("unsupported BFF intersection")
                patterns.extend(
                    _STRINGS.validate_python([part["pattern"]], strict=True)
                )
        if patterns:
            self.aliases.extend(
                [
                    "    AfterValidator(",
                    "        _patterns(",
                    *[f"            {json.dumps(p)}," for p in patterns],
                    "        ),",
                    "    ),",
                ]
            )
        if not patterns and not limits:
            return "str"
        self.aliases.extend(["]", "", ""])
        return alias

    def emit_class(self, node: dict[str, object], name: str) -> None:
        if node.get("additionalProperties") is not False or set(node) - {
            "$schema",
            "type",
            "properties",
            "required",
            "additionalProperties",
        }:
            raise ValueError("BFF objects must be closed")
        properties = _OBJECT.validate_python(node["properties"], strict=True)
        if set(_STRINGS.validate_python(node["required"], strict=True)) != set(
            properties
        ):
            raise ValueError("BFF fields must be explicit and required")
        fields = [
            (
                field,
                self.annotation(_OBJECT.validate_python(value), name + field.title()),
            )
            for field, value in properties.items()
        ]
        self.classes.extend(
            [
                f"class {name}(BaseModel):",
                "    model_config = ConfigDict(",
                '        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True',
                "    )",
                "",
                *[f"    {key}: {kind}" for key, kind in fields],
                "",
                "",
            ]
        )


def bff_admission_model_bytes(raw: bytes) -> bytes:
    if hashlib.sha256(raw).hexdigest() != OWNER_SHA256:
        raise ValueError("BFF publication differs from fixed owner")
    doc = _OBJECT.validate_python(yaml.safe_load(raw), strict=True)
    if _OBJECT.validate_python(doc["info"])["version"] != "1.1.0":
        raise ValueError("BFF internal version differs")
    paths = _OBJECT.validate_python(doc["paths"])
    operation = _OBJECT.validate_python(
        _OBJECT.validate_python(paths["/internal/v1/model-admissions"])["post"]
    )
    if operation["operationId"] != "createAgentModelAdmission" or operation[
        "security"
    ] != [{"AgentModelAdmissionBearer": []}]:
        raise ValueError("BFF operation or dedicated service identity differs")
    schemas = _OBJECT.validate_python(
        _OBJECT.validate_python(doc["components"])["schemas"]
    )
    compiler = _ModelCompiler()
    for name in (
        "AgentModelAdmissionRequest",
        "AgentModelAdmissionResponse",
        "AgentModelAdmissionError",
    ):
        compiler.emit_class(_OBJECT.validate_python(schemas[name]), name)
    return (
        HEADER.replace("OWNER_SHA256", OWNER_SHA256)
        + "\n".join(compiler.aliases + compiler.classes).rstrip()
        + "\n"
    ).encode()


def generate_bff_admission_models(root: Path, *, check: bool = False) -> None:
    result = bff_admission_model_bytes((root / SOURCE).read_bytes())
    path = root / GENERATED
    if check:
        if path.read_bytes() != result:
            raise ValueError("BFF generated budget models are stale")
    else:
        path.write_bytes(result)


def bff_admission_receipt() -> dict[str, object]:
    return {
        "owner": "kokoro-bff",
        "contract": "contract/openapi/internal/v1/openapi.yaml",
        "version": "1.1.0",
        "source_commit": OWNER_COMMIT,
        "sha256": OWNER_SHA256,
        "operations": ["POST /internal/v1/model-admissions"],
    }


def validate_bff_admission_consumer(root: Path) -> None:
    generate_bff_admission_models(root, check=True)
    provenance = _OBJECT.validate_json((root / "contract/provenance.json").read_bytes())
    receipts = _OBJECTS.validate_python(provenance["consumer_dependencies"])
    if [r for r in receipts if r.get("owner") == "kokoro-bff"] != [
        bff_admission_receipt()
    ]:
        raise ValueError("BFF budget consumer receipt differs")
    artifacts = _OBJECTS.validate_python(provenance["generated_artifacts"])
    if [record.get("path") for record in artifacts] != [
        "src/kokoro_agent/protocol/run_failure_generated.py",
        GENERATED,
    ]:
        raise ValueError("BFF generated artifact inventory differs")
    expected = {
        "path": GENERATED,
        "sha256": hashlib.sha256((root / GENERATED).read_bytes()).hexdigest(),
        "source": SOURCE,
        "source_sha256": OWNER_SHA256,
        "generator": "scripts/generate_bff_admission_models.py",
    }
    if [r for r in artifacts if r.get("path") == GENERATED] != [expected]:
        raise ValueError("BFF generated budget provenance differs")
