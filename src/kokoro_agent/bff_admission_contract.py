"""Build-only compiler for the fixed BFF model financial operations, never runtime I/O."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from pydantic import TypeAdapter
import yaml

OWNER_COMMIT = "5a303f2003fabeb50376c7f7e1f66f0d163adebb"
OWNER_SHA256 = "4de1a6c7564f3a994d879cfc93588021c5ef4aeebbcc89959fa574d5b6f16d39"
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

HEADER = '''"""Generated from the fixed BFF internal1.3 publication. Do not edit."""

from __future__ import annotations

from collections.abc import Callable
import re
from typing import Annotated, Literal, TypeAlias

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

SOURCE_SHA256 = "OWNER_SHA256"


def _patterns(*patterns: str, forbidden: tuple[str, ...] = ()) -> Callable[[str], str]:
    def validate(value: str) -> str:
        if not all(
            re.fullmatch(pattern, value, re.ASCII) for pattern in patterns
        ) or any(re.search(pattern, value, re.ASCII) for pattern in forbidden):
            raise ValueError("invalid BFF budget field")
        return value

    return validate


'''


def _branches(node: dict[str, object], depth: int = 0) -> list[list[dict[str, object]]]:
    """Distribute only the accepted finite anyOf/allOf profile at build time."""
    if depth > 12:
        raise ValueError("BFF schema exceeds accepted depth")
    result = [[{k: v for k, v in node.items() if k not in {"allOf", "anyOf"}}]]
    for part in _OBJECTS.validate_python(node.get("allOf", []), strict=True):
        result = [
            left + right for left in result for right in _branches(part, depth + 1)
        ]
    if "anyOf" in node:
        variants = _OBJECTS.validate_python(node["anyOf"], strict=True)
        if not variants:
            raise ValueError("empty BFF alternatives")
        result = [
            left + right
            for left in result
            for part in variants
            for right in _branches(part, depth + 1)
        ]
    if len(result) > 16:
        raise ValueError("BFF schema exceeds accepted alternatives")
    return result


class _ModelCompiler:
    """Only this fixed finite dialect; local mutable emission state, no runtime registry."""

    def __init__(self) -> None:
        self.aliases: list[str] = []
        self.classes: list[str] = []
        self.counter = 0

    def annotation(self, node: dict[str, object], name: str) -> str:
        alternatives: list[str] = []
        for index, terms in enumerate(_branches(node)):
            types = {term["type"] for term in terms if "type" in term}
            if len(types) != 1:
                if len(types) > 1:
                    continue  # Proven empty string/null intersection, never widen it.
                raise ValueError("fixed BFF type is absent")
            kind = next(iter(types))
            if kind == "object":
                nonempty = [term for term in terms if term]
                if len(nonempty) != 1:
                    raise ValueError("unsupported BFF object intersection")
                child_name = (
                    name if len(_branches(node)) == 1 else f"{name}Variant{index}"
                )
                self.emit_class(nonempty[0], child_name)
                alternatives.append(child_name)
            elif kind == "null":
                if any(set(term) - {"type"} for term in terms):
                    raise ValueError("unsupported BFF null constraint")
                alternatives.append("None")
            elif kind == "boolean":
                if any(set(term) - {"type"} for term in terms):
                    raise ValueError("unsupported BFF boolean constraint")
                alternatives.append("bool")
            elif kind == "string":
                alternatives.append(self.string_annotation(terms))
            else:
                raise ValueError("unsupported fixed BFF scalar")
        if not alternatives:
            raise ValueError("empty fixed BFF union")
        return " | ".join(dict.fromkeys(alternatives))

    def string_annotation(self, terms: list[dict[str, object]]) -> str:
        patterns: list[str] = []
        forbidden: list[str] = []
        choices: list[str] | None = None
        lower, upper = 0, None
        for term in terms:
            if set(term) - {
                "type",
                "minLength",
                "maxLength",
                "pattern",
                "not",
                "enum",
                "const",
            }:
                raise ValueError("unsupported fixed BFF string constraint")
            if "pattern" in term:
                patterns.extend(
                    _STRINGS.validate_python([term["pattern"]], strict=True)
                )
            if "not" in term:
                negative = _OBJECT.validate_python(term["not"], strict=True)
                if set(negative) != {"pattern"}:
                    raise ValueError("unsupported BFF negation")
                forbidden.extend(
                    _STRINGS.validate_python([negative["pattern"]], strict=True)
                )
            for key in ("minLength", "maxLength"):
                if key not in term:
                    continue
                value = term[key]
                if type(value) is not int or value < 0:
                    raise ValueError("invalid BFF codepoint limit")
                if key == "minLength":
                    lower = max(lower, value)
                else:
                    upper = value if upper is None else min(upper, value)
            for key in ("enum", "const"):
                if key in term:
                    values = _STRINGS.validate_python(
                        term[key] if key == "enum" else [term[key]], strict=True
                    )
                    choices = (
                        values
                        if choices is None
                        else [v for v in choices if v in values]
                    )
                    if not choices:
                        raise ValueError("empty BFF literal intersection")
        if upper is not None and lower > upper:
            raise ValueError("empty BFF codepoint interval")
        base = "str"
        if choices is not None:
            base = f"Field{self.counter}"
            self.counter += 1
            self.aliases.extend(
                [f"{base}: TypeAlias = Literal[{json.dumps(choices[0])}]", "", ""]
                if len(choices) == 1
                else [
                    f"{base}: TypeAlias = Literal[",
                    *[f"    {json.dumps(v)}," for v in choices],
                    "]",
                    "",
                    "",
                ]
            )
        if not patterns and not forbidden and lower == 0 and upper is None:
            return base
        alias = f"Field{self.counter}"
        self.counter += 1
        self.aliases.extend([f"{alias}: TypeAlias = Annotated[", f"    {base},"])
        bounds = ([f"min_length={lower}"] if lower else []) + (
            [f"max_length={upper}"] if upper is not None else []
        )
        if bounds:
            self.aliases.append("    Field(" + ", ".join(bounds) + "),")
        if patterns or forbidden:
            self.aliases.extend(
                [
                    "    AfterValidator(",
                    "        _patterns(",
                    *[f"            {json.dumps(p)}," for p in patterns],
                ]
            )
            if forbidden:
                self.aliases.extend(
                    [
                        "            forbidden=(",
                        *[f"                {json.dumps(p)}," for p in forbidden],
                        "            ),",
                    ]
                )
            self.aliases.extend(["        ),", "    ),"])
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
                *[
                    line
                    for key, kind in fields
                    for line in (
                        [
                            f"    {key}: (",
                            "        " + kind.split(" | ")[0],
                            *["        | " + part for part in kind.split(" | ")[1:]],
                            "    )",
                        ]
                        if len(f"    {key}: {kind}") > 88 and " | " in kind
                        else [f"    {key}: {kind}"]
                    )
                ],
                "",
                "",
            ]
        )


def bff_admission_model_bytes(raw: bytes) -> bytes:
    if hashlib.sha256(raw).hexdigest() != OWNER_SHA256:
        raise ValueError("BFF publication differs from fixed owner")
    doc = _OBJECT.validate_python(yaml.safe_load(raw), strict=True)
    if _OBJECT.validate_python(doc["info"])["version"] != "1.3.0":
        raise ValueError("BFF internal version differs")
    paths = _OBJECT.validate_python(doc["paths"])
    for path, operation_id in (
        ("/internal/v1/model-admissions", "createAgentModelAdmission"),
        ("/internal/v1/model-admissions/recover", "recoverAgentModelAdmission"),
        (
            "/internal/v1/model-admissions/{admission_id}/settle",
            "settleAgentModelAdmission",
        ),
    ):
        operation = _OBJECT.validate_python(
            _OBJECT.validate_python(paths[path])["post"]
        )
        if operation["operationId"] != operation_id or operation["security"] != [
            {"AgentModelAdmissionBearer": []}
        ]:
            raise ValueError(
                "BFF financial operation or dedicated service identity differs"
            )
    schemas = _OBJECT.validate_python(
        _OBJECT.validate_python(doc["components"])["schemas"]
    )
    compiler = _ModelCompiler()
    for name in (
        "AgentModelAdmissionRequest",
        "AgentModelAdmissionResponse",
        "AgentModelAdmissionError",
        "AgentModelSettlementRequest",
        "AgentModelSettlementResponse",
        "AgentModelSettlementError",
    ):
        compiler.emit_class(_OBJECT.validate_python(schemas[name]), name)
    admission = _OBJECT.validate_python(schemas["AgentModelAdmissionResponse"])
    properties = _OBJECT.validate_python(admission["properties"])
    data = _OBJECT.validate_python(properties["data"])
    fields = _OBJECT.validate_python(data["properties"])
    for name, field in (
        ("AgentModelAdmissionId", "admission_id"),
        ("AgentModelAttemptId", "attempt_id"),
    ):
        scalar = compiler.annotation(_OBJECT.validate_python(fields[field]), name)
        compiler.aliases.extend([f"{name}: TypeAlias = {scalar}", "", ""])
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
        "version": "1.3.0",
        "source_commit": OWNER_COMMIT,
        "sha256": OWNER_SHA256,
        "operations": [
            "POST /internal/v1/model-admissions",
            "POST /internal/v1/model-admissions/recover",
            "POST /internal/v1/model-admissions/{admission_id}/settle",
        ],
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
