#!/usr/bin/env python3
"""Synchronize ADR-014 V2 Agent mirrors from the Root contract checkout.

This is deliberately a sync tool, not a second schema authority.  It copies the
Root identity corpus, invokes protoc for Python/Connect mirrors, and records the
exact source digests used by this standalone repository.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from google.protobuf.descriptor import (
    Descriptor,
    EnumDescriptor,
    FieldDescriptor,
    FileDescriptor,
)

from buf.validate import validate_pb2


ROOT_CONTRACT_COMMIT = "1d60b01"
PROTO_FILES = (
    "kokoro/agent/execution/v2/agent_execution_evidence.proto",
    "kokoro/agent/control/v2/session_agent_control.proto",
)
PROTO_MODULES = (
    (
        "kokoro.agent.execution.v2.agent_execution_evidence_pb2",
        "execution_v2.py",
    ),
    (
        "kokoro.agent.control.v2.session_agent_control_pb2",
        "control_v2.py",
    ),
)
IDENTITY_CORPUS = "corpus/interaction-identity-v2.json"
CONTROL_SPEC = "spec/session-agent-control-v2.yaml"
EXPECTED_SOURCE_SHA256 = {
    IDENTITY_CORPUS: "29a12eca87ed52dd6e4bda6f862c493a6cdd2d6ba2decab2a37de87ffcd1ca26",
    CONTROL_SPEC: "0cfdcb65a4be79528e07a40e857941ee007cf9ed1539ec066af0dd747b5653a3",
    "proto/kokoro/agent/control/v2/session_agent_control.proto": (
        "93db2d54fa8d54d3a369572701c6e115e9575070a22074bea8cfaab424081bd9"
    ),
    "proto/kokoro/agent/execution/v2/agent_execution_evidence.proto": (
        "f1edbdf71e2b9abcc2a3dd4ff294e9c6c38078b5273a76322d6186aa1f0693f7"
    ),
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _require_file(path: Path) -> Path:
    if not path.is_file():
        raise SystemExit(f"required Root contract source is missing: {path}")
    return path


def _protobuf_include(explicit: Path | None) -> Path:
    candidates = [
        explicit,
        Path("/opt/homebrew/include"),
        Path("/usr/local/include"),
        Path("/usr/include"),
    ]
    for candidate in candidates:
        if (
            candidate is not None
            and (candidate / "google/protobuf/timestamp.proto").is_file()
        ):
            return candidate
    raise SystemExit(
        "google/protobuf/timestamp.proto not found; pass --protobuf-include"
    )


def _write_metadata(*, output: Path, contract: Path) -> None:
    sources = {
        IDENTITY_CORPUS: _sha256(_require_file(contract / IDENTITY_CORPUS)),
        CONTROL_SPEC: _sha256(_require_file(contract / CONTROL_SPEC)),
        **{
            f"proto/{relative}": _sha256(_require_file(contract / "proto" / relative))
            for relative in PROTO_FILES
        },
    }
    if sources != EXPECTED_SOURCE_SHA256:
        changed = sorted(
            name
            for name in sources.keys() | EXPECTED_SOURCE_SHA256.keys()
            if sources.get(name) != EXPECTED_SOURCE_SHA256.get(name)
        )
        raise SystemExit(
            "Root contract bytes differ from pinned commit "
            f"{ROOT_CONTRACT_COMMIT}: {changed!r}"
        )
    lines = [
        "# GENERATED — DO NOT EDIT. Source: Root ADR-014 contract bundle",
        "# Regenerate: uv run python scripts/sync_interaction_v2_contract.py --contract-root <path>",
        f'ROOT_CONTRACT_COMMIT = "{ROOT_CONTRACT_COMMIT}"',
        f'IDENTITY_CORPUS_SHA256 = "{sources[IDENTITY_CORPUS]}"',
        "CONTRACT_SOURCE_SHA256 = {",
    ]
    lines.extend(
        f'    "{name}": "{digest}",' for name, digest in sorted(sources.items())
    )
    lines.append("}")
    output.write_text("\n".join(lines) + "\n")


def _protoc(
    *,
    repository: Path,
    contract: Path,
    protobuf_include: Path,
    buf: Path,
    connect_plugin: Path,
) -> None:
    with tempfile.TemporaryDirectory(prefix="kokoro-protovalidate-") as raw_temp:
        dependency_root = Path(raw_temp)
        subprocess.run(
            [
                str(buf),
                "export",
                "buf.build/bufbuild/protovalidate:435963d1631043e694e56e6bcc3c79c3",
                "--output",
                str(dependency_root),
            ],
            check=True,
        )
        sources = [str(contract / "proto" / relative) for relative in PROTO_FILES]
        subprocess.run(
            [
                "protoc",
                "-I",
                str(contract / "proto"),
                "-I",
                str(dependency_root),
                "-I",
                str(protobuf_include),
                f"--python_out={repository / 'src'}",
                f"--pyi_out={repository / 'src'}",
                f"--plugin=protoc-gen-connectrpc={connect_plugin}",
                f"--connectrpc_out={repository / 'src'}",
                *sources,
            ],
            check=True,
        )
    for relative in PROTO_FILES:
        connect_output = (
            repository
            / "src"
            / Path(relative).with_name(f"{Path(relative).stem}_connect.py")
        )
        connect_output.write_text(connect_output.read_text().rstrip() + "\n")


def _field_rules(field: FieldDescriptor):
    options = field.GetOptions()
    if options.HasExtension(validate_pb2.field):
        return options.Extensions[validate_pb2.field]
    return None


def _oneof_is_required(oneof) -> bool:
    options = oneof.GetOptions()
    return bool(
        options.HasExtension(validate_pb2.oneof)
        and options.Extensions[validate_pb2.oneof].required
    )


def _scalar_type(field: FieldDescriptor) -> str:
    mapping = {
        FieldDescriptor.TYPE_DOUBLE: "float",
        FieldDescriptor.TYPE_FLOAT: "float",
        FieldDescriptor.TYPE_INT64: "int",
        FieldDescriptor.TYPE_UINT64: "int",
        FieldDescriptor.TYPE_INT32: "int",
        FieldDescriptor.TYPE_FIXED64: "int",
        FieldDescriptor.TYPE_FIXED32: "int",
        FieldDescriptor.TYPE_BOOL: "bool",
        FieldDescriptor.TYPE_STRING: "str",
        FieldDescriptor.TYPE_BYTES: "bytes",
        FieldDescriptor.TYPE_UINT32: "int",
        FieldDescriptor.TYPE_SFIXED32: "int",
        FieldDescriptor.TYPE_SFIXED64: "int",
        FieldDescriptor.TYPE_SINT32: "int",
        FieldDescriptor.TYPE_SINT64: "int",
    }
    if field.type == FieldDescriptor.TYPE_ENUM:
        assert field.enum_type is not None
        return field.enum_type.name
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        assert field.message_type is not None
        if field.message_type.full_name == "google.protobuf.Timestamp":
            return "datetime"
        return field.message_type.name
    try:
        return mapping[field.type]
    except KeyError as exc:
        raise SystemExit(f"unsupported protobuf field type: {field.full_name}") from exc


def _constraint_arguments(field: FieldDescriptor) -> list[str]:
    rules = _field_rules(field)
    if rules is None:
        return []
    arm = rules.WhichOneof("type")
    if arm is None:
        return []
    typed_rules = getattr(rules, arm)
    present = {descriptor.name: value for descriptor, value in typed_rules.ListFields()}
    arguments: list[str] = []
    if "min_len" in present:
        arguments.append(f"min_length={present['min_len']}")
    if "max_len" in present:
        arguments.append(f"max_length={present['max_len']}")
    if "min_bytes" in present and field.type == FieldDescriptor.TYPE_BYTES:
        arguments.append(f"min_length={present['min_bytes']}")
    if "max_bytes" in present and field.type == FieldDescriptor.TYPE_BYTES:
        arguments.append(f"max_length={present['max_bytes']}")
    if "pattern" in present:
        arguments.append(f"pattern={present['pattern']!r}")
    for name in ("gt", "gte", "lt", "lte"):
        if name in present:
            pydantic_name = {"gte": "ge", "lte": "le"}.get(name, name)
            arguments.append(f"{pydantic_name}={present[name]}")
    return arguments


def _rules_require_value(field: FieldDescriptor) -> bool:
    rules = _field_rules(field)
    if rules is None:
        return False
    if rules.required:
        return True
    arm = rules.WhichOneof("type")
    if arm is None:
        return False
    present = {
        descriptor.name: value for descriptor, value in getattr(rules, arm).ListFields()
    }
    has_nondefault_const = "const" in present and present["const"] not in (
        0,
        "",
        b"",
        False,
    )
    excludes_default_enum = 0 in present.get("not_in", [])
    return bool(
        present.get("min_len", 0)
        or present.get("min_bytes", 0)
        or "gt" in present
        or present.get("gte", 0) > 0
        or has_nondefault_const
        or excludes_default_enum
    )


def _default_for_scalar(field: FieldDescriptor) -> str:
    if field.type == FieldDescriptor.TYPE_STRING:
        return '""'
    if field.type == FieldDescriptor.TYPE_BYTES:
        return 'b""'
    if field.type == FieldDescriptor.TYPE_BOOL:
        return "False"
    if field.type == FieldDescriptor.TYPE_ENUM:
        assert field.enum_type is not None
        zero = next(value for value in field.enum_type.values if value.number == 0)
        return f"{field.enum_type.name}.{zero.name}"
    return "0"


def _field_line(field: FieldDescriptor) -> str:
    value_type = _scalar_type(field)
    rules = _field_rules(field)
    repeated = field.is_repeated
    explicit_optional = (
        field.containing_oneof is not None
        and field.containing_oneof.name.startswith("_")
    )
    in_real_oneof = (
        field.containing_oneof is not None
        and not field.containing_oneof.name.startswith("_")
    )
    message_optional = (
        field.type == FieldDescriptor.TYPE_MESSAGE
        and not _rules_require_value(field)
        and not repeated
    )
    if repeated:
        annotation = f"list[{value_type}]"
        arguments = _constraint_arguments(field)
        if rules is not None and rules.WhichOneof("type") == "repeated":
            present = {
                descriptor.name: value
                for descriptor, value in rules.repeated.ListFields()
            }
            if "min_items" in present:
                arguments.append(f"min_length={present['min_items']}")
            if "max_items" in present:
                arguments.append(f"max_length={present['max_items']}")
        suffix = f"Field(default_factory=list[{value_type}]"
        if arguments:
            suffix += ", " + ", ".join(arguments)
        return f"    {field.name}: {annotation} = {suffix})"
    if explicit_optional or in_real_oneof or message_optional:
        return f"    {field.name}: {value_type} | None = None"
    arguments = _constraint_arguments(field)
    if arguments:
        return f"    {field.name}: {value_type} = Field({', '.join(arguments)})"
    if _rules_require_value(field):
        return f"    {field.name}: {value_type}"
    return f"    {field.name}: {value_type} = {_default_for_scalar(field)}"


def _enum_lines(enum: EnumDescriptor) -> list[str]:
    return [
        f"class {enum.name}(IntEnum):",
        *[f"    {value.name} = {value.number}" for value in enum.values],
        "",
    ]


def _message_lines(message: Descriptor) -> list[str]:
    lines = [f"class {message.name}(StrictModel):"]
    if not message.fields:
        lines.append("    pass")
    else:
        lines.extend(_field_line(field) for field in message.fields)
    for oneof in message.oneofs:
        if oneof.name.startswith("_") or not _oneof_is_required(oneof):
            continue
        fields = ", ".join(repr(field.name) for field in oneof.fields)
        lines.extend(
            [
                "",
                '    @model_validator(mode="after")',
                f"    def _validate_{oneof.name}(self) -> Self:",
                f"        fields = ({fields},)",
                "        if sum(getattr(self, name) is not None for name in fields) != 1:",
                f'            raise ValueError("exactly one {oneof.name} arm is required")',
                "        return self",
            ]
        )
    lines.append("")
    return lines


def _write_pydantic_mirror(*, descriptor: FileDescriptor, output: Path) -> None:
    source = [
        "# GENERATED — DO NOT EDIT. Source: Root contract protobuf descriptor",
        "# Regenerate: uv run python scripts/sync_interaction_v2_contract.py --contract-root <path>",
        "from __future__ import annotations",
        "",
        "from datetime import datetime",
        "from enum import IntEnum",
        "from typing import Self",
        "",
        "from pydantic import BaseModel, ConfigDict, Field, model_validator",
        "",
        "",
        "class StrictModel(BaseModel):",
        '    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")',
        "",
        "",
    ]
    for enum in descriptor.enum_types_by_name.values():
        source.extend(_enum_lines(enum))
    messages = list(descriptor.message_types_by_name.values())
    for message in messages:
        source.extend(_message_lines(message))
    if messages:
        names = ", ".join(message.name for message in messages)
        source.extend(
            [
                f"for _model in ({names},):",
                "    _model.model_rebuild()",
                "",
                "del _model",
                "",
            ]
        )
    output.write_text("\n".join(source))


def _pydantic_mirrors(*, repository: Path, generated: Path) -> None:
    sys.path.insert(0, str(repository / "src"))
    try:
        for module_name, output_name in PROTO_MODULES:
            module = importlib.import_module(module_name)
            _write_pydantic_mirror(
                descriptor=module.DESCRIPTOR,
                output=generated / output_name,
            )
    finally:
        sys.path.pop(0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--contract-root",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "contract",
    )
    parser.add_argument("--protobuf-include", type=Path)
    args = parser.parse_args()

    repository = Path(__file__).resolve().parents[1]
    contract = args.contract_root.resolve()
    generated = repository / "src/kokoro_agent/interaction/generated"
    generated.mkdir(parents=True, exist_ok=True)
    (generated / "__init__.py").touch(exist_ok=True)

    corpus = _require_file(contract / IDENTITY_CORPUS)
    shutil.copyfile(corpus, generated / "interaction_identity_v2.json")
    _write_metadata(output=generated / "contract_metadata.py", contract=contract)

    buf = _require_file(contract / "node_modules/.bin/buf")
    connect_plugin = _require_file(repository / ".venv/bin/protoc-gen-connectrpc")
    _protoc(
        repository=repository,
        contract=contract,
        protobuf_include=_protobuf_include(args.protobuf_include),
        buf=buf,
        connect_plugin=connect_plugin,
    )
    _pydantic_mirrors(repository=repository, generated=generated)


if __name__ == "__main__":
    main()
