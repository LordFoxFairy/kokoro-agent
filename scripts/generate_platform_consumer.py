from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from kokoro_agent.platform_binding_contract import validate_platform_binding_artifact


ROOT = Path(__file__).parents[1]
PIN_PATH = ROOT / "contract" / "platform" / "v1" / "provenance.json"
PROTO_ROOT = PIN_PATH.parent / "proto"
OUTPUT = ROOT / "src" / "kokoro_agent" / "generated"
CODEGEN_PATH = PIN_PATH.parent / "codegen.json"
EXECUTION_OPERATIONS_ROOT = PIN_PATH.parent / "execution-operations" / "v3"


def _run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(command, check=True, cwd=ROOT, env=env)


def _venv_executable(venv: Path, name: str) -> Path:
    candidates = [venv / "bin" / name, venv / "Scripts" / f"{name}.exe"]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise SystemExit(f"isolated codegen executable is missing: {name}")


def _generate(destination: Path) -> None:
    validate_platform_binding_artifact(ROOT)
    pin = json.loads(PIN_PATH.read_text(encoding="utf-8"))
    execution_sources = pin["execution_operations"]["sources"]
    for source in [
        *pin["sources"],
        *pin["generation_inputs"],
        *execution_sources,
    ]:
        actual = hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest()
        if actual != source["sha256"]:
            raise SystemExit(
                f"pinned Platform source digest mismatch: {source['path']}"
            )
    codegen = pin["codegen"]
    config = json.loads(CODEGEN_PATH.read_text(encoding="utf-8"))
    for key in (
        "buf_bin",
        "protoc_gen_py",
        "protoc_gen_connectrpc",
        "connectrpc_parameter",
    ):
        if config[key] != codegen[key]:
            raise SystemExit(f"codegen provenance does not match codegen.json: {key}")
    with tempfile.TemporaryDirectory(
        prefix="kokoro-agent-platform-codegen-"
    ) as temporary:
        venv = Path(temporary) / "venv"
        _run(["uv", "venv", "--python", codegen["python"], str(venv)])
        python = _venv_executable(venv, "python")
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                f"buf-bin=={codegen['buf_bin']}",
                f"protoc-gen-py=={codegen['protoc_gen_py']}",
                f"protoc-gen-connectrpc=={codegen['protoc_gen_connectrpc']}",
            ]
        )
        buf = _venv_executable(venv, "buf")
        py_plugin = _venv_executable(venv, "protoc-gen-py")
        connect_plugin = _venv_executable(venv, "protoc-gen-connectrpc")
        destination.mkdir(parents=True, exist_ok=True)
        template = Path(temporary) / "buf.gen.json"
        template.write_text(
            json.dumps(
                {
                    "version": "v2",
                    "plugins": [
                        {"local": str(py_plugin), "out": str(destination)},
                        {
                            "local": str(connect_plugin),
                            "out": str(destination),
                            "opt": [config["connectrpc_parameter"]],
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        _run(
            [
                str(buf),
                "generate",
                str(PROTO_ROOT),
                "--template",
                str(template),
            ]
        )
        # The pinned Connect plugin emits trailing spaces on keyword-only header
        # parameters. Normalize that deterministic formatting before drift checks.
        for generated in destination.rglob("*.py"):
            original = generated.read_text(encoding="utf-8")
            normalized = "\n".join(line.rstrip(" \t") for line in original.split("\n"))
            if normalized != original:
                generated.write_text(normalized, encoding="utf-8")
        _generate_platform_request_projector(destination)


def _generate_platform_request_projector(destination: Path) -> None:
    pin = json.loads(PIN_PATH.read_text(encoding="utf-8"))
    execution_pin = pin["execution_operations"]
    provenance = json.loads(
        (EXECUTION_OPERATIONS_ROOT / "provenance.json").read_text(encoding="utf-8")
    )
    if provenance["aggregateSha256"] != execution_pin["aggregate_sha256"]:
        raise SystemExit("pinned Platform execution-operation aggregate drift")
    aggregate = hashlib.sha256()
    for source in sorted(provenance["files"], key=lambda item: item["path"].encode()):
        path = EXECUTION_OPERATIONS_ROOT / source["path"]
        payload = path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if len(payload) != source["bytes"] or digest != source["sha256"]:
            raise SystemExit(
                f"pinned Platform execution-operation payload drift: {source['path']}"
            )
        aggregate.update(
            source["path"].encode()
            + b"\0"
            + str(len(payload)).encode()
            + b"\0"
            + digest.encode()
            + b"\n"
        )
    if aggregate.hexdigest() != execution_pin["aggregate_sha256"]:
        raise SystemExit("pinned Platform execution-operation aggregate is invalid")

    manifest = json.loads(
        (EXECUTION_OPERATIONS_ROOT / "manifest.json").read_text(encoding="utf-8")
    )
    if manifest["status"] != "inactive" or manifest["routable"] is not False:
        raise SystemExit("Platform execution-operation artifact unexpectedly routable")
    catalog = json.loads(
        (EXECUTION_OPERATIONS_ROOT / "operation-catalog.json").read_text(
            encoding="utf-8"
        )
    )
    tenant_operations = {
        row["fqMethod"]: row["operation"]
        for row in catalog["operations"]
        if row["class"] == "tenant-execution"
    }
    bindings = json.loads(
        (EXECUTION_OPERATIONS_ROOT / "request-bindings.json").read_text(
            encoding="utf-8"
        )
    )["bindings"]
    if len(bindings) != 24 or set(tenant_operations) != {
        row["fqMethod"] for row in bindings
    }:
        raise SystemExit("Platform tenant operation/binding inventory is not exact")

    lines = [
        "# Generated from the pinned Platform execution-operation artifact. DO NOT EDIT.",
        "from __future__ import annotations",
        "",
        "from kokoro_agent.execution.platform_request_binding_values import (",
        "    PlatformRequestBindingError,",
        "    ProjectedPlatformRequest,",
        "    project_array,",
        "    project_boolean,",
        "    project_command,",
        "    project_identity,",
        "    project_optional_boolean,",
        "    project_optional_string,",
        "    project_owner_scope,",
        "    project_page,",
        "    project_safe_integer,",
        "    project_set,",
        "    project_string,",
        "    project_typed_arguments_digest,",
        ")",
        "from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as p",
        "",
        "FQ_METHOD_BY_OPERATION = {",
    ]
    for row in bindings:
        lines.append(f"    {row['operation']!r}: {row['fqMethod']!r},")
    lines.extend(
        [
            "}",
            "OPERATION_BY_FQ_METHOD = {",
            "    method: operation for operation, method in FQ_METHOD_BY_OPERATION.items()",
            "}",
            "",
            "",
            "def project_request(request: object) -> ProjectedPlatformRequest:",
        ]
    )
    for index, row in enumerate(bindings):
        fq_method = row["fqMethod"]
        operation = row["operation"]
        method = fq_method.rsplit("/", 1)[1]
        prefix = "if" if index == 0 else "elif"
        lines.append(f"    {prefix} type(request) is p.{method}Request:")
        lines.append("        projected = {")
        for member in row["requestMembers"]:
            name, kind = member.split(":", 1)
            expression = _projection_expression(name, kind)
            lines.append(f"            {name!r}: {expression},")
        lines.extend(
            [
                "        }",
                "        return ProjectedPlatformRequest(",
                f"            operation={operation!r},",
                f"            fq_method={fq_method!r},",
                "            request_id=request.request_id,",
                "            request=projected,",
                "        )",
            ]
        )
    lines.extend(
        [
            "    else:",
            "        raise PlatformRequestBindingError(",
            '            "request is not one of the 24 tenant-execution Platform messages"',
            "        )",
            "",
        ]
    )
    (destination / "platform_request_projector.py").write_text(
        "\n".join(lines), encoding="utf-8"
    )


def _projection_expression(name: str, kind: str) -> str:
    attribute = f"request.{name}"
    label = repr(name)
    present = f"{attribute} if request.has_field({name!r}) else None"
    if kind == "string":
        return f"project_string({attribute}, label={label})"
    if kind == "boolean":
        return f"project_boolean({attribute}, label={label})"
    if kind == "enum":
        return f"project_safe_integer({attribute}, label={label})"
    if kind == "array<string>":
        return f"project_array({attribute}, label={label})"
    if kind == "set<string>":
        return f"project_set({attribute}, label={label})"
    if kind == "command":
        return f"project_command({present}, label={label})"
    if kind == "page":
        return f"project_page({present}, label={label})"
    if kind == "msg<OwnerScope>":
        return f"project_owner_scope({present}, label={label})"
    if kind.startswith("id<"):
        wrapper = kind.removeprefix("id<").removesuffix(">")
        return f"project_identity({present}, wrapper={wrapper!r}, label={label})"
    if kind == "opt<string>":
        return f"project_optional_string({present}, label={label})"
    if kind == "opt<boolean>":
        return f"project_optional_boolean({present}, label={label})"
    if name == "typed_arguments_sha256" and kind == "lowercase-64hex":
        return (
            "project_typed_arguments_digest("
            "request.typed_arguments_json, label='typed_arguments_json')"
        )
    raise SystemExit(f"unsupported generated Platform binding member: {name}:{kind}")


_PLATFORM_OUTPUTS = (
    Path("kokoro/platform/v1/platform_runtime_pb.py"),
    Path("kokoro/platform/v1/platform_runtime_connect.py"),
    Path("platform_request_projector.py"),
)
_SHARED_OUTPUTS = (
    Path("__init__.py"),
    Path("kokoro/__init__.py"),
    Path("kokoro/common/__init__.py"),
    Path("kokoro/common/v1/__init__.py"),
    Path("kokoro/common/v1/common_pb.py"),
    Path("kokoro/platform/__init__.py"),
    Path("kokoro/platform/v1/__init__.py"),
)


def _check_platform_outputs(candidate: Path) -> None:
    expected = set(_PLATFORM_OUTPUTS + _SHARED_OUTPUTS)
    actual = {
        file.relative_to(candidate) for file in candidate.rglob("*") if file.is_file()
    }
    if actual != expected:
        raise SystemExit("generated Platform output inventory drift")
    platform_root = OUTPUT / "kokoro/platform"
    expected_platform = {
        relative
        for relative in expected
        if relative.parts[:2] == ("kokoro", "platform")
    }
    actual_platform = {
        entry.relative_to(OUTPUT)
        for entry in platform_root.rglob("*")
        if "__pycache__" not in entry.relative_to(platform_root).parts
        and (entry.is_file() or entry.is_symlink())
    }
    actual_root = {
        entry.relative_to(OUTPUT)
        for entry in OUTPUT.glob("platform_*")
        if entry.is_file() or entry.is_dir() or entry.is_symlink()
    }
    if actual_platform != expected_platform or actual_root != {
        Path("platform_request_projector.py")
    }:
        raise SystemExit("generated Platform output inventory drift")
    for relative in _SHARED_OUTPUTS:
        if (OUTPUT / relative).read_bytes() != (candidate / relative).read_bytes():
            raise SystemExit(f"shared generated input drift: {relative}")
    for relative in _PLATFORM_OUTPUTS:
        if (OUTPUT / relative).read_bytes() != (candidate / relative).read_bytes():
            raise SystemExit(f"generated Platform consumer drift: {relative}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(
        prefix="kokoro-agent-platform-check-"
    ) as temporary:
        candidate = Path(temporary) / "generated"
        _generate(candidate)
        if args.check:
            _check_platform_outputs(candidate)
            return
        for relative in _SHARED_OUTPUTS:
            if (OUTPUT / relative).read_bytes() != (candidate / relative).read_bytes():
                raise SystemExit(f"shared generated input drift: {relative}")
        for relative in _PLATFORM_OUTPUTS:
            shutil.copyfile(candidate / relative, OUTPUT / relative)


if __name__ == "__main__":
    main()
