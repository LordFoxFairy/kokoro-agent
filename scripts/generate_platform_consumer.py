from __future__ import annotations

import argparse
import filecmp
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).parents[1]
PIN_PATH = ROOT / "contract" / "platform" / "v1" / "provenance.json"
PROTO_ROOT = PIN_PATH.parent / "proto"
OUTPUT = ROOT / "src" / "kokoro_agent" / "generated"
CODEGEN_PATH = PIN_PATH.parent / "codegen.json"


def _run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(command, check=True, cwd=ROOT, env=env)


def _venv_executable(venv: Path, name: str) -> Path:
    candidates = [venv / "bin" / name, venv / "Scripts" / f"{name}.exe"]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise SystemExit(f"isolated codegen executable is missing: {name}")


def _generate(destination: Path) -> None:
    pin = json.loads(PIN_PATH.read_text(encoding="utf-8"))
    for source in [*pin["sources"], *pin["generation_inputs"]]:
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


def _assert_equal(expected: Path, actual: Path) -> None:
    comparison = filecmp.dircmp(expected, actual)
    if (
        comparison.left_only
        or comparison.right_only
        or comparison.diff_files
        or comparison.funny_files
    ):
        raise SystemExit(
            "generated Platform consumer drift: "
            f"missing={comparison.left_only}, extra={comparison.right_only}, changed={comparison.diff_files}"
        )
    for directory in comparison.common_dirs:
        _assert_equal(expected / directory, actual / directory)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        with tempfile.TemporaryDirectory(
            prefix="kokoro-agent-platform-check-"
        ) as temporary:
            candidate = Path(temporary) / "generated"
            _generate(candidate)
            _assert_equal(OUTPUT, candidate)
        return
    if OUTPUT.exists():
        shutil.rmtree(OUTPUT)
    _generate(OUTPUT)


if __name__ == "__main__":
    main()
