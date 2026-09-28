"""Regenerate only the Storage v2 client from fixed owner contract bytes."""

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
PIN = ROOT / "contract/storage/v2/provenance.json"
PROTO = ROOT / "contract/storage/v2/proto"
GENERATED = ROOT / "src/kokoro_agent/generated/kokoro"
STORAGE_OUTPUT = GENERATED / "storage"
COMMON_OUTPUT = GENERATED / "common/v1/common_pb.py"


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_pin() -> dict[str, object]:
    pin = json.loads(PIN.read_text(encoding="utf-8"))
    if pin["owner_commit"] != "d5cfc442c675e32363ae767f5ec662a9e0d9eaea":
        raise SystemExit("Storage owner commit drift")
    for source in pin["sources"]:
        if _file_hash(ROOT / source["path"]) != source["sha256"]:
            raise SystemExit(f"Storage source digest drift: {source['path']}")
    owner = pin["owner_provenance"]
    owner_path = ROOT / owner["path"]
    if _file_hash(owner_path) != owner["sha256"]:
        raise SystemExit("Storage owner provenance digest drift")
    owner_data = json.loads(owner_path.read_text(encoding="utf-8"))
    if owner_data["combinedSha256"] != owner["combined_sha256"]:
        raise SystemExit("Storage owner combined digest drift")
    if owner_data["files"] != [
        "kokoro/common/v1/common.proto",
        "kokoro/storage/v2/storage.proto",
    ]:
        raise SystemExit("Storage owner source inventory drift")
    return pin["codegen"]


def _exe(venv: Path, name: str) -> str:
    path = venv / "bin" / name
    if not path.is_file():
        raise SystemExit(f"missing isolated codegen executable: {name}")
    return str(path)


def _generate(destination: Path) -> None:
    config = _verify_pin()
    with tempfile.TemporaryDirectory(prefix="agent-storage-codegen-") as temp:
        temporary = Path(temp)
        venv = temporary / "venv"
        subprocess.run(
            ["uv", "venv", "--python", config["python"], str(venv)], check=True
        )
        subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                _exe(venv, "python"),
                f"buf-bin=={config['buf_bin']}",
                f"protoc-gen-py=={config['protoc_gen_py']}",
                f"protoc-gen-connectrpc=={config['protoc_gen_connectrpc']}",
            ],
            check=True,
        )
        output = temporary / "generated"
        template = temporary / "buf.gen.json"
        template.write_text(
            json.dumps(
                {
                    "version": "v2",
                    "plugins": [
                        {"local": _exe(venv, "protoc-gen-py"), "out": str(output)},
                        {
                            "local": _exe(venv, "protoc-gen-connectrpc"),
                            "out": str(output),
                            "opt": [config["connectrpc_parameter"]],
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        subprocess.run(
            [_exe(venv, "buf"), "generate", str(PROTO), "--template", str(template)],
            check=True,
            cwd=ROOT,
        )
        for generated in output.rglob("*.py"):
            original = generated.read_text(encoding="utf-8")
            generated.write_text(
                "\n".join(line.rstrip(" \t") for line in original.split("\n")),
                encoding="utf-8",
            )
        common = output / "kokoro/common/v1/common_pb.py"
        if not filecmp.cmp(common, COMMON_OUTPUT, shallow=False):
            raise SystemExit("Storage and Platform common.v1 generated types diverge")
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copytree(output / "kokoro/storage", destination, dirs_exist_ok=True)


def _assert_equal(expected: Path, actual: Path) -> None:
    compare = filecmp.dircmp(expected, actual)
    if compare.left_only or compare.right_only or compare.diff_files:
        raise SystemExit("Storage generated client drift")
    for folder in compare.common_dirs:
        _assert_equal(expected / folder, actual / folder)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="agent-storage-output-") as temp:
        candidate = Path(temp) / "storage"
        _generate(candidate)
        if args.check:
            _assert_equal(STORAGE_OUTPUT, candidate)
        else:
            if STORAGE_OUTPUT.exists():
                shutil.rmtree(STORAGE_OUTPUT)
            shutil.copytree(candidate, STORAGE_OUTPUT)


if __name__ == "__main__":
    main()
