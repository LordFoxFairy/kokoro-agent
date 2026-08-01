#!/usr/bin/env python3
"""Reproducibly sync AgentPresentation V1 Python/Connect artifacts from Root."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import tempfile
from pathlib import Path

ROOT_CONTRACT_SOURCE_REVISION = "agent-presentation@v1:c282e2fc"
EXPECTED_PROTOC_VERSION = "libprotoc 33.4"
PROTO_FILES = (
    "kokoro/common/v2/command_envelope.proto",
    "kokoro/agent/presentation/v1/agent_presentation.proto",
)
EXPECTED_SOURCE_SHA256 = {
    "kokoro/common/v2/command_envelope.proto": (
        "dd4451ec255413c5c3eb7f7a6caadc961d72e00828bdb54aa88fec5aed0a71e7"
    ),
    "kokoro/agent/presentation/v1/agent_presentation.proto": (
        "c282e2fca33205c49a0e4270bd166e1905332c5c1ed14c499398b59025241bc1"
    ),
}
ARTIFACTS = (
    "src/kokoro/common/v2/command_envelope_pb2.py",
    "src/kokoro/common/v2/command_envelope_pb2.pyi",
    "src/kokoro/agent/presentation/v1/agent_presentation_pb2.py",
    "src/kokoro/agent/presentation/v1/agent_presentation_pb2.pyi",
    "src/kokoro/agent/presentation/v1/agent_presentation_connect.py",
)


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _required(path: Path) -> Path:
    if not path.is_file():
        raise SystemExit(f"required file missing: {path}")
    return path


def _generate(
    *,
    output: Path,
    repository: Path,
    contract: Path,
    protoc: Path,
    protobuf_include: Path,
) -> None:
    version = subprocess.run(
        [str(_required(protoc)), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if version != EXPECTED_PROTOC_VERSION:
        raise SystemExit(
            f"protoc drift: expected {EXPECTED_PROTOC_VERSION!r}, got {version!r}"
        )
    actual = {
        relative: _digest(_required(contract / "proto" / relative))
        for relative in PROTO_FILES
    }
    if actual != EXPECTED_SOURCE_SHA256:
        raise SystemExit(
            "Root contract bytes differ from "
            f"{ROOT_CONTRACT_SOURCE_REVISION}: {actual!r}"
        )
    (output / "src").mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="kokoro-presentation-validate-") as raw:
        dependency = Path(raw)
        subprocess.run(
            [
                str(_required(contract / "node_modules/.bin/buf")),
                "export",
                "buf.build/bufbuild/protovalidate:435963d1631043e694e56e6bcc3c79c3",
                "--output",
                str(dependency),
            ],
            check=True,
        )
        subprocess.run(
            [
                str(protoc),
                "-I",
                str(contract / "proto"),
                "-I",
                str(dependency),
                "-I",
                str(protobuf_include),
                f"--python_out={output / 'src'}",
                f"--pyi_out={output / 'src'}",
                (
                    "--plugin=protoc-gen-connectrpc="
                    + str(_required(repository / ".venv/bin/protoc-gen-connectrpc"))
                ),
                f"--connectrpc_out={output / 'src'}",
                *(str(contract / "proto" / relative) for relative in PROTO_FILES),
            ],
            check=True,
        )
    connect = output / ARTIFACTS[-1]
    connect.write_text(connect.read_text().rstrip() + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protoc", type=Path, required=True)
    parser.add_argument("--protobuf-include", type=Path, default=Path("/opt/homebrew/include"))
    parser.add_argument("--contract-root", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    contract = (
        args.contract_root.resolve()
        if args.contract_root is not None
        else repository.parent / "contract"
    )
    if args.check:
        with tempfile.TemporaryDirectory(prefix="kokoro-presentation-v1-") as raw:
            output = Path(raw)
            _generate(
                output=output,
                repository=repository,
                contract=contract,
                protoc=args.protoc.resolve(),
                protobuf_include=args.protobuf_include.resolve(),
            )
            drift = [
                artifact
                for artifact in ARTIFACTS
                if _digest(_required(repository / artifact))
                != _digest(_required(output / artifact))
            ]
            if drift:
                raise SystemExit(f"AgentPresentation generated artifact drift: {drift!r}")
        return
    _generate(
        output=repository,
        repository=repository,
        contract=contract,
        protoc=args.protoc.resolve(),
        protobuf_include=args.protobuf_include.resolve(),
    )


if __name__ == "__main__":
    main()
