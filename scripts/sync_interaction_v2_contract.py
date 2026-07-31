#!/usr/bin/env python3
"""Synchronize dormant ADR-014 Agent artifacts from the Root contract.

The script deliberately distributes only Agent-owned identity vectors.  It
requires an explicit protoc path, records exact source/output digests, and can
regenerate into a temporary tree to prove that the committed artifacts do not
drift.  The protoc binary is not yet a hermetically published Root artifact, so
runtime activation remains blocked even though this sync is reproducible with
the explicitly certified version.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from typing import cast


ROOT_CONTRACT_COMMIT = "1d60b01"
EXPECTED_PROTOC_VERSION = "libprotoc 33.4"
PROTO_FILES = (
    "kokoro/agent/execution/v2/agent_execution_evidence.proto",
    "kokoro/agent/control/v2/session_agent_control.proto",
)
IDENTITY_CORPUS = "corpus/interaction-identity-v2.json"
CONTROL_SPEC = "spec/session-agent-control-v2.yaml"
AGENT_IDENTITY_KINDS = (
    "application_request",
    "interaction_owner",
    "projection_event",
    "group_projection",
)
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
GENERATED_ARTIFACTS = (
    "src/kokoro/agent/control/v2/session_agent_control_connect.py",
    "src/kokoro/agent/control/v2/session_agent_control_pb2.py",
    "src/kokoro/agent/control/v2/session_agent_control_pb2.pyi",
    "src/kokoro/agent/execution/v2/agent_execution_evidence_connect.py",
    "src/kokoro/agent/execution/v2/agent_execution_evidence_pb2.py",
    "src/kokoro/agent/execution/v2/agent_execution_evidence_pb2.pyi",
    "src/kokoro_agent/interaction/generated/agent_interaction_identity_v2.json",
)
METADATA = "src/kokoro_agent/interaction/generated/contract_metadata.py"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _require_file(path: Path) -> Path:
    if not path.is_file():
        raise SystemExit(f"required file is missing: {path}")
    return path


def _protobuf_include(explicit: Path | None) -> Path:
    candidates = (
        explicit,
        Path("/opt/homebrew/include"),
        Path("/usr/local/include"),
        Path("/usr/include"),
    )
    for candidate in candidates:
        if (
            candidate is not None
            and (candidate / "google/protobuf/timestamp.proto").is_file()
        ):
            return candidate
    raise SystemExit(
        "google/protobuf/timestamp.proto not found; pass --protobuf-include"
    )


def _assert_protoc(protoc: Path) -> str:
    executable = _require_file(protoc.resolve())
    completed = subprocess.run(
        [str(executable), "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    version = completed.stdout.strip()
    if version != EXPECTED_PROTOC_VERSION:
        raise SystemExit(
            f"protoc version drift: expected {EXPECTED_PROTOC_VERSION!r}, "
            f"received {version!r}"
        )
    return version


def _source_digests(contract: Path) -> dict[str, str]:
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
    return sources


def _write_agent_corpus(*, source: Path, output: Path) -> None:
    raw = cast(dict[str, object], json.loads(source.read_text()))
    vectors = cast(list[dict[str, object]], raw["vectors"])
    selected = [
        vector for vector in vectors if vector.get("kind") in AGENT_IDENTITY_KINDS
    ]
    if tuple(str(vector["kind"]) for vector in selected) != AGENT_IDENTITY_KINDS:
        raise SystemExit("Root identity corpus is missing or reordering Agent planes")
    scoped = {
        "schema": "kokoro.agent-interaction-identity.v2",
        "source_schema": raw["schema"],
        "root_corpus_sha256": _sha256(source),
        "algorithm": raw["algorithm"],
        "canonical_profile": raw["canonical_profile"],
        "vectors": selected,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(scoped, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _protoc(
    *,
    output_root: Path,
    contract: Path,
    protobuf_include: Path,
    buf: Path,
    protoc: Path,
    connect_plugin: Path,
) -> None:
    (output_root / "src").mkdir(parents=True, exist_ok=True)
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
                str(protoc),
                "-I",
                str(contract / "proto"),
                "-I",
                str(dependency_root),
                "-I",
                str(protobuf_include),
                f"--python_out={output_root / 'src'}",
                f"--pyi_out={output_root / 'src'}",
                f"--plugin=protoc-gen-connectrpc={connect_plugin}",
                f"--connectrpc_out={output_root / 'src'}",
                *sources,
            ],
            check=True,
        )
    for relative in PROTO_FILES:
        connect_output = (
            output_root
            / "src"
            / Path(relative).with_name(f"{Path(relative).stem}_connect.py")
        )
        connect_output.write_text(
            connect_output.read_text(encoding="utf-8").rstrip() + "\n",
            encoding="utf-8",
        )


def _write_metadata(
    *, output_root: Path, sources: dict[str, str], protoc_version: str
) -> None:
    artifacts = {
        relative: _sha256(_require_file(output_root / relative))
        for relative in GENERATED_ARTIFACTS
    }
    lines = [
        "# GENERATED — DO NOT EDIT. Source: Root ADR-014 contract bundle",
        "# Regenerate with an explicit certified protoc path; see activation blockers.",
        f'ROOT_CONTRACT_COMMIT = "{ROOT_CONTRACT_COMMIT}"',
        f'ROOT_IDENTITY_CORPUS_SHA256 = "{sources[IDENTITY_CORPUS]}"',
        (
            'AGENT_IDENTITY_CORPUS_SHA256 = "'
            + artifacts[
                "src/kokoro_agent/interaction/generated/agent_interaction_identity_v2.json"
            ]
            + '"'
        ),
        f'PROTOC_VERSION = "{protoc_version}"',
        "CONTRACT_SOURCE_SHA256 = {",
        *[f'    "{name}": "{digest}",' for name, digest in sorted(sources.items())],
        "}",
        "GENERATED_ARTIFACT_SHA256 = {",
        *[f'    "{name}": "{digest}",' for name, digest in sorted(artifacts.items())],
        "}",
    ]
    metadata = output_root / METADATA
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _generate(
    *,
    output_root: Path,
    repository: Path,
    contract: Path,
    protobuf_include: Path,
    protoc: Path,
    protoc_version: str,
) -> None:
    sources = _source_digests(contract)
    _protoc(
        output_root=output_root,
        contract=contract,
        protobuf_include=protobuf_include,
        buf=_require_file(contract / "node_modules/.bin/buf"),
        protoc=protoc,
        connect_plugin=_require_file(repository / ".venv/bin/protoc-gen-connectrpc"),
    )
    _write_agent_corpus(
        source=contract / IDENTITY_CORPUS,
        output=(
            output_root
            / "src/kokoro_agent/interaction/generated/agent_interaction_identity_v2.json"
        ),
    )
    _write_metadata(
        output_root=output_root,
        sources=sources,
        protoc_version=protoc_version,
    )


def _check(repository: Path, generated_root: Path) -> None:
    drift = [
        relative
        for relative in (*GENERATED_ARTIFACTS, METADATA)
        if not (repository / relative).is_file()
        or (repository / relative).read_bytes()
        != (generated_root / relative).read_bytes()
    ]
    if drift:
        raise SystemExit(f"generated Interaction V2 artifact drift: {drift!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--contract-root",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "contract",
    )
    parser.add_argument("--protobuf-include", type=Path)
    parser.add_argument("--protoc", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    repository = Path(__file__).resolve().parents[1]
    contract = args.contract_root.resolve()
    protoc = args.protoc.resolve()
    protoc_version = _assert_protoc(protoc)
    protobuf_include = _protobuf_include(args.protobuf_include)

    if args.check:
        with tempfile.TemporaryDirectory(prefix="kokoro-interaction-v2-regen-") as raw:
            generated_root = Path(raw)
            _generate(
                output_root=generated_root,
                repository=repository,
                contract=contract,
                protobuf_include=protobuf_include,
                protoc=protoc,
                protoc_version=protoc_version,
            )
            _check(repository, generated_root)
        return

    _generate(
        output_root=repository,
        repository=repository,
        contract=contract,
        protobuf_include=protobuf_include,
        protoc=protoc,
        protoc_version=protoc_version,
    )
    generated = repository / "src/kokoro_agent/interaction/generated"
    (generated / "__init__.py").touch(exist_ok=True)
    for obsolete in (
        generated / "control_v2.py",
        generated / "execution_v2.py",
        generated / "interaction_identity_v2.json",
    ):
        if obsolete.exists():
            obsolete.unlink()


if __name__ == "__main__":
    main()
