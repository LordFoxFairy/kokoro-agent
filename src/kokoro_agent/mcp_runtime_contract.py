"""Build-only validation of the one pinned MCP Runtime owner publication."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from pydantic import TypeAdapter
from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
)

MCP_RUNTIME_SOURCES = (
    (
        "contract/platform/v1/mcp-runtime/mcp-runtime-v1.json",
        "contract/execution-operations/mcp-runtime-v1.json",
        "16b052f16193c01b1d110df9b9fb75aec66698771f87ee3ff0e595e1c47d09d4",
    ),
    (
        "contract/platform/v1/proto/kokoro/platform/v1/mcp_runtime.proto",
        "contract/proto/kokoro/platform/v1/mcp_runtime.proto",
        "1c0e942b56c9b711207f9f4e1155c7b5d24ce2632e18adc98cf9884542ab0b17",
    ),
    (
        "contract/platform/v1/mcp-runtime/mcp-runtime-v1.binpb",
        "contract/descriptor/mcp-runtime-v1.binpb",
        "41fcab4fce74502e2a74e00a8c03d8d7ac913d5ee9e41adb764ef72f0620b955",
    ),
    (
        "contract/platform/v1/mcp-runtime/mcp_runtime_pb.ts",
        "src/generated/proto/kokoro/platform/v1/mcp_runtime_pb.ts",
        "0dad7af6d2d9d6b359f88d37fa113f7f53db9267356126f9f7fe7ff1c499136e",
    ),
    (
        "contract/platform/v1/mcp-runtime/mcp-runtime-request-binding.ts",
        "src/modules/mcp/runtime/mcp-runtime-request-binding.ts",
        "a6c8aede60a5fa5f4666d311fa3cfc3577c6af94aacfaa68c5349c3a60f2efdf",
    ),
    (
        "contract/platform/v1/mcp-runtime/mcp-runtime-rpc-input.ts",
        "src/modules/mcp/runtime/mcp-runtime-rpc-input.ts",
        "5f8f8c7f3abe0c0372aa6feeb251b5da2b749a440067bcd21ea1e6ad352f2522",
    ),
    (
        "contract/platform/v1/mcp-runtime/mcp-runtime-request-vectors-v1.json",
        "contract/execution-operations/mcp-runtime-request-vectors-v1.json",
        "1217049c5a0833349765ab6d364771264f2dcfda410cb1c63b24abc7dcf2a397",
    ),
)

_OBJECT = TypeAdapter(dict[str, object])
_RECORDS = TypeAdapter(list[dict[str, object]])


def validate_mcp_runtime_artifact(root: Path, provenance: Mapping[str, object]) -> None:
    """Fixed source inventory, byte hashes and classification; never runtime loading."""
    runtime = _OBJECT.validate_python(provenance.get("runtime_material"), strict=True)
    expected = [
        {"path": path, "owner_path": owner_path, "sha256": digest}
        for path, owner_path, digest in MCP_RUNTIME_SOURCES
    ]
    if (
        set(runtime) != {"owner_commit", "profile", "version", "sources"}
        or runtime.get("owner_commit") != "c3decfdc0910d1f9ddd992b90629d9ce3350b1b6"
        or runtime.get("profile") != "mcp-runtime-material-v1"
        or runtime.get("version") != "1.1.0"
        or runtime.get("sources") != expected
    ):
        raise PlatformRequestBindingError("MCP runtime publication pin drift")
    for path, _, digest in MCP_RUNTIME_SOURCES:
        source = root / path
        if (
            source.is_symlink()
            or not source.is_file()
            or hashlib.sha256(source.read_bytes()).hexdigest() != digest
        ):
            raise PlatformRequestBindingError("MCP runtime source digest drift")
    directory = root / "contract/platform/v1/mcp-runtime"
    if (
        directory.is_symlink()
        or any(entry.is_symlink() for entry in directory.rglob("*"))
        or {entry.name for entry in directory.iterdir()}
        != {
            Path(path).name
            for path, _, _ in MCP_RUNTIME_SOURCES
            if path.startswith("contract/platform/v1/mcp-runtime/")
        }
    ):
        raise PlatformRequestBindingError("MCP runtime source tree drift")
    profile = _OBJECT.validate_json(
        (root / MCP_RUNTIME_SOURCES[0][0]).read_bytes(), strict=True
    )
    if (
        profile.get("profile") != "mcp-runtime-material-v1"
        or profile.get("version") != "1.1.0"
        or profile.get("status") != "prepared"
        or profile.get("routable") is not False
        or profile.get("runtime_verified") is not False
    ):
        raise PlatformRequestBindingError(
            "MCP runtime publication classification drift"
        )
    inputs = _RECORDS.validate_python(profile.get("inputs"), strict=True)
    if inputs != [
        {"path": owner_path, "bytes": (root / path).stat().st_size, "sha256": digest}
        for path, owner_path, digest in MCP_RUNTIME_SOURCES[1:]
    ]:
        raise PlatformRequestBindingError("MCP runtime owner input closure drift")
