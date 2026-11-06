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
        "c35cc7adaeeca98f88449a3d51b08db73071cde6007bbe021030ec5328442b90",
    ),
    (
        "contract/platform/v1/proto/kokoro/platform/v1/mcp_runtime.proto",
        "contract/proto/kokoro/platform/v1/mcp_runtime.proto",
        "245ab6b081bf91207caf1d2ea51b453fafea6fbc07c040befbd06937ef9489d7",
    ),
    (
        "contract/platform/v1/mcp-runtime/mcp-runtime-v1.binpb",
        "contract/descriptor/mcp-runtime-v1.binpb",
        "56388f48b7d2f563a7aa9f56e0b9d15e73b2a453d640b3c8297502354fee6dc2",
    ),
    (
        "contract/platform/v1/mcp-runtime/mcp_runtime_pb.ts",
        "src/generated/proto/kokoro/platform/v1/mcp_runtime_pb.ts",
        "5a4fda4a36b4924a1bd6836f7f28a8a9976d3f903e58ed331aff71a0608981ec",
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
        or runtime.get("owner_commit") != "8cfead647856ef54ec851a3bebe169a3ca14d212"
        or runtime.get("profile") != "mcp-runtime-material-v1"
        or runtime.get("version") != "1.5.0"
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
        or profile.get("version") != "1.5.0"
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
