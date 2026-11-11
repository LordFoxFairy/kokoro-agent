"""Build-only validation of the one pinned CustomAPI Runtime owner publication."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from pydantic import TypeAdapter
from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
)

CUSTOM_API_RUNTIME_SOURCES = (
    (
        "contract/platform/v1/custom-api-runtime/custom-api-runtime-v1.json",
        "contract/execution-operations/custom-api-runtime-v1.json",
        "befaf5e61a4b92fb378f345fda1409af3aa5fe1c7a9589bfa51ea7e3f412aeae",
    ),
    (
        "contract/platform/v1/proto/kokoro/platform/v1/custom_api_runtime.proto",
        "contract/proto/kokoro/platform/v1/custom_api_runtime.proto",
        "e30c37d6caa3c814486ac3d802b38b64bc3385db987304319100baad5641a81b",
    ),
    (
        "contract/platform/v1/custom-api-runtime/custom-api-runtime-v1.binpb",
        "contract/descriptor/custom-api-runtime-v1.binpb",
        "956b393735bb607d06d564748f1e581d0db1ea54a5e79b469f9a7f1df042d992",
    ),
    (
        "contract/platform/v1/custom-api-runtime/custom_api_runtime_pb.ts",
        "src/generated/proto/kokoro/platform/v1/custom_api_runtime_pb.ts",
        "088d515c88be4f3b2fc8b88203fe9dfd7b7b32429734d38138c246a45352168a",
    ),
    (
        "contract/platform/v1/custom-api-runtime/custom-api-runtime-binding.ts",
        "src/modules/mcp/custom-api/custom-api-runtime-binding.ts",
        "7fb506d1aa4d8e5480ad41a7ceff4a598a60f6a99df980de431d350e782424b3",
    ),
    (
        "contract/platform/v1/custom-api-runtime/custom-api-runtime-environment.ts",
        "src/modules/mcp/custom-api/custom-api-runtime-environment.ts",
        "d935186ba3d1b00260b161f95e94624572511a9b92c895c109a9c1079461580e",
    ),
    (
        "contract/platform/v1/custom-api-runtime/custom-api-runtime-request-vectors-v1.json",
        "contract/execution-operations/custom-api-runtime-request-vectors-v1.json",
        "0666c8ea054b45dd7c529af22cbf6797784af5d505e65ba36cb8cf831e26e018",
    ),
)

_OBJECT = TypeAdapter(dict[str, object])
_RECORDS = TypeAdapter(list[dict[str, object]])


def validate_custom_api_runtime_artifact(
    root: Path, provenance: Mapping[str, object]
) -> None:
    """Fixed source inventory, byte hashes and classification; never runtime loading."""
    runtime = _OBJECT.validate_python(provenance.get("custom_api_runtime"), strict=True)
    expected = [
        {"path": path, "owner_path": owner_path, "sha256": digest}
        for path, owner_path, digest in CUSTOM_API_RUNTIME_SOURCES
    ]
    if (
        set(runtime) != {"owner_commit", "profile", "version", "sources"}
        or runtime.get("owner_commit") != "44fcc5da599b4a5cec3077e41aaef4375fe4add2"
        or runtime.get("profile") != "custom-api-runtime-material-v1"
        or runtime.get("version") != "1.0.0"
        or runtime.get("sources") != expected
    ):
        raise PlatformRequestBindingError("CustomAPI runtime publication pin drift")
    for path, _, digest in CUSTOM_API_RUNTIME_SOURCES:
        source = root / path
        if (
            source.is_symlink()
            or not source.is_file()
            or hashlib.sha256(source.read_bytes()).hexdigest() != digest
        ):
            raise PlatformRequestBindingError("CustomAPI runtime source digest drift")
    directory = root / "contract/platform/v1/custom-api-runtime"
    if (
        directory.is_symlink()
        or any(entry.is_symlink() for entry in directory.rglob("*"))
        or {entry.name for entry in directory.iterdir()}
        != {
            Path(path).name
            for path, _, _ in CUSTOM_API_RUNTIME_SOURCES
            if path.startswith("contract/platform/v1/custom-api-runtime/")
        }
    ):
        raise PlatformRequestBindingError("CustomAPI runtime source tree drift")
    profile = _OBJECT.validate_json(
        (root / CUSTOM_API_RUNTIME_SOURCES[0][0]).read_bytes(), strict=True
    )
    if (
        profile.get("profile") != "custom-api-runtime-material-v1"
        or profile.get("version") != "1.0.0"
        or profile.get("status") != "prepared"
        or profile.get("default_enabled") is not False
        or profile.get("runtime_verified") is not False
    ):
        raise PlatformRequestBindingError(
            "CustomAPI runtime publication classification drift"
        )
    inputs = _RECORDS.validate_python(profile.get("inputs"), strict=True)
    if inputs != [
        {"path": owner_path, "sha256": digest}
        for _, owner_path, digest in CUSTOM_API_RUNTIME_SOURCES[1:]
    ]:
        raise PlatformRequestBindingError("CustomAPI runtime owner input closure drift")
