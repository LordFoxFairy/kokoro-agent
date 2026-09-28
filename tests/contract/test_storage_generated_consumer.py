"""Storage v2 consumer uses the exact pinned owner wire, not an invented DTO."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).parents[2]
PIN = ROOT / "contract/storage/v2/provenance.json"
SOURCE_DIGESTS = {
    "contract/storage/v2/proto/kokoro/common/v1/common.proto": "4604725ec7d5896c9d74b53c6f06d19b20ee758d5ab9e1cb90177ede95bba9fd",
    "contract/storage/v2/proto/kokoro/storage/v2/storage.proto": "5a5dcaec2e1fd0d5eed369b8f79477fd0f8f653b32f9ebe14a8c339f4eb713ac",
}


def test_storage_vendor_inputs_match_owner_commit_and_bytes() -> None:
    pin = json.loads(PIN.read_text(encoding="utf-8"))
    assert pin["owner_commit"] == "d5cfc442c675e32363ae767f5ec662a9e0d9eaea"
    assert {
        source["path"]: source["sha256"] for source in pin["sources"]
    } == SOURCE_DIGESTS
    for path, expected in SOURCE_DIGESTS.items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected


def test_storage_generated_client_exposes_exact_owner_methods() -> None:
    from kokoro_agent.generated.kokoro.storage.v2 import storage_connect
    from kokoro_agent.generated.kokoro.storage.v2 import storage_pb

    methods = {
        name
        for name, value in vars(storage_connect.StorageServiceClient).items()
        if callable(value) and not name.startswith("_") and name != "close"
    }
    assert methods == {
        "create_upload",
        "complete_upload",
        "abort_upload",
        "get_upload_status",
        "get_asset",
        "list_assets",
        "get_package_reference",
        "get_scan_status",
        "create_artifact",
        "finalize_artifact",
        "get_download_reference",
        "list_final_artifacts",
        "get_final_artifact",
        "get_final_artifact_download_reference",
    }
    assert storage_pb.ArtifactKind
    assert storage_pb.CreateArtifactRequest
