"""Storage v3 consumer uses the exact pinned owner wire, not an invented DTO."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).parents[2]
PIN = ROOT / "contract/storage/v3/provenance.json"
SOURCE_DIGESTS = {
    "contract/storage/v3/proto/kokoro/common/v1/common.proto": "4604725ec7d5896c9d74b53c6f06d19b20ee758d5ab9e1cb90177ede95bba9fd",
    "contract/storage/v3/proto/kokoro/storage/v3/storage.proto": "1df62524f9c61db1c74fc39a0d06912ef851796ffdf0792b6521eaa9715bfc87",
}


def test_storage_v3_is_the_only_vendored_owner_pin() -> None:
    assert PIN.is_file()
    assert not (ROOT / "contract/storage/v2").exists()
    assert not (ROOT / "src/kokoro_agent/generated/kokoro/storage/v2").exists()


def test_storage_vendor_inputs_match_owner_commit_and_bytes() -> None:
    pin = json.loads(PIN.read_text(encoding="utf-8"))
    assert pin["owner_commit"] == "3f18577dda6fd57d3cd3be2ee132192db170e1b6"
    assert {
        source["path"]: source["sha256"] for source in pin["sources"]
    } == SOURCE_DIGESTS
    assert pin["owner_provenance"] == {
        "path": "contract/storage/v3/owner-provenance.json",
        "sha256": "c9a82c8b7e356d1cae904e44ec3f52225ea8fc9dc6c67741e0ced302879e064c",
        "combined_sha256": "6f6bfb8893d28aa2ea366a7cdbafdaae6f0694004f662da3be382a0613232ba5",
    }
    for path, expected in SOURCE_DIGESTS.items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected


def test_storage_generated_client_exposes_exact_owner_methods() -> None:
    from kokoro_agent.generated.kokoro.storage.v3 import storage_connect
    from kokoro_agent.generated.kokoro.storage.v3 import storage_pb

    methods = {
        name
        for name, value in vars(storage_connect.StorageServiceClient).items()
        if callable(value) and not name.startswith("_") and name != "close"
    }
    assert methods == {
        "delete_asset",
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
        "release_project_scope",
        "release_conversation_scope",
    }
    assert storage_pb.ArtifactKind
    assert storage_pb.CreateArtifactRequest
    assert storage_pb.UploadPurpose.ARTIFACT.value == 3
    assert storage_pb.UploadPurpose.CONNECTOR_ICON.value == 5
    assert "source_version_id" in storage_pb.CompleteUploadRequest.__slots__
    assert "source_version_id" in storage_pb.GetUploadStatusResponse.__slots__
