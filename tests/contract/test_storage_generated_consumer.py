"""Storage v3 consumer uses the exact pinned owner wire, not an invented DTO."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).parents[2]
PIN = ROOT / "contract/storage/v3/provenance.json"
SOURCE_DIGESTS = {
    "contract/storage/v3/proto/kokoro/common/v1/common.proto": "4604725ec7d5896c9d74b53c6f06d19b20ee758d5ab9e1cb90177ede95bba9fd",
    "contract/storage/v3/proto/kokoro/storage/v3/storage.proto": "dbd622696f66817465f6481f86da03cfec871ade9960a8f4194d85631a9981df",
}


def test_storage_v3_is_the_only_vendored_owner_pin() -> None:
    assert PIN.is_file()
    assert not (ROOT / "contract/storage/v2").exists()
    assert not (ROOT / "src/kokoro_agent/generated/kokoro/storage/v2").exists()


def test_storage_vendor_inputs_match_owner_commit_and_bytes() -> None:
    pin = json.loads(PIN.read_text(encoding="utf-8"))
    assert pin["owner_commit"] == "b60931e9e4e92d596b0678cbdabd9116e898b432"
    assert {
        source["path"]: source["sha256"] for source in pin["sources"]
    } == SOURCE_DIGESTS
    assert pin["owner_provenance"] == {
        "path": "contract/storage/v3/owner-provenance.json",
        "sha256": "057aaeb0947e693ad8842bd0cf24e11bbd370d97891a60d273b44c1769bb130e",
        "combined_sha256": "3da3346a760a6275251aba56f75129189e06e3522e81b4c0b110386a4cdff02f",
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
        "delete_final_artifact",
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
        "release_conversation_workspace_outputs",
    }
    assert storage_pb.ArtifactKind
    assert storage_pb.CreateArtifactRequest
    assert storage_pb.UploadPurpose.ARTIFACT.value == 3
    assert storage_pb.UploadPurpose.CONNECTOR_ICON.value == 5
    assert storage_pb.UploadPurpose.WORKSPACE_OUTPUT.value == 6
    assert "source_version_id" in storage_pb.CompleteUploadRequest.__slots__
    assert "source_version_id" in storage_pb.GetUploadStatusResponse.__slots__
