from __future__ import annotations

import json
from pathlib import Path

from google.protobuf.descriptor import ServiceDescriptor


ROOT = Path(__file__).resolve().parents[2]
GENERATED_PACKAGE = ROOT / "src/kokoro/agent/presentation/v1"


def test_generated_presentation_service_matches_root_hard_cut() -> None:
    from kokoro.agent.presentation.v1 import presentation_pb2

    service: ServiceDescriptor = presentation_pb2.DESCRIPTOR.services_by_name[
        "PresentationService"
    ]

    assert [method.name for method in service.methods] == [
        "CheckActive",
        "PullRecords",
        "AcknowledgeAdmissions",
        "QuarantineSubmission",
        "GetDeliveryStatus",
    ]
    assert (GENERATED_PACKAGE / "presentation_connect.py").is_file()


def test_old_agent_presentation_generated_unit_is_absent() -> None:
    assert not (GENERATED_PACKAGE / "agent_presentation_pb2.py").exists()
    assert not (GENERATED_PACKAGE / "agent_presentation_pb2.pyi").exists()
    assert not (GENERATED_PACKAGE / "agent_presentation_connect.py").exists()


def test_generated_provenance_covers_new_presentation_unit() -> None:
    provenance_path = ROOT / "src/generated/provenance.json"
    provenance = json.loads(provenance_path.read_text())
    outputs = {entry["path"] for entry in provenance["outputs"]}

    assert provenance["boundaryId"] == "generated-kokoro-agent@v1"
    assert provenance["consumerRepository"] == "kokoro-agent"
    assert {
        "src/kokoro/agent/presentation/v1/presentation_pb2.py",
        "src/kokoro/agent/presentation/v1/presentation_pb2.pyi",
        "src/kokoro/agent/presentation/v1/presentation_connect.py",
    } <= outputs
