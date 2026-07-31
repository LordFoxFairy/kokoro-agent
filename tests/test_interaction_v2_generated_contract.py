from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from kokoro.agent.control.v2 import session_agent_control_pb2 as control_pb2
from kokoro.agent.execution.v2 import agent_execution_evidence_pb2 as evidence_pb2
from kokoro_agent.interaction.activation import INTERACTION_V2_ACTIVATION_BLOCKERS
from kokoro_agent.interaction.generated.contract_metadata import (
    CONTRACT_SOURCE_SHA256,
    GENERATED_ARTIFACT_SHA256,
    PROTOC_VERSION,
)


def test_v2_protobuf_mirrors_expose_root_services() -> None:
    assert evidence_pb2.DESCRIPTOR.package == "kokoro.agent.execution.v2"
    assert control_pb2.DESCRIPTOR.package == "kokoro.agent.control.v2"
    assert set(evidence_pb2.DESCRIPTOR.services_by_name) == {
        "AgentExecutionEvidenceService"
    }
    assert set(control_pb2.DESCRIPTOR.services_by_name) == {
        "SessionAgentControlRecoveryService"
    }


def test_incomplete_pydantic_mirrors_are_not_distributed_as_root_equivalent() -> None:
    generated = Path(__file__).resolve().parents[1] / (
        "src/kokoro_agent/interaction/generated"
    )
    assert not (generated / "control_v2.py").exists()
    assert not (generated / "execution_v2.py").exists()
    assert any(
        "protovalidate" in blocker.lower()
        for blocker in INTERACTION_V2_ACTIVATION_BLOCKERS
    )


def test_agent_v2_wire_has_no_business_or_framework_route_axes() -> None:
    forbidden_fields = {
        "site_id",
        "user_id",
        "payment_id",
        "graph_route",
        "langgraph_interrupt_ref",
        "stable_task_path",
    }
    fields = {
        field.name
        for descriptor in (
            evidence_pb2.DESCRIPTOR,
            control_pb2.DESCRIPTOR,
        )
        for message in descriptor.message_types_by_name.values()
        for field in message.fields
    }
    assert fields.isdisjoint(forbidden_fields)


def test_generated_control_wire_keeps_plaintext_value_out_of_wire() -> None:
    fields = control_pb2.EncryptedDecisionValueV2.DESCRIPTOR.fields_by_name
    assert "value" not in fields
    assert "plaintext" not in fields
    assert "ciphertext" in fields


def test_generated_sources_are_pinned_to_exact_root_bytes() -> None:
    root = Path(__file__).resolve().parents[2] / "contract"
    if not root.is_dir():
        pytest.skip("standalone Agent clone has no Root contract checkout")
    for relative, expected in CONTRACT_SOURCE_SHA256.items():
        assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected
    repository = Path(__file__).resolve().parents[1]
    for relative, expected in GENERATED_ARTIFACT_SHA256.items():
        assert (
            hashlib.sha256((repository / relative).read_bytes()).hexdigest() == expected
        )
    assert PROTOC_VERSION == "libprotoc 33.4"


def test_generator_requires_explicit_protoc_and_has_temp_regeneration_check() -> None:
    generator = (
        Path(__file__).resolve().parents[1] / "scripts/sync_interaction_v2_contract.py"
    ).read_text()
    assert 'parser.add_argument("--protoc", type=Path, required=True)' in generator
    assert 'parser.add_argument("--check", action="store_true")' in generator
    assert 'subprocess.run(["protoc"' not in generator
    assert "_write_pydantic_mirror" not in generator


def test_v2_foundation_is_not_imported_by_existing_runtime() -> None:
    production = Path(__file__).resolve().parents[1] / "src/kokoro_agent"
    imports: list[Path] = []
    for source in production.rglob("*.py"):
        if "interaction" in source.relative_to(production).parts:
            continue
        text = source.read_text()
        if "kokoro_agent.interaction" in text or "kokoro.agent.execution.v2" in text:
            imports.append(source)
    assert imports == []
