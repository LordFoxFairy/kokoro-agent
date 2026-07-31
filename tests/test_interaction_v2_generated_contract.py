from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from pydantic import ValidationError

from kokoro.agent.control.v2 import session_agent_control_pb2 as control_pb2
from kokoro.agent.execution.v2 import agent_execution_evidence_pb2 as evidence_pb2
from kokoro_agent.interaction.generated import control_v2, execution_v2
from kokoro_agent.interaction.generated.contract_metadata import CONTRACT_SOURCE_SHA256


def test_v2_protobuf_mirrors_expose_root_services() -> None:
    assert evidence_pb2.DESCRIPTOR.package == "kokoro.agent.execution.v2"
    assert control_pb2.DESCRIPTOR.package == "kokoro.agent.control.v2"
    assert set(evidence_pb2.DESCRIPTOR.services_by_name) == {
        "AgentExecutionEvidenceService"
    }
    assert set(control_pb2.DESCRIPTOR.services_by_name) == {
        "SessionAgentControlRecoveryService"
    }


def test_generated_pydantic_mirror_is_strict_frozen_and_oneof_closed() -> None:
    ref = execution_v2.InteractionOwnerRevisionRefV2(
        interaction_owner_ref="iown_fixture", owner_revision=1
    )
    with pytest.raises(ValidationError):
        execution_v2.InteractionOwnerRevisionRefV2.model_validate(
            {"interaction_owner_ref": "iown_fixture", "owner_revision": "1"}
        )
    with pytest.raises(ValidationError):
        execution_v2.InteractionPresentationV2()
    with pytest.raises(ValidationError):
        execution_v2.InteractionPresentationV2(
            approval=execution_v2.ApprovalPresentationV2(
                prompt=execution_v2.SafeInteractionPromptV2(title="Approve"),
                allowed_decisions=[
                    execution_v2.InteractionDecisionKindV2.INTERACTION_DECISION_KIND_V2_APPROVE
                ],
            ),
            question=execution_v2.QuestionPresentationV2(
                prompt=execution_v2.SafeInteractionPromptV2(title="Question"),
                allowed_decisions=[
                    execution_v2.InteractionDecisionKindV2.INTERACTION_DECISION_KIND_V2_RESPOND
                ],
            ),
        )
    with pytest.raises(ValidationError):
        execution_v2.InteractionOwnerRevisionRefV2.model_validate(
            {
                "interaction_owner_ref": "iown_fixture",
                "owner_revision": 1,
                "extra_axis": "forbidden",
            }
        )
    with pytest.raises(ValidationError):
        ref.owner_revision = 2


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


def test_generated_control_mirror_keeps_plaintext_value_out_of_wire() -> None:
    fields = control_v2.EncryptedDecisionValueV2.model_fields
    assert "value" not in fields
    assert "plaintext" not in fields
    assert "ciphertext" in fields


def test_generated_sources_are_pinned_to_exact_root_bytes() -> None:
    root = Path(__file__).resolve().parents[2] / "contract"
    if not root.is_dir():
        pytest.skip("standalone Agent clone has no Root contract checkout")
    for relative, expected in CONTRACT_SOURCE_SHA256.items():
        assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected
    generated = Path(execution_v2.__file__).read_text()
    assert generated.startswith("# GENERATED — DO NOT EDIT.")
    assert "Root contract protobuf descriptor" in generated.splitlines()[0]


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
