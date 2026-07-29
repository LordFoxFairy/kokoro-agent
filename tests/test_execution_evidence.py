from __future__ import annotations

import hashlib
import json
from typing import cast

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.request import RequestContext
from pydantic import ValidationError

from kokoro.agent.execution.v1 import agent_execution_evidence_pb2 as evidence_pb2

from kokoro_agent.evidence.models import (
    MAX_CANONICAL_PAYLOAD_BYTES,
    DurableExecutionEvidence,
    evidence_kind_for_event,
    make_durable_execution_evidence,
)
from kokoro_agent.evidence.service import AgentExecutionEvidenceConnectService

def test_evidence_canonicalizes_payload_and_derives_stable_owner_reference() -> None:
    evidence = make_durable_execution_evidence(
        run_id="run-1",
        durable_seq=7,
        event_id="evt-7",
        event_kind="run.completed",
        payload_json='{"status":"completed","token_usage":{"input_tokens":2,"output_tokens":1}}',
        recorded_at_ms=1_724_800_123_456,
        producer_instance_ref="agent-pod-a",
        producer_generation=3,
    )

    canonical = evidence_pb2.DurableExecutionCanonicalPayloadV1.FromString(
        evidence.canonical_payload
    )
    assert canonical.WhichOneof("payload") == "run_completed"
    assert canonical.run_completed.status == evidence_pb2.RUN_COMPLETED_EVIDENCE_STATUS_COMPLETED
    assert canonical.run_completed.token_usage.input_tokens == 2
    assert canonical.run_completed.token_usage.output_tokens == 1
    assert evidence.payload_sha256 == hashlib.sha256(evidence.canonical_payload).hexdigest()
    assert evidence.evidence_ref == make_durable_execution_evidence(
        run_id="run-1",
        durable_seq=7,
        event_id="evt-7",
        event_kind="run.completed",
        payload_json=' { "token_usage": {"output_tokens": 1, "input_tokens": 2}, "status": "completed" } ',
        recorded_at_ms=1_724_800_123_456,
        producer_instance_ref="agent-pod-a",
        producer_generation=3,
    ).evidence_ref
    assert evidence.kind == "run.completed"
    assert evidence.evidence_version == 1


@pytest.mark.parametrize(
    ("event_kind", "evidence_kind"),
    [
        ("run.started", "run.started"),
        ("tool.awaiting_approval", "action_owner"),
        ("plan.proposed", "plan_owner"),
        ("run.owner.completed", "run.owner.completed"),
        ("run.completed", "run.completed"),
        ("run.failed", "run.failed"),
    ],
)
def test_only_agent_owned_durable_kinds_are_exposed(
    event_kind: str, evidence_kind: str
) -> None:
    assert evidence_kind_for_event(event_kind) == evidence_kind


def test_control_receipts_are_not_execution_owner_evidence() -> None:
    assert evidence_kind_for_event("run.control.receipt") is None


def test_action_owner_is_renderable_bounded_and_redacted() -> None:
    payload = {
        "segment_id": "seg-action",
        "tool_id": "call-action",
        "name": "deploy",
        "args": {
            "password": "do-not-expose",
            "region": "us-east-1",
            "headers": {"Authorization": "Bearer do-not-expose"},
        },
        "description": "Deploy with api_key=do-not-expose",
        "allowed_decisions": ["approve", "edit", "reject"],
        "kind": "tool_approval",
        "risk": {"level": "high", "source": "policy", "reason": "writes prod"},
        "editable": True,
        "input_schema": {
            "type": "object",
            "properties": {"password": {"default": "do-not-expose"}},
        },
        "pending_tool_ids": ["call-action", "call-next"],
    }
    evidence = make_durable_execution_evidence(
        run_id="run-action",
        durable_seq=2,
        event_id="evt-action",
        event_kind="tool.awaiting_approval",
        payload_json=json.dumps(payload),
        recorded_at_ms=10,
        producer_instance_ref="agent-pod-a",
        producer_generation=1,
    )

    canonical = evidence_pb2.DurableExecutionCanonicalPayloadV1.FromString(
        evidence.canonical_payload
    )
    owner = canonical.action_owner
    assert owner.owner_ref == "call-action"
    assert owner.awaiting_kind == evidence_pb2.ACTION_AWAITING_KIND_V1_TOOL_APPROVAL
    assert list(owner.pending_owner_refs) == ["call-action", "call-next"]
    assert owner.risk.level == "high"
    assert "do-not-expose" not in owner.description
    assert json.loads(owner.safe_request_json) == {
        "headers": {"Authorization": "[REDACTED]"},
        "password": "[REDACTED]",
        "region": "us-east-1",
    }
    assert len(owner.safe_request_json) <= 16 * 1024
    assert len(owner.safe_input_schema_json) <= 16 * 1024
    assert json.loads(owner.safe_input_schema_json) == {
        "properties": {"password": {"default": "[REDACTED]"}},
        "type": "object",
    }
    assert owner.input_schema_ref.startswith("sha256:")
    assert not owner.HasField("safe_result_preview")


def test_plan_owner_is_the_complete_typed_render_source() -> None:
    evidence = make_durable_execution_evidence(
        run_id="run-plan",
        durable_seq=3,
        event_id="evt-plan",
        event_kind="plan.proposed",
        payload_json=json.dumps(
            {
                "segment_id": "seg-plan",
                "owner_ref": "call-plan",
                "owner_version": 1,
                "proposal": {
                    "summary": "Ship safely",
                    "steps": [
                        {"step_ref": "step-1", "label": "Review", "status": "pending"},
                        {
                            "step_ref": "step-2",
                            "label": "Release",
                            "status": "in_progress",
                        },
                    ],
                    "allowed_actions": ["accept", "reject"],
                },
            }
        ),
        recorded_at_ms=10,
        producer_instance_ref="agent-pod-a",
        producer_generation=1,
    )

    owner = evidence_pb2.DurableExecutionCanonicalPayloadV1.FromString(
        evidence.canonical_payload
    ).plan_owner
    assert owner.summary == "Ship safely"
    assert [(step.step_ref, step.label, step.status) for step in owner.steps] == [
        ("step-1", "Review", evidence_pb2.PLAN_STEP_STATUS_V1_PENDING),
        ("step-2", "Release", evidence_pb2.PLAN_STEP_STATUS_V1_IN_PROGRESS),
    ]
    assert list(owner.allowed_decisions) == [
        evidence_pb2.PLAN_DECISION_V1_ACCEPT,
        evidence_pb2.PLAN_DECISION_V1_REJECT,
    ]


def test_safe_action_json_never_serializes_nonstandard_numbers() -> None:
    payload = {
        "segment_id": "seg-action",
        "tool_id": "call-action",
        "name": "calculate",
        "args": {"nan": float("nan"), "positive": float("inf"), "negative": -float("inf")},
        "description": "Review numbers",
        "allowed_decisions": ["approve", "reject"],
        "kind": "tool_approval",
        "editable": False,
        "pending_tool_ids": ["call-action"],
    }
    evidence = make_durable_execution_evidence(
        run_id="run-numbers",
        durable_seq=1,
        event_id="evt-numbers",
        event_kind="tool.awaiting_approval",
        payload_json=json.dumps(payload),
        recorded_at_ms=10,
        producer_instance_ref="agent-pod-a",
        producer_generation=1,
    )
    safe = evidence_pb2.DurableExecutionCanonicalPayloadV1.FromString(
        evidence.canonical_payload
    ).action_owner.safe_request_json
    assert b"NaN" not in safe and b"Infinity" not in safe
    assert json.loads(safe) == {
        "nan": "[REDACTED_NON_FINITE_NUMBER]",
        "negative": "[REDACTED_NON_FINITE_NUMBER]",
        "positive": "[REDACTED_NON_FINITE_NUMBER]",
    }


def test_evidence_rejects_invalid_event_payloads() -> None:
    with pytest.raises(ValueError, match="EVIDENCE_PAYLOAD_INVALID"):
        make_durable_execution_evidence(
            run_id="run-1",
            durable_seq=1,
            event_id="evt-1",
            event_kind="run.started",
            payload_json="not-json",
            recorded_at_ms=1,
            producer_instance_ref="agent-pod-a",
            producer_generation=1,
        )


def test_evidence_model_rejects_business_identity_and_hash_drift() -> None:
    canonical = evidence_pb2.DurableExecutionCanonicalPayloadV1(
        run_started=evidence_pb2.RunStartedEvidenceV1()
    ).SerializeToString(deterministic=True)
    raw = {
        "evidence_ref": "aee_ref",
        "evidence_version": 1,
        "run_id": "run-1",
        "durable_seq": 1,
        "event_id": "evt-1",
        "kind": "run.started",
        "canonical_payload": canonical,
        "payload_sha256": "0" * 64,
        "recorded_at_ms": 1,
        "producer_instance_ref": "agent-pod-a",
        "producer_generation": 1,
    }
    with pytest.raises(ValidationError, match="payload sha256"):
        DurableExecutionEvidence.model_validate(raw)
    raw["payload_sha256"] = hashlib.sha256(canonical).hexdigest()
    raw["site_id"] = "site-forbidden"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DurableExecutionEvidence.model_validate(raw)


def test_evidence_model_rejects_payload_above_contract_limit() -> None:
    with pytest.raises(ValidationError, match="at most 65536 bytes"):
        DurableExecutionEvidence(
            evidence_ref="aee_ref",
            evidence_version=1,
            run_id="run-1",
            durable_seq=1,
            event_id="evt-1",
            kind="run.started",
            canonical_payload=b"x" * (MAX_CANONICAL_PAYLOAD_BYTES + 1),
            payload_sha256=hashlib.sha256(
                b"x" * (MAX_CANONICAL_PAYLOAD_BYTES + 1)
            ).hexdigest(),
            recorded_at_ms=1,
            producer_instance_ref="agent-pod-a",
            producer_generation=1,
        )


class FakeEvidenceReader:
    def __init__(self, evidence: list[DurableExecutionEvidence]) -> None:
        self.evidence = evidence

    async def pull_durable_execution_evidence(
        self, run_id: str, after_durable_seq: int, limit: int
    ) -> list[DurableExecutionEvidence]:
        return [
            item
            for item in self.evidence
            if item.run_id == run_id and item.durable_seq > after_durable_seq
        ][:limit]

    async def get_durable_execution_evidence(
        self, run_id: str, evidence_ref: str
    ) -> DurableExecutionEvidence | None:
        return next(
            (
                item
                for item in self.evidence
                if item.run_id == run_id and item.evidence_ref == evidence_ref
            ),
            None,
        )

    async def get_run_durable_checkpoint(
        self, run_id: str
    ) -> DurableExecutionEvidence | None:
        return next(
            (
                item
                for item in reversed(self.evidence)
                if item.run_id == run_id and item.kind == "run.owner.completed"
            ),
            None,
        )


def _started(run_id: str, seq: int) -> DurableExecutionEvidence:
    return make_durable_execution_evidence(
        run_id=run_id,
        durable_seq=seq,
        event_id=f"evt-{seq}",
        event_kind="run.started",
        payload_json="{}",
        recorded_at_ms=1_724_800_123_456 + seq,
        producer_instance_ref="agent-pod-a",
        producer_generation=3,
    )


async def test_connect_service_pages_in_exact_durable_sequence_order() -> None:
    service = AgentExecutionEvidenceConnectService(
        FakeEvidenceReader([_started("run-1", seq) for seq in (1, 2, 3)])
    )
    response = await service.pull_durable_execution_evidence(
        evidence_pb2.PullDurableExecutionEvidenceRequest(
            run_id="run-1", after_durable_seq=1, page_size=1
        ),
        cast(
            RequestContext[
                evidence_pb2.PullDurableExecutionEvidenceRequest,
                evidence_pb2.PullDurableExecutionEvidenceResponse,
            ],
            object(),
        ),
    )
    assert [item.durable_seq for item in response.evidence] == [2]
    assert response.has_more is True
    assert response.next_after_durable_seq == 2
    assert response.evidence[0].recorded_at.ToMilliseconds() == 1_724_800_123_458


async def test_connect_service_returns_non_disclosing_not_found() -> None:
    service = AgentExecutionEvidenceConnectService(FakeEvidenceReader([]))
    response = await service.get_durable_execution_evidence(
        evidence_pb2.GetDurableExecutionEvidenceRequest(
            run_id="run-secret", evidence_ref="aee-unknown"
        ),
        cast(
            RequestContext[
                evidence_pb2.GetDurableExecutionEvidenceRequest,
                evidence_pb2.GetDurableExecutionEvidenceResponse,
            ],
            object(),
        ),
    )
    assert response.WhichOneof("outcome") == "not_found"
    checkpoint = await service.get_run_durable_checkpoint(
        evidence_pb2.GetRunDurableCheckpointRequest(run_id="run-secret"),
        cast(
            RequestContext[
                evidence_pb2.GetRunDurableCheckpointRequest,
                evidence_pb2.GetRunDurableCheckpointResponse,
            ],
            object(),
        ),
    )
    assert checkpoint.WhichOneof("outcome") == "not_found"


async def test_connect_service_rejects_invalid_bounds_before_storage() -> None:
    service = AgentExecutionEvidenceConnectService(FakeEvidenceReader([]))
    with pytest.raises(ConnectError) as raised:
        await service.pull_durable_execution_evidence(
            evidence_pb2.PullDurableExecutionEvidenceRequest(run_id="", page_size=0),
            cast(
                RequestContext[
                    evidence_pb2.PullDurableExecutionEvidenceRequest,
                    evidence_pb2.PullDurableExecutionEvidenceResponse,
                ],
                object(),
            ),
        )
    assert raised.value.code == Code.INVALID_ARGUMENT
