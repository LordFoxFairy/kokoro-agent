from __future__ import annotations

import hashlib
import json
from typing import cast

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.request import RequestContext
from pydantic import BaseModel, ValidationError

from kokoro.agent.execution.v1 import agent_execution_evidence_pb2 as evidence_pb2

from kokoro_agent.evidence.models import (
    MAX_CANONICAL_PAYLOAD_BYTES,
    DurableOutputDraft,
    DurableOutputRecord,
    DurableExecutionEvidence,
    append_output_digest,
    durable_output_draft_for_event,
    durable_output_drafts_for_event,
    evidence_kind_for_event,
    initial_output_digest,
    make_durable_output_record,
    make_durable_execution_evidence,
)
from kokoro_agent.contract import (
    DeliveryCreatedPayload,
    MessageCompletedPayload,
    MessageDeltaPayload,
    PlanProposal,
    PlanProposedPayload,
    PlanStep,
    SubagentFinishedPayload,
    SubagentStartedPayload,
    SubagentTextDeltaPayload,
    SubagentThinkingDeltaPayload,
    ThinkingDeltaPayload,
    ToolAwaitingApprovalPayload,
    ToolInvokedPayload,
    ToolOutputDeltaPayload,
    ToolReturnedPayload,
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


def test_durable_output_record_has_independent_sequence_and_frozen_digest_chain() -> (
    None
):
    draft = durable_output_draft_for_event(
        MessageDeltaPayload(segment_id="segment-1", delta="hello")
    )
    assert draft is not None
    record = make_durable_output_record(
        run_id="run-output",
        output_seq=1,
        draft=draft,
        replaces_through_output_seq=0,
        recorded_at_ms=1_724_800_123_456,
        producer_instance_ref="agent-pod-a",
        producer_generation=3,
    )
    assert isinstance(record, DurableOutputRecord)
    assert record.output_seq == 1
    assert not hasattr(record, "durable_seq")
    canonical = evidence_pb2.DurableOutputCanonicalPayloadV1.FromString(
        record.canonical_payload
    )
    assert canonical.WhichOneof("payload") == "text_delta"
    assert canonical.text_delta.part_ref == "segment-1"
    assert canonical.text_delta.delta == "hello"
    chain0 = initial_output_digest("run-output")
    assert chain0 == hashlib.sha256(b"kokoro-output-chain-v1\0run-output").hexdigest()
    assert (
        append_output_digest(chain0, 1, record.payload_sha256)
        == hashlib.sha256(
            bytes.fromhex(chain0)
            + (1).to_bytes(8, "big")
            + bytes.fromhex(record.payload_sha256)
        ).hexdigest()
    )


def test_output_draft_rejects_text_part_binding_mismatch() -> None:
    canonical = evidence_pb2.DurableOutputCanonicalPayloadV1(
        text_delta=evidence_pb2.TextDeltaOutputV1(
            part_ref="canonical-part", delta="hello"
        )
    ).SerializeToString(deterministic=True)
    with pytest.raises(ValidationError, match="text part marker mismatch"):
        DurableOutputDraft(
            canonical_payload=canonical,
            text_part_ref="different-part",
        )


def _output_payloads(
    payload: BaseModel,
) -> list[evidence_pb2.DurableOutputCanonicalPayloadV1]:
    return [
        evidence_pb2.DurableOutputCanonicalPayloadV1.FromString(draft.canonical_payload)
        for draft in durable_output_drafts_for_event(payload)
    ]


def test_safe_output_mapping_covers_supported_families_without_raw_material() -> None:
    text = _output_payloads(
        MessageCompletedPayload(segment_id="segment-1", content="safe answer")
    )
    assert [item.WhichOneof("payload") for item in text] == ["text_snapshot"]
    assert text[0].text_snapshot.replaces_through_output_seq == 0

    started = _output_payloads(
        ToolInvokedPayload(
            segment_id="segment-1",
            tool_id="tool-1",
            name="search",
            args={
                "authorization": "RAW_TOOL_ARGUMENT",
                "local_path": "/Users/private/tool-argument",
            },
        )
    )
    assert [item.WhichOneof("payload") for item in started] == ["tool_started"]
    assert not started[0].tool_started.HasField("redacted_input_summary_json")
    assert b"RAW_TOOL_ARGUMENT" not in started[0].SerializeToString(deterministic=True)
    assert b"/Users/private/tool-argument" not in started[0].SerializeToString(
        deterministic=True
    )

    finished = _output_payloads(
        ToolReturnedPayload(
            segment_id="segment-1",
            tool_id="tool-1",
            name="search",
            result="RAW_TOOL_RESULT from /Users/private/tool-result",
            is_error=True,
        )
    )
    assert [item.WhichOneof("payload") for item in finished] == [
        "tool_finished",
        "error",
    ]
    assert not any(
        field.name == "safe_result_preview"
        for field, _value in finished[0].tool_finished.ListFields()
    )
    assert b"RAW_TOOL_RESULT" not in b"".join(
        item.SerializeToString(deterministic=True) for item in finished
    )
    assert _output_payloads(
        ToolOutputDeltaPayload(
            segment_id="segment-1",
            tool_id="tool-1",
            name="search",
            delta="RAW_TOOL_OUTPUT_DELTA",
        )
    ) == []
    assert _output_payloads(
        ToolAwaitingApprovalPayload(
            segment_id="segment-1",
            tool_id="tool-approval-1",
            name="execute",
            args={"raw": "RAW_APPROVAL_ARGUMENT"},
            description="RAW_APPROVAL_DESCRIPTION",
            allowed_decisions=["approve", "reject"],
            kind="tool_approval",
            editable=False,
            input_schema={"example": "RAW_EXECUTABLE_SCHEMA"},
            pending_tool_ids=["tool-approval-1"],
        )
    ) == []

    plan = _output_payloads(
        PlanProposedPayload(
            segment_id="segment-plan",
            owner_ref="plan-1",
            owner_version=1,
            proposal=PlanProposal(
                summary="Safe plan",
                steps=[PlanStep(step_ref="step-1", label="Review", status="pending")],
                allowed_actions=["accept", "reject"],
            ),
        )
    )
    assert [item.WhichOneof("payload") for item in plan] == ["plan_progress"]

    started_subagent = _output_payloads(
        SubagentStartedPayload(
            segment_id="segment-1",
            subagent_id="subagent-1",
            name="researcher",
            description="RAW_SUBAGENT_DESCRIPTION",
            subagent_type="research",
            source="built-in",
        )
    )
    finished_subagent = _output_payloads(
        SubagentFinishedPayload(
            segment_id="segment-1",
            subagent_id="subagent-1",
            name="researcher",
            subagent_type="research",
            source="built-in",
            failed=True,
            error="RAW_SUBAGENT_ERROR at /tmp/private",
        )
    )
    assert started_subagent[0].WhichOneof("payload") == "subagent_progress"
    assert [item.WhichOneof("payload") for item in finished_subagent] == [
        "subagent_progress",
        "error",
    ]
    encoded_subagents = b"".join(
        item.SerializeToString(deterministic=True)
        for item in [*started_subagent, *finished_subagent]
    )
    assert b"RAW_SUBAGENT_DESCRIPTION" not in encoded_subagents
    assert b"RAW_SUBAGENT_ERROR" not in encoded_subagents

    content_hash = "a" * 64
    delivery = _output_payloads(
        DeliveryCreatedPayload(
            path="/Users/private/RAW_DELIVERY_PATH.png",
            title="RAW_DELIVERY_TITLE",
            mime="image/png",
            size=10,
            content_hash=content_hash,
            note="RAW_DELIVERY_NOTE",
        )
    )
    assert [item.WhichOneof("payload") for item in delivery] == ["notice"]
    assert delivery[0].notice.notice_ref == f"delivery:sha256:{content_hash}"
    assert delivery[0].notice.code == "delivery.created"
    assert delivery[0].notice.message == "Delivery created"
    encoded_delivery = b"".join(
        item.SerializeToString(deterministic=True) for item in delivery
    )
    assert b"RAW_DELIVERY_PATH" not in encoded_delivery
    assert b"RAW_DELIVERY_TITLE" not in encoded_delivery
    assert b"RAW_DELIVERY_NOTE" not in encoded_delivery


def test_output_mapping_excludes_reasoning_but_preserves_user_visible_text() -> None:
    assert (
        durable_output_drafts_for_event(
            ThinkingDeltaPayload(segment_id="segment-1", delta="hidden chain")
        )
        == ()
    )
    assert (
        durable_output_drafts_for_event(
            SubagentThinkingDeltaPayload(
                segment_id="segment-1",
                subagent_id="subagent-1",
                delta="hidden chain",
            )
        )
        == ()
    )
    for unsafe in (
        "Bearer top-secret",
        "read /Users/private/key.txt",
        "site_id=site-secret",
        "api_key=top-secret",
    ):
        outputs = _output_payloads(
            MessageDeltaPayload(segment_id="segment-1", delta=unsafe)
        )
        assert len(outputs) == 1
        assert outputs[0].text_delta.delta == unsafe
    subagent_text = _output_payloads(
        SubagentTextDeltaPayload(
            segment_id="segment-subagent",
            subagent_id="subagent-1",
            text="explain /Users/example and api_key placeholders",
        )
    )
    assert subagent_text[0].text_delta.delta == (
        "explain /Users/example and api_key placeholders"
    )
    plan_text = _output_payloads(
        PlanProposedPayload(
            segment_id="segment-plan",
            owner_ref="plan-2",
            owner_version=1,
            proposal=PlanProposal(
                summary="Review /Users/example and site_id placeholders",
                steps=[
                    PlanStep(
                        step_ref="step-2",
                        label="Document api_key examples",
                        status="pending",
                    )
                ],
                allowed_actions=["accept"],
            ),
        )
    )
    assert plan_text[0].plan_progress.safe_summary.startswith("Review /Users")


def test_output_mapping_hashes_non_opaque_reference_shapes_without_losing_text() -> None:
    first = _output_payloads(
        MessageDeltaPayload(segment_id="/Users/private/segment", delta="safe text")
    )
    replay = _output_payloads(
        MessageDeltaPayload(segment_id="/Users/private/segment", delta="safe text")
    )
    other = _output_payloads(
        MessageDeltaPayload(segment_id="/Users/private/other", delta="safe text")
    )
    assert first[0].text_delta.delta == "safe text"
    assert first[0].text_delta.part_ref == replay[0].text_delta.part_ref
    assert first[0].text_delta.part_ref != other[0].text_delta.part_ref
    assert first[0].text_delta.part_ref.startswith("opaque_ref_")
    assert "/Users/private" not in first[0].text_delta.part_ref


def test_output_mapping_chunks_long_utf8_delta_without_losing_text() -> None:
    text = "界🙂" * 6_000
    outputs = _output_payloads(
        MessageDeltaPayload(segment_id="segment-long-delta", delta=text)
    )

    assert len(outputs) > 1
    assert all(item.WhichOneof("payload") == "text_delta" for item in outputs)
    assert all(len(item.text_delta.delta.encode()) <= 16 * 1024 for item in outputs)
    assert "".join(item.text_delta.delta for item in outputs) == text


def test_output_mapping_represents_long_snapshot_completely() -> None:
    text = "界🙂" * 15_000
    outputs = _output_payloads(
        MessageCompletedPayload(segment_id="segment-long-snapshot", content=text)
    )

    assert len(outputs) > 1
    assert outputs[0].WhichOneof("payload") == "text_snapshot"
    assert len(outputs[0].text_snapshot.text.encode()) <= 60 * 1024
    assert all(item.WhichOneof("payload") == "text_delta" for item in outputs[1:])
    assert all(len(item.text_delta.delta.encode()) <= 16 * 1024 for item in outputs[1:])
    assert outputs[0].text_snapshot.text + "".join(
        item.text_delta.delta for item in outputs[1:]
    ) == text


def test_output_mapping_omits_unvalidated_artifact_reference() -> None:
    assert (
        durable_output_drafts_for_event(
            DeliveryCreatedPayload(
                path="/report.pdf",
                title="Report",
                mime="application/pdf",
                size=10,
                content_hash="not-a-content-digest",
            )
        )
        == ()
    )


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
    def __init__(
        self,
        evidence: list[DurableExecutionEvidence],
        outputs: list[DurableOutputRecord] | None = None,
    ) -> None:
        self.evidence = evidence
        self.outputs = outputs or []

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

    async def pull_durable_output_records(
        self, run_id: str, after_output_seq: int, limit: int
    ) -> list[DurableOutputRecord]:
        return [
            item
            for item in self.outputs
            if item.run_id == run_id and item.output_seq > after_output_seq
        ][:limit]


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


def _output(run_id: str, seq: int) -> DurableOutputRecord:
    draft = durable_output_draft_for_event(
        MessageDeltaPayload(segment_id="segment-1", delta=f"delta-{seq}")
    )
    assert draft is not None
    return make_durable_output_record(
        run_id=run_id,
        output_seq=seq,
        draft=draft,
        replaces_through_output_seq=0,
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


async def test_connect_service_pages_output_by_exclusive_sequence_cursor() -> None:
    service = AgentExecutionEvidenceConnectService(
        FakeEvidenceReader([], [_output("run-1", seq) for seq in (1, 2, 3)])
    )
    response = await service.pull_durable_output_records(
        evidence_pb2.PullDurableOutputRecordsRequest(
            run_id="run-1", after_output_seq=1, page_size=1
        ),
        cast(
            RequestContext[
                evidence_pb2.PullDurableOutputRecordsRequest,
                evidence_pb2.PullDurableOutputRecordsResponse,
            ],
            object(),
        ),
    )
    assert [item.output_seq for item in response.records] == [2]
    assert response.has_more is True
    assert response.next_after_output_seq == 2
    assert response.records[0].recorded_at.ToMilliseconds() == 1_724_800_123_458
    assert response.records[0].producer_generation == 3


async def test_connect_service_rejects_output_page_above_contract_cap() -> None:
    service = AgentExecutionEvidenceConnectService(FakeEvidenceReader([]))
    with pytest.raises(ConnectError) as raised:
        await service.pull_durable_output_records(
            evidence_pb2.PullDurableOutputRecordsRequest(run_id="run-1", page_size=65),
            cast(
                RequestContext[
                    evidence_pb2.PullDurableOutputRecordsRequest,
                    evidence_pb2.PullDurableOutputRecordsResponse,
                ],
                object(),
            ),
        )
    assert raised.value.code == Code.INVALID_ARGUMENT


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
