from __future__ import annotations

from google.protobuf.timestamp_pb2 import Timestamp

from kokoro.agent.presentation.v1 import presentation_pb2 as wire
from kokoro.common.v2 import command_envelope_pb2 as common_wire
from kokoro_agent.presentation.integrity import (
    ACK_EFFECT_DOMAIN,
    QUARANTINE_EFFECT_DOMAIN,
    delivery_status_digest,
    effect_digest,
    producer_fence,
)
from kokoro_agent.presentation.adapters.connect import PresentationConnectService
from kokoro_agent.presentation.delivery import AGENT_PRESENTATION_CONTRACT_REVISION


RUN_ID = "internal.run.1"
PRODUCER = producer_fence("agent.instance.1", 7)
SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64
SUBMISSION_REF = "presentation.submission:sha256:" + "c" * 64
RECORD_REF = "presentation.record:sha256:" + "d" * 64


def _command(command_id: str) -> common_wire.CommandIdentityV2:
    return common_wire.CommandIdentityV2(
        command_id=command_id,
        idempotency_key=f"idempotency.{command_id}",
        digest_algorithm=(
            common_wire.COMMAND_DIGEST_ALGORITHM_V2_SHA256_COMMAND_ENVELOPE
        ),
        request_digest="e" * 64,
    )


def _status(
    *,
    revision: int = 1,
    acknowledged: int = 0,
    command: common_wire.CommandIdentityV2 | None = None,
) -> wire.DeliveryStatus:
    status = wire.DeliveryStatus(
        run_id=RUN_ID,
        producer=PRODUCER,
        acknowledged_through_delivery_seq=acknowledged,
        status_revision=revision,
        updated_at=Timestamp(seconds=1_785_589_201),
    )
    if acknowledged:
        status.acknowledged_head_record_digest = SHA_A
    if command is not None:
        status.last_command.CopyFrom(command)
    status.status_digest = delivery_status_digest(status)
    return status


class _Store:
    active_checks = 0
    acknowledged: wire.AcknowledgeAdmissionsRequest | None = None
    quarantined: wire.QuarantineSubmissionRequest | None = None

    async def check_presentation_provider_active(self) -> None:
        self.active_checks += 1

    async def presentation_provider_fence(self, run_id: str) -> wire.ProducerFence:
        assert run_id == RUN_ID
        return PRODUCER

    async def presentation_provider_head(self, run_id: str) -> int:
        assert run_id == RUN_ID
        return 0

    async def presentation_provider_record(
        self, run_id: str, delivery_seq: int
    ) -> wire.DeliveryRecord | None:
        raise AssertionError("empty stream has no record")

    async def pull_presentation_provider_records(
        self,
        run_id: str,
        after_delivery_seq: int,
        through_delivery_seq: int,
        limit: int,
    ) -> tuple[wire.DeliveryRecord, ...]:
        return ()

    async def presentation_provider_status(
        self,
        run_id: str | None,
        original_command: common_wire.CommandIdentityV2 | None,
    ) -> wire.DeliveryStatus:
        assert run_id == RUN_ID and original_command is None
        return _status()

    async def acknowledge_presentation_provider_admissions(
        self, request: wire.AcknowledgeAdmissionsRequest
    ) -> wire.DeliveryStatus:
        self.acknowledged = request
        return _status(revision=2, acknowledged=1, command=request.command)

    async def quarantine_presentation_provider_submission(
        self, request: wire.QuarantineSubmissionRequest
    ) -> wire.DeliveryStatus:
        self.quarantined = request
        status = _status(revision=2, command=request.command)
        status.quarantine.CopyFrom(
            wire.QuarantineStatus(
                delivery_seq=1,
                record_ref=RECORD_REF,
                submission_ref=SUBMISSION_REF,
                rejection_class=wire.REJECTION_CLASS_PERMANENT,
                reason_code="SCHEMA_INVALID",
                session_rejection_digest=SHA_B,
                quarantined_at=Timestamp(seconds=1_785_589_202),
            )
        )
        status.status_digest = delivery_status_digest(status)
        return status


async def test_pull_binds_authoritative_producer_and_empty_snapshot_digest() -> None:
    service = PresentationConnectService(_Store())
    response = await service.pull_records(
        wire.PullRecordsRequest(
            run_id=RUN_ID,
            producer=PRODUCER,
            page_size=16,
        ),
        None,
    )
    assert response.run_id == RUN_ID
    assert response.producer == PRODUCER
    assert response.snapshot_through_delivery_seq == 0
    assert response.records == []
    assert response.delivery_status.status_digest == _status().status_digest


async def test_check_active_probes_presentation_store_not_evidence_listener() -> None:
    store = _Store()
    service = PresentationConnectService(store)

    response = await service.check_active(wire.CheckActiveRequest(), None)

    assert store.active_checks == 1
    assert response.contract_revision == AGENT_PRESENTATION_CONTRACT_REVISION


async def test_acknowledge_admissions_validates_effect_and_calls_store() -> None:
    store = _Store()
    service = PresentationConnectService(store)
    command = _command("a" * 32)
    effect = wire.AcknowledgeAdmissionsEffect(
        run_id=RUN_ID,
        producer=PRODUCER,
        expected_acknowledged_through=0,
        expected_status_revision=1,
        idempotency_ref="ack.request.1",
        receipts=[
            wire.AdmissionReceipt(
                previous_delivery_seq=0,
                delivery_seq=1,
                record_ref=RECORD_REF,
                record_digest=SHA_A,
                submission_ref=SUBMISSION_REF,
                submission_digest=SHA_B,
                session_admission_receipt_ref="session.receipt.1",
                session_effect_digest=SHA_A,
            )
        ],
        effect_digest_domain=ACK_EFFECT_DOMAIN,
        effect_digest="sha256:" + "0" * 64,
    )
    effect.effect_digest = effect_digest(effect, ACK_EFFECT_DOMAIN)
    request = wire.AcknowledgeAdmissionsRequest(command=command, effect=effect)

    response = await service.acknowledge_admissions(request, None)

    assert store.acknowledged == request
    assert response.status.acknowledged_through_delivery_seq == 1
    assert response.status.last_command == command


async def test_quarantine_submission_validates_effect_and_calls_store() -> None:
    store = _Store()
    service = PresentationConnectService(store)
    command = _command("b" * 32)
    effect = wire.QuarantineSubmissionEffect(
        run_id=RUN_ID,
        producer=PRODUCER,
        expected_acknowledged_through=0,
        expected_status_revision=1,
        idempotency_ref="quarantine.request.1",
        delivery_seq=1,
        record_ref=RECORD_REF,
        record_digest=SHA_A,
        submission_ref=SUBMISSION_REF,
        submission_digest=SHA_B,
        rejection_class=wire.REJECTION_CLASS_PERMANENT,
        reason_code="SCHEMA_INVALID",
        session_rejection_digest=SHA_B,
        effect_digest_domain=QUARANTINE_EFFECT_DOMAIN,
        effect_digest="sha256:" + "0" * 64,
    )
    effect.effect_digest = effect_digest(effect, QUARANTINE_EFFECT_DOMAIN)
    request = wire.QuarantineSubmissionRequest(command=command, effect=effect)

    response = await service.quarantine_submission(request, None)

    assert store.quarantined == request
    assert response.status.quarantine.delivery_seq == 1
    assert response.status.quarantine.reason_code == "SCHEMA_INVALID"
