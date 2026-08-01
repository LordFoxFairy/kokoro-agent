from __future__ import annotations

from google.protobuf.timestamp_pb2 import Timestamp

from kokoro.agent.presentation.v1 import agent_presentation_pb2 as wire
from kokoro.common.v2 import command_envelope_pb2 as common_wire
from kokoro_agent.presentation.integrity import (
    delivery_status_digest,
    producer_fence,
)
from kokoro_agent.presentation.provider import (
    AGENT_PRESENTATION_CONTRACT_REVISION,
    AgentPresentationConnectService,
)


RUN_ID = "internal.run.1"
PRODUCER = producer_fence("agent.instance.1", 7)


def _status() -> wire.PresentationDeliveryStatus:
    status = wire.PresentationDeliveryStatus(
        run_id=RUN_ID,
        producer=PRODUCER,
        status_revision=1,
        updated_at=Timestamp(seconds=1_785_589_201),
    )
    status.status_digest = delivery_status_digest(status)
    return status


class _Store:
    active_checks = 0

    async def check_presentation_provider_active(self) -> None:
        self.active_checks += 1

    async def presentation_provider_fence(
        self, run_id: str
    ) -> wire.PresentationProducerFence:
        assert run_id == RUN_ID
        return PRODUCER

    async def presentation_provider_head(self, run_id: str) -> int:
        assert run_id == RUN_ID
        return 0

    async def presentation_provider_record(
        self, run_id: str, presentation_seq: int
    ) -> wire.PresentationCandidateRecord | None:
        raise AssertionError("empty stream has no record")

    async def pull_presentation_provider_records(
        self,
        run_id: str,
        after_presentation_seq: int,
        through_presentation_seq: int,
        limit: int,
    ) -> tuple[wire.PresentationCandidateRecord, ...]:
        return ()

    async def presentation_provider_status(
        self,
        run_id: str | None,
        original_command: common_wire.CommandIdentityV2 | None,
    ) -> wire.PresentationDeliveryStatus:
        assert run_id == RUN_ID and original_command is None
        return _status()

async def test_pull_binds_authoritative_producer_and_empty_snapshot_digest() -> None:
    service = AgentPresentationConnectService(_Store())
    response = await service.pull_candidate_batches(
        wire.PullCandidateBatchesRequest(
            run_id=RUN_ID,
            producer=PRODUCER,
            page_size=16,
        ),
        None,  # type: ignore[arg-type]
    )
    assert response.run_id == RUN_ID
    assert response.producer == PRODUCER
    assert response.snapshot_through_presentation_seq == 0
    assert response.records == []
    assert response.delivery_status.status_digest == _status().status_digest


async def test_check_active_probes_presentation_store_not_evidence_listener() -> None:
    store = _Store()
    service = AgentPresentationConnectService(store)

    response = await service.check_active(
        wire.CheckActiveRequest(),
        None,  # type: ignore[arg-type]
    )

    assert store.active_checks == 1
    assert response.contract_revision == AGENT_PRESENTATION_CONTRACT_REVISION


async def test_status_read_verifies_authoritative_producer_and_status_digest() -> None:
    service = AgentPresentationConnectService(_Store())

    response = await service.get_delivery_status(
        wire.GetDeliveryStatusRequest(run_id=RUN_ID, producer=PRODUCER),
        None,  # type: ignore[arg-type]
    )

    assert response.status == _status()
