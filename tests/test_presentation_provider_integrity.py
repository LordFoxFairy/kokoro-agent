from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from google.protobuf.timestamp_pb2 import Timestamp

from kokoro.agent.presentation.v1 import agent_presentation_pb2 as wire
from kokoro_agent.presentation.integrity import (
    delivery_status_digest,
    producer_fence,
    record_chain_genesis_digest,
    snapshot_head_digest,
)


def _corpus() -> dict[str, object]:
    root = Path(__file__).resolve().parents[2]
    return cast(dict[str, object], json.loads(
        (root / "contract/corpus/agent-presentation-integrity-v1.json").read_text()
    ))


def test_root_cross_language_presentation_integrity_vector() -> None:
    corpus = _corpus()
    producer_data = cast(dict[str, object], corpus["producer"])
    producer = producer_fence(
        str(producer_data["producerInstanceRef"]),
        int(str(producer_data["producerGeneration"])),
    )
    assert producer.producer_fence_digest == producer_data["producerFenceDigest"]
    run_id = str(corpus["runId"])
    assert record_chain_genesis_digest(run_id, producer) == corpus["genesisRecordDigest"]
    assert snapshot_head_digest(run_id, producer, 0, None) == corpus["emptySnapshotHeadDigest"]


def test_root_terminal_status_digest_binds_terminal_evidence_and_head() -> None:
    corpus = _corpus()
    producer_data = cast(dict[str, object], corpus["producer"])
    terminal = cast(dict[str, object], corpus["terminalStatus"])
    producer = producer_fence(
        str(producer_data["producerInstanceRef"]),
        int(str(producer_data["producerGeneration"])),
    )
    timestamp = Timestamp(seconds=1_785_589_201)
    seal = wire.PresentationTerminalSeal(
        sealed_through_presentation_seq=int(
            str(terminal["sealedThroughPresentationSeq"])
        ),
        sealed_head_record_digest=str(terminal["sealedHeadRecordDigest"]),
        terminal_evidence_ref=str(terminal["terminalEvidenceRef"]),
        terminal_evidence_payload_digest=str(
            terminal["terminalEvidencePayloadDigest"]
        ),
        terminal_disposition=wire.PRESENTATION_TERMINAL_DISPOSITION_COMPLETED,
        sealed_at=timestamp,
    )
    status = wire.PresentationDeliveryStatus(
        run_id=str(corpus["runId"]),
        producer=producer,
        acknowledged_through_presentation_seq=2,
        acknowledged_head_record_digest=str(terminal["sealedHeadRecordDigest"]),
        status_revision=3,
        updated_at=timestamp,
        terminal_seal=seal,
    )
    assert delivery_status_digest(status) == terminal["statusDigest"]
