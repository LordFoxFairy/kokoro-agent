from __future__ import annotations

import json
import hashlib
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
from kokoro_agent.presentation.generated.contract_metadata import (
    ROOT_CORPUS_SHA256,
    ROOT_PRESENTATION_CONTRACT_REVISION,
)


def _corpus() -> dict[str, object]:
    root = Path(__file__).resolve().parents[1]
    return cast(dict[str, object], json.loads(
        (root / "src/kokoro_agent/presentation/generated/agent_presentation_integrity_v1.json").read_text()
    ))


def test_integrity_corpus_is_a_root_provenance_bound_generated_mirror() -> None:
    root = Path(__file__).resolve().parents[1]
    mirror = root / "src/kokoro_agent/presentation/generated/agent_presentation_integrity_v1.json"
    assert ROOT_PRESENTATION_CONTRACT_REVISION == "agent-presentation@v1:c282e2fc"
    assert hashlib.sha256(mirror.read_bytes()).hexdigest() == ROOT_CORPUS_SHA256


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
