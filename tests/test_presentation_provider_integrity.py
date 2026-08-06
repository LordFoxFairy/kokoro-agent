from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

from google.protobuf.timestamp_pb2 import Timestamp

from kokoro.agent.presentation.v1 import presentation_pb2 as wire
from kokoro_agent.presentation.integrity import (
    delivery_status_digest,
    producer_fence,
    record_chain_genesis_digest,
    snapshot_head_digest,
)


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "src/generated/contracts/presentation/corpus-v1.json"
PROVENANCE = ROOT / "src/generated/provenance.json"


def _delivery() -> dict[str, object]:
    corpus = cast(dict[str, object], json.loads(CORPUS.read_text()))
    return cast(dict[str, object], corpus["delivery"])


def test_integrity_corpus_is_covered_by_root_generated_provenance() -> None:
    provenance = cast(dict[str, object], json.loads(PROVENANCE.read_text()))
    outputs = {
        cast(dict[str, str], row)["path"]: cast(dict[str, str], row)["sha256"]
        for row in cast(list[object], provenance["outputs"])
    }
    relative = "src/generated/contracts/presentation/corpus-v1.json"

    assert provenance["boundaryId"] == "generated-kokoro-agent@v1"
    assert outputs[relative] == f"sha256:{hashlib.sha256(CORPUS.read_bytes()).hexdigest()}"


def test_root_cross_language_presentation_integrity_vector() -> None:
    delivery = _delivery()
    producer_data = cast(dict[str, object], delivery["producer"])
    producer = producer_fence(
        str(producer_data["producerInstanceRef"]),
        int(str(producer_data["producerGeneration"])),
    )
    assert producer.producer_fence_digest == producer_data["producerFenceDigest"]
    run_id = str(delivery["runId"])
    assert record_chain_genesis_digest(run_id, producer) == delivery[
        "genesisRecordDigest"
    ]
    assert snapshot_head_digest(run_id, producer, 0, None) == delivery[
        "emptyDeliveryHeadDigest"
    ]


def test_root_terminal_status_digest_binds_terminal_evidence_and_head() -> None:
    delivery = _delivery()
    producer_data = cast(dict[str, object], delivery["producer"])
    terminal = cast(dict[str, object], delivery["terminalStatus"])
    producer = producer_fence(
        str(producer_data["producerInstanceRef"]),
        int(str(producer_data["producerGeneration"])),
    )
    timestamp = Timestamp(seconds=1_785_589_201)
    seal = wire.TerminalSeal(
        sealed_through_delivery_seq=int(
            str(terminal["sealedThroughDeliverySeq"])
        ),
        sealed_head_record_digest=str(terminal["sealedHeadRecordDigest"]),
        terminal_evidence_ref=str(terminal["terminalEvidenceRef"]),
        terminal_evidence_payload_digest=str(
            terminal["terminalEvidencePayloadDigest"]
        ),
        terminal_disposition=wire.TERMINAL_DISPOSITION_COMPLETED,
        sealed_at=timestamp,
    )
    status = wire.DeliveryStatus(
        run_id=str(delivery["runId"]),
        producer=producer,
        acknowledged_through_delivery_seq=2,
        acknowledged_head_record_digest=str(terminal["sealedHeadRecordDigest"]),
        status_revision=3,
        updated_at=timestamp,
        terminal_seal=seal,
    )
    assert delivery_status_digest(status) == terminal["statusDigest"]
