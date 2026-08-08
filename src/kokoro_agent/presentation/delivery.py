"""Presentation delivery application service and its single durable store port."""

from __future__ import annotations

import re
from typing import Protocol

from kokoro.agent.presentation.v1 import presentation_pb2 as wire
from kokoro.common.v2 import command_envelope_pb2 as common_wire
from kokoro_agent.presentation.integrity import (
    ACK_EFFECT_DOMAIN,
    QUARANTINE_EFFECT_DOMAIN,
    delivery_record_digest,
    delivery_status_digest,
    effect_digest,
    producer_fence,
    record_chain_genesis_digest,
    reject_unknown_fields,
    snapshot_head_digest,
)


def _producer_fenced() -> ValueError:
    return ValueError("PRESENTATION_PRODUCER_FENCED")

AGENT_PRESENTATION_CONTRACT_REVISION = 'agent-presentation@v1'
_HEX_SHA256 = re.compile('^[0-9a-f]{64}$')

class PresentationProviderStore(Protocol):

    async def check_presentation_provider_active(self) -> None:
        ...

    async def presentation_provider_fence(self, run_id: str) -> wire.ProducerFence:
        ...

    async def presentation_provider_head(self, run_id: str) -> int:
        ...

    async def presentation_provider_record(self, run_id: str, delivery_seq: int) -> wire.DeliveryRecord | None:
        ...

    async def pull_presentation_provider_records(self, run_id: str, after_delivery_seq: int, through_delivery_seq: int, limit: int) -> tuple[wire.DeliveryRecord, ...]:
        ...

    async def presentation_provider_status(self, run_id: str | None, original_command: common_wire.CommandIdentityV2 | None) -> wire.DeliveryStatus:
        ...

    async def acknowledge_presentation_provider_admissions(self, request: wire.AcknowledgeAdmissionsRequest) -> wire.DeliveryStatus:
        ...

    async def quarantine_presentation_provider_submission(self, request: wire.QuarantineSubmissionRequest) -> wire.DeliveryStatus:
        ...

def _producer_equal(left: wire.ProducerFence, right: wire.ProducerFence) -> bool:
    return left.SerializeToString(deterministic=True) == right.SerializeToString(deterministic=True)

def _validate_command(command: common_wire.CommandIdentityV2) -> None:
    if not re.fullmatch('[0-9a-f]{32}', command.command_id) or not command.idempotency_key or command.digest_algorithm != common_wire.COMMAND_DIGEST_ALGORITHM_V2_SHA256_COMMAND_ENVELOPE or (_HEX_SHA256.fullmatch(command.request_digest) is None):
        raise ValueError('PRESENTATION_COMMAND_IDENTITY_INVALID')

def _validate_status(status: wire.DeliveryStatus, *, run_id: str, producer: wire.ProducerFence) -> wire.DeliveryStatus:
    if status.run_id != run_id:
        raise ValueError('PRESENTATION_STATUS_RUN_INVALID')
    if not _producer_equal(status.producer, producer):
        raise ValueError('PRESENTATION_STATUS_PRODUCER_INVALID')
    if status.status_revision < 1 or status.status_digest != delivery_status_digest(status):
        raise ValueError('PRESENTATION_STATUS_DIGEST_INVALID')
    return status

def _validate_records(*, run_id: str, producer: wire.ProducerFence, after_delivery_seq: int, previous_record_digest: str, records: tuple[wire.DeliveryRecord, ...]) -> None:
    expected_previous = previous_record_digest
    for offset, record in enumerate(records, start=1):
        expected_sequence = after_delivery_seq + offset
        if record.previous_delivery_seq != expected_sequence - 1 or record.delivery_seq != expected_sequence or (not _producer_equal(record.producer, producer)) or (record.previous_record_digest != expected_previous) or (record.record_digest != delivery_record_digest(run_id, record)):
            raise ValueError('PRESENTATION_RECORD_CHAIN_INVALID')
        expected_previous = record.record_digest

class DeliveryService:
    """Thin generated-wire adapter; durable decisions remain in the store."""

    def __init__(self, store: PresentationProviderStore) -> None:
        self._store = store

    async def check_active(self, request: wire.CheckActiveRequest) -> wire.CheckActiveResponse:
        reject_unknown_fields(request)
        await self._store.check_presentation_provider_active()
        return wire.CheckActiveResponse(contract_revision=AGENT_PRESENTATION_CONTRACT_REVISION)

    async def _authorize_producer(self, run_id: str, claimed: wire.ProducerFence) -> wire.ProducerFence:
        expected = producer_fence(claimed.producer_instance_ref, claimed.producer_generation)
        if not _producer_equal(claimed, expected):
            raise _producer_fenced()
        authoritative = await self._store.presentation_provider_fence(run_id)
        if not _producer_equal(claimed, authoritative):
            raise _producer_fenced()
        return authoritative

    async def pull_records(self, request: wire.PullRecordsRequest) -> wire.PullRecordsResponse:
        reject_unknown_fields(request)
        if not request.run_id or not 1 <= request.page_size <= 128:
            raise ValueError('PRESENTATION_CURSOR_INVALID')
        producer = await self._authorize_producer(request.run_id, request.producer)
        current_head = await self._store.presentation_provider_head(request.run_id)
        frozen = request.snapshot_through_delivery_seq if request.HasField('snapshot_through_delivery_seq') else current_head
        if request.after_delivery_seq > frozen or frozen > current_head:
            raise ValueError('PRESENTATION_SNAPSHOT_INVALID')
        head = await self._store.presentation_provider_record(request.run_id, frozen) if frozen > 0 else None
        if (frozen > 0) != (head is not None):
            raise ValueError('PRESENTATION_SNAPSHOT_NOT_READY')
        cursor_record = await self._store.presentation_provider_record(request.run_id, request.after_delivery_seq) if request.after_delivery_seq > 0 else None
        if (request.after_delivery_seq > 0) != (cursor_record is not None):
            raise ValueError('PRESENTATION_CURSOR_INVALID')
        records = await self._store.pull_presentation_provider_records(request.run_id, request.after_delivery_seq, frozen, request.page_size)
        _validate_records(run_id=request.run_id, producer=producer, after_delivery_seq=request.after_delivery_seq, previous_record_digest=cursor_record.record_digest if cursor_record is not None else record_chain_genesis_digest(request.run_id, producer), records=records)
        page_end = records[-1].delivery_seq if records else request.after_delivery_seq
        has_more = page_end < frozen
        status = _validate_status(await self._store.presentation_provider_status(request.run_id, None), run_id=request.run_id, producer=producer)
        response = wire.PullRecordsResponse(run_id=request.run_id, producer=producer, page_after_delivery_seq=request.after_delivery_seq, snapshot_through_delivery_seq=frozen, records=records, has_more=has_more, delivery_status=status, snapshot_head_digest=snapshot_head_digest(request.run_id, producer, frozen, head.record_digest if head is not None else None))
        if has_more:
            response.next_after_delivery_seq = page_end
        if head is not None:
            response.snapshot_head_record_digest = head.record_digest
        return response

    async def acknowledge_admissions(self, request: wire.AcknowledgeAdmissionsRequest) -> wire.AcknowledgeAdmissionsResponse:
        reject_unknown_fields(request)
        _validate_command(request.command)
        effect = request.effect
        if effect.effect_digest_domain != ACK_EFFECT_DOMAIN or effect.effect_digest != effect_digest(effect, ACK_EFFECT_DOMAIN) or effect.expected_status_revision < 1 or (not effect.receipts):
            raise ValueError('PRESENTATION_ACK_EFFECT_INVALID')
        expected = effect.expected_acknowledged_through
        for receipt in effect.receipts:
            if receipt.previous_delivery_seq != expected or receipt.delivery_seq != expected + 1:
                raise ValueError('PRESENTATION_ACK_SEQUENCE_INVALID')
            expected = receipt.delivery_seq
        producer = await self._authorize_producer(effect.run_id, effect.producer)
        status = _validate_status(await self._store.acknowledge_presentation_provider_admissions(request), run_id=effect.run_id, producer=producer)
        if status.acknowledged_through_delivery_seq != effect.receipts[-1].delivery_seq or not status.HasField('last_command') or status.last_command != request.command:
            raise ValueError('PRESENTATION_ACK_STATUS_INVALID')
        return wire.AcknowledgeAdmissionsResponse(status=status)

    async def quarantine_submission(self, request: wire.QuarantineSubmissionRequest) -> wire.QuarantineSubmissionResponse:
        reject_unknown_fields(request)
        _validate_command(request.command)
        effect = request.effect
        if effect.effect_digest_domain != QUARANTINE_EFFECT_DOMAIN or effect.effect_digest != effect_digest(effect, QUARANTINE_EFFECT_DOMAIN) or effect.expected_status_revision < 1 or (effect.delivery_seq != effect.expected_acknowledged_through + 1) or (effect.rejection_class != wire.REJECTION_CLASS_PERMANENT):
            raise ValueError('PRESENTATION_QUARANTINE_EFFECT_INVALID')
        producer = await self._authorize_producer(effect.run_id, effect.producer)
        status = _validate_status(await self._store.quarantine_presentation_provider_submission(request), run_id=effect.run_id, producer=producer)
        if not status.HasField('quarantine') or status.quarantine.delivery_seq != effect.delivery_seq or status.quarantine.record_ref != effect.record_ref or (status.quarantine.submission_ref != effect.submission_ref) or (not status.HasField('last_command')) or (status.last_command != request.command):
            raise ValueError('PRESENTATION_QUARANTINE_STATUS_INVALID')
        return wire.QuarantineSubmissionResponse(status=status)

    async def get_delivery_status(self, request: wire.GetDeliveryStatusRequest) -> wire.GetDeliveryStatusResponse:
        reject_unknown_fields(request)
        lookup = request.WhichOneof('lookup')
        if lookup == 'run_id' and request.run_id:
            run_id: str | None = request.run_id
            original_command: common_wire.CommandIdentityV2 | None = None
        elif lookup == 'original_command':
            _validate_command(request.original_command)
            run_id = None
            original_command = request.original_command
        else:
            raise ValueError('PRESENTATION_STATUS_LOOKUP_INVALID')
        status = await self._store.presentation_provider_status(run_id, original_command)
        authoritative = await self._authorize_producer(status.run_id, request.producer)
        return wire.GetDeliveryStatusResponse(status=_validate_status(status, run_id=status.run_id, producer=authoritative))

__all__ = ["AGENT_PRESENTATION_CONTRACT_REVISION", "DeliveryService", "PresentationProviderStore"]
