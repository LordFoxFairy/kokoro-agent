"""Read-only foundation for the Root AgentPresentation V1 Connect boundary.

The durable write/CAS methods are intentionally not exposed until their Mongo
unit-of-work implementation can be activated with the terminal owner seal.
"""

from __future__ import annotations

from typing import Protocol

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.request import RequestContext

from kokoro.agent.presentation.v1 import agent_presentation_pb2 as wire
from kokoro.common.v2 import command_envelope_pb2 as common_wire
from kokoro_agent.presentation.integrity import (
    delivery_status_digest,
    producer_fence,
    reject_unknown_fields,
    snapshot_head_digest,
)

AGENT_PRESENTATION_CONTRACT_REVISION = "agent-presentation@v1"


class PresentationProviderStore(Protocol):
    async def check_presentation_provider_active(self) -> None: ...

    async def presentation_provider_fence(
        self, run_id: str
    ) -> wire.PresentationProducerFence: ...

    async def presentation_provider_head(self, run_id: str) -> int: ...

    async def presentation_provider_record(
        self, run_id: str, presentation_seq: int
    ) -> wire.PresentationCandidateRecord | None: ...

    async def pull_presentation_provider_records(
        self,
        run_id: str,
        after_presentation_seq: int,
        through_presentation_seq: int,
        limit: int,
    ) -> tuple[wire.PresentationCandidateRecord, ...]: ...

    async def presentation_provider_status(
        self,
        run_id: str | None,
        original_command: common_wire.CommandIdentityV2 | None,
    ) -> wire.PresentationDeliveryStatus: ...

def _producer_equal(
    left: wire.PresentationProducerFence,
    right: wire.PresentationProducerFence,
) -> bool:
    return left.SerializeToString(deterministic=True) == right.SerializeToString(
        deterministic=True
    )


def _producer_fenced() -> ConnectError:
    return ConnectError(
        Code.FAILED_PRECONDITION,
        "PRESENTATION_PRODUCER_FENCED",
        details=[
            wire.PresentationPermanentErrorDetail(
                kind=wire.PRESENTATION_PERMANENT_ERROR_KIND_PRODUCER_FENCED,
                retryable=False,
                correlation_ref="agent-presentation-provider",
            )
        ],
    )


class AgentPresentationConnectService:
    def __init__(self, store: PresentationProviderStore) -> None:
        self._store = store

    async def check_active(
        self,
        request: wire.CheckActiveRequest,
        ctx: RequestContext[wire.CheckActiveRequest, wire.CheckActiveResponse] | None,
    ) -> wire.CheckActiveResponse:
        del ctx
        try:
            reject_unknown_fields(request)
            await self._store.check_presentation_provider_active()
            return wire.CheckActiveResponse(
                contract_revision=AGENT_PRESENTATION_CONTRACT_REVISION
            )
        except ConnectError:
            raise
        except Exception as error:
            raise ConnectError(
                Code.UNAVAILABLE, "PRESENTATION_PROVIDER_NOT_ACTIVE"
            ) from error

    async def _authorize_producer(
        self, run_id: str, claimed: wire.PresentationProducerFence
    ) -> wire.PresentationProducerFence:
        expected = producer_fence(
            claimed.producer_instance_ref, claimed.producer_generation
        )
        if not _producer_equal(claimed, expected):
            raise _producer_fenced()
        authoritative = await self._store.presentation_provider_fence(run_id)
        if not _producer_equal(claimed, authoritative):
            raise _producer_fenced()
        return authoritative

    async def pull_candidate_batches(
        self,
        request: wire.PullCandidateBatchesRequest,
        ctx: RequestContext[
            wire.PullCandidateBatchesRequest,
            wire.PullCandidateBatchesResponse,
        ] | None,
    ) -> wire.PullCandidateBatchesResponse:
        del ctx
        try:
            reject_unknown_fields(request)
            if (
                not request.run_id
                or request.page_size < 1
                or request.page_size > 128
            ):
                raise ValueError("PRESENTATION_CURSOR_INVALID")
            producer = await self._authorize_producer(request.run_id, request.producer)
            current_head = await self._store.presentation_provider_head(request.run_id)
            frozen = (
                request.snapshot_through_presentation_seq
                if request.HasField("snapshot_through_presentation_seq")
                else current_head
            )
            if request.after_presentation_seq > frozen or frozen > current_head:
                raise ValueError("PRESENTATION_SNAPSHOT_INVALID")
            head = (
                await self._store.presentation_provider_record(request.run_id, frozen)
                if frozen > 0
                else None
            )
            if (frozen > 0) != (head is not None):
                raise ValueError("PRESENTATION_SNAPSHOT_NOT_READY")
            records = await self._store.pull_presentation_provider_records(
                request.run_id,
                request.after_presentation_seq,
                frozen,
                request.page_size,
            )
            page_end = records[-1].presentation_seq if records else request.after_presentation_seq
            has_more = page_end < frozen
            status = await self._store.presentation_provider_status(request.run_id, None)
            if status.run_id != request.run_id:
                raise ValueError("PRESENTATION_STATUS_RUN_INVALID")
            if not _producer_equal(status.producer, producer):
                raise ValueError("PRESENTATION_STATUS_PRODUCER_INVALID")
            if status.status_digest != delivery_status_digest(status):
                raise ValueError("PRESENTATION_STATUS_DIGEST_INVALID")
            response = wire.PullCandidateBatchesResponse(
                run_id=request.run_id,
                producer=producer,
                page_after_presentation_seq=request.after_presentation_seq,
                snapshot_through_presentation_seq=frozen,
                records=records,
                has_more=has_more,
                delivery_status=status,
                snapshot_head_digest=snapshot_head_digest(
                    request.run_id,
                    producer,
                    frozen,
                    head.record_digest if head is not None else None,
                ),
            )
            if has_more:
                response.next_after_presentation_seq = page_end
            if head is not None:
                response.snapshot_head_record_digest = head.record_digest
            return response
        except ConnectError:
            raise
        except ValueError as error:
            raise ConnectError(Code.INVALID_ARGUMENT, str(error)) from error

    async def get_delivery_status(
        self,
        request: wire.GetDeliveryStatusRequest,
        ctx: RequestContext[
            wire.GetDeliveryStatusRequest,
            wire.GetDeliveryStatusResponse,
        ] | None,
    ) -> wire.GetDeliveryStatusResponse:
        del ctx
        try:
            reject_unknown_fields(request)
            lookup = request.WhichOneof("lookup")
            if lookup == "run_id":
                if not request.run_id:
                    raise ValueError("PRESENTATION_STATUS_LOOKUP_INVALID")
                run_id: str | None = request.run_id
                original_command: common_wire.CommandIdentityV2 | None = None
            elif lookup == "original_command":
                if (
                    not request.original_command.command_id
                    or not request.original_command.idempotency_key
                    or not request.original_command.request_digest
                ):
                    raise ValueError("PRESENTATION_STATUS_LOOKUP_INVALID")
                run_id = None
                original_command = request.original_command
            else:
                raise ValueError("PRESENTATION_STATUS_LOOKUP_INVALID")

            status = await self._store.presentation_provider_status(
                run_id, original_command
            )
            authoritative = await self._authorize_producer(
                status.run_id, request.producer
            )
            if not _producer_equal(status.producer, authoritative):
                raise ValueError("PRESENTATION_STATUS_PRODUCER_INVALID")
            if status.status_digest != delivery_status_digest(status):
                raise ValueError("PRESENTATION_STATUS_DIGEST_INVALID")
            return wire.GetDeliveryStatusResponse(status=status)
        except ConnectError:
            raise
        except ValueError as error:
            raise ConnectError(Code.INVALID_ARGUMENT, str(error)) from error


__all__ = [
    "AGENT_PRESENTATION_CONTRACT_REVISION",
    "AgentPresentationConnectService",
    "PresentationProviderStore",
]
