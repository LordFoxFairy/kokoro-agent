"""ConnectRPC adapter for Agent-owned durable evidence reads."""

from __future__ import annotations

from typing import Protocol

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.request import RequestContext
from google.protobuf.timestamp_pb2 import Timestamp

from kokoro.agent.execution.v1 import agent_execution_evidence_pb2 as wire
from kokoro_agent.evidence.models import DurableExecutionEvidence, DurableOutputRecord


class ExecutionEvidenceReader(Protocol):
    async def pull_durable_execution_evidence(
        self, run_id: str, after_durable_seq: int, limit: int
    ) -> list[DurableExecutionEvidence]: ...

    async def get_durable_execution_evidence(
        self, run_id: str, evidence_ref: str
    ) -> DurableExecutionEvidence | None: ...

    async def get_run_durable_checkpoint(
        self, run_id: str
    ) -> DurableExecutionEvidence | None: ...

    async def pull_durable_output_records(
        self, run_id: str, after_output_seq: int, limit: int
    ) -> list[DurableOutputRecord]: ...


_WIRE_KIND = {
    "run.started": wire.DURABLE_EXECUTION_EVIDENCE_KIND_RUN_STARTED,
    "action_owner": wire.DURABLE_EXECUTION_EVIDENCE_KIND_ACTION_OWNER,
    "plan_owner": wire.DURABLE_EXECUTION_EVIDENCE_KIND_PLAN_OWNER,
    "run.owner.completed": wire.DURABLE_EXECUTION_EVIDENCE_KIND_RUN_OWNER_COMPLETED,
    "run.completed": wire.DURABLE_EXECUTION_EVIDENCE_KIND_RUN_COMPLETED,
    "run.failed": wire.DURABLE_EXECUTION_EVIDENCE_KIND_RUN_FAILED,
}


def _invalid() -> ConnectError:
    return ConnectError(Code.INVALID_ARGUMENT, "request invalid")


def _run_id(value: str) -> str:
    if not value or len(value) > 128 or value.strip() != value:
        raise _invalid()
    return value


def _to_wire(record: DurableExecutionEvidence) -> wire.DurableExecutionEvidence:
    recorded_at = Timestamp()
    recorded_at.FromMilliseconds(record.recorded_at_ms)
    return wire.DurableExecutionEvidence(
        evidence_ref=record.evidence_ref,
        evidence_version=record.evidence_version,
        run_id=record.run_id,
        durable_seq=record.durable_seq,
        event_id=record.event_id,
        kind=_WIRE_KIND[record.kind],
        canonical_payload=record.canonical_payload,
        payload_sha256=record.payload_sha256,
        recorded_at=recorded_at,
        producer_instance_ref=record.producer_instance_ref,
        producer_generation=record.producer_generation,
    )


def _output_to_wire(record: DurableOutputRecord) -> wire.DurableOutputRecord:
    recorded_at = Timestamp()
    recorded_at.FromMilliseconds(record.recorded_at_ms)
    return wire.DurableOutputRecord(
        output_ref=record.output_ref,
        output_version=record.output_version,
        run_id=record.run_id,
        output_seq=record.output_seq,
        canonical_payload=record.canonical_payload,
        payload_sha256=record.payload_sha256,
        recorded_at=recorded_at,
        producer_instance_ref=record.producer_instance_ref,
        producer_generation=record.producer_generation,
    )


class AgentExecutionEvidenceConnectService:
    def __init__(self, reader: ExecutionEvidenceReader) -> None:
        self._reader = reader

    async def pull_durable_execution_evidence(
        self,
        request: wire.PullDurableExecutionEvidenceRequest,
        ctx: RequestContext[
            wire.PullDurableExecutionEvidenceRequest,
            wire.PullDurableExecutionEvidenceResponse,
        ],
    ) -> wire.PullDurableExecutionEvidenceResponse:
        run_id = _run_id(request.run_id)
        if request.page_size < 1 or request.page_size > 256:
            raise _invalid()
        records = await self._reader.pull_durable_execution_evidence(
            run_id, request.after_durable_seq, request.page_size + 1
        )
        page = records[: request.page_size]
        response = wire.PullDurableExecutionEvidenceResponse(
            evidence=[_to_wire(record) for record in page],
            has_more=len(records) > request.page_size,
        )
        if page:
            response.next_after_durable_seq = page[-1].durable_seq
        return response

    async def get_durable_execution_evidence(
        self,
        request: wire.GetDurableExecutionEvidenceRequest,
        ctx: RequestContext[
            wire.GetDurableExecutionEvidenceRequest,
            wire.GetDurableExecutionEvidenceResponse,
        ],
    ) -> wire.GetDurableExecutionEvidenceResponse:
        run_id = _run_id(request.run_id)
        if (
            not request.evidence_ref
            or len(request.evidence_ref) > 256
            or request.evidence_ref.strip() != request.evidence_ref
        ):
            raise _invalid()
        record = await self._reader.get_durable_execution_evidence(
            run_id, request.evidence_ref
        )
        if record is None:
            return wire.GetDurableExecutionEvidenceResponse(
                not_found=wire.DurableExecutionEvidenceNotFound()
            )
        return wire.GetDurableExecutionEvidenceResponse(evidence=_to_wire(record))

    async def get_run_durable_checkpoint(
        self,
        request: wire.GetRunDurableCheckpointRequest,
        ctx: RequestContext[
            wire.GetRunDurableCheckpointRequest,
            wire.GetRunDurableCheckpointResponse,
        ],
    ) -> wire.GetRunDurableCheckpointResponse:
        record = await self._reader.get_run_durable_checkpoint(_run_id(request.run_id))
        if record is None:
            return wire.GetRunDurableCheckpointResponse(
                not_found=wire.DurableExecutionEvidenceNotFound()
            )
        return wire.GetRunDurableCheckpointResponse(evidence=_to_wire(record))

    async def pull_durable_output_records(
        self,
        request: wire.PullDurableOutputRecordsRequest,
        ctx: RequestContext[
            wire.PullDurableOutputRecordsRequest,
            wire.PullDurableOutputRecordsResponse,
        ],
    ) -> wire.PullDurableOutputRecordsResponse:
        run_id = _run_id(request.run_id)
        if request.page_size < 1 or request.page_size > 64:
            raise _invalid()
        records = await self._reader.pull_durable_output_records(
            run_id, request.after_output_seq, request.page_size + 1
        )
        page = records[: request.page_size]
        response = wire.PullDurableOutputRecordsResponse(
            records=[_output_to_wire(record) for record in page],
            has_more=len(records) > request.page_size,
        )
        if page:
            response.next_after_output_seq = page[-1].output_seq
        return response
