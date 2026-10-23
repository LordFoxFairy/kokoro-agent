"""Recover original financial delivery from owner facts, independently of execution leases."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import math
from typing import Protocol
from uuid import uuid4

from kokoro_agent.model.budget import ModelBudgetCommand, ModelBudgetGrant
from kokoro_agent.model.settlement import ModelSettlementCommand, ModelSettlementReceipt


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelFinancialWork:
    tenant_id: str
    budget: ModelBudgetCommand
    evidence: ModelSettlementCommand
    admission_ref: str | None


class ModelFinancialRepository(Protocol):
    async def next_work(
        self, tenant_id: str, attempt_id: str
    ) -> ModelFinancialWork | None: ...
    async def latest_ack(
        self, tenant_id: str, attempt_id: str
    ) -> ModelSettlementReceipt | None: ...
    async def record_admission(
        self, work: ModelFinancialWork, admission_ref: str
    ) -> None: ...
    async def record_ack(
        self, work: ModelFinancialWork, value: ModelSettlementReceipt
    ) -> ModelSettlementReceipt: ...


class ModelFinancialClient(Protocol):
    async def recover(
        self, *, tenant_id: str, command: ModelBudgetCommand, key: str, request_id: str
    ) -> ModelBudgetGrant | None: ...
    async def settle(
        self,
        *,
        tenant_id: str,
        admission_ref: str,
        command: ModelSettlementCommand,
        key: str,
        request_id: str,
    ) -> ModelSettlementReceipt: ...


class ModelFinancialReconciler:
    def __init__(
        self,
        *,
        repository: ModelFinancialRepository,
        client: ModelFinancialClient,
        timeout_s: float = 20,
    ) -> None:
        if (
            type(timeout_s) not in {int, float}
            or not math.isfinite(timeout_s)
            or not 0 < timeout_s <= 30
        ):
            raise ValueError("invalid financial reconciliation bound")
        self._repository = repository
        self._client = client
        self._timeout_s = timeout_s

    async def reconcile(
        self, tenant_id: str, attempt_id: str
    ) -> ModelSettlementReceipt | None:
        async with asyncio.timeout(self._timeout_s):
            work = await self._repository.next_work(tenant_id, attempt_id)
            if work is None:
                return await self._repository.latest_ack(tenant_id, attempt_id)
            admission = work.admission_ref
            if admission is None:
                recovered = await self._client.recover(
                    tenant_id=work.tenant_id,
                    command=work.budget,
                    key=work.budget.request_ref,
                    request_id=str(uuid4()),
                )
                if recovered is None:
                    return None  # A point-in-time miss never proves an in-flight reserve was cancelled.
                admission = recovered.admission_ref
            await self._repository.record_admission(work, admission)
            value = await self._client.settle(
                tenant_id=work.tenant_id,
                admission_ref=admission,
                command=work.evidence,
                key="agent-model-settle:" + work.evidence.event_id,
                request_id=str(uuid4()),
            )
            return await self._repository.record_ack(work, value)
