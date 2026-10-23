"""Worker-owned BFF pool and fair recovery; never part of execution heartbeats."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
import json
import logging
import math
import re
from typing import Protocol

from pydantic import SecretStr, TypeAdapter

from kokoro_agent.clients.model_budget import BffModelBudgetClient, ModelBudgetError

from dataclasses import dataclass
from kokoro_agent.domain.run.models import LeasedRun
from kokoro_agent.model.budget import ModelBudgetAuthorizer
from kokoro_agent.model.execution import ModelCallExecutor
from kokoro_agent.model.call_models import ModelCallRepository
from kokoro_agent.model.financial_reconciliation import ModelFinancialReconciler
from kokoro_agent.execution.execution_proof_signer import ExecutionProofSigner
from kokoro_agent.execution.execution_proof_supplier import (
    ExecutionProofLeaseReadPort,
    create_execution_proof_supplier,
)

LOGGER = logging.getLogger(__name__)
_SECRETS = TypeAdapter(dict[str, str])


def _members(pairs: list[tuple[str, object]]) -> dict[str, object]:
    values: dict[str, object] = {}
    for key, value in pairs:
        if key in values:
            raise ValueError
        values[key] = value
    return values


class ModelAdmissionSecrets:
    """An immutable env-only workload snapshot, not a user or model credential store."""

    def __init__(self, value: SecretStr) -> None:
        parsed: dict[str, str] | None = None
        try:
            raw = value.get_secret_value().encode("utf-8", errors="strict")
            if not 1 <= len(raw) <= 65536:
                raise ValueError
            values = _SECRETS.validate_python(
                json.loads(raw, object_pairs_hook=_members), strict=True
            )
            if not 1 <= len(values) <= 32 or len(set(values.values())) != len(values):
                raise ValueError
            for tenant, secret in values.items():
                if not re.fullmatch(
                    r"[^\s\x00-\x1f\x7f]{1,191}", tenant
                ) or not re.fullmatch(r"[\x21-\x7e]{32,4096}", secret):
                    raise ValueError
            parsed = values
        except (ValueError, TypeError, RecursionError):
            pass
        if parsed is None:
            raise ValueError("BFF model billing credentials unavailable")
        self._values = {tenant: SecretStr(secret) for tenant, secret in parsed.items()}

    def __repr__(self) -> str:
        return "ModelAdmissionSecrets(<redacted>)"

    def __call__(self, tenant_id: str) -> SecretStr:
        value = self._values.get(tenant_id)
        if value is None:
            raise ModelBudgetError("MODEL_BUDGET_CREDENTIAL_UNAVAILABLE")
        return value


@asynccontextmanager
async def worker_model_billing(
    *,
    base_url: str | None,
    secret_snapshot: SecretStr | None,
) -> AsyncGenerator[BffModelBudgetClient, None]:
    if base_url is None or secret_snapshot is None:
        raise ValueError("BFF model billing configuration is required")
    credentials = ModelAdmissionSecrets(secret_snapshot)
    async with BffModelBudgetClient(base_url, credentials) as client:
        yield client


class PendingFinancialCalls(Protocol):
    async def list_pending(
        self, *, limit: int, after: tuple[str, str] | None = None
    ) -> list[tuple[str, str]]: ...


class FinancialReconciliation(Protocol):
    async def reconcile(self, tenant_id: str, attempt_id: str) -> object: ...


class ModelFinancialRecovery:
    """One bounded page per tick; a stuck original event cannot starve later attempts."""

    def __init__(
        self,
        repository: PendingFinancialCalls,
        reconciler: FinancialReconciliation,
        *,
        batch_size: int = 16,
        interval_s: float = 1,
    ) -> None:
        if (
            type(batch_size) is not int
            or not 1 <= batch_size <= 32
            or type(interval_s) not in {int, float}
            or not math.isfinite(interval_s)
            or not 0 < interval_s <= 60
        ):
            raise ValueError("invalid model financial recovery bounds")
        self._repository = repository
        self._reconciler = reconciler
        self._batch_size = batch_size
        self._interval_s = interval_s
        self._after: tuple[str, str] | None = None

    async def step(self) -> None:
        rows = await self._repository.list_pending(
            limit=self._batch_size, after=self._after
        )
        self._after = rows[-1] if rows else None
        for tenant, attempt in rows:
            try:
                await self._reconciler.reconcile(tenant, attempt)
            except Exception as error:
                # Keep durable work unchanged. Do not log exception text or owner payloads.
                LOGGER.warning(
                    "model financial reconciliation deferred",
                    extra={"failure_type": type(error).__name__},
                )

    async def run(self) -> None:
        while True:
            try:
                await self.step()
            except Exception as error:
                LOGGER.warning(
                    "model financial scan deferred",
                    extra={"failure_type": type(error).__name__},
                )
            await asyncio.sleep(self._interval_s)


@asynccontextmanager
async def active_model_financial_recovery(
    recovery: ModelFinancialRecovery,
) -> AsyncGenerator[None, None]:
    task = asyncio.create_task(recovery.run(), name="model-financial-recovery")
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class WorkerModelExecutionFactory:
    repository: ModelCallRepository
    budget: BffModelBudgetClient
    financial: ModelFinancialReconciler
    signer: ExecutionProofSigner
    lease_reader: ExecutionProofLeaseReadPort

    def __call__(self, leased_run: LeasedRun) -> ModelCallExecutor:
        proofs = create_execution_proof_supplier(
            leased_run=leased_run, signer=self.signer, lease_reader=self.lease_reader
        )
        return ModelCallExecutor(
            repository=self.repository,
            authorizer=ModelBudgetAuthorizer(
                repository=self.repository,
                budget=self.budget,
                proofs=proofs,
                leased_run=leased_run,
            ),
            financial=self.financial,
        )
