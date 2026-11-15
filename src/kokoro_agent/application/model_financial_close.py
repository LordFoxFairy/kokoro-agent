"""One explicit operator target, with local checks before and after owner I/O."""

from __future__ import annotations

from collections.abc import Mapping
from uuid import uuid4
import os

from kokoro_agent.config import AppConfig
from kokoro_agent.clients.model_budget import (
    BffModelBudgetClient,
    ModelAdmissionSecrets,
)
from kokoro_agent.infrastructure.postgres_model_financial_repository import (
    PostgresModelFinancialRepository,
)
from kokoro_agent.model.financial_close import (
    ModelFinancialCloseCommand,
    ModelFinancialCloseReceipt,
)


async def close_unreserved(
    environment: Mapping[str, str] | None, command: ModelFinancialCloseCommand
) -> ModelFinancialCloseReceipt:
    config = AppConfig.from_env(os.environ if environment is None else environment)
    if config.bff_base_url is None or config.bff_model_admission_secrets is None:
        raise ValueError("BFF model billing configuration is required")
    credentials = ModelAdmissionSecrets(config.bff_model_admission_secrets)
    repository = PostgresModelFinancialRepository(
        config.database_url, config.database_schema
    )
    work = await repository.prepare_close(command)
    if work.receipt is not None:
        return work.receipt
    async with BffModelBudgetClient(config.bff_base_url, credentials) as client:
        receipt = await client.close_unreserved(
            tenant_id=command.tenant_id,
            command=work,
            key=work.budget.request_ref,
            request_id=str(uuid4()),
        )
    return await repository.record_close(work, receipt)
