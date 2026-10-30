"""Explicit no-budget fixture for unrelated or mandatory-denial tests, never production."""

from __future__ import annotations

from typing import Literal

from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.domain.run.models import LeasedRun
from kokoro_agent.model.call_models import (
    ModelCallAuthorizationUnavailable,
    PreparedModelCall,
    ReadSummaryResult,
)
from kokoro_agent.model.execution import AuthorizedModelCall
from kokoro_agent.model.settlement import ModelSettlementReceipt
from kokoro_agent.model.summary_result import SummaryReplayReceipt, SummarySealInput
from kokoro_agent.model.usage_observation import PhysicalUsageObservation


class UnavailableModelExecution:
    async def replay_summary(
        self, command: ReadSummaryResult
    ) -> SummaryReplayReceipt | None:
        raise ModelCallAuthorizationUnavailable(
            "model-call summary consumer is unavailable"
        )

    async def authorize(self, prepared: PreparedModelCall) -> AuthorizedModelCall:
        raise ModelCallAuthorizationUnavailable(
            "model-call authorization consumer is unavailable"
        )

    async def seal(
        self,
        *,
        call: AuthorizedModelCall,
        route: ResolvedModel,
        observation: PhysicalUsageObservation,
        outcome: Literal["completed", "failed", "cancelled", "unknown"],
        summary: SummarySealInput | None = None,
    ) -> ModelSettlementReceipt:
        raise AssertionError("unavailable execution cannot have dispatched a model")


def unavailable_model_execution(leased_run: LeasedRun) -> UnavailableModelExecution:
    return UnavailableModelExecution()
