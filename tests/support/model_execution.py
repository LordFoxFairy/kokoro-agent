"""Explicit no-budget fixture for unrelated or mandatory-denial tests, never production."""

from __future__ import annotations
from typing import Literal

from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.domain.run.models import LeasedRun
from kokoro_agent.model.execution import AuthorizedModelCall
from kokoro_agent.model.call_models import (
    PreparedModelCall,
    ModelCallAuthorizationUnavailable,
)
from kokoro_agent.model.usage_observation import PhysicalUsageObservation
from kokoro_agent.model.settlement import ModelSettlementReceipt


class UnavailableModelExecution:
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
    ) -> ModelSettlementReceipt:
        raise AssertionError("unavailable execution cannot have dispatched a model")


def unavailable_model_execution(leased_run: LeasedRun) -> UnavailableModelExecution:
    return UnavailableModelExecution()
