"""One final request's authorization, unique dispatch and durable usage delivery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.model.call_models import (
    ModelCallAuthorizationReceipt,
    ModelCallAuthorizationUnavailable,
    ModelCallDispatchPermit,
    ModelCallRepository,
    ModelCallSettlementPending,
    PreparedModelCall,
    ReadSummaryResult,
    SealModelCallObservation,
    StartModelCallDispatch,
)
from kokoro_agent.model.settlement import ModelSettlementReceipt
from kokoro_agent.model.summary_result import SummaryReplayReceipt, SummarySealInput
from kokoro_agent.model.usage_observation import PhysicalUsageObservation
from kokoro_agent.model.usage_producer import (
    actual_from_response,
    normalize_observed_usage,
)


class CallAuthorizer(Protocol):
    async def authorize(
        self, prepared: PreparedModelCall
    ) -> ModelCallAuthorizationReceipt: ...


class CallFinancialDelivery(Protocol):
    async def reconcile(
        self, tenant_id: str, attempt_id: str
    ) -> ModelSettlementReceipt | None: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class AuthorizedModelCall:
    prepared: PreparedModelCall
    authorization: ModelCallAuthorizationReceipt
    dispatch: ModelCallDispatchPermit


class ModelCallExecution(Protocol):
    async def replay_summary(
        self, command: ReadSummaryResult
    ) -> SummaryReplayReceipt | None: ...
    async def authorize(self, prepared: PreparedModelCall) -> AuthorizedModelCall: ...
    async def seal(
        self,
        *,
        call: AuthorizedModelCall,
        route: ResolvedModel,
        observation: PhysicalUsageObservation,
        outcome: Literal["completed", "failed", "cancelled", "unknown"],
        summary: SummarySealInput | None = None,
    ) -> ModelSettlementReceipt: ...


class ModelCallExecutor:
    def __init__(
        self,
        *,
        repository: ModelCallRepository,
        authorizer: CallAuthorizer,
        financial: CallFinancialDelivery,
    ) -> None:
        self._repository = repository
        self._authorizer = authorizer
        self._financial = financial

    async def replay_summary(
        self, command: ReadSummaryResult
    ) -> SummaryReplayReceipt | None:
        stored = await self._repository.read_summary_result(command)
        if stored is None:
            return None
        if stored.state == "pending":
            await self._financial.reconcile(
                command.binding.tenant_ref, command.binding.attempt_id
            )
            stored = await self._repository.read_summary_result(command)
            if stored is None:
                raise ModelCallAuthorizationUnavailable(
                    "native summary result disappeared"
                )
        if stored.state != "captured":
            raise ModelCallSettlementPending(
                "native summary financial result requires reconciliation"
            )
        return stored

    async def authorize(self, prepared: PreparedModelCall) -> AuthorizedModelCall:
        grant = await self._authorizer.authorize(prepared)
        if (grant.tenant_id, grant.run_id, grant.attempt_id) != (
            prepared.tenant_id,
            prepared.run_id,
            prepared.attempt_id,
        ):
            raise ModelCallAuthorizationUnavailable("model-call budget scope conflicts")
        dispatch = await self._repository.start_dispatch(
            StartModelCallDispatch(
                tenant_id=prepared.tenant_id,
                run_id=prepared.run_id,
                attempt_id=prepared.attempt_id,
                lease=prepared.lease,
                attempt_binding_digest=prepared.attempt_binding_digest,
                planned_digest=prepared.planned_digest,
                authorization_digest=grant.authorization_digest,
                request_ref=grant.request_ref,
                admission_ref=grant.admission_ref,
            )
        )
        if not dispatch.newly_started:
            raise ModelCallAuthorizationUnavailable("model-call was already dispatched")
        return AuthorizedModelCall(
            prepared=prepared, authorization=grant, dispatch=dispatch
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
        p = call.prepared
        sealed = await self._repository.seal_observation(
            SealModelCallObservation(
                tenant_id=p.tenant_id,
                run_id=p.run_id,
                attempt_id=p.attempt_id,
                lease=p.lease,
                attempt_binding_digest=p.attempt_binding_digest,
                planned_digest=p.planned_digest,
                authorization_digest=call.authorization.authorization_digest,
                outcome=outcome,
                summary=summary,
                actual=actual_from_response(
                    route=route, prepared=p, observation=observation
                ),
                usage=normalize_observed_usage(observation),
                provider_response_id=observation.provider_response_id
                if observation.status == "complete"
                and observation.compression_integrity == "validated"
                else None,
            )
        )
        receipt = await self._financial.reconcile(p.tenant_id, p.attempt_id)
        if receipt is None or (
            receipt.admission_ref,
            receipt.run_id,
            receipt.attempt_id,
            receipt.event_id,
            receipt.revision,
            receipt.digest,
        ) != (
            call.authorization.admission_ref,
            p.run_id,
            p.attempt_id,
            sealed.event_id,
            str(sealed.revision),
            sealed.evidence_digest,
        ):
            raise ModelCallAuthorizationUnavailable(
                "model-call financial result requires reconciliation"
            )
        if receipt.status == "pending_reconciliation":
            raise ModelCallSettlementPending(
                "model-call financial result requires reconciliation"
            )
        if receipt.status != "captured":
            raise ModelCallAuthorizationUnavailable(
                "model-call financial result requires reconciliation"
            )
        return receipt
