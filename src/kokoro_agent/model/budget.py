"""Attempt-scoped budget facts; no price, provider dispatch, or wallet ownership."""

from __future__ import annotations
import asyncio
import hashlib
import json
import math
from uuid import uuid4
from dataclasses import dataclass
from typing import Protocol
from pydantic import SecretStr
from kokoro_agent.domain.run.models import LeaseFence, LeasedRun
from kokoro_agent.protocol import RunRequest
from kokoro_agent.model.call_artifacts import (
    encode_requested_artifact,
    strict_granted_artifact,
)
from kokoro_agent.model.call_models import (
    ModelCallRepository,
    PreparedModelCall,
    ModelCallAuthorizationReceipt,
    ModelCallInputInvalid,
    ModelCallIdentityConflict,
    BeginModelCallAuthorization,
    GrantModelCallAuthorization,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelBudgetCommand:
    run_id: str
    attempt_id: str
    request_ref: str
    attempt_binding_digest: str
    planned_digest: str
    final_request_digest: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelBudgetGrant:
    admission_ref: str


class ModelBudgetClient(Protocol):
    async def reserve(
        self,
        *,
        tenant_id: str,
        command: ModelBudgetCommand,
        proof: SecretStr,
        key: str,
        request_id: str,
    ) -> ModelBudgetGrant: ...


class BudgetProofs(Protocol):
    async def issue(self, operation: str, request_binding_sha256: str) -> str: ...


def model_budget_request_ref(attempt_id: str) -> str:
    """One stable operation identity; never derive it from refresh time or proof jti."""
    return "agent-model-admit:" + attempt_id


def model_budget_request_digest(
    tenant_id: str, payer_user_id: str, command: ModelBudgetCommand
) -> str:
    """The fixed Billing model-admit:1 UTF-8 JSON tuple, not JCS or a proof hash."""
    raw = json.dumps(
        [
            "kokoro.billing.model-admit:1",
            tenant_id,
            payer_user_id,
            command.run_id,
            command.attempt_id,
            command.request_ref,
            command.attempt_binding_digest,
            command.planned_digest,
            command.final_request_digest,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8", errors="strict")
    return hashlib.sha256(raw).hexdigest()


class ModelBudgetAuthorizer:
    """Bind a permanent reservation to the existing attempt journal, without dispatch."""

    def __init__(
        self,
        *,
        repository: ModelCallRepository,
        budget: ModelBudgetClient,
        proofs: BudgetProofs,
        leased_run: LeasedRun,
        timeout_s: float = 10,
    ) -> None:
        if (
            type(timeout_s) not in {int, float}
            or not math.isfinite(timeout_s)
            or not 0 < timeout_s <= 10
        ):
            raise ValueError("invalid model budget deadline")
        request = RunRequest.model_validate(
            leased_run.request.model_dump(mode="python")
        )
        identity = request.execution_identity
        if (
            identity.actor.kind != "user"
            or identity.subject.kind != "user"
            or identity.actor.opaque_ref != identity.subject.opaque_ref
        ):
            raise ModelCallInputInvalid(
                "model budget requires the submitted direct user"
            )
        self._request = request
        self._lease = LeaseFence(
            owner=leased_run.lease.owner, generation=leased_run.lease.generation
        )
        self._repository = repository
        self._budget = budget
        self._proofs = proofs
        self._timeout_s = timeout_s

    async def authorize(
        self, prepared: PreparedModelCall
    ) -> ModelCallAuthorizationReceipt:
        prepared = PreparedModelCall.model_validate(prepared.model_dump(mode="python"))
        if (
            prepared.tenant_id != self._request.execution_identity.tenant_ref
            or prepared.run_id != self._request.run_id
            or prepared.lease != self._lease
        ):
            raise ModelCallInputInvalid("prepared budget scope differs from Run")
        request_ref = model_budget_request_ref(prepared.attempt_id)
        requested_bytes, requested_digest = encode_requested_artifact(request_ref)
        command = ModelBudgetCommand(
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            request_ref=request_ref,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            final_request_digest=prepared.final_request_digest,
        )
        base = dict(
            tenant_id=prepared.tenant_id,
            run_id=prepared.run_id,
            attempt_id=prepared.attempt_id,
            lease=self._lease,
            attempt_binding_digest=prepared.attempt_binding_digest,
            planned_digest=prepared.planned_digest,
            request_ref=request_ref,
        )
        task = asyncio.current_task()
        async with asyncio.timeout(self._timeout_s) as timeout:

            def check() -> None:
                if task is None or task.cancelling() or timeout.expired():
                    raise asyncio.CancelledError

            check()
            pending = await self._repository.begin_authorization(
                BeginModelCallAuthorization.model_validate(base)
            )
            check()
            if (
                pending.tenant_id,
                pending.run_id,
                pending.attempt_id,
                pending.request_ref,
            ) != (
                prepared.tenant_id,
                prepared.run_id,
                prepared.attempt_id,
                request_ref,
            ):
                raise ModelCallIdentityConflict("budget observation scope conflicts")
            if (
                pending.authorization_bytes == requested_bytes
                and pending.authorization_digest == requested_digest
            ):
                digest = model_budget_request_digest(
                    prepared.tenant_id,
                    self._request.execution_identity.subject.opaque_ref,
                    command,
                )
                proof = await self._proofs.issue("billing.credit.consume", digest)
                check()
                grant = await self._budget.reserve(
                    tenant_id=prepared.tenant_id,
                    command=command,
                    proof=SecretStr(proof),
                    key=request_ref,
                    request_id=str(uuid4()),
                )
                check()
                admission_ref = grant.admission_ref
            else:
                granted, digest = strict_granted_artifact(pending.authorization_bytes)
                if (
                    digest != pending.authorization_digest
                    or granted.request_ref != request_ref
                ):
                    raise ModelCallIdentityConflict(
                        "budget granted observation conflicts"
                    )
                admission_ref = granted.admission_ref
            receipt = await self._repository.grant_authorization(
                GrantModelCallAuthorization.model_validate(
                    {
                        **base,
                        "expected_authorization_digest": requested_digest,
                        "admission_ref": admission_ref,
                    }
                )
            )
            check()
            return receipt
