"""Native per-model-call gate backed by the durable Agent attempt journal."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID, uuid5

import rfc8785
from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse

from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.model.call_models import (
    ModelCallAuthorizationUnavailable,
    ModelCallInputInvalid,
    ModelCallRepository,
    PrepareModelCall,
)
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_generated import PlannedBinding

_CALL_NAMESPACE = UUID("a06d00e2-defa-5fe3-8f2c-10ecb19c1161")
_ATTEMPT_NAMESPACE = UUID("64675e75-8af6-50b4-ade3-d413e5144dc9")


class ModelCallRuntimeMiddleware(AgentMiddleware):
    """Prepare one stable attempt and fail closed while Billing is unpublished."""

    def __init__(
        self,
        *,
        repository: ModelCallRepository,
        request: RunRequest,
        lease: LeaseFence,
        planned: PlannedBinding,
        recipe: StaticRecipeBinding,
        peer_path: tuple[str, ...],
    ) -> None:
        super().__init__()
        self._repository = repository
        self._request = request
        self._lease = lease
        self._planned = planned
        self._recipe = recipe
        self._peer_path = peer_path

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        del handler
        call_id, attempt_id = _logical_identity(request, self._request, self._peer_path)
        await self._repository.prepare(
            PrepareModelCall(
                request=self._request,
                lease=self._lease,
                call_id=call_id,
                attempt_id=attempt_id,
                planned=self._planned,
                peer_path=self._peer_path,
                recipe=self._recipe,
            )
        )
        raise ModelCallAuthorizationUnavailable(
            "model-call authorization consumer is unavailable"
        )


def _logical_identity(
    model_request: ModelRequest,
    run_request: RunRequest,
    peer_path: tuple[str, ...],
) -> tuple[str, str]:
    execution = model_request.runtime.execution_info
    if execution is None:
        raise ModelCallInputInvalid("model-call execution identity is unavailable")
    checkpoint_id = execution.checkpoint_id
    checkpoint_ns = execution.checkpoint_ns
    task_id = execution.task_id
    if (
        type(checkpoint_id) is not str
        or not checkpoint_id
        or type(checkpoint_ns) is not str
        or type(task_id) is not str
        or not task_id
    ):
        raise ModelCallInputInvalid("model-call execution identity is invalid")
    locator = {
        "version": 1,
        "tenant_ref": run_request.execution_identity.tenant_ref,
        "run_id": run_request.run_id,
        "peer_path": list(peer_path),
        "checkpoint_id": checkpoint_id,
        "checkpoint_ns": checkpoint_ns,
        "task_id": task_id,
    }
    try:
        name = rfc8785.dumps(locator).decode("utf-8")
    except (TypeError, ValueError, UnicodeError) as error:
        raise ModelCallInputInvalid(
            "model-call execution identity is invalid"
        ) from error
    call_id = str(uuid5(_CALL_NAMESPACE, name))
    return call_id, str(uuid5(_ATTEMPT_NAMESPACE, f"{call_id}:1"))


__all__ = ["ModelCallRuntimeMiddleware"]
