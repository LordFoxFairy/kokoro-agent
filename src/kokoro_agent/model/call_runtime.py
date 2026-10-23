"""Native per-model-call gate backed by the durable Agent attempt journal."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass

from kokoro_agent.clients.system import ResolvedModel
from kokoro_agent.model.call_artifacts import (
    encode_final_request_digest,
    encode_planned_artifact,
)
from kokoro_agent.model.request_profile import validate_request_profile
from uuid import UUID, uuid5
from typing import Never

import rfc8785
from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse

from kokoro_agent.domain.run.models import LeaseFence, StaticRecipeBinding
from kokoro_agent.model.call_models import (
    ModelCallAuthorizationUnavailable,
    ModelCallInputInvalid,
    ModelCallRepository,
    PrepareModelCall,
)
from kokoro_agent.model.call_scope import register_current_model_call_task
from kokoro_agent.model.execution import ModelCallExecution, AuthorizedModelCall
from kokoro_agent.model.usage_observation import capture_model_usage
from kokoro_agent.protocol import RunRequest
from kokoro_agent.protocol.model_usage_v2_generated import PlannedBinding

_CALL_NAMESPACE = UUID("a06d00e2-defa-5fe3-8f2c-10ecb19c1161")
_ATTEMPT_NAMESPACE = UUID("64675e75-8af6-50b4-ade3-d413e5144dc9")


@dataclass(slots=True)
class _FinalRequestScope:
    owner: asyncio.Task[object]
    execution: ModelCallExecution
    repository: ModelCallRepository
    request: RunRequest
    lease: LeaseFence
    call_id: str
    attempt_id: str
    planned: PlannedBinding
    recipe: StaticRecipeBinding
    peer_path: tuple[str, ...]
    call: AuthorizedModelCall | None = None
    claimant: asyncio.Task[object] | None = None
    closed: bool = False
    denial: Exception | None = None

    def require_active(self) -> None:
        if self.owner.cancelling():
            raise asyncio.CancelledError
        if self.closed or self.owner.done():
            if self.denial is not None:
                raise self.denial
            raise ModelCallAuthorizationUnavailable("model-call final scope is closed")

    def close(self) -> None:
        self.closed = True
        child = self.claimant
        if child is not None and child is not self.owner and not child.done():
            child.cancel()


_FINAL_SCOPE: ContextVar[_FinalRequestScope | None] = ContextVar(
    "kokoro_agent_final_request_scope", default=None
)


def reject_final_model_request(
    error: ModelCallInputInvalid | ModelCallAuthorizationUnavailable,
) -> Never:
    """Preserve an owned pre-body failure across the locked SDK wrapper."""
    scope = _FINAL_SCOPE.get()
    if scope is not None:
        scope.require_active()
        if scope.denial is None:
            scope.denial = error
    raise error


def _route_matches(planned: PlannedBinding, route: ResolvedModel) -> bool:
    return (
        planned.model == route.model_id
        and planned.provider == route.provider_id
        and planned.revision_id == route.revision_id
        and planned.revision == str(route.revision)
        and planned.digest == route.digest
        and planned.generation == route.generation
        and planned.tenant_generation == route.tenant_generation
        and planned.provider_model_name == route.provider_model_name
        and planned.request_model == route.request_model
        and planned.feature_key == route.feature_key
        and planned.label == route.label_key
    )


async def freeze_final_model_request(
    *,
    route: ResolvedModel,
    raw: bytes,
    method: str,
    endpoint: str,
    expected_endpoint: str,
    headers: Sequence[tuple[str, str]],
) -> None:
    """Unique physical claimant; return only after one durable authorized dispatch."""
    scope = _FINAL_SCOPE.get()
    try:
        if scope is not None:
            scope.require_active()
            if (
                scope.claimant is not None
                or scope.denial is not None
                or not _route_matches(scope.planned, route)
            ):
                scope.close()
                raise ModelCallInputInvalid(
                    "model-call physical request claim conflicts"
                )
            task = asyncio.current_task()
            if task is None:
                raise ModelCallInputInvalid("model-call request task is unavailable")
            scope.claimant = task
            register_current_model_call_task()
            scope.require_active()
        if any(
            name.lower() == "x-stainless-async" and value != "async:asyncio"
            for name, value in headers
        ):
            raise ModelCallInputInvalid("model request async profile is invalid")
        validate_request_profile(
            raw=raw,
            method=method,
            endpoint=endpoint,
            expected_endpoint=expected_endpoint,
            headers=headers,
            model_name=route.request_model,
            max_output_tokens=route.max_output_tokens,
        )
        if scope is None:
            raise ModelCallAuthorizationUnavailable(
                "model-call final scope is unavailable"
            )
        _, planned_digest = encode_planned_artifact(scope.planned)
        digest = encode_final_request_digest(
            endpoint=endpoint, planned_digest=planned_digest, raw=raw
        )
        # No await separates the claim/active check from entry to real preparation.
        prepared = await scope.repository.prepare(
            PrepareModelCall(
                request=scope.request,
                lease=scope.lease,
                call_id=scope.call_id,
                attempt_id=scope.attempt_id,
                planned=scope.planned,
                peer_path=scope.peer_path,
                recipe=scope.recipe,
                final_request_binding_version=1,
                final_request_digest=digest,
            )
        )
        scope.require_active()
        scope.call = await scope.execution.authorize(prepared)
        scope.require_active()
    except Exception as error:
        if scope is not None and scope.denial is None:
            scope.denial = error
        raise


class ModelCallRuntimeMiddleware(AgentMiddleware):
    """Own one SDK call, its unique dispatch and same-stream durable financial evidence."""

    def __init__(
        self,
        *,
        repository: ModelCallRepository,
        execution: ModelCallExecution,
        route: ResolvedModel,
        request: RunRequest,
        lease: LeaseFence,
        planned: PlannedBinding,
        recipe: StaticRecipeBinding,
        peer_path: tuple[str, ...],
    ) -> None:
        super().__init__()
        self._repository = repository
        self._execution = execution
        self._route = route
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
        register_current_model_call_task()
        owner = asyncio.current_task()
        if owner is None:
            raise ModelCallInputInvalid("model-call lifecycle owner is unavailable")
        call_id, attempt_id = _logical_identity(request, self._request, self._peer_path)
        scope = _FinalRequestScope(
            owner=owner,
            repository=self._repository,
            execution=self._execution,
            request=self._request,
            lease=self._lease,
            call_id=call_id,
            attempt_id=attempt_id,
            planned=self._planned,
            recipe=self._recipe,
            peer_path=self._peer_path,
        )
        token = _FINAL_SCOPE.set(scope)
        primary: BaseException | None = None
        response: ModelResponse | None = None
        capture = capture_model_usage(self._route)
        try:
            try:
                with capture:
                    response = await handler(request)
                    scope.require_active()
                    if scope.denial is not None:
                        raise scope.denial
                    if scope.call is None:
                        raise ModelCallAuthorizationUnavailable(
                            "model-call final request is unavailable"
                        )
                    capture.mark_sdk_consumption_complete()
            except BaseException as error:
                primary = error
            if scope.call is not None:
                try:
                    await scope.execution.seal(
                        call=scope.call,
                        route=self._route,
                        observation=capture.snapshot(),
                        outcome="completed"
                        if primary is None
                        else "cancelled"
                        if isinstance(primary, asyncio.CancelledError)
                        else "failed",
                    )
                except BaseException as error:
                    if primary is None:
                        primary = error
                    else:
                        primary.add_note(
                            "model evidence finalization also failed: "
                            + type(error).__name__
                        )
            if owner.cancelling():
                raise asyncio.CancelledError
            if primary is not None:
                denial = scope.denial
                if denial is not None and (
                    primary is denial or primary.__cause__ is denial
                ):
                    raise denial from None
                raise primary
            if response is None:
                raise ModelCallAuthorizationUnavailable(
                    "model-call SDK response is unavailable"
                )
            return response
        finally:
            scope.close()
            _FINAL_SCOPE.reset(token)


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
