"""Classify execution failures and construct the single safe Run failure payload."""

from __future__ import annotations

from langgraph.errors import GraphRecursionError
from openai import APIConnectionError, APIError, APITimeoutError

from kokoro_agent.model.personal_context import PersonalContextWindowExceeded
from kokoro_agent.domain.run.models import StaticRecipeIncompatible
from kokoro_agent.clients.system import ModelResolutionError
from kokoro_agent.clients.model_budget import ModelBudgetError
from kokoro_agent.clients.custom_api_runtime import (
    CustomApiConfigurationError,
    CustomApiDependencyError,
)
from kokoro_agent.model.call_models import (
    ModelCallAuthorizationUnavailable,
    ModelCallAuthorizationDenied,
    ModelCallDeadlineExceeded,
    ModelCallSettlementPending,
    ModelCallInputInvalid,
)
from kokoro_agent.protocol import RunErrorCode, RunFailedPayload
from kokoro_agent.tools.middleware import TokenBudgetExceeded

_MODEL_FAILURE_CODES: dict[str, RunErrorCode] = {
    "MODEL_UNAVAILABLE": "model_unavailable",
    "SYSTEM_UNAVAILABLE": "dependency_unavailable",
    "MODEL_RESOLUTION_UNAVAILABLE": "dependency_unavailable",
    "POLICY_DENIED": "model_access_denied",
    "FORBIDDEN": "model_access_denied",
    "ROUTE_NOT_FOUND": "assembly_failed",
    "INVALID_ARGUMENT": "assembly_failed",
    "service_auth_failed": "assembly_failed",
    "MODEL_RESOLVER_NOT_CONFIGURED": "assembly_failed",
    "MODEL_REQUEST_INVALID": "assembly_failed",
    "MODEL_RESPONSE_INVALID": "contract_incompatible",
    "MODEL_RESPONSE_TOO_LARGE": "contract_incompatible",
    "MODEL_RESOLUTION_FAILED": "internal_error",
}

_FINANCIAL_UNAVAILABLE_CODES = frozenset(
    {
        "MODEL_BUDGET_UNAVAILABLE",
        "MODEL_BUDGET_CREDENTIAL_UNAVAILABLE",
        "MODEL_SETTLEMENT_UNAVAILABLE",
    }
)


def failure_code(error: BaseException) -> RunErrorCode:
    """Classify typed execution failures without rendering exception diagnostics."""
    if isinstance(error, StaticRecipeIncompatible):
        return "contract_incompatible"
    if isinstance(error, CustomApiConfigurationError):
        return "assembly_failed"
    if isinstance(error, CustomApiDependencyError):
        return "dependency_unavailable"
    if isinstance(error, ModelCallInputInvalid):
        return "contract_incompatible"
    if isinstance(error, ModelCallSettlementPending):
        return "model_settlement_pending"
    if isinstance(error, ModelCallAuthorizationDenied):
        return "model_access_denied"
    if isinstance(error, ModelCallAuthorizationUnavailable):
        return "dependency_unavailable"
    if (
        isinstance(error, ModelBudgetError)
        and error.code in _FINANCIAL_UNAVAILABLE_CODES
    ):
        return "dependency_unavailable"
    if (
        isinstance(
            error, APIConnectionError | APITimeoutError | ModelCallDeadlineExceeded
        )
        or type(error) is APIError
    ):
        return "model_unavailable"
    if isinstance(error, ModelResolutionError):
        return _MODEL_FAILURE_CODES.get(error.code, "internal_error")
    if isinstance(error, PersonalContextWindowExceeded):
        return "context_window_exceeded"
    if isinstance(error, TokenBudgetExceeded):
        return "token_budget_exceeded"
    if isinstance(error, GraphRecursionError):
        return "recursion_limit_exceeded"
    return "internal_error"


def run_failed_payload(
    error: BaseException, *, code: RunErrorCode | None = None
) -> RunFailedPayload:
    # Assembly callers supply their ordinary-error default; typed owner failures
    # retain their verified classification on both initial build and resume.
    if isinstance(error, PersonalContextWindowExceeded):
        return RunFailedPayload(code="context_window_exceeded", retryable=False)
    if isinstance(error, StaticRecipeIncompatible | ModelCallInputInvalid):
        return RunFailedPayload(code="contract_incompatible", retryable=False)
    if isinstance(error, CustomApiConfigurationError):
        return RunFailedPayload(code="assembly_failed", retryable=False)
    if isinstance(error, CustomApiDependencyError):
        return RunFailedPayload(code="dependency_unavailable", retryable=False)
    if isinstance(error, ModelCallSettlementPending):
        return RunFailedPayload(code="model_settlement_pending", retryable=False)
    if isinstance(error, ModelCallAuthorizationDenied):
        return RunFailedPayload(code="model_access_denied", retryable=False)
    if isinstance(error, ModelCallAuthorizationUnavailable):
        return RunFailedPayload(code="dependency_unavailable", retryable=False)
    if (
        isinstance(error, ModelBudgetError)
        and error.code in _FINANCIAL_UNAVAILABLE_CODES
    ):
        return RunFailedPayload(code="dependency_unavailable", retryable=False)
    if (
        isinstance(
            error, APIConnectionError | APITimeoutError | ModelCallDeadlineExceeded
        )
        or type(error) is APIError
    ):
        # The locked SDK raises the base APIError for an in-band SSE error.
        # Do not swallow status/validation subclasses or grant permission to retry dispatched work.
        return RunFailedPayload(code="model_unavailable", retryable=False)
    typed = isinstance(error, ModelResolutionError)
    classified = failure_code(error) if typed else code or failure_code(error)
    retryable = False
    if typed:
        if type(error.retryable) is not bool or (
            error.retryable
            and classified
            not in {"model_unavailable", "dependency_unavailable", "internal_error"}
        ):
            return RunFailedPayload(code="contract_incompatible", retryable=False)
        if classified in {"model_unavailable", "dependency_unavailable"}:
            retryable = error.retryable
    return RunFailedPayload(code=classified, retryable=retryable)
