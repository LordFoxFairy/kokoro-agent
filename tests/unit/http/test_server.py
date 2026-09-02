"""HTTP host authentication behavior."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from kokoro_agent.config import AppConfig
from kokoro_agent.http.server import dispatch_request


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/v1/runs"), ("GET", "/readyz")],
)
async def test_non_health_requests_reject_missing_service_auth_configuration(
    method: str, path: str
) -> None:
    status, payload = await dispatch_request(
        AppConfig(), method, path, {}, {"x-request-id": "request-1"}, {}
    )

    assert status == 503
    assert payload == {
        "error": {
            "code": "service_auth_not_configured",
            "message": "Agent ingress service authentication is not configured",
        },
        "meta": {"request_id": "request-1"},
    }


@pytest.mark.asyncio
async def test_healthz_remains_available_without_service_auth_configuration() -> None:
    status, payload = await dispatch_request(AppConfig(), "GET", "/healthz", {}, {}, None)

    assert status == 200
    assert payload == {"status": "ok", "service": "kokoro-agent"}


@pytest.mark.asyncio
async def test_standard_authorization_is_case_insensitive_and_request_id_is_preserved() -> None:
    config = AppConfig(internal_secret_agent=SecretStr("secret"))

    status, payload = await dispatch_request(
        config,
        "GET",
        "/readyz",
        {},
        {"Authorization": "Basic secret", "X-Request-Id": "request-1"},
        None,
    )

    assert status == 401
    assert payload["meta"] == {"request_id": "request-1"}


@pytest.mark.asyncio
async def test_control_requires_idempotency_key_after_standard_auth() -> None:
    config = AppConfig(internal_secret_agent=SecretStr("secret"))

    status, payload = await dispatch_request(
        config,
        "POST",
        "/v1/runs/run-1/control",
        {},
        {"Authorization": "Bearer secret", "X-Request-Id": "request-1"},
        {"kind": "run.cancel", "session_id": "session-1"},
    )

    assert status == 400
    assert payload["error"] == {
        "code": "idempotency_key_required",
        "message": "Control requests require Idempotency-Key",
    }
