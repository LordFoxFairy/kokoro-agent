"""HTTP host authentication behavior."""

from __future__ import annotations

import pytest

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
        AppConfig(), method, path, {}, {"x-kokoro-request-id": "request-1"}, {}
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
