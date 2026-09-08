from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from kokoro_agent.clients.system import ModelResolutionError, SystemModelClient


def route_payload() -> dict[str, Any]:
    return {
        "data": {
            "model_id": "11111111-1111-4111-8111-111111111111",
            "provider_id": "22222222-2222-4222-8222-222222222222",
            "revision_id": "33333333-3333-4333-8333-333333333333",
            "revision": 2,
            "digest": "a" * 64,
            "generation": "9007199254740993",
            "tenant_generation": "3",
            "provider_model_name": "provider-model",
            "transport": "litellm",
            "gateway_model_name": "gateway-model",
            "feature_key": "chat",
            "label_key": "fast",
        }
    }


def client(
    transport: httpx.AsyncBaseTransport, *, timeout: float = 1
) -> SystemModelClient:
    return SystemModelClient(
        "http://system.test",
        SecretStr("service-secret"),
        timeout_s=timeout,
        transport=transport,
    )


async def test_resolves_trusted_tenant_and_opaque_label_without_provider_credentials() -> (
    None
):
    def handle(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "http://system.test/v1/system/model-catalog/resolve"
        assert request.headers["authorization"] == "Bearer service-secret"
        assert request.headers["x-kokoro-service"] == "kokoro-agent"
        assert request.headers["x-kokoro-tenant-id"] == "tenant-1"
        assert request.headers["x-request-id"] == "request-1"
        assert "x-kokoro-iam-permissions" not in request.headers
        assert json.loads(request.content) == {
            "feature_key": "chat",
            "label_key": "fast",
        }
        return httpx.Response(
            200, json=route_payload(), headers={"x-request-id": "request-1"}
        )

    async with client(httpx.MockTransport(handle)) as resolver:
        route = await resolver.resolve(
            tenant_id="tenant-1",
            feature_key="chat",
            label="fast",
            request_id="request-1",
        )
    assert route.gateway_model_name == "gateway-model"
    assert route.generation == "9007199254740993"


async def test_omits_missing_label_for_owner_default_policy() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content) == {"feature_key": "chat"}
        return httpx.Response(
            200,
            json=route_payload(),
            headers={"x-request-id": request.headers["x-request-id"]},
        )

    async with client(httpx.MockTransport(handle)) as resolver:
        result = await resolver.resolve(
            tenant_id="tenant", feature_key="chat", label=None, request_id=None
        )
    assert result.label_key == "fast"


@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (403, "POLICY_DENIED", False),
        (404, "ROUTE_NOT_FOUND", False),
        (503, "MODEL_UNAVAILABLE", True),
    ],
)
async def test_owner_errors_are_sanitized(
    status: int, code: str, retryable: bool
) -> None:
    def handle(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            json={
                "error": {
                    "code": code,
                    "message": "sensitive service-secret",
                    "retryable": retryable,
                }
            },
        )

    async with client(httpx.MockTransport(handle)) as resolver:
        with pytest.raises(ModelResolutionError) as error:
            await resolver.resolve(
                tenant_id="tenant", feature_key="chat", label=None, request_id=None
            )
    assert error.value.code == code
    assert error.value.retryable is retryable
    assert "sensitive" not in str(error.value)
    assert "service-secret" not in str(error.value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("transport", "openai"),
        ("feature_key", "other"),
        ("label_key", "other"),
        ("generation", 1),
        ("revision", True),
        ("revision", 9007199254740992),
        ("model_id", "11111111-1111-0111-8111-111111111111"),
        ("provider_id", "22222222-2222-4222-1222-222222222222"),
        ("digest", "invalid"),
        ("gateway_model_name", ""),
        ("provider_id", "not-a-uuid"),
    ],
)
async def test_rejects_invalid_or_mismatched_route(field: str, value: object) -> None:
    payload = route_payload()
    payload["data"][field] = value
    async with client(
        httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as resolver:
        with pytest.raises(ModelResolutionError, match="MODEL_RESPONSE_INVALID"):
            await resolver.resolve(
                tenant_id="tenant",
                feature_key="chat",
                label="fast",
                request_id="request",
            )


async def test_rejects_legacy_envelope_and_unexpected_secret_field() -> None:
    for payload in [
        {**route_payload(), "meta": {"request_id": "old"}},
        {"data": {**route_payload()["data"], "secret": "credential"}},
    ]:
        async with client(
            httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
        ) as resolver:
            with pytest.raises(ModelResolutionError, match="MODEL_RESPONSE_INVALID"):
                await resolver.resolve(
                    tenant_id="tenant", feature_key="chat", label=None, request_id=None
                )


async def test_bounds_total_time_and_does_not_retry() -> None:
    attempts = 0

    async def handle(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with client(httpx.MockTransport(handle), timeout=0.01) as resolver:
        with pytest.raises(ModelResolutionError) as error:
            await resolver.resolve(
                tenant_id="tenant", feature_key="chat", label=None, request_id=None
            )
    assert error.value.retryable is True
    assert attempts == 1


async def test_propagates_cancellation() -> None:
    async def handle(_: httpx.Request) -> httpx.Response:
        raise asyncio.CancelledError

    async with client(httpx.MockTransport(handle)) as resolver:
        with pytest.raises(asyncio.CancelledError):
            await resolver.resolve(
                tenant_id="tenant", feature_key="chat", label=None, request_id=None
            )


async def test_rejects_oversized_response_and_redirect_without_following() -> None:
    for response in [
        httpx.Response(200, content=b" " * 65537),
        httpx.Response(307, headers={"location": "http://other.test"}),
    ]:
        async with client(httpx.MockTransport(lambda _: response)) as resolver:
            with pytest.raises(ModelResolutionError):
                await resolver.resolve(
                    tenant_id="tenant", feature_key="chat", label=None, request_id=None
                )


@pytest.mark.parametrize(
    "url",
    [
        "ftp://system.test",
        "http://user:secret@system.test",
        "http://system.test/?token=secret",
        "http://system.test/#frag",
    ],
)
def test_rejects_unsafe_configuration(url: str) -> None:
    with pytest.raises(ValueError):
        SystemModelClient(url, SecretStr("secret"))
