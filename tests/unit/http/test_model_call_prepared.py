"""Billing sees current frozen preparation, never a provider dispatch permit."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

import kokoro_agent.interfaces.http.server as server
from kokoro_agent.infrastructure.postgres_run_repository import RunRepositorySettings
from kokoro_agent.streams.factory import StreamSettings

ATTEMPT = "00000000-0000-4000-8000-000000000001"
PATH = f"/v1/model-call-attempts/{ATTEMPT}/prepared"
NOW = datetime(2026, 10, 22, tzinfo=UTC)


@dataclass(frozen=True)
class Config:
    stream: StreamSettings = StreamSettings(redis_url="redis://unused")
    run_repository: RunRepositorySettings = RunRepositorySettings(
        database_url="postgres://unused", schema_name="unused", lease_ttl_ms=1
    )
    database_url: str = "postgres://unused"
    database_schema: str = "unused"
    internal_secret_agent: SecretStr | None = SecretStr("bff")
    billing_model_call_reader_secret: SecretStr | None = SecretStr("billing")


def _headers() -> dict[str, str]:
    return {
        "authorization": "Bearer billing",
        "x-kokoro-tenant-ref": "tenant",
        "x-request-id": "prepared-read",
    }


@pytest.mark.parametrize(
    ("headers", "query", "body", "expected"),
    [
        ({"authorization": "Bearer bff"}, {}, None, 401),
        ({"authorization": "Bearer billing"}, {}, None, 403),
        ({"x-kokoro-actor-ref": "injected"}, {}, None, 403),
        ({"x-kokoro-service": "injected"}, {}, None, 403),
        ({"content-length": "1"}, {}, None, 403),
        ({"transfer-encoding": "chunked"}, {}, None, 403),
        ({}, {"request_ref": ["caller"]}, None, 400),
        ({}, {}, {}, 403),
    ],
)
async def test_prepared_auth_and_framing_stop_before_database(
    headers: dict[str, str],
    query: dict[str, list[str]],
    body: dict[str, object] | None,
    expected: int,
) -> None:
    actual = _headers() | headers
    if headers == {"authorization": "Bearer billing"}:
        actual.pop("x-kokoro-tenant-ref")
    status, _ = await server.dispatch_request(
        Config(), "GET", PATH, query, actual, body
    )
    assert status == expected


async def test_prepared_wire_preserves_exact_bytes_and_current_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reads: list[tuple[str, str]] = []

    class Repository:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def read_prepared(self, tenant: str, attempt: str) -> SimpleNamespace:
            reads.append((tenant, attempt))
            return SimpleNamespace(
                attempt_id=ATTEMPT,
                status="authorization_pending",
                binding_version="2.0.0",
                attempt_binding_bytes=b'{"identity":"original"}',
                attempt_binding_digest="a" * 64,
                planned_bytes=b'{"model":"original"}',
                planned_digest="b" * 64,
                authorization_bytes=b'{"request_ref":"original","state":"requested_unknown"}',
                authorization_digest="c" * 64,
                final_request_binding_version=1,
                final_request_digest="d" * 64,
                prepared_at=NOW,
                observed_at=NOW + timedelta(seconds=1),
                lease_expires_at=NOW + timedelta(seconds=30),
            )

    monkeypatch.setattr(server, "PostgresModelCallRepository", Repository)
    status, payload = await server.dispatch_request(
        Config(), "GET", PATH, {}, _headers(), None
    )
    assert status == 200
    assert reads == [("tenant", ATTEMPT)]
    assert payload == {
        "data": {
            "binding_version": "2.0.0",
            "attempt_id": ATTEMPT,
            "status": "authorization_pending",
            "attempt_binding_bytes_base64url": base64.urlsafe_b64encode(
                b'{"identity":"original"}'
            )
            .rstrip(b"=")
            .decode(),
            "attempt_binding_digest": "a" * 64,
            "planned_bytes_base64url": base64.urlsafe_b64encode(b'{"model":"original"}')
            .rstrip(b"=")
            .decode(),
            "planned_digest": "b" * 64,
            "authorization_bytes_base64url": base64.urlsafe_b64encode(
                b'{"request_ref":"original","state":"requested_unknown"}'
            )
            .rstrip(b"=")
            .decode(),
            "authorization_digest": "c" * 64,
            "final_request_binding_version": 1,
            "final_request_digest": "d" * 64,
            "prepared_at": "2026-10-22T00:00:00.000Z",
            "observed_at": "2026-10-22T00:00:01.000Z",
            "lease_expires_at": "2026-10-22T00:00:30.000Z",
        },
        "meta": {"request_id": "prepared-read"},
    }
