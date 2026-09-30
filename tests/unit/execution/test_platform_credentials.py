"""Worker credential snapshot and tenant token exchange boundary."""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from kokoro_agent.clients.platform_credentials import CredentialFile, CredentialError
from kokoro_agent.clients.platform_tokens import PlatformTokenProvider

RESOURCE = "https://kokoro.dev/resources/platform-internal"


def write_credentials(
    path: Path, *, generation: int = 1, secret: str = "secret"
) -> None:
    path.write_text(
        json.dumps(
            [
                dict(
                    tenant_id="tenant",
                    generation=generation,
                    credential_ref_version="v1",
                    client_id="agent",
                    client_secret=secret,
                    resource=RESOURCE,
                    scope="platform:execution.invoke",
                )
            ]
        )
    )
    path.chmod(0o600)


def test_exact_owner_only_file_and_redacted_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    reader = CredentialFile(str(path))
    assert reader.snapshot("tenant").generation == 1
    assert "secret" not in repr(reader.snapshot("tenant"))
    path.chmod(0o644)
    with pytest.raises(CredentialError):
        reader.snapshot("tenant")


def test_duplicate_unknown_and_symlink_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    alias = tmp_path / "alias"
    alias.symlink_to(path)
    with pytest.raises(CredentialError):
        CredentialFile(str(alias)).snapshot("tenant")
    path.write_text('[{"tenant_id":"tenant","tenant_id":"tenant"}]')
    with pytest.raises(CredentialError):
        CredentialFile(str(path)).snapshot("tenant")


def test_generation_is_monotonic_and_same_generation_is_immutable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    reader = CredentialFile(str(path))
    reader.snapshot("tenant")
    write_credentials(path, secret="changed")
    with pytest.raises(CredentialError):
        reader.snapshot("tenant")
    write_credentials(path, generation=2)
    assert reader.snapshot("tenant").generation == 2
    write_credentials(path)
    with pytest.raises(CredentialError):
        reader.snapshot("tenant")


async def test_exact_exchange_cache_singleflight_and_independent_cancel(
    tmp_path: Path,
) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    started, release = asyncio.Event(), asyncio.Event()
    calls: list[httpx.Request] = []

    async def exchange(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        started.set()
        await release.wait()
        return httpx.Response(
            200,
            json={
                "access_token": "TOKEN",
                "token_type": "Bearer",
                "expires_in": 60,
                "scope": "platform:execution.invoke",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(exchange)) as http:
        provider = PlatformTokenProvider(
            "https://iam.test", CredentialFile(str(path)), http=http
        )
        first = asyncio.create_task(provider.token("tenant"))
        await started.wait()
        second = asyncio.create_task(provider.token("tenant"))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        assert await second == "TOKEN"
        assert await provider.token("tenant") == "TOKEN"
        assert len(calls) == 1
        request = calls[0]
        assert str(request.url) == "https://iam.test/iam/oauth2/token"
        assert (
            request.headers["authorization"]
            == "Basic " + base64.b64encode(b"agent:secret").decode()
        )
        assert parse_qs(request.content.decode()) == {
            "grant_type": ["client_credentials"],
            "resource": [RESOURCE],
            "scope": ["platform:execution.invoke"],
        }
        await provider.aclose()


async def test_rotation_during_exchange_never_returns_old_token(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)

    async def exchange(request: httpx.Request) -> httpx.Response:
        write_credentials(path, generation=2, secret="rotated")
        return httpx.Response(
            200, json={"access_token": "OLD", "token_type": "Bearer", "expires_in": 60}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(exchange)) as http:
        provider = PlatformTokenProvider(
            "https://iam.test", CredentialFile(str(path)), http=http
        )
        with pytest.raises(CredentialError):
            await provider.token("tenant")
        await provider.aclose()


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown",
        "duplicate_tenant",
        "duplicate_client",
        "float_generation",
        "bool_generation",
        "wrong_resource",
        "wrong_scope",
        "blank_secret",
    ],
)
def test_credential_exact_shape_rejections(tmp_path: Path, mutation: str) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    items = json.loads(path.read_text())
    if mutation == "unknown":
        items[0]["extra"] = "value"
    elif mutation == "duplicate_tenant":
        items.append(dict(items[0], client_id="second"))
    elif mutation == "duplicate_client":
        items.append(dict(items[0], tenant_id="second"))
    else:
        field, value = {
            "float_generation": ("generation", 1.0),
            "bool_generation": ("generation", True),
            "wrong_resource": ("resource", "other"),
            "wrong_scope": ("scope", "other"),
            "blank_secret": ("client_secret", ""),
        }[mutation]
        items[0][field] = value
    path.write_text(json.dumps(items))
    with pytest.raises(CredentialError):
        CredentialFile(str(path)).snapshot("tenant")


def test_fifo_and_read_time_mutation_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    path = tmp_path / "credentials.json"
    os.mkfifo(path, mode=0o600)
    with pytest.raises(CredentialError):
        CredentialFile(str(path)).snapshot("tenant")
    path.unlink()
    write_credentials(path)
    real_read = os.read

    def changed(fd: int, length: int) -> bytes:
        data = real_read(fd, length)
        path.chmod(0o644)
        return data

    monkeypatch.setattr(os, "read", changed)
    with pytest.raises(CredentialError):
        CredentialFile(str(path)).snapshot("tenant")


@pytest.mark.parametrize(
    "status,code",
    [
        (401, "UNAUTHENTICATED"),
        (403, "UNAUTHENTICATED"),
        (429, "RATE_LIMITED"),
        (503, "UNAVAILABLE"),
        (302, "UNAVAILABLE"),
    ],
)
async def test_token_errors_classified_no_sensitive_context(
    tmp_path: Path, status: int, code: str
) -> None:
    from kokoro_agent.clients.platform_tokens import PlatformTokenError

    path = tmp_path / "credentials.json"
    write_credentials(path)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(status, text="SECRET_SENTINEL")
        )
    ) as http:
        provider = PlatformTokenProvider(
            "https://iam.test", CredentialFile(str(path)), http=http
        )
        with pytest.raises(PlatformTokenError, match=code) as captured:
            await provider.token("tenant")
        assert "SECRET_SENTINEL" not in str(captured.value)
        await provider.aclose()


async def test_last_waiter_cancels_exchange_and_shutdown_harvests(
    tmp_path: Path,
) -> None:
    path = tmp_path / "credentials.json"
    write_credentials(path)
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def exchange(request: httpx.Request) -> httpx.Response:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        raise AssertionError

    async with httpx.AsyncClient(transport=httpx.MockTransport(exchange)) as http:
        provider = PlatformTokenProvider(
            "https://iam.test", CredentialFile(str(path)), http=http
        )
        caller = asyncio.create_task(provider.token("tenant"))
        await started.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert cancelled.is_set()
        await provider.aclose()


async def test_malformed_token_response_has_no_secret_exception_chain(
    tmp_path: Path,
) -> None:
    from kokoro_agent.clients.platform_tokens import PlatformTokenError

    path = tmp_path / "credentials.json"
    write_credentials(path)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "access_token": "SECRET_SENTINEL",
                    "expires_in": "bad",
                    "token_type": "Bearer",
                },
            )
        )
    ) as http:
        provider = PlatformTokenProvider(
            "https://iam.test", CredentialFile(str(path)), http=http
        )
        with pytest.raises(PlatformTokenError) as error:
            await provider.token("tenant")
        assert error.value.__context__ is None
        await provider.aclose()


@pytest.mark.parametrize(
    "extensions",
    [
        {"expires_at": 1_799_999_999},
        {"expires_at": 0},
        {"expires_at": 10**15},
        {
            "expires_at": "ignored",
            "extension": {"secret": ["EXTENSION_SENTINEL", None]},
        },
    ],
)
async def test_oauth_success_extensions_use_only_expires_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    extensions: dict[str, object],
) -> None:
    from kokoro_agent.clients import platform_tokens

    path = tmp_path / "credentials.json"
    write_credentials(path)
    now = [100.0]
    monkeypatch.setattr(platform_tokens, "monotonic", lambda: now[0])
    calls = 0

    def exchange(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "access_token": f"TOKEN-{calls}",
                "token_type": "Bearer",
                "expires_in": 60,
                "scope": "platform:execution.invoke",
                **extensions,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(exchange)) as http:
        provider = PlatformTokenProvider(
            "https://iam.test", CredentialFile(str(path)), http=http
        )
        try:
            assert await provider.token("tenant") == "TOKEN-1"
            now[0] = 154.0
            assert await provider.token("tenant") == "TOKEN-1"
            assert calls == 1
            now[0] = 155.0
            assert await provider.token("tenant") == "TOKEN-2"
            assert calls == 2
            assert "EXTENSION_SENTINEL" not in repr(vars(provider)) + caplog.text
        finally:
            await provider.aclose()


@pytest.mark.parametrize(
    "field,value",
    [
        ("access_token", 1),
        ("access_token", ""),
        ("access_token", "bad token"),
        ("token_type", "MAC"),
        ("token_type", 1),
        ("expires_in", "60"),
        ("expires_in", 60.0),
        ("expires_in", True),
        ("expires_in", 5),
        ("expires_in", 0),
        ("expires_in", -1),
        ("scope", 1),
        ("scope", "other"),
        ("scope", "platform:execution.invoke other"),
    ],
)
async def test_extensions_do_not_relax_known_token_fields(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    from kokoro_agent.clients.platform_tokens import PlatformTokenError

    path = tmp_path / "credentials.json"
    write_credentials(path)
    payload = {
        "access_token": "TOKEN",
        "token_type": "Bearer",
        "expires_in": 60,
        "scope": "platform:execution.invoke",
        "expires_at": 10**15,
        "unknown": "EXTENSION_SENTINEL",
        field: value,
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as http:
        provider = PlatformTokenProvider(
            "https://iam.test", CredentialFile(str(path)), http=http
        )
        try:
            with pytest.raises(PlatformTokenError) as captured:
                await provider.token("tenant")
            assert str(captured.value) == "PLATFORM_TOKEN_INVALID_RESPONSE"
            assert captured.value.__context__ is None
            assert "EXTENSION_SENTINEL" not in repr(captured.value)
        finally:
            await provider.aclose()


@pytest.mark.parametrize("missing", ["access_token", "token_type", "expires_in"])
async def test_extensions_never_substitute_required_token_fields(
    tmp_path: Path,
    missing: str,
) -> None:
    from kokoro_agent.clients.platform_tokens import PlatformTokenError

    path = tmp_path / "credentials.json"
    write_credentials(path)
    payload: dict[str, object] = {
        "access_token": "TOKEN",
        "token_type": "Bearer",
        "expires_in": 60,
        "expires_at": 10**15,
        "unknown": "EXTENSION_SENTINEL",
    }
    del payload[missing]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as http:
        provider = PlatformTokenProvider(
            "https://iam.test", CredentialFile(str(path)), http=http
        )
        try:
            with pytest.raises(
                PlatformTokenError, match="^PLATFORM_TOKEN_INVALID_RESPONSE$"
            ):
                await provider.token("tenant")
        finally:
            await provider.aclose()
