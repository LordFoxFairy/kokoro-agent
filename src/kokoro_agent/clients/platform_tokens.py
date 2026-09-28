"""Tenant-scoped IAM client_credentials exchange and process-owned single flight."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from time import monotonic
from typing import Literal
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from kokoro_agent.clients.platform_credentials import (
    CredentialError,
    CredentialFile,
    PlatformCredential,
)


class PlatformTokenError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class _TokenResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    access_token: SecretStr
    token_type: Literal["Bearer", "bearer"]
    expires_in: int = Field(gt=5)
    scope: str | None = None


@dataclass(slots=True)
class _Flight:
    task: asyncio.Task[str] = field(repr=False)
    waiters: int = 0


@dataclass(frozen=True, slots=True, repr=False)
class _Cached:
    credential: PlatformCredential
    token: str
    expires_at: float


def validate_owner_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ValueError("owner base URL must be an absolute origin")
    if parsed.scheme == "http" and parsed.hostname not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        raise ValueError("owner base URL requires TLS outside loopback")
    return value.rstrip("/")


class PlatformTokenProvider:
    def __init__(
        self, base_url: str, credentials: CredentialFile, *, http: httpx.AsyncClient
    ) -> None:
        self._url = validate_owner_url(base_url) + "/iam/oauth2/token"
        self._credentials = credentials
        self._http = http
        self._cache: dict[str, _Cached] = {}
        self._flights: dict[PlatformCredential, _Flight] = {}
        self._closed = False

    async def token(self, tenant_id: str) -> str:
        if self._closed:
            raise PlatformTokenError("PLATFORM_TOKEN_CLOSED")
        credential = self._credentials.snapshot(tenant_id)
        cached = self._cache.get(tenant_id)
        if cached is not None:
            if cached.credential == credential and cached.expires_at - monotonic() > 5:
                return cached.token
            self._cache.pop(tenant_id, None)
        flight = self._flights.get(credential)
        if flight is None:
            flight = _Flight(task=asyncio.create_task(self._exchange(credential)))
            self._flights[credential] = flight
        flight.waiters += 1
        try:
            token = await asyncio.shield(flight.task)
            if self._closed:
                raise PlatformTokenError("PLATFORM_TOKEN_CLOSED")
            if self._credentials.snapshot(tenant_id) != credential:
                raise CredentialError("platform credentials changed")
            return token
        finally:
            flight.waiters -= 1
            if flight.waiters == 0:
                self._flights.pop(credential, None)
                if not flight.task.done():
                    flight.task.cancel()
                await asyncio.gather(flight.task, return_exceptions=True)

    async def _exchange(self, credential: PlatformCredential) -> str:
        started = monotonic()
        try:
            async with asyncio.timeout(10):
                async with self._http.stream(
                    "POST",
                    self._url,
                    auth=httpx.BasicAuth(
                        credential.client_id,
                        credential.client_secret.get_secret_value(),
                    ),
                    data={
                        "grant_type": "client_credentials",
                        "resource": credential.resource,
                        "scope": credential.scope,
                    },
                    follow_redirects=False,
                    timeout=5,
                ) as response:
                    if response.status_code != 200:
                        code = (
                            "PLATFORM_TOKEN_RATE_LIMITED"
                            if response.status_code == 429
                            else "PLATFORM_TOKEN_UNAUTHENTICATED"
                            if response.status_code in {400, 401, 403}
                            else "PLATFORM_TOKEN_UNAVAILABLE"
                        )
                        raise PlatformTokenError(code)
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > 1_048_576:
                            raise PlatformTokenError("PLATFORM_TOKEN_INVALID_RESPONSE")
            result = _TokenResponse.model_validate_json(body)
            token = result.access_token.get_secret_value()
            if re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", token) is None:
                raise ValueError
            if result.scope is not None and result.scope != credential.scope:
                raise ValueError
            if self._credentials.snapshot(credential.tenant_id) != credential:
                raise CredentialError("platform credentials changed")
            expires = started + result.expires_in
            if expires - monotonic() <= 5:
                raise PlatformTokenError("PLATFORM_TOKEN_EXPIRED")
            self._cache[credential.tenant_id] = _Cached(
                credential=credential, token=token, expires_at=expires
            )
            return token
        except (CredentialError, PlatformTokenError):
            raise
        except (TimeoutError, httpx.TimeoutException):
            failure = "PLATFORM_TOKEN_TIMEOUT"
        except httpx.HTTPError:
            failure = "PLATFORM_TOKEN_UNAVAILABLE"
        except Exception:
            failure = "PLATFORM_TOKEN_INVALID_RESPONSE"
        raise PlatformTokenError(failure) from None

    async def aclose(self) -> None:
        self._closed = True
        tasks = [flight.task for flight in self._flights.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._cache.clear()
