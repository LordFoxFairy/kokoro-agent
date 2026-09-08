"""System-owned model route HTTP boundary; no provider credentials or inference."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Annotated, Literal, Protocol, Self
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    ValidationError,
)

_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")
_UUID = (
    r"^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|"
    r"00000000-0000-0000-0000-000000000000|"
    r"ffffffff-ffff-ffff-ffff-ffffffffffff)$"
)
_Id = Annotated[str, StringConstraints(pattern=_UUID)]
_Text = Annotated[str, StringConstraints(min_length=1, max_length=255)]
_Generation = Annotated[str, StringConstraints(pattern=r"^[1-9][0-9]*$")]
_MAX_RESPONSE_BYTES = 65_536
_OWNER_ERRORS = frozenset(
    {
        "POLICY_DENIED",
        "ROUTE_NOT_FOUND",
        "MODEL_UNAVAILABLE",
        "SYSTEM_UNAVAILABLE",
        "INVALID_ARGUMENT",
        "FORBIDDEN",
        "service_auth_failed",
        "service_auth_not_configured",
    }
)


class ModelResolutionError(Exception):
    """Sanitized owner/transport failure, never raw HTTP content or credentials."""

    def __init__(self, code: str, *, retryable: bool = False) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(code)


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolvedModel:
    model_id: str
    provider_id: str
    revision_id: str
    revision: int
    digest: str
    generation: str
    tenant_generation: str
    provider_model_name: str
    gateway_model_name: str
    feature_key: str
    label_key: str
    transport: Literal["litellm"] = "litellm"


class ModelResolver(Protocol):
    async def resolve(
        self,
        *,
        tenant_id: str,
        feature_key: str,
        label: str | None,
        request_id: str | None,
    ) -> ResolvedModel: ...


class _Route(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    model_id: _Id
    provider_id: _Id
    revision_id: _Id
    revision: int = Field(gt=0, le=9007199254740991)
    digest: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
    generation: _Generation
    tenant_generation: _Generation
    provider_model_name: _Text
    gateway_model_name: _Text
    feature_key: _Text
    label_key: _Text
    transport: Literal["litellm"]


class _Envelope(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    data: _Route


class _OwnerError(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    code: str
    message: str
    retryable: bool


class _ErrorEnvelope(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    error: _OwnerError


class SystemModelClient:
    """One process-owned pool, bounded reads/deadline, no redirects or retries."""

    def __init__(
        self,
        base_url: str,
        token: SecretStr,
        *,
        timeout_s: float = 5,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError(
                "System base URL must be an HTTP(S) origin without credentials"
            )
        if not 0 < timeout_s <= 60 or not token.get_secret_value().strip():
            raise ValueError("System token and timeout must be configured")
        self._timeout_s = timeout_s
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={
                "authorization": f"Bearer {token.get_secret_value()}",
                "x-kokoro-service": "kokoro-agent",
            },
            timeout=httpx.Timeout(timeout_s),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        )

    async def __aenter__(self) -> Self:
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._client.aclose()

    async def resolve(
        self,
        *,
        tenant_id: str,
        feature_key: str,
        label: str | None,
        request_id: str | None,
    ) -> ResolvedModel:
        correlation_id = request_id or str(uuid4())
        if (
            not _REQUEST_ID.fullmatch(correlation_id)
            or not 1 <= len(tenant_id) <= 160
            or not tenant_id.isascii()
            or any(ord(char) < 33 for char in tenant_id)
            or not 1 <= len(feature_key) <= 128
            or (label is not None and not 1 <= len(label) <= 128)
        ):
            raise ModelResolutionError("MODEL_REQUEST_INVALID")
        body = {"feature_key": feature_key}
        if label is not None:
            body["label_key"] = label
        try:
            async with asyncio.timeout(self._timeout_s):
                async with self._client.stream(
                    "POST",
                    "/v1/system/model-catalog/resolve",
                    json=body,
                    headers={
                        "x-kokoro-tenant-id": tenant_id,
                        "x-request-id": correlation_id,
                    },
                ) as response:
                    chunks = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(chunks) + len(chunk) > _MAX_RESPONSE_BYTES:
                            raise ModelResolutionError("MODEL_RESPONSE_TOO_LARGE")
                        chunks.extend(chunk)
                    if response.status_code != 200:
                        self._raise_owner_error(response.status_code, bytes(chunks))
                    route = _Envelope.model_validate_json(bytes(chunks)).data
        except (TimeoutError, httpx.TransportError):
            raise ModelResolutionError(
                "MODEL_RESOLUTION_UNAVAILABLE", retryable=True
            ) from None
        except ValidationError:
            raise ModelResolutionError("MODEL_RESPONSE_INVALID") from None
        if route.feature_key != feature_key or (
            label is not None and route.label_key != label
        ):
            raise ModelResolutionError("MODEL_RESPONSE_INVALID")
        return ResolvedModel(
            model_id=route.model_id,
            provider_id=route.provider_id,
            revision_id=route.revision_id,
            revision=route.revision,
            digest=route.digest,
            generation=route.generation,
            tenant_generation=route.tenant_generation,
            provider_model_name=route.provider_model_name,
            gateway_model_name=route.gateway_model_name,
            feature_key=route.feature_key,
            label_key=route.label_key,
            transport=route.transport,
        )

    @staticmethod
    def _raise_owner_error(status: int, body: bytes) -> None:
        if 300 <= status < 400:
            raise ModelResolutionError("MODEL_RESPONSE_INVALID")
        try:
            error = _ErrorEnvelope.model_validate_json(body).error
        except ValidationError:
            raise ModelResolutionError("MODEL_RESPONSE_INVALID") from None
        code = error.code if error.code in _OWNER_ERRORS else "MODEL_RESOLUTION_FAILED"
        raise ModelResolutionError(code, retryable=status >= 500 and error.retryable)
