"""Typed mTLS ConnectRPC client for Agent-only MCP secret material resolution.

The Hub owns secret ciphertext and namespace ownership. Agent sends only the opaque runtime
namespace and the exact handle set selected during assembly. Plaintext is bounded, kept in memory,
and never included in exceptions or logs.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

import pyqwest
from connectrpc.errors import ConnectError
from pydantic import BaseModel, ConfigDict, Field

from kokoro.platform.capability.v1 import capability_catalog_pb2 as capability_pb
from kokoro.platform.capability.v1.capability_catalog_connect import HubRuntimeServiceClient

_HANDLE = re.compile(r"^srt_[0-9a-f]{32}$")
_HOSTNAME = re.compile(r"^[A-Za-z0-9.-]{1,253}$")
_MAX_HANDLES = 128
_MAX_SECRET_BYTES = 8 * 1024
_MAX_RESPONSE_BYTES = 2 * 1024 * 1024


class SecretResolveError(Exception):
    """Stable failure surface that never contains Hub response bodies or secret material."""

    def __init__(self, code: str = "HUB_SECRET_RESOLVE_FAILED") -> None:
        super().__init__(code)
        self.code = code


class SecretResolver(Protocol):
    async def resolve(self, namespace: str, handles: Sequence[str]) -> Mapping[str, str]: ...


class AsyncHubRuntimeClient(Protocol):
    async def resolve_mcp_secrets(
        self,
        request: capability_pb.ResolveMcpSecretsRequest,
        *,
        timeout_ms: int | None = None,
    ) -> capability_pb.ResolveMcpSecretsResponse: ...


class HubSecretSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    rpc_url: str
    server_name: str
    ca_file: str
    cert_file: str
    key_file: str
    timeout_ms: int = Field(default=5_000, ge=100, le=5_000)


class HubSecretResolver:
    """Process-scoped Hub runtime client; each assembly sends at most one bounded unary RPC."""

    def __init__(
        self,
        settings: HubSecretSettings,
        *,
        client: AsyncHubRuntimeClient | None = None,
    ) -> None:
        self._timeout_ms = settings.timeout_ms
        address = _hub_address(settings.rpc_url, settings.server_name)
        if client is None:
            ca = _tls_file(settings.ca_file, "ca")
            cert = _tls_file(settings.cert_file, "cert")
            key = _tls_file(settings.key_file, "key", private=True)
            if b"BEGIN CERTIFICATE" not in ca or b"BEGIN CERTIFICATE" not in cert:
                raise ValueError("HUB_SECRET_TLS_CERTIFICATE_INVALID")
            if b"PRIVATE KEY" not in key:
                raise ValueError("HUB_SECRET_TLS_PRIVATE_KEY_INVALID")
            http_client = pyqwest.Client(
                pyqwest.HTTPTransport(
                    tls_ca_cert=ca,
                    tls_include_system_certs=False,
                    tls_key=key,
                    tls_cert=cert,
                    http_version=pyqwest.HTTPVersion.HTTP2,
                    enable_cookie_store=False,
                )
            )
            client = HubRuntimeServiceClient(
                address,
                timeout_ms=settings.timeout_ms,
                read_max_bytes=_MAX_RESPONSE_BYTES,
                http_client=http_client,
            )
        self._client = client

    async def resolve(self, namespace: str, handles: Sequence[str]) -> Mapping[str, str]:
        requested = list(handles)
        if not requested:
            return {}
        if (
            not _reference(namespace, 256)
            or len(requested) > _MAX_HANDLES
            or len(set(requested)) != len(requested)
            or any(_HANDLE.fullmatch(handle) is None for handle in requested)
        ):
            raise SecretResolveError("HUB_SECRET_RESOLVE_REQUEST_INVALID")
        try:
            response = await self._client.resolve_mcp_secrets(
                capability_pb.ResolveMcpSecretsRequest(
                    namespace=namespace,
                    handles=requested,
                ),
                timeout_ms=self._timeout_ms,
            )
        except ConnectError:
            raise SecretResolveError() from None
        except Exception:
            raise SecretResolveError() from None

        resolved: dict[str, str] = {}
        for material in response.secrets:
            if (
                _HANDLE.fullmatch(material.handle) is None
                or material.handle in resolved
                or not material.value
                or len(material.value.encode("utf-8")) > _MAX_SECRET_BYTES
            ):
                raise SecretResolveError("HUB_SECRET_RESOLVE_RESPONSE_INVALID")
            resolved[material.handle] = material.value
        if set(resolved) != set(requested) or len(resolved) != len(requested):
            raise SecretResolveError("HUB_SECRET_RESOLVE_RESPONSE_INVALID")
        return resolved


def hub_secret_resolver_from_env(env: Mapping[str, str]) -> HubSecretResolver | None:
    """Build the only production Hub secret adapter; partial mTLS config fails startup."""
    names = {
        "rpc_url": "KOKORO_HUB_RPC_URL",
        "server_name": "KOKORO_HUB_RPC_SERVER_NAME",
        "ca_file": "KOKORO_HUB_RPC_CA_FILE",
        "cert_file": "KOKORO_HUB_RPC_CERT_FILE",
        "key_file": "KOKORO_HUB_RPC_KEY_FILE",
    }
    values = {field: env.get(name, "").strip() for field, name in names.items()}
    present = {field for field, value in values.items() if value}
    if not present:
        return None
    if present != set(values):
        raise ValueError("HUB_SECRET_MTLS_CONFIGURATION_INCOMPLETE")
    raw_timeout = env.get("KOKORO_HUB_RPC_TIMEOUT_MS", "5000")
    try:
        timeout_ms = int(raw_timeout)
    except ValueError as error:
        raise ValueError("HUB_SECRET_TIMEOUT_INVALID") from error
    return HubSecretResolver(HubSecretSettings(**values, timeout_ms=timeout_ms))


def _hub_address(value: str, server_name: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
        or _HOSTNAME.fullmatch(server_name) is None
        or parsed.hostname.casefold() != server_name.casefold()
    ):
        raise ValueError("HUB_SECRET_RPC_IDENTITY_INVALID")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _tls_file(value: str, kind: str, *, private: bool = False) -> bytes:
    path = Path(value)
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ValueError(f"HUB_SECRET_TLS_{kind.upper()}_INVALID") from error
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not path.is_file()
        or metadata.st_size < 1
        or metadata.st_size > 256 * 1024
        or (private and metadata.st_mode & 0o077 != 0)
    ):
        raise ValueError(f"HUB_SECRET_TLS_{kind.upper()}_INVALID")
    try:
        material = path.read_bytes()
    except OSError as error:
        raise ValueError(f"HUB_SECRET_TLS_{kind.upper()}_INVALID") from error
    if len(material) != metadata.st_size:
        raise ValueError(f"HUB_SECRET_TLS_{kind.upper()}_INVALID")
    return material


def _reference(value: str, maximum: int) -> bool:
    return 1 <= len(value) <= maximum and value.strip() == value
