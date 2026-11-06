"""Narrow CustomAPI metadata and one-execution environment consumer.

No HTTP proxy, credential cache, environment mutation or persistent material.
Python strings/Protobuf bytes cannot promise physical memory erasure.
"""

from __future__ import annotations

import asyncio
import math
import re
import time
from dataclasses import dataclass, field
from typing import Protocol
from uuid import uuid4

from pydantic import SecretStr

from kokoro_agent.clients.platform_transport import PlatformCallError, PlatformRequest
from kokoro_agent.execution.custom_api_request_binding import (
    CustomApiRequest,
    custom_api_identity,
)
from kokoro_agent.execution.platform_request_binding import project_request_binding
from kokoro_agent.generated.kokoro.platform.v1 import custom_api_runtime_pb as pb


class CustomApiClientError(RuntimeError):
    """Only stable non-sensitive codes cross this boundary."""


class CustomApiConfigurationError(CustomApiClientError):
    """The selected profile is no longer usable for this run."""


class CustomApiDependencyError(CustomApiClientError):
    """The capability owner could not be reached for this run."""


class CustomApiSender(Protocol):
    async def send(
        self, request: PlatformRequest, *, timeout_s: float = 10
    ) -> object: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class CustomApiProfileRef:
    profile_id: str
    expected_state_version: int

    def __post_init__(self) -> None:
        custom_api_identity(self.profile_id)
        if (
            type(self.expected_state_version) is not int
            or not 1 <= self.expected_state_version <= 2147483647
        ):
            raise CustomApiClientError("CUSTOM_API_PROFILE_INVALID")


@dataclass(frozen=True, slots=True, kw_only=True)
class CustomApiProfileDescription:
    profile_id: str
    state_version: int
    name: str
    instructions: str
    environment_names: tuple[str, ...]


# Same closed runtime-control policy as the pinned owner publication.
_RESERVED = frozenset(
    "PATH HOME USER LOGNAME SHELL IFS ENV BASH_ENV SHELLOPTS BASHOPTS CDPATH PWD OLDPWD TMPDIR TMP TEMP LANG LC_ALL TZ ZDOTDIR NODE_OPTIONS NODE_PATH RUBYOPT RUBYLIB PERL5OPT PERL5LIB JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS CLASSPATH GCONV_PATH GETCONF_DIR HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY SSL_CERT_FILE SSL_CERT_DIR CURL_CA_BUNDLE REQUESTS_CA_BUNDLE BASH_FUNC ENV_SUPATH SSLKEYLOGFILE GIT_CONFIG".split()
)
_PREFIXES = (
    "__",
    "LD_",
    "DYLD_",
    "PYTHON",
    "NODE_",
    "OPENSSL_",
    "DOCKER_",
    "E2B_",
    "KOKORO_",
    "BASH",
    "GIT_CONFIG_",
)


def _environment_name(name: str) -> str:
    if (
        type(name) is not str
        or re.fullmatch(r"[A-Z_][A-Z0-9_]{0,127}", name) is None
        or name in _RESERVED
        or name.startswith(_PREFIXES)
    ):
        raise CustomApiClientError("CUSTOM_API_RESPONSE_INVALID")
    return name


def _clear_material(response: object) -> None:
    if isinstance(response, pb.AcquireCustomApiEnvironmentResponse):
        for item in response.environment:
            item.value = b""
        response.environment.clear()


def _description_text(value: str, *, name: bool) -> str:
    valid = False
    try:
        # Platform CRUD's text domain is UTF-8 bytes: name 1..400,
        # instructions 1..4000 when set, empty string when absent on this wire.
        size = len(value.encode("utf-8", errors="strict"))
        valid = size <= (400 if name else 4000) and "\x00" not in value
        if name:
            valid = valid and bool(value.strip())
    except (UnicodeError, AttributeError):
        pass
    if not valid:
        raise CustomApiClientError("CUSTOM_API_RESPONSE_INVALID") from None
    return value


def _references(
    profiles: tuple[CustomApiProfileRef, ...],
) -> list[pb.CustomApiRuntimeProfileRef]:
    if (
        type(profiles) is not tuple
        or not 1 <= len(profiles) <= 32
        or any(type(p) is not CustomApiProfileRef for p in profiles)
        or len({p.profile_id for p in profiles}) != len(profiles)
    ):
        raise CustomApiClientError("CUSTOM_API_PROFILE_INVALID")
    return [
        pb.CustomApiRuntimeProfileRef(
            profile_id=p.profile_id, expected_state_version=p.expected_state_version
        )
        for p in profiles
    ]


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class PlatformCustomApiClient:
    sender: CustomApiSender = field(repr=False)
    tenant_id: str
    run_id: str
    execution_session_id: str

    def __post_init__(self) -> None:
        if re.fullmatch(r"[\x21-\x7e]{1,255}", self.tenant_id) is None:
            raise CustomApiClientError("CUSTOM_API_IDENTITY_INVALID")
        custom_api_identity(self.run_id)
        custom_api_identity(self.execution_session_id)

    async def _send(
        self, request: CustomApiRequest, *, deadline_monotonic: float
    ) -> (
        pb.DescribeCustomApiEnvironmentResponse | pb.AcquireCustomApiEnvironmentResponse
    ):
        remaining = deadline_monotonic - time.monotonic()
        if not math.isfinite(remaining) or remaining <= 0:
            raise CustomApiClientError("CUSTOM_API_DEADLINE_EXCEEDED")
        binding = project_request_binding(tenant_ref=self.tenant_id, request=request)
        expected = (
            pb.DescribeCustomApiEnvironmentResponse
            if type(request) is pb.DescribeCustomApiEnvironmentRequest
            else pb.AcquireCustomApiEnvironmentResponse
        )
        cancelled = False
        delivered = False
        response: object = None
        try:
            # Bound the complete sender even when a transport double ignores its timeout.
            async with asyncio.timeout(min(10, remaining)):
                response = await self.sender.send(request, timeout_s=min(10, remaining))
            if isinstance(response, expected) and type(response) is expected:
                if (
                    response.to_binary()
                    == response.to_binary(write_unknown_fields=False)
                    and response.request_binding_sha256 == binding.sha256
                    and response.run_id == self.run_id
                    and response.execution_session_id == self.execution_session_id
                    and time.monotonic() < deadline_monotonic
                ):
                    delivered = True
                    return response
        except asyncio.CancelledError:
            cancelled = True
        except PlatformCallError as error:
            if error.code in {
                "PLATFORM_FAILED_PRECONDITION",
                "PLATFORM_NOT_FOUND",
            }:
                raise CustomApiConfigurationError(
                    "CUSTOM_API_CONFIGURATION_INVALID"
                ) from None
            if error.code in {
                "PLATFORM_UNAVAILABLE",
                "PLATFORM_DEADLINE_EXCEEDED",
            }:
                raise CustomApiDependencyError("CUSTOM_API_OWNER_UNAVAILABLE") from None
            raise CustomApiClientError("CUSTOM_API_OWNER_UNAVAILABLE") from None
        except Exception:
            pass
        finally:
            if not delivered:
                _clear_material(response)
                response = None
        # No upstream exception/context retains response bodies or credential bytes.
        if cancelled:
            raise asyncio.CancelledError from None
        raise CustomApiClientError("CUSTOM_API_OWNER_UNAVAILABLE") from None

    async def describe(
        self, profiles: tuple[CustomApiProfileRef, ...], *, deadline_monotonic: float
    ) -> tuple[CustomApiProfileDescription, ...]:
        request = pb.DescribeCustomApiEnvironmentRequest(
            request_id=str(uuid4()),
            invocation_id=str(uuid4()),
            profiles=_references(profiles),
        )
        response = await self._send(request, deadline_monotonic=deadline_monotonic)
        if not isinstance(response, pb.DescribeCustomApiEnvironmentResponse) or len(
            response.profiles
        ) != len(profiles):
            raise CustomApiClientError("CUSTOM_API_RESPONSE_INVALID")
        expected = {p.profile_id: p.expected_state_version for p in profiles}
        seen: set[str] = set()
        environment: set[str] = set()
        result: list[CustomApiProfileDescription] = []
        for profile in response.profiles:
            if (
                profile.to_binary() != profile.to_binary(write_unknown_fields=False)
                or profile.profile_id in seen
                or expected.get(profile.profile_id) != profile.state_version
            ):
                raise CustomApiClientError("CUSTOM_API_RESPONSE_INVALID")
            # User-owned descriptive data, not system instruction authority.
            _description_text(profile.name, name=True)
            _description_text(profile.instructions, name=False)
            seen.add(profile.profile_id)
            for name in profile.environment_names:
                _environment_name(name)
                if name in environment or len(environment) >= 128:
                    raise CustomApiClientError("CUSTOM_API_RESPONSE_INVALID")
                environment.add(name)
            result.append(
                CustomApiProfileDescription(
                    profile_id=profile.profile_id,
                    state_version=profile.state_version,
                    name=profile.name,
                    instructions=profile.instructions,
                    environment_names=tuple(profile.environment_names),
                )
            )
        return tuple(result)

    async def acquire(
        self, profiles: tuple[CustomApiProfileRef, ...], *, deadline_monotonic: float
    ) -> tuple[tuple[str, SecretStr], ...]:
        request = pb.AcquireCustomApiEnvironmentRequest(
            request_id=str(uuid4()),
            invocation_id=str(uuid4()),
            profiles=_references(profiles),
        )
        response = await self._send(request, deadline_monotonic=deadline_monotonic)
        if not isinstance(response, pb.AcquireCustomApiEnvironmentResponse):
            raise CustomApiClientError("CUSTOM_API_RESPONSE_INVALID")
        result: list[tuple[str, SecretStr]] = []
        names: set[str] = set()
        size = 0
        valid = True
        try:
            if len(response.environment) > 128:
                raise ValueError
            for item in response.environment:
                name = _environment_name(item.name)
                if (
                    item.to_binary() != item.to_binary(write_unknown_fields=False)
                    or name in names
                    or type(item.value) is not bytes
                    or not 1 <= len(item.value) <= 4096
                    or any(c in item.value for c in (0, 10, 13))
                ):
                    raise ValueError
                value = item.value.decode("utf-8", errors="strict")
                size += len(name.encode("utf-8")) + len(item.value)
                if size > 65536:
                    raise ValueError
                names.add(name)
                result.append((name, SecretStr(value)))
        except Exception:
            valid = False
        finally:
            # Drop protobuf-held material immediately; immutable bytes may have other
            # copies, so this is lifetime minimization, not a zeroization claim.
            _clear_material(response)
            value = ""
        if not valid:
            result.clear()
            raise CustomApiClientError("CUSTOM_API_RESPONSE_INVALID") from None
        return tuple(result)


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class CustomApiEnvironmentProvider:
    client: PlatformCustomApiClient = field(repr=False)
    profiles: tuple[CustomApiProfileRef, ...]

    def __post_init__(self) -> None:
        _references(self.profiles)

    async def acquire(
        self, *, deadline_monotonic: float
    ) -> tuple[tuple[str, SecretStr], ...]:
        return await self.client.acquire(
            self.profiles, deadline_monotonic=deadline_monotonic
        )
