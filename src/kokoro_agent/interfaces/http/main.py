"""HTTP-only process configuration and composition root."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import ipaddress
import logging
import os
import signal
import threading
import time
from types import FrameType

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from kokoro_agent.config_file import load_http_config_file
from kokoro_agent.infrastructure.postgres_run_repository import (
    DEFAULT_LEASE_TTL_S,
    RunRepositorySettings,
)
from kokoro_agent.interfaces.http.execution_proof_jwks import (
    ExecutionProofJwksState,
    HttpExecutionProofConfig,
    load_execution_proof_jwks,
)
from kokoro_agent.interfaces.http.server import create_http_server
from kokoro_agent.streams.factory import StreamSettings

LOGGER = logging.getLogger(__name__)
_PROOF_PATH = "KOKORO_AGENT_EXECUTION_PROOF_PUBLIC_JWKS_FILE"
_PROOF_KID = "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_KID"
_PROOF_THUMBPRINT = "KOKORO_AGENT_EXECUTION_PROOF_HTTP_ACTIVE_JWK_THUMBPRINT_SHA256"
_DRAIN_TIMEOUT_S = 2.0


class AgentHttpConfigError(RuntimeError):
    """Stable sanitized failure at the HTTP configuration boundary."""


class AgentHttpConfig(BaseModel):
    """The complete and only configuration consumed by the HTTP process."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    host: str = "127.0.0.1"
    port: int = Field(default=4401, ge=1, le=65535)
    redis_url: str = Field(default="redis://127.0.0.1:56380/9", repr=False)
    database_url: str = Field(
        default="postgresql://kokoro:kokoro@127.0.0.1:55433/kokoro_worker_agent",
        repr=False,
    )
    database_schema: str = Field(default="kokoro_agent", repr=False)
    lease_ttl_s: int = Field(default=DEFAULT_LEASE_TTL_S, gt=0)
    internal_secret_agent: SecretStr | None = Field(default=None, repr=False)
    execution_proof: HttpExecutionProofConfig | None = Field(default=None, repr=False)

    @field_validator("host", "redis_url", "database_url", "database_schema")
    @classmethod
    def _nonempty(cls, value: str) -> str:
        if type(value) is not str or not value.strip():
            raise ValueError("HTTP business configuration is invalid")
        return value.strip()

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> AgentHttpConfig:
        config: AgentHttpConfig | None = None
        failed = False
        try:
            config = cls._from_env(source)
        except Exception:
            failed = True
        if failed or config is None:
            raise AgentHttpConfigError("Agent HTTP configuration is invalid")
        return config

    @classmethod
    def _from_env(cls, source: Mapping[str, str]) -> AgentHttpConfig:
        file_values = load_http_config_file(source.get("KOKORO_AGENT_CONFIG"))
        proof = _public_descriptor(source)

        def value(name: str, default: object) -> object:
            environment = source.get(name)
            return (
                file_values.get(name, default) if environment is None else environment
            )

        return cls.model_validate(
            {
                "host": value("KOKORO_AGENT_HTTP_HOST", "127.0.0.1"),
                "port": value("KOKORO_AGENT_HTTP_PORT", 4401),
                "redis_url": value("KOKORO_REDIS_URL", "redis://127.0.0.1:56380/9"),
                "database_url": value(
                    "KOKORO_AGENT_DATABASE_URL",
                    "postgresql://kokoro:kokoro@127.0.0.1:55433/kokoro_worker_agent",
                ),
                "database_schema": value(
                    "KOKORO_AGENT_DATABASE_SCHEMA", "kokoro_agent"
                ),
                "lease_ttl_s": value("KOKORO_LEASE_TTL_S", DEFAULT_LEASE_TTL_S),
                "internal_secret_agent": value("KOKORO_INTERNAL_SECRET_AGENT", None),
                "execution_proof": proof,
            }
        )

    @property
    def stream(self) -> StreamSettings:
        return StreamSettings(redis_url=self.redis_url)

    @property
    def run_repository(self) -> RunRepositorySettings:
        return RunRepositorySettings(
            database_url=self.database_url,
            schema_name=self.database_schema,
            lease_ttl_ms=self.lease_ttl_s * 1000,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class AgentHttpComposition:
    config: AgentHttpConfig = field(repr=False)
    jwks: ExecutionProofJwksState = field(repr=False)


def _public_descriptor(source: Mapping[str, str]) -> HttpExecutionProofConfig | None:
    path = source.get(_PROOF_PATH)
    kid = source.get(_PROOF_KID)
    thumbprint = source.get(_PROOF_THUMBPRINT)
    descriptor: HttpExecutionProofConfig | None = None
    failed = False
    if path is None and kid is None and thumbprint is None:
        return None
    try:
        if type(path) is not str or type(kid) is not str or type(thumbprint) is not str:
            raise ValueError
        descriptor = HttpExecutionProofConfig(
            public_jwks_file=path,
            active_kid=kid,
            active_jwk_thumbprint_sha256=thumbprint,
        )
    except Exception:
        failed = True
    if failed:
        return None
    return descriptor


def build_http_composition(source: Mapping[str, str]) -> AgentHttpComposition:
    config = AgentHttpConfig.from_env(source)
    jwks = (
        ExecutionProofJwksState.unavailable()
        if config.execution_proof is None
        else load_execution_proof_jwks(config.execution_proof)
    )
    return AgentHttpComposition(config=config, jwks=jwks)


def log_http_config_summary(
    composition: AgentHttpComposition, logger: logging.Logger
) -> None:
    config = composition.config
    secret = config.internal_secret_agent
    logger.info(
        "kokoro-agent HTTP config: %s",
        {
            "bind_mode": _bind_mode(config.host),
            "port": config.port,
            "lease_ttl_s": config.lease_ttl_s,
            "service_auth_configured": bool(
                secret is not None and secret.get_secret_value().strip()
            ),
            "proof_descriptor_configured": config.execution_proof is not None,
            "ring_available": composition.jwks.available,
        },
    )


def _bind_mode(host: str) -> str:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return "named"
    if address.is_loopback:
        return "loopback"
    if address.is_unspecified:
        return "wildcard"
    return "address"


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    composition: AgentHttpComposition | None = None
    failed = False
    try:
        composition = build_http_composition(os.environ)
    except AgentHttpConfigError:
        failed = True
    if failed or composition is None:
        LOGGER.error("Agent HTTP configuration is invalid")
        raise SystemExit(2)
    log_http_config_summary(composition, LOGGER)
    server = create_http_server(
        composition.config,
        composition.config.host,
        composition.config.port,
        execution_proof_jwks=composition.jwks,
    )
    LOGGER.info(
        "kokoro-agent HTTP ingress listening: bind_mode=%s port=%d",
        _bind_mode(composition.config.host),
        composition.config.port,
    )
    stop = threading.Event()

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        server.start_draining()
        stop.set()

    previous = {
        item: signal.signal(item, request_stop)
        for item in (signal.SIGINT, signal.SIGTERM)
    }
    server_thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.1},
        name="kokoro-agent-http-server",
        daemon=True,
    )
    try:
        server_thread.start()
        while server_thread.is_alive() and not stop.wait(0.1):
            pass
    except KeyboardInterrupt:
        server.start_draining()
        stop.set()
    finally:
        deadline = time.monotonic() + _DRAIN_TIMEOUT_S
        server.start_draining()
        if server_thread.is_alive():
            server.shutdown()
        server.wait_for_active_handlers(max(0.0, deadline - time.monotonic()))
        server.server_close()
        server_thread.join(timeout=2)
        for item, handler in previous.items():
            signal.signal(item, handler)
        if server_thread.is_alive():
            raise RuntimeError("kokoro-agent HTTP shutdown exceeded its bound")


__all__ = [
    "AgentHttpConfigError",
    "AgentHttpConfig",
    "build_http_composition",
    "log_http_config_summary",
    "main",
]
