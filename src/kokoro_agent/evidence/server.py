"""Production ConnectRPC host for Agent-owned durable execution evidence."""

from __future__ import annotations

import ssl
from pathlib import Path
from typing import Self

from hypercorn.config import Config as HypercornConfig
from pydantic import BaseModel, ConfigDict, Field, model_validator

from kokoro_agent.config import AppConfig

EXPECTED_CALLERS = frozenset({"kokoro-session", "kokoro-platform"})


class EvidenceServerSettings(BaseModel):
    """Fail-closed server settings; the CA bundle is the cryptographic caller allowlist."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    tls_cert: Path
    tls_key: Path
    caller_ca_bundle: Path
    allowed_callers: frozenset[str]

    @model_validator(mode="after")
    def validate_security_boundary(self) -> Self:
        if self.allowed_callers != EXPECTED_CALLERS:
            raise ValueError("EVIDENCE_CALLER_ALLOWLIST_INVALID")
        for path in (self.tls_cert, self.tls_key, self.caller_ca_bundle):
            if not path.is_file():
                raise ValueError("EVIDENCE_TLS_FILE_MISSING")
        return self

    @classmethod
    def from_config(cls, config: AppConfig) -> Self:
        if (
            config.evidence_tls_cert is None
            or config.evidence_tls_key is None
            or config.evidence_caller_ca_bundle is None
        ):
            raise ValueError("EVIDENCE_MTLS_REQUIRED")
        callers = frozenset(
            caller.strip()
            for caller in config.evidence_allowed_callers.split(",")
            if caller.strip()
        )
        return cls(
            host=config.evidence_host,
            port=config.evidence_port,
            tls_cert=Path(config.evidence_tls_cert),
            tls_key=Path(config.evidence_tls_key),
            caller_ca_bundle=Path(config.evidence_caller_ca_bundle),
            allowed_callers=callers,
        )


def build_hypercorn_config(settings: EvidenceServerSettings) -> HypercornConfig:
    """Build an HTTP/2-only mTLS listener with no plaintext fallback."""
    config = HypercornConfig()
    host = f"[{settings.host}]" if ":" in settings.host else settings.host
    config.bind = [f"{host}:{settings.port}"]
    config.certfile = str(settings.tls_cert)
    config.keyfile = str(settings.tls_key)
    config.ca_certs = str(settings.caller_ca_bundle)
    config.verify_mode = ssl.VerifyMode.CERT_REQUIRED
    config.verify_flags = (
        ssl.VerifyFlags.VERIFY_X509_STRICT
        | ssl.VerifyFlags.VERIFY_X509_PARTIAL_CHAIN
    )
    config.alpn_protocols = ["h2"]
    return config
