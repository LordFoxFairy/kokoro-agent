from __future__ import annotations

import ssl
from pathlib import Path

import pytest
from pydantic import ValidationError

from kokoro_agent.config import AppConfig
from kokoro_agent.evidence.server import EvidenceServerSettings, build_hypercorn_config


def _tls_file(tmp_path: Path, name: str) -> str:
    path = tmp_path / name
    path.write_text("fixture")
    return str(path)


def _config(tmp_path: Path, **overrides: str) -> AppConfig:
    source = {
        "KOKORO_AGENT_EVIDENCE_TLS_CERT": _tls_file(tmp_path, "server.crt"),
        "KOKORO_AGENT_EVIDENCE_TLS_KEY": _tls_file(tmp_path, "server.key"),
        "KOKORO_AGENT_EVIDENCE_CALLER_CA_BUNDLE": _tls_file(tmp_path, "callers.pem"),
        **overrides,
    }
    return AppConfig.from_env(source)


def test_server_is_http2_only_and_requires_client_certificates(tmp_path: Path) -> None:
    settings = EvidenceServerSettings.from_config(_config(tmp_path))
    server = build_hypercorn_config(settings)

    assert server.bind == ["0.0.0.0:8443"]
    assert server.alpn_protocols == ["h2"]
    assert server.verify_mode is ssl.VerifyMode.CERT_REQUIRED
    assert server.verify_flags == (
        ssl.VerifyFlags.VERIFY_X509_STRICT
        | ssl.VerifyFlags.VERIFY_X509_PARTIAL_CHAIN
    )
    assert settings.allowed_callers == frozenset(
        {"kokoro-session", "kokoro-platform"}
    )


def test_server_rejects_plaintext_or_partial_mtls_configuration() -> None:
    with pytest.raises(ValueError, match="EVIDENCE_MTLS_REQUIRED"):
        EvidenceServerSettings.from_config(AppConfig.from_env({}))


def test_server_rejects_caller_set_drift(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        KOKORO_AGENT_EVIDENCE_ALLOWED_CALLERS="kokoro-session,kokoro-platform,other",
    )
    with pytest.raises(ValidationError, match="EVIDENCE_CALLER_ALLOWLIST_INVALID"):
        EvidenceServerSettings.from_config(config)


def test_server_rejects_missing_tls_files(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config = config.model_copy(
        update={"evidence_tls_key": str(tmp_path / "missing.key")}
    )
    with pytest.raises(ValidationError, match="EVIDENCE_TLS_FILE_MISSING"):
        EvidenceServerSettings.from_config(config)
