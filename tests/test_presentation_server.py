from __future__ import annotations

import ssl
from pathlib import Path

import pytest
from pydantic import ValidationError

from kokoro_agent.config import AppConfig
from kokoro_agent.presentation.server import (
    PresentationServerSettings,
    build_hypercorn_config,
)


def _tls_file(tmp_path: Path, name: str) -> str:
    path = tmp_path / name
    path.write_text("fixture")
    return str(path)


def _config(tmp_path: Path, **overrides: str) -> AppConfig:
    return AppConfig.from_env(
        {
            "KOKORO_AGENT_PRESENTATION_TLS_CERT": _tls_file(
                tmp_path, "server.crt"
            ),
            "KOKORO_AGENT_PRESENTATION_TLS_KEY": _tls_file(
                tmp_path, "server.key"
            ),
            "KOKORO_AGENT_PRESENTATION_CALLER_CA_BUNDLE": _tls_file(
                tmp_path, "callers.pem"
            ),
            **overrides,
        }
    )


def _settings(config: AppConfig) -> PresentationServerSettings:
    return PresentationServerSettings.from_values(
        host=config.presentation_host,
        port=config.presentation_port,
        tls_cert=config.presentation_tls_cert,
        tls_key=config.presentation_tls_key,
        caller_ca_bundle=config.presentation_caller_ca_bundle,
        allowed_callers=config.presentation_allowed_callers,
    )


def test_server_is_http2_only_and_accepts_only_session_mtls(tmp_path: Path) -> None:
    settings = _settings(_config(tmp_path))
    server = build_hypercorn_config(settings)

    assert server.bind == ["0.0.0.0:8444"]
    assert server.alpn_protocols == ["h2"]
    assert server.verify_mode is ssl.VerifyMode.CERT_REQUIRED
    assert server.verify_flags == (
        ssl.VerifyFlags.VERIFY_X509_STRICT
        | ssl.VerifyFlags.VERIFY_X509_PARTIAL_CHAIN
    )
    assert settings.allowed_callers == frozenset({"kokoro-session"})


def test_server_rejects_plaintext_or_partial_mtls_configuration() -> None:
    with pytest.raises(ValueError, match="PRESENTATION_MTLS_REQUIRED"):
        _settings(AppConfig.from_env({}))


def test_server_rejects_caller_set_drift(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        KOKORO_AGENT_PRESENTATION_ALLOWED_CALLERS="kokoro-session,kokoro-platform",
    )
    with pytest.raises(
        ValidationError, match="PRESENTATION_CALLER_ALLOWLIST_INVALID"
    ):
        _settings(config)


def test_server_rejects_missing_tls_files(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(
        update={"presentation_tls_key": str(tmp_path / "missing.key")}
    )
    with pytest.raises(ValidationError, match="PRESENTATION_TLS_FILE_MISSING"):
        _settings(config)
