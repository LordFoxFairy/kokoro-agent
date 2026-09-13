"""Configuration logs expose only explicitly safe deployment facts."""

from __future__ import annotations

import logging

from pydantic import SecretStr
import pytest

from kokoro_agent.config import AppConfig, log_config_summary


def test_worker_summary_does_not_dump_urls_or_credentials(
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = AppConfig(
        redis_url="redis://REDIS_SENTINEL",
        database_url="postgres://DB_SENTINEL",
        internal_secret_agent=SecretStr("TOKEN_SENTINEL"),
    )
    with caplog.at_level(logging.INFO):
        log_config_summary(config, logging.getLogger("worker-config-test"))
    assert "REDIS_SENTINEL" not in caplog.text
    assert "DB_SENTINEL" not in caplog.text
    assert "TOKEN_SENTINEL" not in caplog.text
    assert "configured" in caplog.text
