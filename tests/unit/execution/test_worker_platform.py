"""Worker-only Platform resources have one explicit lifetime."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kokoro_agent.config import AppConfig
from kokoro_agent.worker.platform import worker_platform_runtime


def test_partial_platform_configuration_is_rejected() -> None:
    with pytest.raises(ValidationError):
        AppConfig.from_env({"KOKORO_PLATFORM_BASE_URL": "https://platform.test"})


async def test_unconfigured_worker_has_no_platform_resources() -> None:
    async with worker_platform_runtime(AppConfig.from_env({})) as runtime:
        assert runtime is None
