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
    from unittest.mock import Mock
    from kokoro_agent.execution.execution_proof_signer import ExecutionProofSigner
    from kokoro_agent.execution.execution_proof_supplier import (
        ExecutionProofLeaseReadPort,
    )

    async with worker_platform_runtime(
        AppConfig.from_env({}),
        signer=Mock(spec=ExecutionProofSigner),
        lease_reader=Mock(spec=ExecutionProofLeaseReadPort),
    ) as runtime:
        assert runtime is None


def test_billing_signing_does_not_require_platform_oauth_credentials() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_AGENT_EXECUTION_PROOF_ISSUER": "urn:unit:agent",
            "KOKORO_AGENT_EXECUTION_PROOF_PRIVATE_KEY_FILE": "/tmp/unit-key.pem",
            "KOKORO_AGENT_EXECUTION_PROOF_WORKER_ACTIVE_KID": "unit-key",
            "KOKORO_AGENT_EXECUTION_PROOF_WORKER_ACTIVE_JWK_THUMBPRINT_SHA256": "a"
            * 43,
        }
    )
    assert (
        config.platform_base_url
        is config.iam_base_url
        is config.platform_credentials_file
        is None
    )
