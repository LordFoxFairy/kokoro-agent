"""Worker owner-client composition remains optional and outside Run input."""

from kokoro_agent.worker.dependencies import WorkerClients
from kokoro_agent.worker.main import worker_model_resolver
from kokoro_agent.clients.system import SystemModelClient
from kokoro_agent.config import AppConfig
import pytest


async def test_default_worker_clients_leave_agent_core_available() -> None:
    clients = WorkerClients()
    assert clients.skill_client is None
    assert clients.skill_reader is None
    assert clients.mcp is None
    assert clients.delivery is None
    assert clients.model_resolver is None


async def test_worker_requires_explicit_system_and_gateway_configuration() -> None:
    with pytest.raises(ValueError, match="System"):
        async with worker_model_resolver(AppConfig.from_env({}), None):
            raise AssertionError("unconfigured worker must not become ready")


async def test_worker_constructs_real_system_resolver() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_SYSTEM_BASE_URL": "http://system.test",
            "KOKORO_INTERNAL_SECRET_AGENT": "internal-secret",
            "KOKORO_LITELLM_ENABLED": "1",
            "KOKORO_LITELLM_BASE_URL": "http://gateway.test",
            "KOKORO_LITELLM_API_KEY": "gateway-secret",
            "KOKORO_SYSTEM_TIMEOUT_S": "2",
        }
    )
    assert config.system_timeout_s == 2
    async with worker_model_resolver(config, None) as resolver:
        assert isinstance(resolver, SystemModelClient)
