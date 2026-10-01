"""Worker owner-client composition remains optional and outside Run input."""

import pytest
from pydantic import ValidationError

from support.fakes import FakeRunRepository
from kokoro_agent.clients.system import SystemModelClient
from kokoro_agent.config import AppConfig
from kokoro_agent.worker.dependencies import WorkerClients
from kokoro_agent.worker.main import worker_model_resolver, worker_storage_delivery


def test_storage_worker_configuration_is_all_or_none() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_STORAGE_BASE_URL": "http://127.0.0.1:4010",
            "KOKORO_STORAGE_OBJECT_ORIGIN": "http://127.0.0.1:9000",
            "KOKORO_STORAGE_SERVICE_SECRET": "test-secret",
        }
    )
    assert config.storage_base_url == "http://127.0.0.1:4010"
    assert config.storage_object_origin == "http://127.0.0.1:9000"
    assert config.storage_service_secret is not None
    with pytest.raises(ValidationError):
        AppConfig.from_env({"KOKORO_STORAGE_BASE_URL": "http://127.0.0.1:4010"})


async def test_default_worker_clients_leave_agent_core_available() -> None:
    clients = WorkerClients()
    assert not hasattr(clients, "skill_client")
    assert not hasattr(clients, "skill_reader")
    assert clients.mcp is None
    assert clients.delivery is None
    assert clients.model_resolver is None


async def test_standard_worker_constructs_storage_adapter_when_configured() -> None:
    config = AppConfig.from_env(
        {
            "KOKORO_STORAGE_BASE_URL": "http://127.0.0.1:4010",
            "KOKORO_STORAGE_OBJECT_ORIGIN": "http://127.0.0.1:9000",
            "KOKORO_STORAGE_SERVICE_SECRET": "test-secret",
        }
    )
    async with worker_storage_delivery(config, None, FakeRunRepository()) as client:
        assert client is not None
    async with worker_storage_delivery(
        AppConfig.from_env({}), None, FakeRunRepository()
    ) as client:
        assert client is None


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


def test_skill_object_origin_does_not_require_storage_write_credentials() -> None:
    config = AppConfig.from_env(
        {"KOKORO_STORAGE_OBJECT_ORIGIN": "https://objects.test"}
    )
    assert config.storage_object_origin == "https://objects.test"
    assert config.storage_base_url is None


def test_worker_dependencies_has_one_required_runtime_policy() -> None:
    from dataclasses import fields, MISSING
    from kokoro_agent.worker.dependencies import WorkerDependencies

    names = {field.name: field for field in fields(WorkerDependencies)}
    assert "runtime_policy" in names
    assert "manifest" in names
    assert "run_token_budget" not in names
    assert names["runtime_policy"].default is MISSING
    assert names["manifest"].default is MISSING


async def test_real_worker_composition_sends_same_policy_to_factory_and_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import asynccontextmanager
    from collections.abc import AsyncGenerator
    from importlib import import_module
    from unittest.mock import AsyncMock, Mock
    import asyncio
    from kokoro_agent.worker.dependencies import WorkerDependencies

    module = import_module("kokoro_agent.worker.main")
    config = AppConfig.from_env(
        {"KOKORO_RUN_TOKEN_BUDGET": "71", "KOKORO_RECURSION_LIMIT": "37"}
    )
    captures: list[WorkerDependencies] = []

    @asynccontextmanager
    async def resource(*args: object, **kwargs: object) -> AsyncGenerator[object, None]:
        yield Mock()

    for name in (
        "worker_platform_runtime",
        "worker_model_resolver",
        "make_checkpointer",
        "make_run_repository",
        "worker_storage_delivery",
        "make_memory_store",
        "make_chat_repository",
    ):
        monkeypatch.setattr(module, name, resource)

    def stream(settings: object) -> Mock:
        return Mock()

    monkeypatch.setattr(module, "make_stream", stream)

    def factory(dependencies: WorkerDependencies) -> Mock:
        captures.append(dependencies)
        return Mock()

    monkeypatch.setattr(module, "AgentFactory", factory)
    supervisor = Mock()
    supervisor.serve = AsyncMock(return_value=None)
    supervisor_constructor = Mock(return_value=supervisor)
    monkeypatch.setattr(module, "RunSupervisor", supervisor_constructor)
    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", Mock())
    await module.serve(config)
    assert len(captures) == 1
    policy = captures[0].runtime_policy
    assert policy.run_token_budget == 71
    assert policy.recursion_limit == 37
    assert (
        supervisor_constructor.call_args.kwargs["recursion_limit"]
        == policy.recursion_limit
    )
    # The unchanged executor passes that exact limit into the native config.
    from kokoro_agent.execution import run_agent

    assert (
        getattr(run_agent, "_config")("thread", None, policy.recursion_limit)[
            "recursion_limit"
        ]
        == 37
    )
