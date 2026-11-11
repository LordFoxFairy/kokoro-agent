from dataclasses import replace
from pathlib import Path
from typing import Literal
import asyncio

import pytest
from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.state import StateBackend
from support.fakes import FakeRunRepository, request

from kokoro_agent.clients.custom_api_runtime import (
    CustomApiClientError,
    CustomApiProfileDescription,
)
from kokoro_agent.config import AppConfig
from kokoro_agent.execution.custom_api_selection import (
    custom_api_instructions,
    custom_api_workspace,
    feature_for_custom_api,
)
from kokoro_agent.features.chat import CHAT_FEATURE
from kokoro_agent.protocol.control import CustomApiProfileSelection
from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.sandbox import backend as backend_module
from pydantic import SecretStr


def selected(run_id: str = "run-api"):
    return request(run_id).model_copy(
        update={
            "feature_key": "chat",
            "selected_custom_api_profiles": (
                CustomApiProfileSelection(profile_id="api-1", expected_state_version=3),
            ),
        }
    )


def test_only_selected_chat_changes_executor_and_catalog_is_unchanged(tmp_path: Path):
    settings = AppConfig.from_env(
        {
            "KOKORO_DOCKER_IMAGE": "fixture",
            "KOKORO_AGENT_LOCAL_SHELL_ROOT": str(tmp_path),
        }
    ).sandbox
    assert (
        feature_for_custom_api(CHAT_FEATURE, request("ordinary"), settings)
        is CHAT_FEATURE
    )
    chosen = feature_for_custom_api(CHAT_FEATURE, selected(), settings)
    assert chosen.agents[0].backend == "docker"
    assert CHAT_FEATURE.agents[0].backend == "state"
    assert chosen.agents[0].permissions == CHAT_FEATURE.agents[0].permissions


@pytest.mark.parametrize("kind", ["local_shell", "custom"])
def test_selected_api_never_falls_back_to_host_or_custom_backend(
    kind: Literal["local_shell", "custom"],
):
    feature = replace(
        CHAT_FEATURE, agents=(replace(CHAT_FEATURE.agents[0], backend=kind),)
    )
    with pytest.raises(CustomApiClientError, match="ISOLATED"):
        feature_for_custom_api(feature, selected(), AppConfig.from_env({}).sandbox)


def test_missing_isolated_configuration_fails_before_any_io():
    with pytest.raises(CustomApiClientError, match="UNAVAILABLE"):
        feature_for_custom_api(CHAT_FEATURE, selected(), AppConfig.from_env({}).sandbox)
    settings = AppConfig.from_env({"KOKORO_E2B_API_KEY": "test-key"}).sandbox
    assert (
        feature_for_custom_api(CHAT_FEATURE, selected(), settings).agents[0].backend
        == "e2b"
    )


def test_selected_run_workspace_is_a_sibling_of_the_ordinary_mount(tmp_path: Path):
    settings = AppConfig.from_env(
        {"KOKORO_AGENT_LOCAL_SHELL_ROOT": str(tmp_path)}
    ).sandbox
    session_id = "conv_00000000-0000-4000-8000-000000000001"
    ordinary_run = request("ordinary", session_id=session_id)
    first = selected("run-a").model_copy(update={"session_id": session_id})
    ordinary_key = custom_api_workspace(ordinary_run)
    selected_key = custom_api_workspace(first)
    assert ordinary_key == f"{RunScope.of(ordinary_run).namespace}:{session_id}"
    ordinary = backend_module.make_backend(
        "local_shell", settings, workspace=ordinary_key
    )
    selected_files = backend_module.make_backend(
        "local_shell", settings, workspace=selected_key
    )
    assert isinstance(ordinary, LocalShellBackend)
    assert isinstance(selected_files, LocalShellBackend)
    assert selected_files.cwd.is_dir()
    assert selected_files.cwd.parent == ordinary.cwd.parent == tmp_path.resolve()
    assert selected_files.cwd != ordinary.cwd
    assert selected_key == custom_api_workspace(first.model_copy())
    for variant in (
        first.model_copy(update={"run_id": "run-b"}),
        first.model_copy(
            update={"session_id": "conv_00000000-0000-4000-8000-000000000002"}
        ),
        first.model_copy(
            update={
                "execution_identity": first.execution_identity.model_copy(
                    update={"tenant_ref": "other-tenant"}
                )
            }
        ),
        first.model_copy(
            update={
                "execution_identity": first.execution_identity.model_copy(
                    update={
                        "subject": first.execution_identity.subject.model_copy(
                            update={"opaque_ref": "other-subject"}
                        )
                    }
                )
            }
        ),
    ):
        assert selected_key != custom_api_workspace(variant)


def test_description_is_data_and_only_environment_names_are_in_model_context():
    description = CustomApiProfileDescription(
        profile_id="api-1",
        state_version=3,
        name="Example API",
        instructions="Use /v1/items",
        environment_names=("EXAMPLE_TOKEN",),
    )
    prompt = custom_api_instructions((description,))
    assert "user-provided API documentation" in prompt
    assert "EXAMPLE_TOKEN" in prompt and "/v1/items" in prompt
    assert "profile_id" not in prompt
    assert custom_api_instructions(()) == ""


class Environment:
    async def acquire(
        self, *, deadline_monotonic: float
    ) -> tuple[tuple[str, SecretStr], ...]:
        return (("API_TOKEN", SecretStr("fixture-secret")),)


async def test_sandbox_assembly_carries_provider_and_original_event_loop(
    monkeypatch: pytest.MonkeyPatch,
):
    contexts: list[backend_module.SandboxContext] = []
    provider = Environment()

    def connector(context: backend_module.SandboxContext) -> StateBackend:
        contexts.append(context)
        return StateBackend()

    monkeypatch.setattr(backend_module, "_CONNECTORS", {"e2b": connector})
    run = selected()
    store = FakeRunRepository()
    lease = await store.try_claim(run)
    assert lease is not None
    await backend_module.make_backend_for_run(
        "e2b",
        AppConfig.from_env({}).sandbox,
        workspace=custom_api_workspace(run),
        run_id=run.run_id,
        lease=lease,
        sandbox_store=store,
        environment_provider=provider,
    )
    assert contexts[0].environment_provider is provider
    assert contexts[0].environment_loop is asyncio.get_running_loop()


@pytest.mark.parametrize("kind", ["state", "local_shell", "custom"])
async def test_provider_is_rejected_for_nonisolated_executor_before_store(
    kind: Literal["state", "local_shell", "custom"],
):
    class ForbiddenStore(FakeRunRepository):
        async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool:
            pytest.fail("must reject before any store access")

    with pytest.raises(ValueError, match="isolated"):
        await backend_module.make_backend_for_run(
            kind,
            AppConfig.from_env({}).sandbox,
            workspace="unused",
            run_id="unused",
            lease=LeaseFence(owner="worker", generation=1),
            sandbox_store=ForbiddenStore(),
            environment_provider=Environment(),
        )


def test_config_only_copies_explicit_docker_cli_context():
    config = AppConfig.from_env(
        {
            "HOME": "/fixture/home",
            "DOCKER_CONTEXT": "desktop-linux",
            "OPENAI_API_KEY": "do-not-inherit",
            "ARBITRARY_SECRET": "do-not-inherit-either",
        }
    )
    assert dict(config.sandbox.docker.cli_environment) == {
        "HOME": "/fixture/home",
        "DOCKER_CONTEXT": "desktop-linux",
    }
