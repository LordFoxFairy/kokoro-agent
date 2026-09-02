"""custom backend 规格（ADR-010 BYO）：module:attr 加载、契约校验 fail-loud、
config 透传、可选 sandbox_id 生命周期绑定（与 docker/e2b 同构的统一收口）。
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
from deepagents.backends.state import StateBackend

from support.fakes import request
from kokoro_agent.sandbox.backend import SandboxSettings, make_backend_for_run
from kokoro_agent.sandbox.custom_backend import (
    CustomBackendContext,
    CustomBackendSettings,
    connect_custom_sandbox,
)
from kokoro_agent.persistence.repository import RunRepository

SEEN_CONTEXTS: list[CustomBackendContext] = []


class _BoundStateBackend(StateBackend):
    """真 BackendProtocol 实现 + 统一生命周期面：工厂示例兼绑定规格用。"""

    def __init__(self, sandbox_id: str) -> None:
        super().__init__()
        self._bound_id = sandbox_id

    @property
    def sandbox_id(self) -> str:
        return self._bound_id


def make_ok_backend(context: CustomBackendContext) -> StateBackend:
    SEEN_CONTEXTS.append(context)
    return StateBackend()


def make_bound_backend(context: CustomBackendContext) -> _BoundStateBackend:
    SEEN_CONTEXTS.append(context)
    # resume 现场重连自己的沙箱：prior 在即复用其 id（编排层 keep-first 不重绑）。
    return _BoundStateBackend(context.prior_sandbox_id or f"custom_{context.run_id}")


def make_broken_backend(context: CustomBackendContext) -> object:
    return object()


NOT_CALLABLE = "not a factory"


def _settings(ref: str | None, config_path: str | None = None) -> CustomBackendSettings:
    return CustomBackendSettings(factory_ref=ref, config_path=config_path)


def _dispatch_settings(ref: str) -> SandboxSettings:
    return SandboxSettings.model_validate(
        {
            "local_shell_root": None,
            "local_shell_inherit_env": False,
            "local_shell_timeout": 30,
            "local_shell_max_output_bytes": 100000,
            "workspace": None,
            "workspace_s3_access_key": None,
            "workspace_s3_secret_key": None,
            "e2b": {"api_key": None, "template": None, "timeout": 1800},
            "docker": {"image": None, "ttl": 1800},
            "custom": {"factory_ref": ref, "config_path": None},
        }
    )


@pytest.fixture(autouse=True)
def reset_seen() -> None:
    SEEN_CONTEXTS.clear()


# pytest 的 tests 目录不是可导入包：把工厂挂到注册进 sys.modules 的探针模块上，
# module:attr 引用走真实 importlib 路径。
_probe = types.ModuleType("kokoro_custom_probe")
for _name, _attr in (
    ("make_ok_backend", make_ok_backend),
    ("make_bound_backend", make_bound_backend),
    ("make_broken_backend", make_broken_backend),
    ("NOT_CALLABLE", NOT_CALLABLE),
):
    setattr(_probe, _name, _attr)
sys.modules["kokoro_custom_probe"] = _probe


def _connect(ref: str | None, config_path: str | None = None):
    return connect_custom_sandbox(
        _settings(ref, config_path=config_path),
        run_id="run_1",
        workspace="ns:s1",
        workspace_root=None,
        prior_sandbox_id=None,
    )


class TestLoading:
    def test_factory_loads_and_receives_context(self) -> None:
        backend = _connect("kokoro_custom_probe:make_ok_backend")
        assert isinstance(backend, StateBackend)
        assert SEEN_CONTEXTS[0].workspace == "ns:s1"

    @pytest.mark.parametrize(
        ("ref", "match"),
        [
            (None, "KOKORO_CUSTOM_BACKEND"),
            ("no-colon-ref", "pkg.module:attribute"),
            ("kokoro_custom_probe:missing_attr", "missing_attr"),
            ("kokoro_missing_pkg.mod:factory", "kokoro_missing_pkg"),
            ("kokoro_custom_probe:NOT_CALLABLE", "not callable"),
            ("kokoro_custom_probe:make_broken_backend", "must subclass deepagents BackendProtocol"),
        ],
    )
    def test_fail_loud_matrix(self, ref: str | None, match: str) -> None:
        with pytest.raises((ValueError, TypeError, ModuleNotFoundError, AttributeError), match=match):
            _connect(ref)

    def test_config_yaml_passed_through(self, tmp_path: Path) -> None:
        config = tmp_path / "custom.yaml"
        config.write_text("pool: gpu\nreplicas: 2\n")
        connect_custom_sandbox(
            _settings("kokoro_custom_probe:make_ok_backend", str(config)),
            run_id="run_1", workspace="ns:s1", workspace_root=None, prior_sandbox_id=None,
        )
        assert SEEN_CONTEXTS[0].config == {"pool": "gpu", "replicas": 2}

    def test_non_mapping_config_fail_loud(self, tmp_path: Path) -> None:
        config = tmp_path / "custom.yaml"
        config.write_text("- just\n- a list\n")
        with pytest.raises(TypeError, match="mapping"):
            connect_custom_sandbox(
                _settings("kokoro_custom_probe:make_ok_backend", str(config)),
                run_id="run_1", workspace="ns:s1", workspace_root=None, prior_sandbox_id=None,
            )


class TestLifecycleBinding:
    @pytest.mark.asyncio
    async def test_bound_backend_lands_in_repository_and_resume_reuses(
        self, run_repository: RunRepository
    ) -> None:
        settings = _dispatch_settings("kokoro_custom_probe:make_bound_backend")
        # 生产路径：run 先经 supervisor 认领（建 run 文档），沙箱绑定才落账（keep-first）。
        await run_repository.try_claim(request("run_c"), "owner")
        first = await make_backend_for_run(
            "custom", settings, workspace="ns:s1", run_id="run_c", sandbox_store=run_repository
        )
        assert getattr(first, "sandbox_id", None) == "custom_run_c"
        assert await run_repository.get_sandbox_id("run_c") == "custom_run_c"
        # HITL resume：prior 经 context 透传，工厂重连同一沙箱。
        await make_backend_for_run(
            "custom", settings, workspace="ns:s1", run_id="run_c", sandbox_store=run_repository
        )
        assert SEEN_CONTEXTS[1].prior_sandbox_id == "custom_run_c"
        assert await run_repository.get_sandbox_id("run_c") == "custom_run_c"

    @pytest.mark.asyncio
    async def test_unbound_backend_skips_repository(self, run_repository: RunRepository) -> None:
        settings = _dispatch_settings("kokoro_custom_probe:make_ok_backend")
        await run_repository.try_claim(request("run_u"), "owner")
        await make_backend_for_run(
            "custom", settings, workspace="ns:s1", run_id="run_u", sandbox_store=run_repository
        )
        assert await run_repository.get_sandbox_id("run_u") is None
