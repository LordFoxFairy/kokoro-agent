"""custom backend 规格（ADR-010 BYO）：module:attr 加载、契约校验 fail-loud、
config 透传、可选 sandbox_id 生命周期绑定（与 docker/e2b 同构的统一收口）。
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
from deepagents.backends.state import StateBackend

from support.fakes import FakeRunRepository, request
from kokoro_agent.domain.run.models import LeaseFence, SandboxBackendKind
from kokoro_agent.sandbox.backend import SandboxSettings, make_backend_for_run
from kokoro_agent.sandbox.custom_backend import (
    CustomBackendContext,
    CustomBackendSettings,
    connect_custom_sandbox,
)
from kokoro_agent.domain.run.repository import RunRepository

SEEN_CONTEXTS: list[CustomBackendContext] = []
DESTROYED_SANDBOXES: list[str] = []


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


def destroy_bound_backend(sandbox_id: str) -> None:
    DESTROYED_SANDBOXES.append(sandbox_id)


def destroy_failing_backend(sandbox_id: str) -> None:
    DESTROYED_SANDBOXES.append(sandbox_id)
    raise RuntimeError("transient custom teardown failure")


NOT_CALLABLE = "not a factory"


def _settings(
    ref: str | None,
    config_path: str | None = None,
    teardown_ref: str | None = None,
) -> CustomBackendSettings:
    payload: dict[str, object] = {
        "factory_ref": ref,
        "config_path": config_path,
        "teardown_ref": (
            teardown_ref
            if teardown_ref is not None or ref is None
            else "kokoro_custom_probe:destroy_bound_backend"
        ),
    }
    return CustomBackendSettings.model_validate(payload)


def _dispatch_settings(ref: str, *, teardown_ref: str | None = None) -> SandboxSettings:
    custom: dict[str, object] = {
        "factory_ref": ref,
        "config_path": None,
        "teardown_ref": teardown_ref or "kokoro_custom_probe:destroy_bound_backend",
    }
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
            "custom": custom,
        }
    )


@pytest.fixture(autouse=True)
def reset_seen() -> None:
    SEEN_CONTEXTS.clear()
    DESTROYED_SANDBOXES.clear()


# pytest 的 tests 目录不是可导入包：把工厂挂到注册进 sys.modules 的探针模块上，
# module:attr 引用走真实 importlib 路径。
_probe = types.ModuleType("kokoro_custom_probe")
for _name, _attr in (
    ("make_ok_backend", make_ok_backend),
    ("make_bound_backend", make_bound_backend),
    ("make_broken_backend", make_broken_backend),
    ("destroy_bound_backend", destroy_bound_backend),
    ("destroy_failing_backend", destroy_failing_backend),
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
    def test_factory_without_teardown_fails_before_resource_creation(self) -> None:
        with pytest.raises(ValueError, match="KOKORO_CUSTOM_BACKEND_TEARDOWN"):
            CustomBackendSettings.model_validate(
                {
                    "factory_ref": "kokoro_custom_probe:make_ok_backend",
                    "config_path": None,
                    "teardown_ref": None,
                }
            )

    def test_invalid_teardown_fails_before_factory_allocates_resource(self) -> None:
        with pytest.raises(AttributeError, match="missing_teardown"):
            connect_custom_sandbox(
                _settings(
                    "kokoro_custom_probe:make_ok_backend",
                    teardown_ref="kokoro_custom_probe:missing_teardown",
                ),
                run_id="run_1",
                workspace="ns:s1",
                workspace_root=None,
                prior_sandbox_id=None,
            )
        assert SEEN_CONTEXTS == []

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
            (
                "kokoro_custom_probe:make_broken_backend",
                "must subclass deepagents BackendProtocol",
            ),
        ],
    )
    def test_fail_loud_matrix(self, ref: str | None, match: str) -> None:
        with pytest.raises(
            (ValueError, TypeError, ModuleNotFoundError, AttributeError), match=match
        ):
            _connect(ref)

    def test_config_yaml_passed_through(self, tmp_path: Path) -> None:
        config = tmp_path / "custom.yaml"
        config.write_text("pool: gpu\nreplicas: 2\n")
        connect_custom_sandbox(
            _settings("kokoro_custom_probe:make_ok_backend", str(config)),
            run_id="run_1",
            workspace="ns:s1",
            workspace_root=None,
            prior_sandbox_id=None,
        )
        assert SEEN_CONTEXTS[0].config == {"pool": "gpu", "replicas": 2}

    def test_non_mapping_config_fail_loud(self, tmp_path: Path) -> None:
        config = tmp_path / "custom.yaml"
        config.write_text("- just\n- a list\n")
        with pytest.raises(TypeError, match="mapping"):
            connect_custom_sandbox(
                _settings("kokoro_custom_probe:make_ok_backend", str(config)),
                run_id="run_1",
                workspace="ns:s1",
                workspace_root=None,
                prior_sandbox_id=None,
            )


class TestLifecycleBinding:
    @pytest.mark.asyncio
    async def test_bound_backend_lands_in_repository_and_resume_reuses(
        self, run_repository: RunRepository
    ) -> None:
        settings = _dispatch_settings("kokoro_custom_probe:make_bound_backend")
        # 生产路径：run 先经 supervisor 认领（建 run 文档），沙箱绑定才落账（keep-first）。
        lease = await run_repository.try_claim(request("run_c"), "owner")
        assert lease is not None
        first = await make_backend_for_run(
            "custom",
            settings,
            workspace="ns:s1",
            run_id="run_c",
            lease=lease,
            sandbox_store=run_repository,
        )
        assert getattr(first, "sandbox_id", None) == "custom_run_c"
        assert await run_repository.get_sandbox_id("run_c") == "custom_run_c"
        # HITL resume：prior 经 context 透传，工厂重连同一沙箱。
        await make_backend_for_run(
            "custom",
            settings,
            workspace="ns:s1",
            run_id="run_c",
            lease=lease,
            sandbox_store=run_repository,
        )
        assert SEEN_CONTEXTS[1].prior_sandbox_id == "custom_run_c"
        assert await run_repository.get_sandbox_id("run_c") == "custom_run_c"

    @pytest.mark.asyncio
    async def test_unbound_backend_skips_repository(
        self, run_repository: RunRepository
    ) -> None:
        settings = _dispatch_settings("kokoro_custom_probe:make_ok_backend")
        lease = await run_repository.try_claim(request("run_u"), "owner")
        assert lease is not None
        await make_backend_for_run(
            "custom",
            settings,
            workspace="ns:s1",
            run_id="run_u",
            lease=lease,
            sandbox_store=run_repository,
        )
        assert await run_repository.get_sandbox_id("run_u") is None

    @pytest.mark.asyncio
    async def test_cas_loser_uses_custom_teardown_before_reconnecting_winner(
        self,
    ) -> None:
        class _CasLosingRepository(FakeRunRepository):
            lost_once = False

            async def bind_sandbox_id(
                self,
                run_id: str,
                lease: LeaseFence,
                *,
                expected_sandbox_id: str | None,
                sandbox_id: str,
                backend_kind: SandboxBackendKind,
                teardown_ref: str,
            ) -> str | None:
                if not self.lost_once:
                    self.lost_once = True
                    self.sandbox_ids[run_id] = "custom_winner"
                    return "custom_winner"
                return await super().bind_sandbox_id(
                    run_id,
                    lease,
                    expected_sandbox_id=expected_sandbox_id,
                    sandbox_id=sandbox_id,
                    backend_kind=backend_kind,
                    teardown_ref=teardown_ref,
                )

        repository = _CasLosingRepository()
        lease = await repository.try_claim(request("run_custom_race"), "owner")
        assert lease is not None
        backend = await make_backend_for_run(
            "custom",
            _dispatch_settings(
                "kokoro_custom_probe:make_bound_backend",
                teardown_ref="kokoro_custom_probe:destroy_bound_backend",
            ),
            workspace="ns:s1",
            run_id="run_custom_race",
            lease=lease,
            sandbox_store=repository,
        )

        assert getattr(backend, "sandbox_id", None) == "custom_winner"
        assert DESTROYED_SANDBOXES == ["custom_run_custom_race"]
        assert await repository.get_sandbox_id("run_custom_race") == "custom_winner"

    @pytest.mark.asyncio
    async def test_failed_cas_loser_teardown_keeps_exact_durable_retry_identity(
        self,
    ) -> None:
        class _CasLosingRepository(FakeRunRepository):
            async def bind_sandbox_id(
                self,
                run_id: str,
                lease: LeaseFence,
                *,
                expected_sandbox_id: str | None,
                sandbox_id: str,
                backend_kind: SandboxBackendKind,
                teardown_ref: str,
            ) -> str | None:
                self.sandbox_ids[run_id] = "custom_winner"
                self.sandbox_generations[run_id] = lease.generation
                self.sandbox_bindings[run_id] = (backend_kind, teardown_ref)
                return "custom_winner"

        repository = _CasLosingRepository()
        lease = await repository.try_claim(request("run_custom_retry"), "owner")
        assert lease is not None
        settings = _dispatch_settings(
            "kokoro_custom_probe:make_bound_backend",
            teardown_ref="kokoro_custom_probe:destroy_failing_backend",
        )

        with pytest.raises(RuntimeError, match="transient custom teardown failure"):
            await make_backend_for_run(
                "custom",
                settings,
                workspace="ns:s1",
                run_id="run_custom_retry",
                lease=lease,
                sandbox_store=repository,
            )

        pending = await repository.claim_sandbox_cleanups(
            "retry-worker", run_id="run_custom_retry", limit=10, lease_ms=1_000
        )
        assert len(pending) == 1
        assert pending[0].sandbox_id == "custom_run_custom_retry"
        assert pending[0].teardown_ref == "kokoro_custom_probe:destroy_failing_backend"
        assert DESTROYED_SANDBOXES == ["custom_run_custom_retry"]
