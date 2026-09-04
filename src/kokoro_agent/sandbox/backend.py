"""filesystem 权限与执行 backend 接线：选择来自 GA Agent/Feature 声明，参数由 worker 注入。"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Protocol

from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware.filesystem import FilesystemPermission
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from kokoro_agent.policy import Backend, FilesystemPerm
from kokoro_agent.sandbox.archive import (
    ArchivingLocalShellBackend,
    LocalWorkspace,
    S3Archiver,
    S3Workspace,
)
from kokoro_agent.sandbox.custom_backend import (
    CustomBackendSettings,
    connect_custom_sandbox,
    teardown_custom_sandbox,
    teardown_custom_sandbox_ref,
)
from kokoro_agent.sandbox.docker_backend import (
    destroy_docker_sandbox,
    ArchivingDockerShellBackend,
    DockerSettings,
    connect_docker_sandbox,
)
from kokoro_agent.sandbox.e2b_backend import (
    E2BSettings,
    connect_e2b_sandbox,
    kill_e2b_sandbox,
)
from kokoro_agent.repositories.run_repository import (
    LeaseFence,
    SandboxBackendKind,
    SandboxCleanupIntent,
)

LOGGER = logging.getLogger("kokoro_agent.sandbox")
DOCKER_TEARDOWN_REF = "kokoro-agent:builtin:docker:v1"
E2B_TEARDOWN_REF = "kokoro-agent:builtin:e2b:v1"


class SandboxSettings(BaseModel):
    """各 backend 的进程级参数；具体选择由受信 Agent/Feature 声明决定。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    local_shell_root: str | None
    local_shell_inherit_env: bool
    local_shell_timeout: Annotated[int, Field(gt=0)]
    local_shell_max_output_bytes: Annotated[int, Field(gt=0)]
    # 存储形态（ADR-009，与 session 读同一 yaml）：None=local 默认档，无归档动作。
    workspace: LocalWorkspace | S3Workspace | None
    workspace_s3_access_key: SecretStr | None
    workspace_s3_secret_key: SecretStr | None
    # e2b 云沙箱（ADR-009 1b）：api_key 缺失时选择 e2b backend 即 fail-loud。
    e2b: E2BSettings
    # docker 沙箱（ADR-009）：执行隔离进容器、文件面留宿主 workspace；image 缺失 fail-loud。
    docker: DockerSettings
    # custom 沙箱（ADR-010 BYO）：`pkg.module:factory` 自带实现；选 custom 未配引用即 fail-loud。
    custom: CustomBackendSettings

    @model_validator(mode="after")
    def _s3_requires_credentials(self) -> SandboxSettings:
        if isinstance(self.workspace, S3Workspace) and (
            self.workspace_s3_access_key is None or self.workspace_s3_secret_key is None
        ):
            raise ValueError(
                "workspace type s3 requires KOKORO_WORKSPACE_S3_ACCESS_KEY/SECRET_KEY"
            )
        return self


def build_filesystem_permissions(perm: FilesystemPerm) -> list[FilesystemPermission]:
    if perm == "workspace_write":
        return []
    return [FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")]


def make_backend(
    kind: Backend, settings: SandboxSettings, *, workspace: str | None = None
) -> BackendProtocol | None:
    if kind == "state":
        return None
    if kind == "local_shell":
        # 工作区约定：{root}/{namespace:session_id}/ ——session files 端点按同约定直读。
        root = _workspace_root(settings, workspace)
        # s3 档（ADR-009）：写时归档装饰——session 读对象存储，写侧真源处增量推送。
        if (
            isinstance(settings.workspace, S3Workspace)
            and workspace is not None
            and root is not None
            and settings.workspace_s3_access_key is not None
            and settings.workspace_s3_secret_key is not None
        ):
            return ArchivingLocalShellBackend(
                root=Path(root),
                archiver=S3Archiver(
                    settings.workspace,
                    access_key=settings.workspace_s3_access_key,
                    secret_key=settings.workspace_s3_secret_key,
                ),
                prefix=workspace,
                timeout=settings.local_shell_timeout,
                max_output_bytes=settings.local_shell_max_output_bytes,
                inherit_env=settings.local_shell_inherit_env,
            )
        return LocalShellBackend(
            root_dir=root,
            # 虚拟根：模型的绝对路径（"/note.md"）映射进工作区，绝不触宿主真实根（越界即安全缺陷）。
            virtual_mode=True,
            timeout=settings.local_shell_timeout,
            max_output_bytes=settings.local_shell_max_output_bytes,
            inherit_env=settings.local_shell_inherit_env,
        )
    # docker/e2b/custom 有 run 级生命周期（run_repository 记录），走 make_backend_for_run 的 async 编排。
    raise ValueError(
        f"backend {kind} requires run-scoped assembly via make_backend_for_run"
    )


def _workspace_root(settings: SandboxSettings, workspace: str | None) -> str | None:
    root = settings.local_shell_root
    if workspace is not None and root is not None:
        sub = Path(root) / workspace
        sub.mkdir(parents=True, exist_ok=True)
        return str(sub)
    return root


class RunSandboxStore(Protocol):
    """run 级沙箱记录存取（RunRepository 子集）：装配路径只依赖这两个方法。"""

    async def bind_sandbox_id(
        self,
        run_id: str,
        lease: LeaseFence,
        *,
        expected_sandbox_id: str | None,
        sandbox_id: str,
        backend_kind: SandboxBackendKind,
        teardown_ref: str,
    ) -> str | None: ...

    async def get_sandbox_id(self, run_id: str) -> str | None: ...

    async def is_lease_current(self, run_id: str, lease: LeaseFence) -> bool: ...

    async def register_sandbox_cleanup(
        self,
        *,
        run_id: str,
        lease_generation: int,
        backend_kind: SandboxBackendKind,
        sandbox_id: str,
        teardown_ref: str,
    ) -> SandboxCleanupIntent: ...

    async def complete_sandbox_cleanup(self, cleanup_id: str) -> bool: ...

    async def reschedule_sandbox_cleanup(
        self, cleanup_id: str, error: str, *, retry_delay_ms: int
    ) -> bool: ...


class SandboxLeaseSuperseded(RuntimeError):
    """Sandbox assembly lost its execution generation before it could bind."""


@dataclass(frozen=True)
class SandboxContext:
    """连接器唯一入参：本 run 的装配现场。"""

    settings: SandboxSettings
    workspace: str
    run_id: str
    # run_repository 既往记录（HITL resume 现场）：重连既往箱/容器而非新建。
    prior_sandbox_id: str | None


class SandboxConnector(Protocol):
    """每档一个连接器（Strategy）：sync 构造（编排层 to_thread），返回 None=无 backend。"""

    def __call__(self, context: SandboxContext) -> BackendProtocol | None: ...


def _connect_state(context: SandboxContext) -> BackendProtocol | None:
    return None


def _connect_local_shell(context: SandboxContext) -> BackendProtocol | None:
    return make_backend("local_shell", context.settings, workspace=context.workspace)


def _connect_docker(context: SandboxContext) -> BackendProtocol | None:
    root = _workspace_root(context.settings, context.workspace)
    if root is None:
        raise ValueError("backend docker requires KOKORO_AGENT_LOCAL_SHELL_ROOT")
    settings = context.settings
    backend = connect_docker_sandbox(
        settings.docker,
        root=Path(root),
        container_id=context.prior_sandbox_id,
        run_id=context.run_id,
        exec_timeout=settings.local_shell_timeout,
        max_output_bytes=settings.local_shell_max_output_bytes,
    )
    # docker + s3 组合档：与 local_shell 同享写时归档（缺此即静默丢文件面）。
    if (
        isinstance(settings.workspace, S3Workspace)
        and settings.workspace_s3_access_key is not None
        and settings.workspace_s3_secret_key is not None
    ):
        return ArchivingDockerShellBackend(
            root=Path(root),
            container_id=backend.container_id,
            archiver=S3Archiver(
                settings.workspace,
                access_key=settings.workspace_s3_access_key,
                secret_key=settings.workspace_s3_secret_key,
            ),
            prefix=context.workspace,
            timeout=settings.local_shell_timeout,
            max_output_bytes=settings.local_shell_max_output_bytes,
        )
    return backend


def _connect_e2b(context: SandboxContext) -> BackendProtocol | None:
    return connect_e2b_sandbox(
        context.settings.e2b, sandbox_id=context.prior_sandbox_id
    )


def _connect_custom(context: SandboxContext) -> BackendProtocol | None:
    return connect_custom_sandbox(
        context.settings.custom,
        run_id=context.run_id,
        workspace=context.workspace,
        workspace_root=_workspace_root(context.settings, context.workspace),
        prior_sandbox_id=context.prior_sandbox_id,
    )


# 注册表分派：加新档 = 写连接器 + 注册一行；覆盖度由 test_connectors_cover_backend_enum 守卫。
_CONNECTORS: dict[Backend, SandboxConnector] = {
    "state": _connect_state,
    "local_shell": _connect_local_shell,
    "docker": _connect_docker,
    "e2b": _connect_e2b,
    "custom": _connect_custom,
}


def sandbox_teardown_ref(kind: Backend, settings: SandboxSettings) -> str | None:
    """Return the immutable executor identity persisted with a managed sandbox."""

    if kind == "docker":
        return DOCKER_TEARDOWN_REF
    if kind == "e2b":
        return E2B_TEARDOWN_REF
    if kind == "custom":
        return settings.custom.teardown_ref
    return None


def _managed_backend_kind(kind: Backend) -> SandboxBackendKind:
    if kind == "docker":
        return "docker"
    if kind == "e2b":
        return "e2b"
    if kind == "custom":
        return "custom"
    raise ValueError(f"backend {kind!r} does not own a run-scoped sandbox")


async def _cleanup_abandoned_sandbox(
    kind: SandboxBackendKind,
    settings: SandboxSettings,
    *,
    run_id: str,
    lease: LeaseFence,
    sandbox_id: str,
    teardown_ref: str,
    sandbox_store: RunSandboxStore,
) -> None:
    """Persist a CAS loser's identity before its only in-memory handle is lost."""

    try:
        intent = await sandbox_store.register_sandbox_cleanup(
            run_id=run_id,
            lease_generation=lease.generation,
            backend_kind=kind,
            sandbox_id=sandbox_id,
            teardown_ref=teardown_ref,
        )
    except Exception:
        # The durable store being unavailable must fail assembly, but make one
        # best-effort strict destruction attempt while the only handle is alive.
        await teardown_backend_for_run(
            kind,
            settings,
            sandbox_id,
            teardown_ref=teardown_ref,
            strict=True,
        )
        raise
    try:
        await teardown_backend_for_run(
            kind,
            settings,
            sandbox_id,
            teardown_ref=intent.teardown_ref,
            strict=True,
        )
    except Exception as error:
        await sandbox_store.reschedule_sandbox_cleanup(
            intent.cleanup_id, str(error), retry_delay_ms=0
        )
        raise
    await sandbox_store.complete_sandbox_cleanup(intent.cleanup_id)


async def make_backend_for_run(
    kind: Backend,
    settings: SandboxSettings,
    *,
    workspace: str,
    run_id: str,
    lease: LeaseFence,
    sandbox_store: RunSandboxStore,
) -> BackendProtocol | None:
    """统一装配入口：每个 lease generation 只确认一个权威沙箱。"""
    connector = _CONNECTORS.get(kind)
    if connector is None:
        raise NotImplementedError(f"backend {kind!r} has no registered connector")
    if not await sandbox_store.is_lease_current(run_id, lease):
        raise SandboxLeaseSuperseded(
            f"run {run_id!r} lease was superseded before sandbox assembly"
        )
    prior = await sandbox_store.get_sandbox_id(run_id)
    for _attempt in range(3):
        context = SandboxContext(
            settings=settings,
            workspace=workspace,
            run_id=run_id,
            prior_sandbox_id=prior,
        )
        # 连接器一律 sync（docker CLI / SDK 网络调用秒级阻塞）：to_thread 让出事件循环。
        backend = await asyncio.to_thread(connector, context)
        bound = getattr(backend, "sandbox_id", None)
        if not isinstance(bound, str) or not bound:
            if not await sandbox_store.is_lease_current(run_id, lease):
                raise SandboxLeaseSuperseded(
                    f"run {run_id!r} lease was superseded during sandbox assembly"
                )
            return backend

        teardown_ref = sandbox_teardown_ref(kind, settings)
        if teardown_ref is None:
            raise RuntimeError(
                f"backend {kind!r} exposed sandbox_id without a teardown identity"
            )
        managed_kind = _managed_backend_kind(kind)
        authoritative = await sandbox_store.bind_sandbox_id(
            run_id,
            lease,
            expected_sandbox_id=prior,
            sandbox_id=bound,
            backend_kind=managed_kind,
            teardown_ref=teardown_ref,
        )
        if authoritative is None:
            # Only a newly created loser belongs to this worker. Never destroy a
            # reconnected prior id that a newer generation may already own.
            if bound != prior:
                await _cleanup_abandoned_sandbox(
                    managed_kind,
                    settings,
                    run_id=run_id,
                    lease=lease,
                    sandbox_id=bound,
                    teardown_ref=teardown_ref,
                    sandbox_store=sandbox_store,
                )
            raise SandboxLeaseSuperseded(
                f"run {run_id!r} lease was superseded during sandbox assembly"
            )
        if authoritative == bound:
            return backend

        # Another same-generation assembler won the CAS. Dispose our fresh
        # loser and reconnect the authoritative id instead of returning an
        # untracked backend.
        if bound != prior:
            await _cleanup_abandoned_sandbox(
                managed_kind,
                settings,
                run_id=run_id,
                lease=lease,
                sandbox_id=bound,
                teardown_ref=teardown_ref,
                sandbox_store=sandbox_store,
            )
        prior = authoritative

    raise RuntimeError(f"sandbox binding did not converge for run {run_id!r}")


async def teardown_backend_for_run(
    kind: Backend,
    settings: SandboxSettings,
    sandbox_id: str | None,
    *,
    teardown_ref: str | None = None,
    strict: bool = False,
) -> None:
    """回收 run 沙箱；严格模式将失败交给持久化清理队列重试。"""
    if sandbox_id is None:
        return
    try:
        if kind == "docker":
            if teardown_ref not in {None, DOCKER_TEARDOWN_REF}:
                raise ValueError("docker sandbox teardown identity is invalid")
            await asyncio.to_thread(destroy_docker_sandbox, sandbox_id)
        elif kind == "e2b":
            if teardown_ref not in {None, E2B_TEARDOWN_REF}:
                raise ValueError("e2b sandbox teardown identity is invalid")
            await asyncio.to_thread(kill_e2b_sandbox, settings.e2b, sandbox_id)
        elif kind == "custom":
            if teardown_ref is None:
                await asyncio.to_thread(
                    teardown_custom_sandbox, settings.custom, sandbox_id
                )
            else:
                await asyncio.to_thread(
                    teardown_custom_sandbox_ref, teardown_ref, sandbox_id
                )
    except Exception:
        if strict:
            raise
        LOGGER.warning(
            "sandbox teardown failed kind=%s id=%s", kind, sandbox_id, exc_info=True
        )


def registered_backends() -> frozenset[str]:
    """注册表覆盖面（守卫测试用）：与 Backend 枚举保持一致由测试强制。"""
    return frozenset(_CONNECTORS)
