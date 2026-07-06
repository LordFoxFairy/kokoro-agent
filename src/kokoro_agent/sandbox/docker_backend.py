"""docker 沙箱 backend（ADR-009）：execute 进容器（执行隔离），文件操作留宿主 workspace。

混合体设计：文件工具走 LocalShellBackend 虚拟根（session 直读/S3 归档/canvas 三档全兼容），
只有任意 shell 在容器内跑——宿主仅暴露该 run 的 workspace 子目录（挂 /workspace，-w 定容）。
容器 `sleep TTL` + `--rm` 自清；container_id 入 ledger，HITL resume 复用活容器，
容器亡（TTL）新起——workspace 在宿主，文件永不因容器回收而丢。
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Annotated

from deepagents.backends.local_shell import LocalShellBackend

from kokoro_agent.sandbox.archive import ArchivingWritesMixin, S3Archiver
from deepagents.backends.protocol import ExecuteResponse
from pydantic import BaseModel, ConfigDict, Field

LOGGER = logging.getLogger("kokoro_agent.sandbox.docker")

_MOUNT_POINT = "/workspace"


class DockerSettings(BaseModel):
    """进程级 docker 沙箱参数；image 缺失时选择 docker backend 即 fail-loud。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    image: str | None
    # 容器空闲存活期（秒）：sleep 到期自退 + --rm 自删；resume 复用窗口。
    ttl: Annotated[int, Field(gt=0)]


def _docker(*args: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


class DockerShellBackend(LocalShellBackend):
    """文件面=宿主 workspace 虚拟根（继承）；execute 重定向进容器。"""

    def __init__(
        self,
        *,
        root: Path,
        container_id: str,
        timeout: int,
        max_output_bytes: int,
    ) -> None:
        super().__init__(
            root_dir=str(root),
            virtual_mode=True,
            timeout=timeout,
            max_output_bytes=max_output_bytes,
            inherit_env=False,
        )
        self.container_id = container_id
        self._exec_timeout = timeout
        self._max_output_bytes = max_output_bytes

    @property
    def sandbox_id(self) -> str:
        # 统一生命周期面（ADR-010）：编排层据此落 ledger 绑定，各档同一属性名。
        return self.container_id

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        effective = timeout if timeout is not None else self._exec_timeout
        try:
            result = _docker(
                "exec", "-w", _MOUNT_POINT, self.container_id, "sh", "-lc", command,
                timeout=effective,
            )
        except subprocess.TimeoutExpired:
            return ExecuteResponse(
                output=f"command timed out after {effective}s", exit_code=124, truncated=False
            )
        output = result.stdout + result.stderr
        truncated = len(output.encode()) > self._max_output_bytes
        if truncated:
            output = output.encode()[: self._max_output_bytes].decode(errors="replace")
        return ExecuteResponse(output=output, exit_code=result.returncode, truncated=truncated)


def destroy_docker_sandbox(container_id: str) -> None:
    """终态主动回收（--rm 容器 rm -f 即删）；失败交 TTL 自清兜底，调用方吞错记日志。"""
    _docker("rm", "-f", container_id)


def _container_alive(container_id: str) -> bool:
    result = _docker("inspect", "-f", "{{.State.Running}}", container_id)
    return result.returncode == 0 and result.stdout.strip() == "true"


def connect_docker_sandbox(
    settings: DockerSettings,
    *,
    root: Path,
    container_id: str | None,
    run_id: str,
    exec_timeout: int,
    max_output_bytes: int,
) -> DockerShellBackend:
    """run 级生命周期：既往容器活着即复用（HITL 暂停期间的容器内进程状态在）；
    亡了新起——workspace 在宿主，文件面无损。调用方经 to_thread（docker CLI 同步阻塞）。
    """
    if settings.image is None:
        raise ValueError("backend docker requires KOKORO_DOCKER_IMAGE")
    if container_id is not None and _container_alive(container_id):
        return DockerShellBackend(
            root=root,
            container_id=container_id,
            timeout=exec_timeout,
            max_output_bytes=max_output_bytes,
        )
    result = _docker(
        "run", "-d", "--rm",
        "--label", f"kokoro-run={run_id}",
        # --mount 键值语法：workspace 目录名含 ':'（{namespace}:{session_id} 约定），-v 短语法会被撕裂。
        "--mount", f"type=bind,source={root},target={_MOUNT_POINT}",
        "-w", _MOUNT_POINT,
        settings.image, "sleep", str(settings.ttl),
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"docker run failed: {result.stderr.strip()}")
    return DockerShellBackend(
        root=root,
        container_id=result.stdout.strip(),
        timeout=exec_timeout,
        max_output_bytes=max_output_bytes,
    )


class ArchivingDockerShellBackend(ArchivingWritesMixin, DockerShellBackend):
    """docker 执行隔离 + S3 写时归档（多 pod 无共享卷 + 容器隔离的组合档）。"""

    def __init__(
        self,
        *,
        root: Path,
        container_id: str,
        archiver: S3Archiver,
        prefix: str,
        timeout: int,
        max_output_bytes: int,
    ) -> None:
        DockerShellBackend.__init__(
            self,
            root=root,
            container_id=container_id,
            timeout=timeout,
            max_output_bytes=max_output_bytes,
        )
        self._archive_root = root
        self._archiver = archiver
        self._prefix = prefix
