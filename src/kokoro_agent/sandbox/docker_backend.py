"""docker 沙箱 backend（ADR-009）：execute 进容器（执行隔离），文件操作留宿主 workspace。

混合体设计：文件工具走 LocalShellBackend 虚拟根（session 直读/S3 归档/canvas 三档全兼容），
只有任意 shell 在容器内跑——宿主仅暴露该 run 的 workspace 子目录（挂 /workspace，-w 定容）。
容器 `sleep TTL` + `--rm` 自清；container_id 入 run_repository，HITL resume 复用活容器，
容器亡（TTL）新起——workspace 在宿主，文件永不因容器回收而丢。
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated

from deepagents.backends.local_shell import LocalShellBackend

from kokoro_agent.sandbox.archive import ArchivingWritesMixin, S3Archiver
from kokoro_agent.sandbox.execution_environment import (
    ExecutionEnvironmentProvider,
    ExecutionEnvironmentRunner,
)
from deepagents.backends.protocol import ExecuteResponse
from pydantic import BaseModel, ConfigDict, Field

LOGGER = logging.getLogger("kokoro_agent.sandbox.docker")

_MOUNT_POINT = "/workspace"


def _cli_environment(values: Mapping[str, str] | None) -> dict[str, str]:
    """Explicit non-secret CLI context; never forward worker env wholesale.

    Connect snapshots this context for inspect/create and the resulting backend.
    Teardown callers must pass that same context to destroy_docker_sandbox.
    Selected Custom API variables cannot set these reserved runtime names.
    """
    result = dict(values) if values is not None else {}
    allowed = {
        "HOME",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_CONFIG",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
    }
    for name, value in result.items():
        if (
            name not in allowed
            or not value
            or len(value) > 4096
            or any(ord(c) < 32 for c in value)
        ):
            raise ValueError("invalid_docker_cli_environment")
        if name == "DOCKER_HOST" and (
            not value.startswith(("unix://", "npipe://", "tcp://"))
            or any(c in value for c in "@?#")
        ):
            raise ValueError("invalid_docker_cli_environment")
    return result


class DockerSettings(BaseModel):
    """进程级 docker 沙箱参数；image 缺失时选择 docker backend 即 fail-loud。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    image: str | None
    # 容器空闲存活期（秒）：sleep 到期自退 + --rm 自删；resume 复用窗口。
    ttl: Annotated[int, Field(gt=0)]


def _docker(
    *args: str, timeout: int = 30, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    # Resolve the CLI before supplying an isolated child environment. No worker
    # credentials are inherited; only explicit selected values and a fixed PATH.
    if env is not None:
        executable = shutil.which("docker")
        if executable is None:
            raise RuntimeError("docker_executable_unavailable")
        return subprocess.run(
            [executable, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            env={"PATH": os.defpath, **env},
        )
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


def _docker_in_context(
    *args: str,
    docker_cli_environment: Mapping[str, str] | None,
    timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    if docker_cli_environment is None:
        # Keep the original CLI invocation and inherited context when omitted.
        return _docker(*args, timeout=timeout)
    return _docker(*args, timeout=timeout, env=_cli_environment(docker_cli_environment))


class DockerShellBackend(LocalShellBackend):
    """文件面=宿主 workspace 虚拟根（继承）；execute 重定向进容器。"""

    def __init__(
        self,
        *,
        root: Path,
        container_id: str,
        timeout: int,
        max_output_bytes: int,
        environment_provider: ExecutionEnvironmentProvider | None = None,
        environment_loop: asyncio.AbstractEventLoop | None = None,
        docker_cli_environment: Mapping[str, str] | None = None,
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
        self._docker_cli_environment = (
            _cli_environment(docker_cli_environment)
            if docker_cli_environment is not None
            else None
        )
        self._environment = (
            ExecutionEnvironmentRunner(
                environment_provider,
                max_output_bytes=max_output_bytes,
                owner_loop=environment_loop,
            )
            if environment_provider is not None
            else None
        )

    @property
    def sandbox_id(self) -> str:
        # 统一生命周期面（ADR-010）：编排层据此落 run_repository 记录，各档同一属性名。
        return self.container_id

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        effective = timeout if timeout is not None else self._exec_timeout
        if self._environment is not None:
            return self._environment.execute(
                effective,
                lambda values, remaining: self._execute_environment(
                    command, values, remaining
                ),
            )
        return self._execute_plain(command, effective)

    async def aexecute(
        self, command: str, *, timeout: int | None = None
    ) -> ExecuteResponse:
        if self._environment is None:
            return await super().aexecute(command, timeout=timeout)
        return await self._environment.aexecute(
            timeout if timeout is not None else self._exec_timeout,
            lambda values, remaining: self._execute_environment(
                command, values, remaining
            ),
        )

    def _execute_environment(
        self, command: str, values: dict[str, str], timeout: int
    ) -> ExecuteResponse:
        arguments: list[str] = ["exec"]
        for name in values:
            # Docker copies each named value from its CLI child environment.
            # Values never appear in argv or in the model's command string.
            arguments.extend(("--env", name))
        result = _docker(
            *arguments,
            "-w",
            _MOUNT_POINT,
            self.container_id,
            "sh",
            "-lc",
            command,
            timeout=timeout,
            env={**(self._docker_cli_environment or {}), **values},
        )
        return ExecuteResponse(
            output=result.stdout + result.stderr,
            exit_code=result.returncode,
            truncated=False,
        )

    def _execute_plain(self, command: str, effective: int) -> ExecuteResponse:
        try:
            result = _docker_in_context(
                "exec",
                "-w",
                _MOUNT_POINT,
                self.container_id,
                "sh",
                "-lc",
                command,
                timeout=effective,
                docker_cli_environment=self._docker_cli_environment,
            )
        except subprocess.TimeoutExpired:
            return ExecuteResponse(
                output=f"command timed out after {effective}s",
                exit_code=124,
                truncated=False,
            )
        output = result.stdout + result.stderr
        truncated = len(output.encode()) > self._max_output_bytes
        if truncated:
            output = output.encode()[: self._max_output_bytes].decode(errors="replace")
        return ExecuteResponse(
            output=output, exit_code=result.returncode, truncated=truncated
        )


def destroy_docker_sandbox(
    container_id: str, *, docker_cli_environment: Mapping[str, str] | None = None
) -> None:
    """终态主动回收（--rm 容器 rm -f 即删）；失败交 TTL 自清兜底，调用方吞错记日志。"""
    _docker_in_context(
        "rm", "-f", container_id, docker_cli_environment=docker_cli_environment
    )


def _container_alive(
    container_id: str, *, docker_cli_environment: Mapping[str, str] | None = None
) -> bool:
    result = _docker_in_context(
        "inspect",
        "-f",
        "{{.State.Running}}",
        container_id,
        docker_cli_environment=docker_cli_environment,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def connect_docker_sandbox(
    settings: DockerSettings,
    *,
    root: Path,
    container_id: str | None,
    run_id: str,
    exec_timeout: int,
    max_output_bytes: int,
    environment_provider: ExecutionEnvironmentProvider | None = None,
    environment_loop: asyncio.AbstractEventLoop | None = None,
    docker_cli_environment: Mapping[str, str] | None = None,
) -> DockerShellBackend:
    """run 级生命周期：既往容器活着即复用（HITL 暂停期间的容器内进程状态在）；
    亡了新起——workspace 在宿主，文件面无损。调用方经 to_thread（docker CLI 同步阻塞）。
    """
    if settings.image is None:
        raise ValueError("backend docker requires KOKORO_DOCKER_IMAGE")
    # Validate and copy before *any* Docker I/O, including a reuse probe.
    context = (
        _cli_environment(docker_cli_environment)
        if docker_cli_environment is not None
        else None
    )
    if container_id is not None and _container_alive(
        container_id, docker_cli_environment=context
    ):
        return DockerShellBackend(
            root=root,
            container_id=container_id,
            timeout=exec_timeout,
            max_output_bytes=max_output_bytes,
            environment_provider=environment_provider,
            environment_loop=environment_loop,
            docker_cli_environment=context,
        )
    result = _docker_in_context(
        "run",
        "-d",
        "--rm",
        "--label",
        f"kokoro-run={run_id}",
        # --mount 键值语法：workspace 目录名含 ':'（{namespace}:{session_id} 约定），-v 短语法会被撕裂。
        "--mount",
        f"type=bind,source={root},target={_MOUNT_POINT}",
        "-w",
        _MOUNT_POINT,
        settings.image,
        "sleep",
        str(settings.ttl),
        timeout=120,
        docker_cli_environment=context,
    )
    if result.returncode != 0:
        raise RuntimeError(f"docker run failed: {result.stderr.strip()}")
    return DockerShellBackend(
        root=root,
        container_id=result.stdout.strip(),
        timeout=exec_timeout,
        max_output_bytes=max_output_bytes,
        environment_provider=environment_provider,
        environment_loop=environment_loop,
        docker_cli_environment=context,
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
        environment_provider: ExecutionEnvironmentProvider | None = None,
        environment_loop: asyncio.AbstractEventLoop | None = None,
        docker_cli_environment: Mapping[str, str] | None = None,
    ) -> None:
        DockerShellBackend.__init__(
            self,
            root=root,
            container_id=container_id,
            timeout=timeout,
            max_output_bytes=max_output_bytes,
            environment_provider=environment_provider,
            environment_loop=environment_loop,
            docker_cli_environment=docker_cli_environment,
        )
        self._archive_root = root
        self._archiver = archiver
        self._prefix = prefix
