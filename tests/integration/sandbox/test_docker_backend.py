"""docker 沙箱规格（ADR-009）：执行进容器、文件面留宿主、run 级容器生命周期。

真实资源用例按需检查 Docker/MinIO；显式选择时缺资源直接失败，不在导入期探测。
"""

from __future__ import annotations

import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from support.dev_minio import MINIO_URL, minio_creds
from support.fakes import request
from kokoro_agent.sandbox.backend import SandboxSettings, make_backend_for_run
from kokoro_agent.sandbox.docker_backend import (
    DockerSettings,
    DockerShellBackend,
    connect_docker_sandbox,
)
from kokoro_agent.domain.run.repository import RunRepository

IMAGE = "busybox"


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "info"], capture_output=True, timeout=10, check=False
            ).returncode
            == 0
        )
    except Exception:
        return False


@pytest.fixture(scope="module")
def docker_ready() -> None:
    if not _docker_available():
        raise RuntimeError("Docker integration requires a reachable daemon")


@pytest.fixture
def minio_credentials() -> tuple[str, str]:
    credentials = minio_creds()
    if not MINIO_URL or credentials is None or not credentials[0] or not credentials[1]:
        raise RuntimeError(
            "Docker/S3 integration requires MinIO credentials and endpoint"
        )
    return credentials


_SPAWNED: list[str] = []


def _settings(image: str | None = IMAGE, ttl: int = 120) -> DockerSettings:
    return DockerSettings(image=image, ttl=ttl)


def _dispatch_settings() -> SandboxSettings:
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
            "docker": {"image": IMAGE, "ttl": 120},
            "custom": {"factory_ref": None, "config_path": None},
        }
    )


def _connect(
    root: Path, container_id: str | None = None, run_id: str = "run_x"
) -> DockerShellBackend:
    backend = connect_docker_sandbox(
        _settings(),
        root=root,
        container_id=container_id,
        run_id=run_id,
        exec_timeout=30,
        max_output_bytes=100000,
    )
    _SPAWNED.append(backend.container_id)
    return backend


@pytest.fixture(scope="module")
def cleanup_containers(docker_ready: None) -> Iterator[None]:
    del docker_ready
    yield
    for cid in _SPAWNED:
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True, check=False)


def test_missing_image_fail_loud(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="KOKORO_DOCKER_IMAGE"):
        connect_docker_sandbox(
            _settings(image=None),
            root=tmp_path,
            container_id=None,
            run_id="r",
            exec_timeout=30,
            max_output_bytes=100000,
        )


@pytest.mark.integration
@pytest.mark.usefixtures("docker_ready", "cleanup_containers")
class TestDockerSandbox:
    def test_execute_runs_inside_container(self, tmp_path: Path) -> None:
        backend = _connect(tmp_path)
        result = backend.execute("hostname")
        # 容器内 hostname = container id 前 12 位：铁证 execute 不在宿主跑。
        assert result.output.strip() == backend.container_id[:12]
        assert result.exit_code == 0

    def test_container_writes_land_in_host_workspace(self, tmp_path: Path) -> None:
        backend = _connect(tmp_path)
        backend.execute("echo from-container > shell.txt")
        assert (tmp_path / "shell.txt").read_text() == "from-container\n"

    def test_file_tools_stay_on_host_workspace(self, tmp_path: Path) -> None:
        # 文件工具走宿主虚拟根（session 直读/S3 归档全兼容），无需容器在场。
        backend = _connect(tmp_path)
        backend.write("/plan.md", "# 计划")
        assert (tmp_path / "plan.md").read_text() == "# 计划"
        # 容器内经挂载看得见同一文件。
        result = backend.execute("cat plan.md")
        assert result.output == "# 计划"

    def test_nonzero_exit_is_result(self, tmp_path: Path) -> None:
        backend = _connect(tmp_path)
        result = backend.execute("sh -c 'echo oops >&2; exit 3'")
        assert result.exit_code == 3
        assert "oops" in result.output

    def test_reuse_alive_container(self, tmp_path: Path) -> None:
        first = _connect(tmp_path, run_id="run_reuse")
        second = _connect(tmp_path, container_id=first.container_id, run_id="run_reuse")
        assert second.container_id == first.container_id

    def test_dead_container_replaced(self, tmp_path: Path) -> None:
        first = _connect(tmp_path, run_id="run_dead")
        subprocess.run(
            ["docker", "rm", "-f", first.container_id], capture_output=True, check=True
        )
        second = _connect(tmp_path, container_id=first.container_id, run_id="run_dead")
        assert second.container_id != first.container_id

    @pytest.mark.asyncio
    async def test_run_scoped_binding_and_reuse(
        self, tmp_path: Path, run_repository: RunRepository
    ) -> None:
        run_id = f"run_{uuid.uuid4().hex[:6]}"
        settings = _dispatch_settings().model_copy(
            update={"local_shell_root": str(tmp_path)}
        )
        # 生产路径：run 先被认领（建 run 文档），容器绑定才落账。
        lease = await run_repository.try_claim(request(run_id), "owner")
        assert lease is not None
        first = await make_backend_for_run(
            "docker",
            settings,
            workspace="ns:s1",
            run_id=run_id,
            lease=lease,
            sandbox_store=run_repository,
        )
        assert isinstance(first, DockerShellBackend)
        _SPAWNED.append(first.container_id)
        assert await run_repository.get_sandbox_id(run_id) == first.container_id
        second = await make_backend_for_run(
            "docker",
            settings,
            workspace="ns:s1",
            run_id=run_id,
            lease=lease,
            sandbox_store=run_repository,
        )
        assert isinstance(second, DockerShellBackend)
        assert second.container_id == first.container_id


def test_connectors_cover_backend_enum() -> None:
    # 枚举加值忘注册连接器 = 装配期 NotImplementedError；此守卫把它提前到测试期。
    from typing import get_args

    from kokoro_agent.policy import Backend
    from kokoro_agent.sandbox.backend import registered_backends

    assert registered_backends() == frozenset(get_args(Backend))


@pytest.mark.integration
@pytest.mark.usefixtures("docker_ready", "cleanup_containers")
class TestDockerWithS3Archive:
    def test_docker_execute_archives_to_s3(
        self, tmp_path: Path, minio_credentials: tuple[str, str]
    ) -> None:
        # docker 隔离 + S3 文件面组合档：容器写 → 宿主挂载 → 全量归档推 S3。
        import boto3
        from botocore.config import Config as BotoConfig
        from pydantic import SecretStr

        from kokoro_agent.sandbox.archive import S3Archiver, S3Workspace
        from kokoro_agent.sandbox.docker_backend import ArchivingDockerShellBackend

        access, secret = minio_credentials
        minio = boto3.client(
            "s3",
            endpoint_url=MINIO_URL,
            region_name="us-east-1",
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            config=BotoConfig(
                s3={"addressing_style": "path"},
                connect_timeout=1,
                retries={"max_attempts": 1},
            ),
        )
        bucket = f"kokoro-docker-s3-{uuid.uuid4().hex[:6]}"
        minio.create_bucket(Bucket=bucket)
        plain = _connect(tmp_path, run_id="run_ds3")
        backend = ArchivingDockerShellBackend(
            root=tmp_path,
            container_id=plain.container_id,
            archiver=S3Archiver(
                S3Workspace(type="s3", endpoint=MINIO_URL, bucket=bucket),
                access_key=SecretStr(access),
                secret_key=SecretStr(secret),
            ),
            prefix="ns:ds3",
            timeout=30,
            max_output_bytes=100000,
        )
        result = backend.execute("echo from-docker > out.txt")
        assert result.exit_code == 0
        body = minio.get_object(Bucket=bucket, Key="ns:ds3/out.txt")["Body"].read()
        assert body == b"from-docker\n"
