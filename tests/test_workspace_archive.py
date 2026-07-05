"""workspace S3 归档规格（ADR-009）：type 判别配置 + 写时归档 + execute 全量兜底。

minio 不可达时归档实测组干净 skip（配置矩阵不依赖外部服务，恒跑）。
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import boto3
import pytest
from botocore.config import Config as BotoConfig
from mypy_boto3_s3 import S3Client
from pydantic import SecretStr, ValidationError

from kokoro_agent.sandbox import load_workspace_config, make_backend
from kokoro_agent.sandbox.archive import ArchivingLocalShellBackend, S3Archiver, S3Workspace
from kokoro_agent.sandbox.backend import SandboxSettings

MINIO_URL = "http://127.0.0.1:9100"
CREDS = {"access_key": SecretStr("kokoro"), "secret_key": SecretStr("kokoro-secret")}
BUCKET = f"kokoro-agent-test-{int(time.time())}"


def _sandbox_settings(root: str | None, workspace: object = None) -> SandboxSettings:
    return SandboxSettings.model_validate(
        {
            "local_shell_root": root,
            "local_shell_inherit_env": False,
            "local_shell_timeout": 30,
            "local_shell_max_output_bytes": 100000,
            "workspace": workspace,
            "workspace_s3_access_key": SecretStr("kokoro") if workspace else None,
            "workspace_s3_secret_key": SecretStr("kokoro-secret") if workspace else None,
            "e2b": {"api_key": None, "template": None, "timeout": 1800},
            "docker": {"image": None, "ttl": 1800},
            "custom": {"factory_ref": None, "config_path": None},
        }
    )


def _probe_minio() -> S3Client | None:
    client: S3Client = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        region_name="us-east-1",
        aws_access_key_id="kokoro",
        aws_secret_access_key="kokoro-secret",
        config=BotoConfig(
            s3={"addressing_style": "path"},
            connect_timeout=1,
            read_timeout=2,
            retries={"max_attempts": 1},
        ),
    )
    try:
        client.create_bucket(Bucket=BUCKET)
        return client
    except Exception:
        return None


_MINIO = _probe_minio()
needs_minio = pytest.mark.skipif(_MINIO is None, reason=f"no minio reachable at {MINIO_URL}")


class TestWorkspaceConfig:
    def test_missing_env_means_local_default(self) -> None:
        assert load_workspace_config(None) is None
        assert load_workspace_config("") is None

    def test_s3_defaults(self, tmp_path: Path) -> None:
        file = tmp_path / "ws.yaml"
        file.write_text("workspace:\n  type: s3\n  endpoint: http://x\n  bucket: b\n")
        config = load_workspace_config(str(file))
        assert isinstance(config, S3Workspace)
        assert (config.region, config.force_path_style) == ("us-east-1", True)

    @pytest.mark.parametrize(
        "content",
        [
            "workspace:\n  type: gcs\n  bucket: x\n",
            "workspace:\n  type: s3\n  endpoint: http://x\n",
            "workspace:\n  type: local\n",
            "workspace:\n  type: local\n  root: /a\n  evil: true\n",
            "storage:\n  type: local\n",
            "workspace: 42\n",
        ],
    )
    def test_fail_loud_on_bad_shape(self, tmp_path: Path, content: str) -> None:
        file = tmp_path / "ws.yaml"
        file.write_text(content)
        with pytest.raises(ValidationError):
            load_workspace_config(str(file))

    def test_missing_file_fail_loud(self) -> None:
        with pytest.raises(OSError):
            load_workspace_config("/nonexistent/ws.yaml")

    def test_s3_without_credentials_fail_loud(self) -> None:
        with pytest.raises(ValidationError, match="ACCESS_KEY"):
            SandboxSettings.model_validate(
                {
                    "local_shell_root": "/tmp",
                    "local_shell_inherit_env": False,
                    "local_shell_timeout": 30,
                    "local_shell_max_output_bytes": 100000,
                    "workspace": {"type": "s3", "endpoint": "http://x", "bucket": "b"},
                    "workspace_s3_access_key": None,
                    "workspace_s3_secret_key": None,
                    "e2b": {"api_key": None, "template": None, "timeout": 1800},
            "docker": {"image": None, "ttl": 1800},
            "custom": {"factory_ref": None, "config_path": None},
                }
            )


class TestBackendDispatch:
    def test_local_default_plain_backend(self, tmp_path: Path) -> None:
        backend = make_backend("local_shell", _sandbox_settings(str(tmp_path)), workspace="ns:s1")
        assert backend is not None
        assert not isinstance(backend, ArchivingLocalShellBackend)

    def test_s3_workspace_gets_archiving_backend(self, tmp_path: Path) -> None:
        workspace = {"type": "s3", "endpoint": MINIO_URL, "bucket": BUCKET}
        backend = make_backend(
            "local_shell", _sandbox_settings(str(tmp_path), workspace), workspace="ns:s1"
        )
        assert isinstance(backend, ArchivingLocalShellBackend)


@needs_minio
class TestArchivingBackend:
    def _backend(self, tmp_path: Path, prefix: str) -> ArchivingLocalShellBackend:
        root = tmp_path / prefix
        root.mkdir(parents=True)
        return ArchivingLocalShellBackend(
            root=root,
            archiver=S3Archiver(
                S3Workspace(type="s3", endpoint=MINIO_URL, bucket=BUCKET),
                access_key=CREDS["access_key"],
                secret_key=CREDS["secret_key"],
            ),
            prefix=prefix,
            timeout=30,
            max_output_bytes=100000,
            inherit_env=False,
        )

    def _object(self, key: str) -> bytes | None:
        assert _MINIO is not None
        try:
            return _MINIO.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except Exception:
            return None

    @pytest.mark.asyncio
    async def test_awrite_uploads_incrementally(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        backend = self._backend(tmp_path, prefix)
        await backend.awrite("/plan.md", "# 计划\n本地预览")
        assert self._object(f"{prefix}/plan.md") == "# 计划\n本地预览".encode()

    @pytest.mark.asyncio
    async def test_aedit_reuploads(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        backend = self._backend(tmp_path, prefix)
        await backend.awrite("/plan.md", "draft v1")
        await backend.aedit("/plan.md", "v1", "v2")
        assert self._object(f"{prefix}/plan.md") == b"draft v2"

    @pytest.mark.asyncio
    async def test_aexecute_shell_write_caught_by_full_archive(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        backend = self._backend(tmp_path, prefix)
        await backend.aexecute("echo kokoro-shell-write > shell.txt")
        assert self._object(f"{prefix}/shell.txt") == b"kokoro-shell-write\n"

    @pytest.mark.asyncio
    async def test_hidden_and_junk_dirs_not_archived(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        backend = self._backend(tmp_path, prefix)
        await backend.aexecute("mkdir -p __pycache__ && echo x > __pycache__/junk.pyc && echo y > .hidden")
        assert self._object(f"{prefix}/__pycache__/junk.pyc") is None
        assert self._object(f"{prefix}/.hidden") is None

    @pytest.mark.asyncio
    async def test_archive_failure_does_not_break_tool(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        root = tmp_path / prefix
        root.mkdir(parents=True)
        backend = ArchivingLocalShellBackend(
            root=root,
            archiver=S3Archiver(
                S3Workspace(type="s3", endpoint="http://127.0.0.1:1", bucket="dead"),
                access_key=CREDS["access_key"],
                secret_key=CREDS["secret_key"],
            ),
            prefix=prefix,
            timeout=30,
            max_output_bytes=100000,
            inherit_env=False,
        )
        result = await backend.awrite("/plan.md", "still works")
        assert result.error is None
        assert (root / "plan.md").read_text() == "still works"
