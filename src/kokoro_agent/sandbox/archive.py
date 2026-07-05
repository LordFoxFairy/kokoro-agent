"""workspace S3 归档（ADR-009）：写入口增量上传 + execute/upload 后全量兜底，失败可见不阻塞。"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Literal

import boto3
import yaml
from botocore.config import Config as BotoConfig
from mypy_boto3_s3 import S3Client
from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import EditResult, ExecuteResponse, FileUploadResponse, WriteResult
from pydantic import BaseModel, ConfigDict, SecretStr, TypeAdapter

LOGGER = logging.getLogger("kokoro_agent.sandbox.archive")

# 归档过滤与 session 本地档清单语义对齐：读侧 S3 list 不再过滤，写侧不推杂物即一致。
_IGNORED_DIRS = {".git", "node_modules", "__pycache__", ".venv"}


class LocalWorkspace(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    type: Literal["local"]
    root: str


class S3Workspace(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    type: Literal["s3"]
    endpoint: str
    bucket: str
    region: str = "us-east-1"
    force_path_style: bool = True


class _WorkspaceFile(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    workspace: LocalWorkspace | S3Workspace


_WORKSPACE_ADAPTER: TypeAdapter[_WorkspaceFile] = TypeAdapter(_WorkspaceFile)


def load_workspace_config(path: str | None) -> LocalWorkspace | S3Workspace | None:
    """session/agent 双侧读同一 yaml；缺省 None = local 默认档（agent 侧无归档动作）。"""
    if path is None or path == "":
        return None
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return _WORKSPACE_ADAPTER.validate_python(raw).workspace


class S3Archiver:
    """boto3 同步客户端（线程安全），异步路径经 to_thread；对象键 {prefix}/{rel_path}。"""

    def __init__(
        self,
        config: S3Workspace,
        *,
        access_key: SecretStr,
        secret_key: SecretStr,
    ) -> None:
        self._bucket = config.bucket
        self._client: S3Client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            region_name=config.region,
            aws_access_key_id=access_key.get_secret_value(),
            aws_secret_access_key=secret_key.get_secret_value(),
            config=BotoConfig(
                s3={"addressing_style": "path" if config.force_path_style else "auto"},
                # 归档是尽力而为的旁路：短超时+少重试，绝不把死端点变成写工具的延迟黑洞。
                connect_timeout=3,
                read_timeout=10,
                retries={"max_attempts": 2},
            ),
        )

    def upload_one(self, local_file: Path, key: str) -> None:
        self._client.upload_file(str(local_file), self._bucket, key)

    def archive_tree(self, root: Path, prefix: str) -> int:
        """全量归档（幂等覆盖）：返回上传数。杂物与隐藏文件不推，对齐清单语义。"""
        count = 0
        for file in sorted(root.rglob("*")):
            if not file.is_file():
                continue
            rel = file.relative_to(root)
            if any(part.startswith(".") or part in _IGNORED_DIRS for part in rel.parts):
                continue
            self.upload_one(file, f"{prefix}/{rel.as_posix()}")
            count += 1
        return count


class ArchivingWritesMixin(LocalShellBackend):
    """写时归档层（叠加于 LocalShellBackend 系，含 docker 变体；C3 线性化保证
    super() 落在真正的执行基类上）：write/edit 增量上传；execute/upload_files 后
    全量兜底（shell 直写无法逐文件感知）。归档失败 log 可见、绝不打断工具结果
    （ADR-009 强制规则）。组合类构造须设 _archive_root/_archiver/_prefix。"""

    _archive_root: Path
    _archiver: S3Archiver
    _prefix: str

    def _archive_file(self, file_path: str) -> None:
        rel = file_path.lstrip("/")
        local = self._archive_root / rel
        try:
            self._archiver.upload_one(local, f"{self._prefix}/{rel}")
        except Exception:
            LOGGER.warning("workspace archive failed for %s", rel, exc_info=True)

    def _archive_all(self) -> None:
        try:
            self._archiver.archive_tree(self._archive_root, self._prefix)
        except Exception:
            LOGGER.warning("workspace tree archive failed for %s", self._prefix, exc_info=True)

    def write(self, file_path: str, content: str) -> WriteResult:
        result = super().write(file_path, content)
        self._archive_file(file_path)
        return result

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        result = await super().awrite(file_path, content)
        await asyncio.to_thread(self._archive_file, file_path)
        return result

    def edit(
        self, file_path: str, old_string: str, new_string: str, replace_all: bool = False
    ) -> EditResult:
        result = super().edit(file_path, old_string, new_string, replace_all)
        self._archive_file(file_path)
        return result

    async def aedit(
        self, file_path: str, old_string: str, new_string: str, replace_all: bool = False
    ) -> EditResult:
        result = await super().aedit(file_path, old_string, new_string, replace_all)
        await asyncio.to_thread(self._archive_file, file_path)
        return result

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        result = super().execute(command, timeout=timeout)
        self._archive_all()
        return result

    async def aexecute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        result = await super().aexecute(command, timeout=timeout)
        await asyncio.to_thread(self._archive_all)
        return result

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        result = super().upload_files(files)
        self._archive_all()
        return result

    async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        result = await super().aupload_files(files)
        await asyncio.to_thread(self._archive_all)
        return result


class ArchivingLocalShellBackend(ArchivingWritesMixin):
    def __init__(
        self,
        *,
        root: Path,
        archiver: S3Archiver,
        prefix: str,
        timeout: int,
        max_output_bytes: int,
        inherit_env: bool,
    ) -> None:
        super().__init__(
            root_dir=str(root),
            # 虚拟根：模型绝对路径映射进工作区，绝不触宿主真实根。
            virtual_mode=True,
            timeout=timeout,
            max_output_bytes=max_output_bytes,
            inherit_env=inherit_env,
        )
        self._archive_root = root
        self._archiver = archiver
        self._prefix = prefix
