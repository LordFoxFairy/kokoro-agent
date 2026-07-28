"""成果交付工具（deliver）：读工作区文件字节 → sha256 → 冻结进 deliveries/<ns>/<hash>。

交付即冻结：读到哪份字节冻结哪份（构造上自洽，无需 quiesce）；同内容同 key 天然幂等，
异内容异 key 物理上不可能覆盖。工具恒挂（schema 不随配置变，D9）：无 workspace / 无
deliveries 时调用降级为 error 文本（模型自纠，不炸 run）。
"""

from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import os
import stat
from contextlib import ExitStack
from pathlib import Path
from typing import Literal

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.skills.hub import PackageStore, SkillHubError

DELIVER_TOOL_NAME = "deliver"
MAX_DELIVERY_BYTES = 25 * 1024 * 1024


class DeliveryPathError(Exception):
    """The requested path cannot be opened beneath the workspace without following links."""


class _DeliveryTooLarge(Exception):
    """The source exceeded the delivery byte limit before or during the bounded read."""


def _secure_open_flags() -> tuple[int, int]:
    required = ("O_DIRECTORY", "O_NOFOLLOW", "O_CLOEXEC")
    values = [getattr(os, name, None) for name in required]
    if (
        any(value is None for value in values)
        or not hasattr(os, "supports_dir_fd")
        or os.open not in os.supports_dir_fd
    ):
        raise DeliveryPathError("secure open-beneath is unavailable")
    directory, nofollow, cloexec = values
    assert isinstance(directory, int)
    assert isinstance(nofollow, int)
    assert isinstance(cloexec, int)
    return os.O_RDONLY | directory | nofollow | cloexec, os.O_RDONLY | nofollow | cloexec


def _relative_components(relative_path: Path) -> tuple[str, ...]:
    if relative_path.is_absolute() or not relative_path.parts:
        raise DeliveryPathError("delivery path must be relative and non-empty")
    parts = tuple(relative_path.parts)
    if any(part in {"", ".", ".."} for part in parts):
        raise DeliveryPathError("delivery path contains an unsafe component")
    return parts


def read_delivery_bytes_beneath(workspace_root: Path, relative_path: Path) -> bytes:
    """Open beneath one trusted dirfd, then validate and read the same final fd."""
    parts = _relative_components(relative_path)
    directory_flags, file_flags = _secure_open_flags()
    with ExitStack() as opened:
        current_fd = os.open(workspace_root, directory_flags)
        opened.callback(os.close, current_fd)
        for component in parts[:-1]:
            current_fd = os.open(component, directory_flags, dir_fd=current_fd)
            opened.callback(os.close, current_fd)

        file_fd = os.open(
            parts[-1], file_flags | getattr(os, "O_NONBLOCK", 0), dir_fd=current_fd
        )
        opened.callback(os.close, file_fd)
        source_stat = os.fstat(file_fd)
        if not stat.S_ISREG(source_stat.st_mode):
            raise DeliveryPathError("delivery source is not a regular file")
        if source_stat.st_size > MAX_DELIVERY_BYTES:
            raise _DeliveryTooLarge

        remaining = MAX_DELIVERY_BYTES + 1
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(file_fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > MAX_DELIVERY_BYTES:
            raise _DeliveryTooLarge
        return data


class DeliverArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    path: str = Field(description="工作区内的成果文件路径（虚拟根绝对路径，如 /report.pdf）。")
    title: str = Field(description="成果标题（展示用）。")
    note: str = Field(default="", description="交付说明（可选）。")


class DeliverResult(BaseModel):
    """deliver 工具结构化返回 = delivery.created 追发的解析契约（单一事实来源）。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    status: Literal["delivered"]
    path: str
    title: str
    mime: str
    size: int
    content_hash: str
    note: str


def delivery_ref(namespace: str, content_hash: str) -> str:
    """成果冻结键：content-hash keyed，与 workspace 归档（path-keyed 覆盖写）不同 keyspace。"""
    return f"deliveries/{namespace}/{content_hash}"


def make_deliver_tool(
    workspace_dir: Path | None,
    deliveries: PackageStore | None,
    namespace: str,
) -> StructuredTool:
    """per-run 闭包：工作区目录 / 冻结件存储 / namespace 在装配期捕获。"""

    async def deliver(path: str, title: str, note: str = "") -> str:
        if workspace_dir is None:
            return "error: 当前后端不支持交付（无工作区文件面）。"
        if deliveries is None:
            return "error: 未配置交付存储（storage yaml deliveries 节）。"
        # 虚拟根绝对路径映射为相对组件；secure-open helper 不重新解析 pathname。
        relative_path = Path(path.lstrip("/"))
        try:
            data = await asyncio.to_thread(
                read_delivery_bytes_beneath, workspace_dir, relative_path
            )
        except _DeliveryTooLarge:
            return f"error: 文件 {path!r} 超过交付上限（25 MiB）。"
        except DeliveryPathError:
            return f"error: 路径 {path!r} 越出工作区，拒绝交付。"
        except (OSError, ValueError):
            return f"error: 文件 {path!r} 不存在或不是常规文件。"
        content_hash = hashlib.sha256(data).hexdigest()
        mime = mimetypes.guess_type(relative_path.name)[0] or "application/octet-stream"
        try:
            await deliveries.put(delivery_ref(namespace, content_hash), data)
        except SkillHubError as exc:
            return f"error: {exc}"
        return DeliverResult(
            status="delivered",
            path=path,
            title=title,
            mime=mime,
            size=len(data),
            content_hash=content_hash,
            note=note,
        ).model_dump_json()

    return StructuredTool(
        name=DELIVER_TOOL_NAME,
        description="把工作区里的一份成品交付给用户（交付即冻结：之后工作区怎么改都不影响已交付成果）。",
        args_schema=DeliverArgs,
        coroutine=deliver,
    )
