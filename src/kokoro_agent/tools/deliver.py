"""成果交付工具（deliver）：读工作区文件字节 → sha256 → 冻结进 deliveries/<ns>/<hash>。

交付即冻结：读到哪份字节冻结哪份（构造上自洽，无需 quiesce）；同内容同 key 天然幂等，
异内容异 key 物理上不可能覆盖。工具恒挂（schema 不随配置变，D9）：无 workspace / 无
deliveries 时调用降级为 error 文本（模型自纠，不炸 run）。
"""

from __future__ import annotations

import asyncio
import hashlib
import mimetypes
from pathlib import Path
from typing import Literal

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.skills.hub import PackageStore, SkillHubError

DELIVER_TOOL_NAME = "deliver"
MAX_DELIVERY_BYTES = 25 * 1024 * 1024


def _read_delivery_bytes(target: Path) -> bytes:
    # limit + 1 distinguishes an exact-boundary file from a growing/oversized file
    # without ever loading the whole source into memory.
    with target.open("rb") as source:
        return source.read(MAX_DELIVERY_BYTES + 1)


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
        # 虚拟根绝对路径映射进工作区（同归档 _archive_file 的 lstrip("/") 约定）。
        root = workspace_dir.resolve()
        target = (workspace_dir / path.lstrip("/")).resolve()
        if not target.is_relative_to(root):  # resolve 后仍在工作区内，否则拒（路径穿越/符号链接越界）。
            return f"error: 路径 {path!r} 越出工作区，拒绝交付。"
        if not target.is_file():
            return f"error: 文件 {path!r} 不存在或不是常规文件。"
        if target.stat().st_size > MAX_DELIVERY_BYTES:
            return f"error: 文件 {path!r} 超过交付上限（25 MiB）。"
        data = await asyncio.to_thread(_read_delivery_bytes, target)
        if len(data) > MAX_DELIVERY_BYTES:
            return f"error: 文件 {path!r} 超过交付上限（25 MiB）。"
        content_hash = hashlib.sha256(data).hexdigest()
        mime = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
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
