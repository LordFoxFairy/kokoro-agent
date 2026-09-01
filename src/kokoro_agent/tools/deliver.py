"""Publish a DeepAgents workspace file through Storage's Artifact contract."""

from __future__ import annotations

import hashlib
import mimetypes
from pathlib import PurePosixPath
from typing import Literal

from deepagents.backends.protocol import BackendProtocol
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.clients.storage import (
    DeliveryClient,
    DeliveryRequest,
    StorageClientError,
)
from kokoro_agent.contract import ExecutionIdentity

DELIVER_TOOL_NAME = "deliver"


class DeliverArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    path: str = Field(description="工作区内的成果文件绝对路径，如 /report.pdf。")
    title: str = Field(description="成果标题（展示用）。")
    note: str = Field(default="", description="交付说明（可选）。")


class DeliverResult(BaseModel):
    """Tool result parsed by the product-event publisher."""

    model_config = ConfigDict(strict=True, extra="forbid")

    status: Literal["delivered"]
    path: str
    title: str
    mime: str
    size: int
    content_hash: str
    note: str


def make_deliver_tool(
    backend: BackendProtocol,
    delivery: DeliveryClient,
    *,
    namespace: str,
    run_id: str,
    identity: ExecutionIdentity,
) -> StructuredTool:
    """Bind one run's workspace and Storage facade to the delivery tool."""

    async def deliver(path: str, title: str, note: str = "") -> str:
        normalized = _workspace_path(path)
        if normalized is None:
            return f"error: 路径 {path!r} 不是合法的工作区绝对路径。"

        downloaded = await backend.adownload_files([normalized])
        if len(downloaded) != 1:
            return "error: 工作区未返回请求的文件。"
        file = downloaded[0]
        if file.error is not None or file.content is None:
            return f"error: 文件 {path!r} 无法读取（{file.error or 'empty response'}）。"

        content_hash = hashlib.sha256(file.content).hexdigest()
        mime = mimetypes.guess_type(PurePosixPath(normalized).name)[0]
        mime = mime or "application/octet-stream"
        request_id = hashlib.sha256(
            f"{run_id}\0{normalized}\0{content_hash}".encode()
        ).hexdigest()
        try:
            receipt = await delivery.publish(
                DeliveryRequest(
                    request_id=request_id,
                    run_id=run_id,
                    namespace=namespace,
                    identity=identity,
                    path=normalized,
                    title=title,
                    note=note,
                    mime_type=mime,
                    content_sha256=content_hash,
                    content=file.content,
                )
            )
        except StorageClientError as exc:
            return f"error: 交付存储暂不可用（{exc}）。"

        if (
            receipt.content_sha256 != content_hash
            or receipt.size_bytes != len(file.content)
            or receipt.mime_type != mime
        ):
            return "error: Storage 交付回执与源文件不一致。"

        return DeliverResult(
            status="delivered",
            path=normalized,
            title=title,
            mime=mime,
            size=len(file.content),
            content_hash=content_hash,
            note=note,
        ).model_dump_json()

    return StructuredTool(
        name=DELIVER_TOOL_NAME,
        description="把工作区中的成品发布为用户可访问的冻结产物。",
        args_schema=DeliverArgs,
        coroutine=deliver,
    )


def _workspace_path(path: str) -> str | None:
    candidate = PurePosixPath(path)
    if not path.startswith("/") or ".." in candidate.parts or path.startswith("/.skills/"):
        return None
    normalized = str(candidate)
    if normalized == "/" or path.endswith("/"):
        return None
    return normalized
