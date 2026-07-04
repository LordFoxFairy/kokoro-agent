"""export_artifact：把生成内容导出为用户可下载/预览的产物（音频/文档/任意格式通用）。"""

from __future__ import annotations

import base64
import binascii
from typing import Annotated, Literal

from langchain_core.tools import InjectedToolCallId, StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.storage.artifacts import ArtifactStore

EXPORT_ARTIFACT_TOOL_NAME = "export_artifact"

# 单产物上限：超限 fail-loud（wire 只带引用，但共享库与端点回体要有边界）。
ARTIFACT_MAX_BYTES = 64 * 1024 * 1024


class ExportArtifactArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(min_length=1, description="文件名（含扩展名），如 report.md、track.wav")
    mime: str = Field(min_length=1, description="MIME 类型，如 text/markdown、audio/wav")
    content: str = Field(min_length=1, description="产物内容：文本原文，或二进制的 base64")
    encoding: Literal["text", "base64"] = Field(
        default="text", description="content 编码：text=UTF-8 文本，base64=二进制"
    )
    # 框架注入（模型不可见）：产物幂等 id 的派生源。
    tool_call_id: Annotated[str, InjectedToolCallId]


def make_export_artifact_tool(store: ArtifactStore, run_id: str) -> StructuredTool:
    """产物归属（run_id）与库实例在装配期注入——工具体不含存储选择与租户概念。"""

    async def _export(
        name: str,
        mime: str,
        content: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
        encoding: Literal["text", "base64"] = "text",
    ) -> tuple[str, dict[str, object]]:
        if encoding == "base64":
            try:
                data = base64.b64decode(content, validate=True)
            except (binascii.Error, ValueError) as error:
                raise ValueError(f"invalid base64 content: {error}") from error
        else:
            data = content.encode("utf-8")
        if len(data) > ARTIFACT_MAX_BYTES:
            raise ValueError(f"artifact exceeds {ARTIFACT_MAX_BYTES} bytes: {len(data)}")
        ref = await store.put(run_id, tool_call_id, name, mime, data)
        # content_and_artifact：摘要给模型，引用（Artifact dump）给投影层升 wire。
        summary = f"已导出产物 {ref.name}（{ref.mime}，{ref.bytes} 字节），用户可在会话中预览/下载。"
        return (summary, ref.model_dump())

    return StructuredTool(
        name=EXPORT_ARTIFACT_TOOL_NAME,
        description=(
            "把已生成的内容导出为用户可下载/预览的产物文件（报告、代码、CSV、音频等任意格式）。"
            "文本用 encoding=text；二进制先 base64。导出后用户会在会话中看到预览卡，无需再贴全文。"
        ),
        args_schema=ExportArtifactArgs,
        coroutine=_export,
        response_format="content_and_artifact",
    )
