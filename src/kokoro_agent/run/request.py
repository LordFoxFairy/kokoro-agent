"""领域层：kokoro-session 下发的一次运行请求（严格契约）。"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

# 执行风格：fast 直接作答 / thinking 显式推理。
ExecutionStyle = Literal["fast", "thinking"]
# 权限档位（Claude-Code 式）：auto 全放行 / default 拦外部副作用。
PermissionMode = Literal["auto", "default"]


class RunRequest(BaseModel):
    """来自 kokoro-session 请求流 ``kokoro:runs:requests`` 的一次运行请求。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["run.request"]
    run_id: str
    session_id: str
    conversation_id: str
    input: str
    execution_style: ExecutionStyle = "fast"
    permission_mode: PermissionMode = "auto"
