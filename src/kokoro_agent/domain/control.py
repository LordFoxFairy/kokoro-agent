"""领域层：control 通道的人工审批契约与流终止信号。"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue

ControlDecision = Literal["approve", "reject", "cancel"]


class ControlMessage(BaseModel):
    """control 通道的人工审批消息契约；畸形载荷显式丢弃，不被误判为任一决定。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["control"]
    decision: ControlDecision
    # 仅 approve 有意义：用户在审批暂停时编辑后的工具参数，整体替换模型原参数。
    args: dict[str, JsonValue] | None = None


class ControlChannelClosed(Exception):
    """control 流在等到决定前意外终止：fail-loud 信号，绝不退化为伪造的 reject。"""
