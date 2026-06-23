"""Wire 层：入站运行消息的判别式联合，三种 kind 各路由到对应领域类型。"""

from __future__ import annotations

import logging
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError, model_validator

from kokoro_agent.domain.json_payload import JsonObject
# 直接复用领域层契约，避免重复定义 run.request 结构。
from kokoro_agent.domain.run_request import RunRequest

logger = logging.getLogger(__name__)


class ResumeDecision(BaseModel):
    """HITL 审批决策：操作类型与可选的编辑载荷/拒绝原因。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    type: Literal["approve", "edit", "reject", "respond"]
    # 仅 edit 时携带，允许前端修改工具调用参数后重新下发。
    edited_action: dict[str, JsonValue] | None = None
    # 仅 reject/respond 时携带，传达人工说明文本。
    message: str | None = None

    @model_validator(mode="after")
    def _check_fields_consistent(self) -> "ResumeDecision":
        # edit 必须携带 edited_action，否则下游 langgraph 会 KeyError。
        if self.type == "edit" and self.edited_action is None:
            raise ValueError("edit 型决策必须提供 edited_action")
        # reject/respond 必须携带 message，否则无法向用户传达说明。
        if self.type in {"reject", "respond"} and self.message is None:
            raise ValueError(f"{self.type} 型决策必须提供 message")
        # approve 不应携带任何可选字段，多余数据视为非法调用方错误。
        if self.type == "approve" and (self.edited_action is not None or self.message is not None):
            raise ValueError("approve 型决策不得携带 edited_action 或 message")
        return self


class RunResume(BaseModel):
    """恢复暂停中的 run，携带人工审批决策。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["run.resume"]
    run_id: str
    decision: ResumeDecision


class RunCancel(BaseModel):
    """取消正在进行中的 run。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["run.cancel"]
    run_id: str


# 判别式联合：Pydantic 根据 kind 字段快速路由到对应模型。
InboundMessage = Annotated[
    Union[RunRequest, RunResume, RunCancel],
    Field(discriminator="kind"),
]

_adapter: TypeAdapter[InboundMessage] = TypeAdapter(InboundMessage)


def parse_inbound(raw: JsonObject) -> InboundMessage | None:
    """解析入站帧；未知 kind 或结构非法时返回 None 并记录警告。"""
    try:
        return _adapter.validate_python(raw)
    except ValidationError as exc:
        logger.warning("入站消息解析失败: %s", exc)
        return None
