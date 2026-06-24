"""Wire 层：入站运行消息的判别式联合，三种 kind 各路由到对应领域类型。"""

from __future__ import annotations

import logging
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError

from kokoro_agent.domain.json_payload import JsonObject
# 直接复用领域层契约，避免重复定义 run.request 结构。
from kokoro_agent.domain.run_request import RunRequest

logger = logging.getLogger(__name__)

# HITL 审批决策：按 type 判别联合，各型恰好携带其必需字段——结构即约束，无需手动交叉校验。


class ApproveDecision(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["approve"]


class EditDecision(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["edit"]
    # 前端修改工具调用参数后重新下发。
    edited_action: dict[str, JsonValue]


class RejectDecision(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["reject"]
    message: str


class RespondDecision(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["respond"]
    message: str


ResumeDecision = Annotated[
    Union[ApproveDecision, EditDecision, RejectDecision, RespondDecision],
    Field(discriminator="type"),
]


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
