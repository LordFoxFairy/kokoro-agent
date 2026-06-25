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
# tool_id 显式归属：同帧多个被门控工具时，前端逐工具决策、各自带 tool_id，supervisor 按 pending
# 顺序重排后喂 langchain（其按序匹配 decisions↔interrupt），并支持「只拒其中一个」的部分审批。


class ApproveDecision(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["approve"]
    tool_id: str


class EditDecision(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["edit"]
    tool_id: str
    # 前端修改工具调用参数后重新下发。
    edited_action: dict[str, JsonValue]


class RejectDecision(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["reject"]
    tool_id: str
    message: str


class RespondDecision(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    type: Literal["respond"]
    tool_id: str
    message: str


ResumeDecision = Annotated[
    Union[ApproveDecision, EditDecision, RejectDecision, RespondDecision],
    Field(discriminator="type"),
]


class RunResume(BaseModel):
    """恢复暂停中的 run，携带逐工具人工审批决策（同帧多工具→多决策，与 awaiting 浮现一一对应）。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["run.resume"]
    run_id: str
    decisions: list[ResumeDecision]


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
