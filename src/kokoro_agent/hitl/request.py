"""通用人机暂停原语：HumanRequest 统一载荷 + request_human（包装 langgraph 原生 interrupt）。

任何工具内、任意执行点都能 `request_human(kind=..., schema=..., context=...)` 发起暂停，
不再只有"调用前审批"一个卡点。挂起点由 checkpoint 承载（langgraph interrupt/resume 机制），
resume 后回应值原地返回调用点续跑。现状三场景（approval/question/review）是它的预设形态。
"""

from __future__ import annotations

from typing import Literal

from langgraph.types import interrupt
from pydantic import BaseModel, ConfigDict, Field, JsonValue

# 四 kind：approval/question/review 有 wire 投影；input 为 schema 驱动表单预留（H1 无生产调用方）。
HumanKind = Literal["approval", "question", "review", "input"]

# 本原语信封在 interrupt.value 中的判别键（区别于 langchain HITL 的 action_requests/review_configs 形态）。
_ENVELOPE_KEY = "kokoro_human_request"


class HumanRequest(BaseModel):
    """一次人机暂停请求的统一载荷：kind + schema + context，request_id 为幂等锚。

    request_id 是幂等锚（工具边界场景 = tool_id）：已决请求恢复重放不复活由 checkpoint 保证。
    schema/context 是 kind 相关的展示与回应契约，投影层按 kind 解读为既有 wire 事件。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    request_id: str
    kind: HumanKind
    # 期待回应的 JSON Schema（kind=input 表单驱动预留）；approval/question/review 决策集固定，缺省。
    response_schema: dict[str, JsonValue] | None = None
    # 展示载荷：tool_name/args/result/risk… 由 kind 约定字段，投影层按 kind 校验解读。
    context: dict[str, JsonValue] = Field(default_factory=dict)

    def to_interrupt_value(self) -> dict[str, JsonValue]:
        return {_ENVELOPE_KEY: self.model_dump()}

    @classmethod
    def from_interrupt_value(cls, value: object) -> HumanRequest | None:
        # 非本原语信封（langchain approval 形态等）→ None，让调用方回退旧投影路径；
        # 信封键在但体不合法 → strict 校验 fail-loud，绝不发错帧。
        if not isinstance(value, dict) or _ENVELOPE_KEY not in value:
            return None
        return cls.model_validate(value[_ENVELOPE_KEY])


def request_human(
    *,
    kind: HumanKind,
    request_id: str,
    schema: dict[str, JsonValue] | None = None,
    context: dict[str, JsonValue] | None = None,
) -> JsonValue:
    """人机暂停原语：包装 langgraph 原生 interrupt。

    首跑抛 GraphInterrupt 挂起（checkpoint 承载），resume 后原地返回人给的回应值续跑——
    与 `interrupt` 同为同步调用，async 工具体内直接调用（无需 await），语义等同直用 interrupt。
    request_id 是幂等锚（工具边界场景 = tool_id）。"""
    envelope = HumanRequest(
        request_id=request_id,
        kind=kind,
        response_schema=schema,
        context=dict(context or {}),
    ).to_interrupt_value()
    return interrupt(envelope)
