"""计划提案工具：以真实 tool_call_id 为 HITL owner，批准后才返回模型继续执行。"""

from __future__ import annotations

from typing import Annotated

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

PROPOSE_PLAN_TOOL_NAME = "propose_plan"

PlanSummary = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=4096)
]
PlanStepLabel = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=1024)
]


class ProposePlanStep(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    label: PlanStepLabel


class ProposePlanArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    summary: PlanSummary
    steps: Annotated[list[ProposePlanStep], Field(min_length=1, max_length=256)]


def _plan_accepted(summary: str, steps: list[dict[str, str]]) -> str:
    del summary, steps
    return "The user accepted this plan. Continue with the approved steps."


PROPOSE_PLAN_TOOL = StructuredTool(
    name=PROPOSE_PLAN_TOOL_NAME,
    description=(
        "向用户提交一份需要明确接受或拒绝的执行计划。仅在执行前确实需要用户确认方案时使用；"
        "必须作为本轮唯一工具调用。summary 概括目标，steps 按执行顺序列出。"
    ),
    args_schema=ProposePlanArgs,
    func=_plan_accepted,
)
