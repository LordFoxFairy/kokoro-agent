"""内置交互工具：让模型向用户请求必要输入。"""

from __future__ import annotations

from typing import Literal

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

AskUserQuestionInputType = Literal[
    "text",
    "textarea",
    "single_choice",
    "multi_choice",
    "confirmation",
]


class AskUserQuestionOption(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    id: str
    label: str
    description: str | None = None


class AskUserQuestionInput(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        populate_by_name=True,
    )

    prompt: str
    description: str | None = None
    input_type: AskUserQuestionInputType = Field(alias="inputType")
    options: list[AskUserQuestionOption] | None = None
    required: bool = True
    allow_custom_option: bool = Field(default=False, alias="allowCustomOption")


class AskUserQuestionResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", populate_by_name=True)

    submitted: bool
    value: str | None = None
    selected_option_ids: list[str] | None = Field(default=None, alias="selectedOptionIds")
    values: dict[str, object] | None = None
    cancelled: bool = False


async def ask_user_question(
    prompt: str,
    input_type: AskUserQuestionInputType,
    description: str | None = None,
    options: list[AskUserQuestionOption] | None = None,
    required: bool = True,
    allow_custom_option: bool = False,
) -> str:
    raise RuntimeError(
        "ask_user_question must be resolved through HITL respond; direct execution is invalid"
    )


ASK_USER_QUESTION_TOOL = StructuredTool(
    name="ask_user_question",
    description=(
        "当缺少关键用户输入、需要用户选择或确认时使用。该工具会暂停执行并等待用户回答。"
    ),
    args_schema=AskUserQuestionInput,
    coroutine=ask_user_question,
)
