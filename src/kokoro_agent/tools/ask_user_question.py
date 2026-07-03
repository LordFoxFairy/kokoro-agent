"""ask_user_question 工具：HITL respond 流程的语义暂停点（对齐 CC 的 AskUserQuestion）。"""

from __future__ import annotations

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

ASK_USER_TOOL_NAME = "ask_user_question"


class AskUserArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    question: str
    choices: tuple[str, ...] = ()
    allow_free_text: bool = Field(default=True)


def _ask_user_uninterrupted(
    question: str, choices: tuple[str, ...] = (), allow_free_text: bool = True
) -> str:
    raise RuntimeError("ask_user must be resolved through HITL respond before execution")


ASK_USER_TOOL = StructuredTool(
    name=ASK_USER_TOOL_NAME,
    description="Ask the user for missing information or a choice.",
    args_schema=AskUserArgs,
    func=_ask_user_uninterrupted,
)
