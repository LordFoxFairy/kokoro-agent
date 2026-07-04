"""ask_user_question 工具：HITL respond 流程的语义暂停点（对齐 CC 的 AskUserQuestion）。"""

from __future__ import annotations

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict, Field

from kokoro_agent.tools.guidance import ToolGuidance

ASK_USER_TOOL_NAME = "ask_user_question"

GUIDANCE = ToolGuidance(
    requires=frozenset({ASK_USER_TOOL_NAME}),
    text="""## 何时向用户提问（ask_user_question）
- 只在缺少"只有用户知道"的关键信息（偏好、目标、指代不明）时提问；能自己查证的不要问。
- 一次问清并给出可选项，不连环追问。""",
)


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
