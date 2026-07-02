"""ask_user tool definition for HITL respond flows."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from langchain_core.tools import StructuredTool

from kokoro_agent.tools.names import ASK_USER_TOOL_NAME


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


__all__ = ["ASK_USER_TOOL", "ASK_USER_TOOL_NAME", "AskUserArgs"]
