from __future__ import annotations

# seq 已去除：事件顺序由 transport cursor 保证，不在 wire 层维护单调序号。
from typing import Literal, TypeGuard, get_args

from pydantic import BaseModel, ConfigDict, JsonValue

AgentKind = Literal[
    "run.started",
    "thinking.delta",
    "text.delta",
    "text.completed",
    "tool.invoked",
    "tool.awaiting_approval",
    "tool.returned",
    "todo.updated",
    "subagent.started",
    "subagent.finished",
    "subagent.text.delta",
    "subagent.text.completed",
    "run.completed",
    "run.failed",
]

_AGENT_KINDS: frozenset[str] = frozenset(get_args(AgentKind))


def is_agent_kind(kind: str) -> TypeGuard[AgentKind]:
    return kind in _AGENT_KINDS


class AgentEvent(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    kind: AgentKind
    run_id: str
    payload: dict[str, JsonValue]
