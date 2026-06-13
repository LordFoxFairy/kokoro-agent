from __future__ import annotations

from typing import Literal, TypeGuard, get_args

from pydantic import BaseModel, ConfigDict

AgentKind = Literal[
    "run.started",
    "thinking.delta",
    "text.delta",
    "text.completed",
    "tool.invoked",
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

# Per-kind ``payload`` shapes (the payload stays a loose dict here; strict
# per-kind validation is kokoro-session's job at the Zod boundary). Documented
# so the DeepAgents emitter and the session normalizer share one contract:
#   run.started        {}
#   thinking.delta     {"segment_id": str, "text": str}        # reasoning stream
#   text.delta         {"segment_id": str, "text": str}
#   text.completed     {"segment_id": str, "text": str}
#   tool.invoked       {"segment_id": str, "tool_id": str, "name": str, "args": dict[str, object]}
#   tool.returned      {"segment_id": str, "tool_id": str, "name": str, "result": str, "is_error": bool}
#   todo.updated       {"todos": [{"content": str, "status": "pending"|"in_progress"|"completed"}]}
#   subagent.started   {"segment_id": str, "subagent_id": str, "name": str, "description": str, "subagent_type": str, "source": "built-in"|"config-custom"|"runtime-custom"}
#   subagent.finished  {"segment_id": str, "subagent_id": str, "name": str, "subagent_type": str, "source": "built-in"|"config-custom"|"runtime-custom"}
#   subagent.text.delta {"segment_id": str, "subagent_id": str, "text": str}
#   subagent.text.completed {"segment_id": str, "subagent_id": str, "text": str}
#   run.completed      {"status": str}
#   run.failed         {"error_kind": str, "message": str}


class AgentEvent(BaseModel):
    """A raw execution-side event authored by kokoro-agent.

    The agent only fills execution semantics: ``kind``, ``run_id`` and a
    monotonic ``seq``. It never assigns ``event_id`` / ``cursor`` / ``timestamp``
    / ``owner_id`` — those belong to kokoro-session's normalization layer.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: AgentKind
    run_id: str
    seq: int
    payload: dict[str, object]
