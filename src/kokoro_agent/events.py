from __future__ import annotations

from typing import Literal

ExecutionStyle = Literal["fast", "thinking"]

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

# Per-kind ``payload`` shapes (the payload stays a loose dict here; strict
# per-kind validation is kokoro-session's job at the Zod boundary). Documented
# so the DeepAgents emitter and the session normalizer share one contract:
#   run.started        {}
#   thinking.delta     {"message_ref": str, "text": str}        # reasoning stream
#   text.delta         {"message_ref": str, "text": str}
#   text.completed     {"message_ref": str, "text": str}
#   tool.invoked       {"message_ref": str, "tool_id": str, "name": str, "args": dict[str, object]}
#   tool.returned      {"message_ref": str, "tool_id": str, "name": str, "result": str}
#   todo.updated       {"todos": [{"content": str, "status": "pending"|"in_progress"|"completed"}]}
#   subagent.started   {"message_ref": str, "subagent_id": str, "name": str, "description": str, "subagent_type": str, "source": "built-in"|"config-custom"|"runtime-custom"}
#   subagent.finished  {"message_ref": str, "subagent_id": str, "name": str, "subagent_type": str, "source": "built-in"|"config-custom"|"runtime-custom"}
#   subagent.text.delta {"message_ref": str, "subagent_id": str, "text": str}
#   subagent.text.completed {"message_ref": str, "subagent_id": str, "text": str}
#   run.completed      {"status": str}
#   run.failed         {"error_kind": str, "message": str}


class RunRequest(BaseModel):
    """A run request authored by kokoro-session (stream ``kokoro:runs:requests``)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["run.request"]
    run_id: str
    session_id: str
    conversation_id: str
    input: str
    execution_style: ExecutionStyle = "fast"


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
