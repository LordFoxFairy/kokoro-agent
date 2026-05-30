from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

AgentKind = Literal[
    "run.started",
    "text.delta",
    "text.completed",
    "tool.invoked",
    "tool.returned",
    "thinking.delta",
    "run.completed",
    "run.failed",
]


class RunRequest(BaseModel):
    """A run request authored by kokoro-session (stream ``kokoro:runs:requests``)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["run.request"]
    run_id: str
    session_id: str
    conversation_id: str
    input: str
    execution_style: str = "fast"


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
