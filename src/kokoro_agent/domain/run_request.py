from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

ExecutionStyle = Literal["fast", "thinking"]


class RunRequest(BaseModel):
    """A run request authored by kokoro-session (stream ``kokoro:runs:requests``)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["run.request"]
    run_id: str
    session_id: str
    conversation_id: str
    input: str
    execution_style: ExecutionStyle = "fast"
