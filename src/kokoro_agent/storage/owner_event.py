"""Typed result of the fenced Agent owner-event unit of work."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator

from kokoro_agent.contract import AgentEvent


class OwnerEventCommitResult(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    status: Literal["committed", "idempotent", "fence_lost"]
    event: AgentEvent | None = None

    @model_validator(mode="after")
    def validate_event(self) -> OwnerEventCommitResult:
        if (self.status == "committed") != (self.event is not None):
            raise ValueError("OWNER_EVENT_COMMIT_RESULT_INVALID")
        return self


class OwnerEventFenceLost(RuntimeError):
    """The current executor no longer owns the authoritative producer fence."""
