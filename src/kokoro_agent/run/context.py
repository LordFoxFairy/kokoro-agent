"""Run context carried from session into a single agent run."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class RunContext(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    namespace: str
    site_id: str | None = None
    user_id: str | None = None
    workspace_id: str | None = None
    project_id: str | None = None
    session_id: str | None = None
