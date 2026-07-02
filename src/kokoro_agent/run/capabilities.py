"""Capabilities authorized for a single agent run."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class Capabilities(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    tools: tuple[str, ...] = ()
    skills: tuple[str, ...] = ()
    mcp_servers: tuple[str, ...] = ()
    subagents: tuple[str, ...] = ()
    sandbox: str | None = None
