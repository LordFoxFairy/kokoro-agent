"""MCP server configuration boundary for per-run capabilities."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class McpServerConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    transport: str
    url: str
    allowed_tools: tuple[str, ...] = ()
