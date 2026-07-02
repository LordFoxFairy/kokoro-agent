"""MCP tool exposure helpers."""

from __future__ import annotations


def mcp_tool_name(server: str, tool: str) -> str:
    return f"mcp__{server}__{tool}"
