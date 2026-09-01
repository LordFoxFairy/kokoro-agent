"""Toolset assembly invariants."""

from __future__ import annotations

import pytest
from dataclasses import replace
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict

from kokoro_agent.mcp.config import McpServerConfig, McpServerUnavailable
from kokoro_agent.tools.toolset import Toolset, mcp_outage_definitions
from kokoro_agent.agents.music import MUSIC_AGENT
from kokoro_agent.agents.general import GENERAL_AGENT


class _NoArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


def _tool(name: str) -> StructuredTool:
    def run() -> str:
        return name

    return StructuredTool(
        func=run,
        name=name,
        description=f"{name} tool",
        args_schema=_NoArgs,
    )


def test_toolset_rejects_duplicate_names_across_sources() -> None:
    with pytest.raises(ValueError, match="duplicate tool name.*skill"):
        Toolset.from_tools((_tool("skill"), _tool("skill")))


def test_toolset_extension_rejects_handoff_collision() -> None:
    toolset = Toolset.from_tools((_tool("transfer_to_music"),))

    with pytest.raises(ValueError, match="duplicate tool name.*transfer_to_music"):
        toolset.with_tools((_tool("transfer_to_music"),))


def test_mcp_client_outage_keeps_deployment_and_marks_dynamic_names() -> None:
    local = McpServerConfig(url="https://mcp.example.test", allowed_tools=["search"])

    resolved = mcp_outage_definitions(
        {"local": local}, ("local", "capability-only")
    )

    assert resolved["local"] == local
    assert isinstance(resolved["capability-only"], McpServerUnavailable)


def test_delivery_is_an_explicit_agent_capability() -> None:
    assert MUSIC_AGENT.delivery is True
    assert GENERAL_AGENT.delivery is False
    assert replace(MUSIC_AGENT, delivery=False).delivery is False
