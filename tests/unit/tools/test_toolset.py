"""Toolset assembly invariants."""

from __future__ import annotations

import pytest
from dataclasses import replace
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, ConfigDict

from kokoro_agent.tools.toolset import Toolset
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


def test_delivery_is_an_explicit_agent_capability() -> None:
    assert MUSIC_AGENT.delivery is True
    assert GENERAL_AGENT.delivery is True
    assert replace(MUSIC_AGENT, delivery=False).delivery is False


async def test_declared_mcp_does_not_fall_back_to_deployment() -> None:
    from kokoro_agent.tools.toolset import resolve_declared_mcp
    from kokoro_agent.clients.mcp import McpClientError
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.mcp.config import McpServerConfig
    from kokoro_agent.protocol import (
        RunRequest,
        RunInput,
        ExecutionIdentity,
        IdentityRef,
    )

    request = RunRequest(
        kind="run.request",
        run_id="run",
        session_id="session",
        feature_key="chat",
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant",
            actor=IdentityRef(kind="user", opaque_ref="actor"),
            subject=IdentityRef(kind="user", opaque_ref="subject"),
            identity_assertion_ref="assertion",
        ),
        input=RunInput(message_id="message", content="hello"),
    )
    assert (
        await resolve_declared_mcp(request, Agent(key="base", prompt="base"), None, {})
        == {}
    )
    with pytest.raises(McpClientError):
        await resolve_declared_mcp(
            request,
            Agent(key="mcp", prompt="mcp", mcp=("local",)),
            None,
            {
                "local": McpServerConfig(
                    url="https://mcp.test", allowed_tools=["search"]
                )
            },
        )
