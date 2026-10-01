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


def test_toolbox_plan_is_unbound_and_options_exclude_credentials() -> None:
    from kokoro_agent.tools import toolbox as module
    from kokoro_agent.tools.web_search import SearchProviderSettings
    from pydantic import SecretStr

    assert callable(getattr(module, "plan_toolbox", None)), (
        "shared toolbox plan missing"
    )
    box = module.build_toolbox(
        fetch_allow_private=False,
        search=SearchProviderSettings(
            provider="tavily",
            api_key=SecretStr("private-key"),
            base_url="https://private.example",
        ),
    )
    plan = module.plan_toolbox(box)
    assert plan.names == ("save_memory", "search_memory", "web_fetch", "web_search")
    assert tuple(t.name for t in box.tools_for("namespace-not-profile")) == plan.names
    assert box.profile_options is not None
    assert box.profile_options.search_provider == "tavily"
    assert "private-key" not in repr(plan) + repr(box.profile_options)
    assert "private.example" not in repr(plan) + repr(box.profile_options)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "declared,available", [(False, False), (False, True), (True, False), (True, True)]
)
async def test_toolset_materialization_consumes_the_shared_plan(
    monkeypatch: pytest.MonkeyPatch, declared: bool, available: bool
) -> None:
    from kokoro_agent.tools import toolset as module
    from kokoro_agent.tools.toolbox import ProcessToolbox
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.protocol import (
        RunRequest,
        RunInput,
        ExecutionIdentity,
        IdentityRef,
    )
    from deepagents.backends.protocol import BackendProtocol
    from unittest.mock import Mock
    from kokoro_agent.clients.storage import DeliveryClient

    assert callable(getattr(module, "plan_toolset", None)), (
        "shared toolset plan missing"
    )
    planner = module.plan_toolset
    plans: list[object] = []

    def recording_plan(*args: object, **kwargs: object) -> object:
        plan = planner(*args, **kwargs)
        plans.append(plan)
        return plan

    monkeypatch.setattr(module, "plan_toolset", recording_plan)
    box = ProcessToolbox(configured=(_tool("configured"),))
    agent = Agent(key="base", prompt="base", tools=(_tool("core"),), delivery=declared)
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
    built = await module.build_toolset(
        request,
        agent=agent,
        plan=module.plan_toolset(
            agent=agent, toolbox=box, delivery_available=available
        ),
        toolbox=box,
        mcp_servers={},
        mcp_client=None,
        backend=Mock(spec=BackendProtocol),
        delivery=Mock(spec=DeliveryClient) if available else None,
    )
    assert len(plans) == 1
    expected = planner(agent=agent, toolbox=box, delivery_available=available)
    assert tuple(t.name for t in built.tools) == expected.names
    assert ("deliver" in expected.names) == (declared and available)
    assert built.authorized >= frozenset(expected.names)
