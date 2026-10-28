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
    from kokoro_agent.clients.mcp import McpConnectionRef
    from kokoro_agent.domain.run.models import LeaseFence
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
        project_ref=None,
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant",
            actor=IdentityRef(kind="user", opaque_ref="actor"),
            subject=IdentityRef(kind="user", opaque_ref="subject"),
            identity_assertion_ref="assertion",
        ),
        input=RunInput(message_id="message", content="hello"),
    )
    assert await resolve_declared_mcp(
        request,
        Agent(key="base", prompt="base"),
        None,
        LeaseFence(owner="w", generation=1),
    ) == (None, ())
    with pytest.raises(McpClientError):
        await resolve_declared_mcp(
            request,
            Agent(
                key="mcp", prompt="mcp", mcp=(McpConnectionRef(connection_id="local"),)
            ),
            None,
            LeaseFence(owner="w", generation=1),
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
        project_ref=None,
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
        backend=Mock(spec=BackendProtocol),
        delivery=Mock(spec=DeliveryClient) if available else None,
    )
    assert len(plans) == 1
    expected = planner(agent=agent, toolbox=box, delivery_available=available)
    assert tuple(t.name for t in built.tools) == expected.names
    assert ("deliver" in expected.names) == (declared and available)
    assert built.authorized >= frozenset(expected.names)
    import deepagents
    from typing import Any
    from langchain_core.language_models.fake_chat_models import FakeListChatModel
    from kokoro_agent.agents.native_profile import validate_native_registry

    validate_native_registry()
    create_deep_agent: Any = getattr(deepagents, "create_deep_agent")
    native = create_deep_agent(
        model=FakeListChatModel(responses=["unused"]),
        tools=built.tools,
        backend=Mock(spec=BackendProtocol),
    )
    actual_codes = dict(built.mounted_display_codes(native))
    assert actual_codes.get("deliver") == (
        "tool.file.deliver" if declared and available else None
    )
    delivery_bindings = [
        (name, tool, code)
        for name, tool, code in built.display_bindings
        if name == "deliver"
    ]
    if declared and available:
        assert delivery_bindings == [
            ("deliver", built.by_name["deliver"], "tool.file.deliver")
        ]
    else:
        assert delivery_bindings == []


def test_display_codes_use_original_constructed_toolnode_objects_not_names() -> None:
    import deepagents
    from typing import Any

    create_deep_agent: Any = getattr(deepagents, "create_deep_agent")
    from deepagents.backends import StateBackend
    from langchain_core.language_models.fake_chat_models import FakeListChatModel
    from kokoro_agent.mcp.tools import make_mcp_tools
    from kokoro_agent.tools.web_fetch import make_web_fetch_tool
    from kokoro_agent.domain.chat.projection import ToolDisplayCode

    from kokoro_agent.agents.native_profile import validate_native_registry

    validate_native_registry()
    mcp = make_mcp_tools((), None, run_id="display-run")
    web = make_web_fetch_tool(allow_private=False)
    bindings: tuple[tuple[StructuredTool, ToolDisplayCode], ...] = (
        (mcp[0], "tool.mcp.discover"),
        (mcp[1], "tool.mcp.describe"),
        (mcp[2], "tool.mcp.call"),
        (web, "tool.web.fetch"),
    )
    tools = Toolset.from_tools((*mcp, web), display_bindings=bindings)
    native = create_deep_agent(
        model=FakeListChatModel(responses=["unused"]),
        tools=tools.tools,
        backend=StateBackend(),
    )
    codes = dict(tools.mounted_display_codes(native))
    assert codes == {
        "mcp_list_tools": "tool.mcp.discover",
        "mcp_describe_tool": "tool.mcp.describe",
        "mcp_call": "tool.mcp.call",
        "web_fetch": "tool.web.fetch",
        "read_file": "tool.file.read",
        "write_file": "tool.file.write",
    }
    assert tools.mounted_display_codes(object()) == ()
    counterfeit = create_deep_agent(
        model=FakeListChatModel(responses=["unused"]),
        tools=(*mcp, _tool("web_fetch")),
        backend=StateBackend(),
    )
    with pytest.raises(ValueError, match="actual ToolNode"):
        tools.mounted_display_codes(counterfeit)
    with pytest.raises(ValueError, match="mounted source"):
        Toolset.from_tools(
            (_tool("web_fetch"),), display_bindings=((web, "tool.web.fetch"),)
        )
    # A metadata label copied onto an unrelated object cannot confer a category.
    forged = _tool("web_search")
    forged.metadata = {"display_code": "tool.web.search"}
    unknown = Toolset.from_tools((forged,))
    graph = create_deep_agent(
        model=FakeListChatModel(responses=["unused"]),
        tools=unknown.tools,
        backend=StateBackend(),
    )
    assert "web_search" not in dict(unknown.mounted_display_codes(graph))


def test_missing_or_impostor_toolnode_cannot_grant_native_semantics() -> None:
    from langchain_core.runnables.graph import Graph
    from langgraph.prebuilt import ToolNode
    from typing import Any

    class Impostor(ToolNode):
        pass

    class Runnable:
        def __init__(self, node: ToolNode | None) -> None:
            self.node = node

        def get_graph(self) -> Graph:
            graph = Graph()
            if self.node is not None:
                add_node: Any = getattr(graph, "add_node")
                add_node(self.node, id="tools")
            return graph

    tools = Toolset.from_tools(())
    assert tools.mounted_display_codes(Runnable(None)) == ()
    assert tools.mounted_display_codes(Runnable(Impostor((_tool("read_file"),)))) == ()
    assert tools.mounted_display_codes(Runnable(ToolNode((_tool("read_file"),)))) == ()
    assert tools.mounted_display_codes(Runnable(ToolNode((_tool("write_file"),)))) == ()
    from deepagents.middleware.filesystem import WriteFileSchema

    counterfeit = _tool("write_file")
    counterfeit.args_schema = WriteFileSchema
    forged = Toolset.from_tools((counterfeit,))
    assert forged.mounted_display_codes(Runnable(ToolNode((counterfeit,)))) == ()


@pytest.mark.parametrize("virtual", [True, False])
async def test_detail_sources_require_actual_native_schema_and_run_file_backend(
    virtual: bool,
) -> None:
    import deepagents
    from typing import Any
    from unittest.mock import Mock
    from deepagents.backends.local_shell import LocalShellBackend
    from langchain_core.language_models.fake_chat_models import FakeListChatModel
    from kokoro_agent.clients.storage import DeliveryClient
    from kokoro_agent.clients.mcp import (
        McpDisplayDeclaration,
        McpConnectionRef,
        McpClient,
    )
    from kokoro_agent.tools.toolset import build_toolset, plan_toolset
    from kokoro_agent.tools.toolbox import ProcessToolbox
    from support.fakes import request
    from kokoro_agent.agents.native_profile import validate_native_registry

    validate_native_registry()
    backend = LocalShellBackend(virtual_mode=virtual)
    box = ProcessToolbox(configured=())
    agent = replace(
        GENERAL_AGENT,
        backend="local_shell",
        mcp=(McpConnectionRef(connection_id="conn"),),
    )
    built = await build_toolset(
        request("detail"),
        agent=agent,
        plan=plan_toolset(agent=GENERAL_AGENT, toolbox=box, delivery_available=True),
        toolbox=box,
        backend=backend,
        delivery=Mock(spec=DeliveryClient),
        resolved_mcp=Mock(spec=McpClient),
        mcp_declarations=(
            McpDisplayDeclaration(
                connection_id="conn", selector="search", display_name="Search documents"
            ),
        ),
    )
    create: Any = getattr(deepagents, "create_deep_agent")
    graph = create(
        model=FakeListChatModel(responses=["unused"]),
        tools=built.tools,
        backend=backend,
    )
    sources = {source.name: source for source in built.mounted_display_sources(graph)}
    for tool, field in [
        ("read_file", "file_path"),
        ("write_file", "file_path"),
        ("deliver", "path"),
    ]:
        detail = sources[tool].detail(
            {field: "/docs/report.md", "title": "SECRET", "content": "SECRET"}
        )
        assert (detail.model_dump() if detail else None) == (
            {"kind": "file", "name": "report.md", "virtual_path": "/docs/report.md"}
            if virtual
            else None
        )
    declared = sources["mcp_call"].detail({"server": "conn", "tool": "search"})
    assert declared is not None
    assert declared.model_dump() == {"kind": "mcp_tool", "name": "Search documents"}
    assert sources["mcp_call"].detail({"server": "other", "tool": "search"}) is None
    assert built.mounted_display_sources(object()) == ()
    # A custom tool with a manually assigned category still has no native provenance.
    impostor = _tool("read_file")
    forged = replace(
        Toolset.from_tools(
            (impostor,), display_bindings=((impostor, "tool.file.read"),)
        ),
        backend=backend,
    )
    from langgraph.prebuilt import ToolNode
    from langchain_core.runnables.graph import Graph

    class BareGraph:
        def get_graph(self) -> Graph:
            result = Graph()
            add_node: Any = getattr(result, "add_node")
            add_node(ToolNode((impostor,)), id="tools")
            return result

    assert forged.mounted_display_sources(BareGraph()) == ()


@pytest.mark.parametrize("backend_kind", ["custom", "e2b"])
async def test_unproven_backend_cannot_borrow_state_backend_display_policy(
    backend_kind: str,
) -> None:
    import deepagents
    from typing import Any
    from deepagents.backends import StateBackend
    from langchain_core.language_models.fake_chat_models import FakeListChatModel
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.policy import Backend
    from pydantic import TypeAdapter
    from kokoro_agent.tools.toolset import build_toolset, plan_toolset
    from kokoro_agent.tools.toolbox import ProcessToolbox
    from support.fakes import request

    backend_adapter: TypeAdapter[Backend] = TypeAdapter(Backend)
    agent = Agent(
        key="custom",
        prompt="p",
        backend=backend_adapter.validate_python(backend_kind),
    )
    backend = StateBackend()
    box = ProcessToolbox(configured=())
    tools = await build_toolset(
        request("custom"),
        agent=agent,
        plan=plan_toolset(agent=agent, toolbox=box, delivery_available=False),
        toolbox=box,
        backend=backend,
        delivery=None,
    )
    create: Any = getattr(deepagents, "create_deep_agent")
    graph = create(
        model=FakeListChatModel(responses=["unused"]),
        tools=tools.tools,
        backend=backend,
    )
    read = next(
        source
        for source in tools.mounted_display_sources(graph)
        if source.name == "read_file"
    )
    assert read.detail({"file_path": "/report.md"}) is None


async def test_real_declared_skill_mount_exposes_only_safe_basename() -> None:
    import deepagents
    from typing import Any
    from deepagents.backends import StateBackend
    from deepagents.backends.composite import CompositeBackend
    from langchain_core.language_models.fake_chat_models import FakeListChatModel
    from kokoro_agent.clients.skills import ResolvedSkill
    from kokoro_agent.skills.backend import TypedSkillBackend
    from kokoro_agent.tools.toolset import build_toolset, plan_toolset
    from kokoro_agent.tools.toolbox import ProcessToolbox
    from support.fakes import request

    skill = ResolvedSkill(
        source_ref="skill:one",
        skill_id="one",
        revision=1,
        asset_ref="asset",
        content_digest="a" * 64,
        manifest_identity="identity",
    )
    prefix = f"/.skills/{skill.path_segment}/"
    backend = CompositeBackend(
        default=StateBackend(), routes={"/.skills/": TypedSkillBackend((skill,), None)}
    )
    box = ProcessToolbox(configured=())
    tools = await build_toolset(
        request("skill-detail"),
        agent=GENERAL_AGENT,
        plan=plan_toolset(agent=GENERAL_AGENT, toolbox=box, delivery_available=False),
        toolbox=box,
        backend=backend,
        delivery=None,
        skill_prefixes=(prefix,),
    )
    create: Any = getattr(deepagents, "create_deep_agent")
    graph = create(
        model=FakeListChatModel(responses=["unused"]),
        tools=tools.tools,
        backend=backend,
    )
    sources = {source.name: source for source in tools.mounted_display_sources(graph)}
    read = sources["read_file"].detail({"file_path": prefix + "SKILL.md"})
    assert read is not None and read.model_dump() == {
        "kind": "file",
        "name": "SKILL.md",
        "virtual_path": prefix + "SKILL.md",
    }
    assert (
        sources["read_file"].detail({"file_path": "/.skills/unknown/SKILL.md"}) is None
    )
    assert sources["write_file"].detail({"file_path": prefix + "SKILL.md"}) is None
