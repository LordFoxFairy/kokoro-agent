"""工具面组装：一次 run 挂哪些工具、谁在授权白名单，一处看全。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from deepagents.backends.protocol import BackendProtocol
from langchain_core.tools import BaseTool, StructuredTool
from langchain_core.runnables.graph import Graph
from langgraph.prebuilt import ToolNode
from deepagents.middleware.filesystem import ReadFileSchema, WriteFileSchema
from kokoro_agent.domain.chat.projection import ToolDisplayCode, ToolDisplaySource
from deepagents.backends.composite import CompositeBackend
from deepagents.backends.state import StateBackend
from deepagents.backends.local_shell import LocalShellBackend
from kokoro_agent.sandbox.archive import ArchivingLocalShellBackend
from kokoro_agent.sandbox.docker_backend import (
    DockerShellBackend,
    ArchivingDockerShellBackend,
)
from kokoro_agent.skills.backend import TypedSkillBackend, SKILLS_ROOT
from kokoro_agent.clients.mcp import McpDisplayDeclaration

from kokoro_agent.protocol import RunRequest
from kokoro_agent.agents.definition import Agent
from kokoro_agent.policy import Backend
from kokoro_agent.domain.run.scope import RunScope
from kokoro_agent.mcp.tools import make_mcp_tools
from kokoro_agent.clients.mcp import McpClientError
from kokoro_agent.clients.mcp import McpClient, RunMcpFactory
from kokoro_agent.domain.run.models import LeasedRun
from kokoro_agent.clients.storage import DeliveryClient
from kokoro_agent.domain.run.models import LeaseFence
from kokoro_agent.tools.deliver import make_deliver_tool
from kokoro_agent.tools.registry import RESERVED_TOOL_NAMES, resolve_tools
from kokoro_agent.tools.toolbox import ProcessToolbox, plan_toolbox


@dataclass(frozen=True, slots=True)
class Toolset:
    """本次 run 的工具全集与派生索引：一次构建，装配各步共用。"""

    tools: tuple[BaseTool, ...]
    # ToolPolicyMiddleware fail-closed 白名单：真挂载工具 + deepagents 保留工具（文件/执行/todo/task）。
    authorized: frozenset[str]
    # 子代理装配按名取工具：复用主链工具实例=复用其政策配置。
    by_name: Mapping[str, BaseTool]
    # 审批卡数据源：真挂载工具的自述（deepagents 保留工具不在册，wire 发空串由 web 兜底文案）。
    descriptions: Mapping[str, str]
    display_bindings: tuple[tuple[str, BaseTool, ToolDisplayCode], ...] = ()
    display_sources: tuple[ToolDisplaySource, ...] = ()
    backend: BackendProtocol | None = None
    backend_kind: Backend | None = None
    skill_prefixes: tuple[str, ...] = ()

    @classmethod
    def from_tools(
        cls,
        tools: Sequence[BaseTool],
        *,
        implicit_authorized: frozenset[str] = RESERVED_TOOL_NAMES,
        display_bindings: Sequence[tuple[BaseTool, ToolDisplayCode]] = (),
    ) -> Toolset:
        """Build all derived indexes once and reject cross-source name collisions."""

        by_name: dict[str, BaseTool] = {}
        for tool in tools:
            if tool.name in by_name:
                raise ValueError(
                    f"duplicate tool name across GA sources: {tool.name!r}"
                )
            by_name[tool.name] = tool
        bindings: tuple[tuple[str, BaseTool, ToolDisplayCode], ...] = tuple(
            (tool.name, tool, code) for tool, code in display_bindings
        )
        if len({name for name, _, _ in bindings}) != len(bindings) or any(
            by_name.get(name) is not tool for name, tool, _ in bindings
        ):
            raise ValueError("display binding must identify a unique mounted source")
        return cls(
            tools=tuple(tools),
            authorized=frozenset(by_name) | implicit_authorized,
            by_name=by_name,
            descriptions={
                name: tool.description
                for name, tool in by_name.items()
                if tool.description
            },
            display_bindings=bindings,
        )

    def mounted_display_codes(
        self, runnable: object
    ) -> tuple[tuple[str, ToolDisplayCode], ...]:
        """Observe the official constructed ToolNode, never infer from stream names."""
        get_graph = getattr(runnable, "get_graph", None)
        if not callable(get_graph):
            return ()
        graph = get_graph()
        if not isinstance(graph, Graph):
            return ()
        node = graph.nodes.get("tools")
        data: object = getattr(node, "data", None)
        if not isinstance(data, ToolNode) or type(data) is not ToolNode:
            return ()
        mounted = data.tools_by_name
        if any(
            mounted.get(name) is not tool or tool.name != name
            for name, tool, _ in self.display_bindings
        ):
            raise ValueError("display source differs from the actual ToolNode")
        codes: dict[str, ToolDisplayCode] = {
            name: code for name, _, code in self.display_bindings
        }
        # Reserved-name collision is rejected by the factory before construction.
        # Inspect only the official node's object and the pinned native schema.
        native_read = mounted.get("read_file")
        if (
            "read_file" not in self.by_name
            and native_read is not None
            and native_read.args_schema is ReadFileSchema
        ):
            codes["read_file"] = "tool.file.read"
        native_write = mounted.get("write_file")
        if (
            "write_file" not in self.by_name
            and native_write is not None
            and native_write.args_schema is WriteFileSchema
        ):
            codes["write_file"] = "tool.file.write"
        return tuple(sorted(codes.items()))

    def mounted_display_sources(
        self, runnable: object
    ) -> tuple[ToolDisplaySource, ...]:
        codes = dict(self.mounted_display_codes(runnable))
        backend = self.backend
        skill_prefixes: tuple[str, ...] = ()
        if type(backend) is CompositeBackend:
            if (
                set(backend.routes) == {SKILLS_ROOT}
                and type(backend.routes.get(SKILLS_ROOT)) is TypedSkillBackend
            ):
                skill_prefixes = self.skill_prefixes
                backend = backend.default
            else:
                backend = None  # Unknown file routes do not invalidate independent MCP provenance.
        virtual = self.backend_kind == "state" and type(backend) is StateBackend
        if isinstance(backend, LocalShellBackend) and backend.virtual_mode is True:
            virtual = (
                self.backend_kind == "local_shell"
                and type(backend) in {LocalShellBackend, ArchivingLocalShellBackend}
            ) or (
                self.backend_kind == "docker"
                and type(backend) in {DockerShellBackend, ArchivingDockerShellBackend}
            )
        sources = {source.name: source for source in self.display_sources}
        result: list[ToolDisplaySource] = []
        for name, code in codes.items():
            source = sources.get(name)
            if name not in self.by_name and code in {
                "tool.file.read",
                "tool.file.write",
            }:
                source = ToolDisplaySource(name=name, code=code, origin="native")
            if source is not None:
                result.append(
                    replace(
                        source,
                        file_scope="virtual"
                        if virtual and source.origin in {"native", "delivery"}
                        else "none",
                        skill_prefixes=skill_prefixes
                        if code == "tool.file.read"
                        else (),
                    )
                )
        return tuple(sorted(result, key=lambda source: source.name))

    def with_tools(self, tools: Sequence[BaseTool]) -> Toolset:
        """Add Feature-local tools while preserving the existing authorization base."""

        mounted_names = frozenset(tool.name for tool in self.tools)
        implicit = self.authorized - mounted_names
        return replace(
            self.from_tools(
                (*self.tools, *tools),
                implicit_authorized=implicit,
                display_bindings=tuple(
                    (tool, code) for _, tool, code in self.display_bindings
                ),
            ),
            display_sources=self.display_sources,
            backend=self.backend,
            backend_kind=self.backend_kind,
            skill_prefixes=self.skill_prefixes,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ToolSourceSelection:
    kind: str
    names: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class ToolSelectionPlan:
    """One unbound ordered selection, shared by materialization and profile readers."""

    sources: tuple[ToolSourceSelection, ...]

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(name for source in self.sources for name in source.names)


def plan_toolset(
    *, agent: Agent, toolbox: ProcessToolbox, delivery_available: bool
) -> ToolSelectionPlan:
    sources = [
        ToolSourceSelection(
            kind="core",
            names=tuple(tool.name for tool in resolve_tools([], core=agent.tools)),
        ),
        ToolSourceSelection(kind="toolbox", names=plan_toolbox(toolbox).names),
        ToolSourceSelection(
            kind="mcp", names=("mcp_list_tools", "mcp_describe_tool", "mcp_call")
        ),
    ]
    if agent.delivery and delivery_available:
        sources.append(ToolSourceSelection(kind="delivery", names=("deliver",)))
    return ToolSelectionPlan(sources=tuple(sources))


async def build_toolset(
    request: RunRequest,
    *,
    agent: Agent,
    plan: ToolSelectionPlan,
    toolbox: ProcessToolbox,
    backend: BackendProtocol,
    delivery: DeliveryClient | None,
    lease: LeaseFence | None = None,
    resolved_mcp: McpClient | None = None,
    mcp_declarations: tuple[McpDisplayDeclaration, ...] = (),
    skill_prefixes: tuple[str, ...] = (),
) -> Toolset:
    """五路工具来源合流（顺序即挂载序）：
    ① 注册表工具：wire 点名 + 类型核心工具（对话型=ask_user）
    ② 内置底座（恒挂，toolbox 一口出）：租户态 memory + 进程配置态 web_search/web_fetch
    ③ MCP 稳定三工具（恒挂，schema 不随 server 集/远端漂移变）：list/describe/call
    ④ deliver 工具：只有 Agent 明确声明且 Storage public client 存在时装配
    ⑤ peer handoff 只在 Feature 选择 official Swarm 时由 swarm.py 装配，不混入单 Agent 工具面。
    """
    scope = RunScope.of(request)
    bound: dict[str, Sequence[BaseTool]] = {
        "core": agent.tools,
        "toolbox": toolbox.tools_for(scope.namespace),
    }
    bound["mcp"] = make_mcp_tools(agent.mcp, resolved_mcp, run_id=request.run_id)
    tools: list[BaseTool] = []
    display_bindings: list[tuple[BaseTool, ToolDisplayCode]] = []
    for source in plan.sources:
        if source.kind == "delivery":
            if delivery is None:
                raise ValueError("selected delivery requires its current owner client")
            materialized = (_deliver_tool(request, backend, delivery, lease),)
        else:
            materialized = bound[source.kind]
        if tuple(tool.name for tool in materialized) != source.names:
            raise ValueError("tool materialization differs from selection plan")
        tools.extend(materialized)
        if source.kind == "mcp":
            display_bindings.extend(
                zip(
                    materialized,
                    ("tool.mcp.discover", "tool.mcp.describe", "tool.mcp.call"),
                    strict=True,
                )
            )
        elif source.kind == "delivery":
            display_bindings.append((materialized[0], "tool.file.deliver"))
        elif source.kind == "toolbox":
            for tool in materialized:
                for registered, origin in toolbox.source_bindings:
                    if tool is registered and origin == tool.name:
                        if origin == "web_search":
                            display_bindings.append((tool, "tool.web.search"))
                        elif origin == "web_fetch":
                            display_bindings.append((tool, "tool.web.fetch"))
    declared_connections = {reference.connection_id for reference in agent.mcp}
    sources = tuple(
        ToolDisplaySource(
            name=tool.name,
            code=code,
            origin="mcp"
            if code.startswith("tool.mcp.")
            else "delivery"
            if code == "tool.file.deliver"
            else "web",
            mcp_names=tuple(
                sorted(
                    (item.connection_id, item.selector, item.display_name)
                    for item in mcp_declarations
                    if item.connection_id in declared_connections
                )
            )
            if code.startswith("tool.mcp.")
            else (),
        )
        for tool, code in display_bindings
    )
    return replace(
        Toolset.from_tools(tools, display_bindings=display_bindings),
        display_sources=sources,
        backend=backend,
        backend_kind=agent.backend,
        skill_prefixes=skill_prefixes,
    )


async def resolve_declared_mcp(
    request: RunRequest,
    agent: Agent,
    platform: RunMcpFactory | None,
    lease: LeaseFence,
) -> tuple[McpClient | None, tuple[McpDisplayDeclaration, ...]]:
    """Only run-bound current owner reads, never deployment or name resolution."""
    if not agent.mcp:
        return None, ()
    if platform is None:
        raise McpClientError("MCP_PLATFORM_REQUIRED")
    client = platform.mcp_for_run(LeasedRun(request=request, lease=lease))
    declarations = await client.check(agent.mcp)
    return client, declarations


def _deliver_tool(
    request: RunRequest,
    backend: BackendProtocol,
    delivery: DeliveryClient,
    lease: LeaseFence | None,
) -> StructuredTool:
    """Read through the same DeepAgents backend used by filesystem tools."""
    namespace = RunScope.of(request).namespace
    return make_deliver_tool(
        backend,
        delivery,
        namespace=namespace,
        run_id=request.run_id,
        identity=request.execution_identity,
        lease=lease,
    )
