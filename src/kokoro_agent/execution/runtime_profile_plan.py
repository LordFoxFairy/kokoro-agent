"""Pre-I/O assembly recipe and the exact plans consumed by the real factory.

This is not the persistent Run profile and does not resolve a model or capture its
post-routing harness output. Bound services and tool instances never enter JSON.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
import hashlib
import json
import re

from langchain_core.tools import BaseTool
from langgraph_swarm import create_handoff_tool

from kokoro_agent.agents import subagents
from kokoro_agent.agents.definition import Agent
from kokoro_agent.agents.general import GENERAL_AGENT
from kokoro_agent.agents.native_profile import (
    prepare_native_recipe,
    native_task_description,
)
from kokoro_agent.agents.subagent_catalog import SubagentCatalog
from kokoro_agent.execution.runtime_profile import (
    canonical_json,
    profile_toolbox,
    tool_descriptor,
)
from kokoro_agent.execution.runtime_profile_sources import SourceManifest
from kokoro_agent.features.definition import Feature
from kokoro_agent.mcp.tools import MCP_TOOL_METADATA
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.sandbox.backend import SandboxSettings
from kokoro_agent.sandbox.archive import S3Workspace
from kokoro_agent.tools import toolset
from kokoro_agent.tools.deliver import DELIVER_TOOL_METADATA
from kokoro_agent.tools.guards import validate_guard_policy
from kokoro_agent.tools.memory import MEMORY_TOOL_METADATA
from kokoro_agent.tools.registry import CORE_TOOLS, assert_tool_names_allowed
from kokoro_agent.tools.toolbox import ProcessToolbox


@dataclass(frozen=True, slots=True, kw_only=True)
class RuntimeAssemblyPolicy:
    run_token_budget: int
    recursion_limit: int
    disable_streaming: bool
    openai_reasoning: bool
    litellm_enabled: bool
    backend_recipes: tuple[tuple[str, bytes], ...]

    def __post_init__(self) -> None:
        if type(self.run_token_budget) is not int or self.run_token_budget < 0:
            raise ValueError("invalid run token budget")
        if type(self.recursion_limit) is not int or self.recursion_limit < 1:
            raise ValueError("invalid recursion limit")

    @classmethod
    def from_settings(
        cls,
        *,
        run_token_budget: int,
        recursion_limit: int,
        model: ChatModelSettings,
        sandbox: SandboxSettings,
    ) -> RuntimeAssemblyPolicy:
        shell = {
            "timeout": sandbox.local_shell_timeout,
            "max_output_bytes": sandbox.local_shell_max_output_bytes,
            "inherit_env": sandbox.local_shell_inherit_env,
            "virtual_mode": True,
            "workspace": "s3"
            if isinstance(sandbox.workspace, S3Workspace)
            else "local",
        }
        # Container image/template identifiers are policy, not URLs/credentials.
        for identifier in (sandbox.docker.image, sandbox.e2b.template):
            if identifier is not None and (
                re.search(r"\s|://|[?]", identifier)
                or "@" in identifier
                and "@sha256:" not in identifier
            ):
                raise ValueError(
                    "backend policy identifier contains an address or credential"
                )
        backends = (
            ("state", {"kind": "state"}),
            ("local_shell", {"kind": "local_shell", **shell}),
            (
                "docker",
                {
                    "kind": "docker",
                    **shell,
                    "image": sandbox.docker.image,
                    "ttl": sandbox.docker.ttl,
                },
            ),
            (
                "e2b",
                {
                    "kind": "e2b",
                    "template": sandbox.e2b.template,
                    "timeout": sandbox.e2b.timeout,
                },
            ),
        )
        return cls(
            run_token_budget=run_token_budget,
            recursion_limit=recursion_limit,
            disable_streaming=model.disable_streaming,
            openai_reasoning=model.openai_reasoning,
            litellm_enabled=model.litellm_enabled,
            backend_recipes=tuple(
                (name, canonical_json(values)) for name, values in backends
            ),
        )

    def backend_recipe(self, kind: str) -> object:
        for name, encoded in self.backend_recipes:
            if name == kind:
                result: object = json.loads(encoded)
                return result
        raise ValueError("backend source/policy not registered")

    def as_recipe(self) -> dict[str, object]:
        return {
            "run_token_budget": self.run_token_budget,
            "recursion_limit": self.recursion_limit,
            "disable_streaming": self.disable_streaming,
            "openai_reasoning": self.openai_reasoning,
            "litellm_enabled": self.litellm_enabled,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class PreparedPeerPlan:
    agent: Agent
    tools: toolset.ToolSelectionPlan
    subagents: subagents.SubagentSelectionPlan
    handoffs: tuple[BaseTool, ...]
    tool_metadata: bytes

    def verify_tools(self, tools: Sequence[BaseTool]) -> None:
        actual = tuple(
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.get_input_schema().model_json_schema(),
                "return_direct": tool.return_direct,
                "response_format": tool.response_format,
            }
            for tool in tools
        )
        if canonical_json(actual) != self.tool_metadata:
            raise ValueError("materialized tool metadata differs from prepared plan")


@dataclass(frozen=True, slots=True, kw_only=True)
class PreparedFeaturePlan:
    feature: Feature
    peers: tuple[PreparedPeerPlan, ...]
    runtime_policy: RuntimeAssemblyPolicy
    recipe_bytes: bytes

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.recipe_bytes).hexdigest()


def _tool_descriptors(
    agent: Agent,
    toolbox: ProcessToolbox,
    manifest: SourceManifest,
    delivery_available: bool,
) -> dict[str, dict[str, object]]:
    descriptors: dict[str, dict[str, object]] = {}
    for tool in agent.tools:
        if not any(tool is registered for registered in CORE_TOOLS):
            raise ValueError("core tool source is not registered")
        descriptors[tool.name] = dict(
            tool_descriptor(
                tool, implementation=manifest.descriptor("core"), options={}
            )
        )
    for tool in toolbox.configured:
        bindings = tuple(
            source
            for registered, source in toolbox.source_bindings
            if registered is tool
        )
        if len(bindings) != 1 or bindings[0] != tool.name:
            raise ValueError("configured tool source is not registered")
        options: dict[str, object] = {}
        if toolbox.profile_options is not None:
            if tool.name == "web_fetch":
                options["fetch_allow_private"] = (
                    toolbox.profile_options.fetch_allow_private
                )
            if tool.name == "web_search":
                options["search_provider"] = toolbox.profile_options.search_provider
        descriptors[tool.name] = dict(
            tool_descriptor(
                tool, implementation=manifest.descriptor(bindings[0]), options=options
            )
        )
    metadata_groups = [("memory", MEMORY_TOOL_METADATA), ("mcp", MCP_TOOL_METADATA)]
    if agent.delivery and delivery_available:
        metadata_groups.append(("delivery", DELIVER_TOOL_METADATA))
    for source, entries in metadata_groups:
        for name, description, schema in entries:
            if name in descriptors:
                raise ValueError("duplicate tool metadata")
            descriptors[name] = {
                "name": name,
                "description": description,
                "input_schema": schema.model_json_schema(),
                "return_direct": False,
                "response_format": "content",
                "implementation": manifest.descriptor(source),
                "options": {},
            }
    return descriptors


def prepare_feature(
    feature: Feature,
    *,
    runtime_policy: RuntimeAssemblyPolicy,
    manifest: SourceManifest,
    toolbox: ProcessToolbox,
    subagent_catalog: SubagentCatalog,
    delivery_available: bool,
) -> PreparedFeaturePlan:
    """Validate every peer before the first external preflight, then retain its plan."""
    # This gate precedes all metadata imports that could invoke native lazy loading.
    native = prepare_native_recipe()
    options = profile_toolbox(toolbox)
    peers: list[PreparedPeerPlan] = []
    recipes: list[dict[str, object]] = []
    sources = [
        "composition",
        "assembly",
        "guards",
        "skills",
        "backend",
        "native",
        "langchain",
        "memory",
        "mcp",
    ]
    if feature.handoffs:
        sources.extend(("swarm", "handoff"))
    for agent in feature.agents:
        validate_guard_policy(agent.permissions)
        tools = toolset.plan_toolset(
            agent=agent, toolbox=toolbox, delivery_available=delivery_available
        )
        handoffs = tuple(
            create_handoff_tool(agent_name=target)
            for source, target in feature.handoffs
            if source == agent.key
        )
        names = (*tools.names, *(tool.name for tool in handoffs))
        assert_tool_names_allowed(names)
        selected = subagents.plan_subagents(
            subagent_catalog, frozenset(names), selected=frozenset(agent.subagents)
        )
        descriptors = _tool_descriptors(agent, toolbox, manifest, delivery_available)
        selected_tools = [descriptors[name] for name in tools.names]
        selected_tools.extend(
            dict(
                tool_descriptor(
                    tool, implementation=manifest.descriptor("handoff"), options={}
                )
            )
            for tool in handoffs
        )
        metadata = tuple(
            {
                key: descriptor[key]
                for key in (
                    "name",
                    "description",
                    "input_schema",
                    "return_direct",
                    "response_format",
                )
            }
            for descriptor in selected_tools
        )
        peers.append(
            PreparedPeerPlan(
                agent=agent,
                tools=tools,
                subagents=selected,
                handoffs=handoffs,
                tool_metadata=canonical_json(metadata),
            )
        )
        permissions = agent.permissions
        model = agent.model
        recipes.append(
            {
                "key": agent.key,
                "prompt": agent.prompt,
                "tools": selected_tools,
                "mcp": agent.mcp,
                "subagents": selected.as_profile(),
                "native_task_description": native_task_description(
                    tuple((spec.name, spec.description) for spec in selected.specs)
                ),
                "missing_subagents": selected.missing,
                "subagent_tools_inheritance": tuple(
                    not spec.tools for spec in selected.specs
                ),
                "delivery": {
                    "declared": agent.delivery,
                    "mounted": agent.delivery and delivery_available,
                },
                "model": None
                if model is None
                else {
                    "provider": model.provider,
                    "name": model.name,
                    "effort": model.effort,
                    "thinking": model.thinking,
                },
                "backend": runtime_policy.backend_recipe(agent.backend),
                "permissions": {
                    "approval_tools": permissions.approval_tools,
                    "review_tools": permissions.review_tools,
                    "filesystem": permissions.filesystem,
                    "subagent_create": permissions.subagent_create,
                },
                "pause_tools": tuple(sorted(agent.pause_tools)),
            }
        )
        sources.extend("core" for _ in agent.tools)
        if agent is GENERAL_AGENT:
            sources.append("general_prompt")
        sources.extend(tool.name for tool in toolbox.configured)
        if agent.delivery and delivery_available:
            sources.append("delivery")
        if agent.backend != "state":
            sources.append(agent.backend)
        if any(
            spec.name == "web-researcher" and spec.source == "built-in"
            for spec in selected.specs
        ):
            sources.append("web_researcher")
    recipe = {
        "domain_tag": "kokoro-agent:assembly-recipe:1",
        "feature": {
            "key": feature.key,
            "entry_agent": feature.entry_agent,
            "handoffs": feature.handoffs,
            "agents": recipes,
        },
        "runtime": {
            **runtime_policy.as_recipe(),
            "toolbox": options,
            "delivery_available": delivery_available,
        },
        "implementations": {
            "sources": tuple(
                manifest.descriptor(source) for source in dict.fromkeys(sources)
            ),
            "dynamic_edges": manifest.dynamic_edges,
            "dependencies": manifest.versions,
        },
        "native_recipe": native,
    }
    return PreparedFeaturePlan(
        feature=feature,
        peers=tuple(peers),
        runtime_policy=runtime_policy,
        recipe_bytes=canonical_json(recipe),
    )
