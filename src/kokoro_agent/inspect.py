"""Read-only diagnostics for the authored Agent and Feature surface."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from kokoro_agent.features.catalog import FeatureCatalog


class _Description(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class AgentDescription(_Description):
    key: str
    features: tuple[str, ...]
    tools: tuple[str, ...]
    skills: tuple[str, ...]
    mcp: tuple[str, ...]
    subagents: tuple[str, ...]
    backend: str
    approval_tools: tuple[str, ...]
    pause_tools: tuple[str, ...]


class FeatureDescription(_Description):
    key: str
    agents: tuple[str, ...]
    entry_agent: str
    handoffs: tuple[tuple[str, str], ...]


class CatalogDescription(_Description):
    agents: tuple[AgentDescription, ...]
    features: tuple[FeatureDescription, ...]


def describe_feature(catalog: FeatureCatalog, key: str) -> FeatureDescription:
    feature = catalog.get(key)
    return FeatureDescription(
        key=feature.key,
        agents=tuple(agent.key for agent in feature.agents),
        entry_agent=feature.entry_agent,
        handoffs=feature.handoffs,
    )


def describe_catalog(catalog: FeatureCatalog) -> CatalogDescription:
    features = tuple(
        describe_feature(catalog, key) for key in sorted(catalog.keys())
    )
    agents_by_key = {
        agent.key: agent
        for feature in catalog.entries.values()
        for agent in feature.agents
    }
    agents = tuple(
        AgentDescription(
            key=key,
            features=tuple(
                feature.key
                for feature in catalog.entries.values()
                if key in {agent.key for agent in feature.agents}
            ),
            tools=tuple(tool.name for tool in agents_by_key[key].tools),
            skills=agents_by_key[key].skills,
            mcp=agents_by_key[key].mcp,
            subagents=agents_by_key[key].subagents,
            backend=agents_by_key[key].backend,
            approval_tools=tuple(sorted(agents_by_key[key].permissions.approval_tools)),
            pause_tools=tuple(sorted(agents_by_key[key].pause_tools)),
        )
        for key in sorted(agents_by_key)
    )
    return CatalogDescription(agents=agents, features=features)


def render_catalog(
    catalog: FeatureCatalog, *, output: Literal["text", "json"] | str = "text"
) -> str:
    description = describe_catalog(catalog)
    if output == "json":
        return description.model_dump_json(indent=2)
    if output != "text":
        raise ValueError(f"unsupported inspect output: {output!r}")
    lines = ["Features:"]
    for feature in description.features:
        agents = ", ".join(feature.agents)
        lines.append(f"- {feature.key}: {agents} (entry: {feature.entry_agent})")
    lines.append("Agents:")
    for agent in description.agents:
        lines.append(
            f"- {agent.key}: features={','.join(agent.features)} "
            f"tools={','.join(agent.tools) or '-'} skills={','.join(agent.skills) or '-'} "
            f"mcp={','.join(agent.mcp) or '-'} backend={agent.backend}"
        )
    return "\n".join(lines)


__all__ = [
    "AgentDescription",
    "CatalogDescription",
    "FeatureDescription",
    "describe_catalog",
    "describe_feature",
    "render_catalog",
]
