"""Canonical identity of one fully resolved GA execution assembly."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from langchain_core.tools import BaseTool

from kokoro_agent.contract import AgentType, Backend, ModelConfig, Permissions

ASSEMBLY_IDENTITY_REVISION = 1


def tool_schema_digest(tools: Sequence[BaseTool]) -> str:
    rows = [
        {
            "name": tool.name,
            "description": tool.description,
            "input_schema": tool.get_input_schema().model_json_schema(),
        }
        for tool in tools
    ]
    canonical = json.dumps(
        rows,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class AgentAssemblyFacts:
    namespace: str
    agent_catalog_ref: str
    hub_assembly_digest: str
    agent_type: AgentType
    persona_name: str | None
    persona_prompt_sha256: str
    model: ModelConfig
    skill_package_digest: str
    tool_schema_digest: str
    backend_kind: Backend
    backend_mapping: Mapping[str, str]
    subagents: tuple[str, ...]
    permissions: Permissions

    def digest(self) -> str:
        """Hash canonical business facts; exclude handles and runtime object identity."""
        model_policy = {
            "provider": self.model.provider,
            "name": self.model.name,
            "effort": self.model.effort,
            "thinking": self.model.thinking,
        }
        value = {
            "revision": ASSEMBLY_IDENTITY_REVISION,
            "namespace": self.namespace,
            "agent": {
                "catalog_ref": self.agent_catalog_ref,
                "type": self.agent_type,
                "persona_name": self.persona_name,
                "persona_prompt_sha256": self.persona_prompt_sha256,
            },
            "model_policy": model_policy,
            "hub_assembly_digest": self.hub_assembly_digest,
            "skill_package_digest": self.skill_package_digest,
            "tool_schema_digest": self.tool_schema_digest,
            "backend": {
                "kind": self.backend_kind,
                "mapping": dict(sorted(self.backend_mapping.items())),
            },
            "subagents": list(self.subagents),
            "permissions": self.permissions.model_dump(mode="json"),
        }
        canonical = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()
