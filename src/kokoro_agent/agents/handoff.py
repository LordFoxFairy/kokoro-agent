"""True Agent-to-Agent transfer boundary.

Persona switching is a prompt-state change inside one graph. A real handoff is
an external durable command that targets a separately resolved assembly (model,
tools, policy, skills and backend), creates a new execution owner, and returns a
receipt. It must never reuse ``active_persona``.
"""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field


class _FrozenModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class AgentTransferIntent(_FrozenModel):
    source_run_id: str = Field(min_length=1, max_length=256)
    source_assembly_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_checkpoint_anchor: str = Field(min_length=1, max_length=256)
    source_checkpoint_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_agent_catalog_ref: str = Field(min_length=1, max_length=256)
    target_agent_ref: str = Field(min_length=1, max_length=256)
    reason: str = Field(min_length=1, max_length=2000)


class AgentTransferReceipt(_FrozenModel):
    disposition: Literal["accepted", "rejected", "outcome_unknown"]
    transfer_ref: str = Field(min_length=1, max_length=256)
    owner_version: int = Field(gt=0)
    target_run_id: str | None = Field(default=None, min_length=1, max_length=256)
    target_assembly_digest: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )


class DurableAgentTransferPort(Protocol):
    async def transfer(self, intent: AgentTransferIntent) -> AgentTransferReceipt: ...
