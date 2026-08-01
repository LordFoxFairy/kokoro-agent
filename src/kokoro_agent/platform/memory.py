"""Product long-term memory port owned by Platform, not DeepAgents memory files."""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field


class _FrozenModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class MemorySearch(_FrozenModel):
    namespace: str = Field(min_length=1, max_length=256)
    query: str = Field(min_length=1, max_length=8000)
    limit: int = Field(default=8, ge=1, le=20)


class MemoryItem(_FrozenModel):
    memory_ref: str = Field(min_length=1, max_length=256)
    content: str = Field(min_length=1, max_length=32_000)
    relevance_bps: int = Field(ge=0, le=10_000)


class MemoryWrite(_FrozenModel):
    namespace: str = Field(min_length=1, max_length=256)
    idempotency_ref: str = Field(min_length=1, max_length=256)
    content: str = Field(min_length=1, max_length=32_000)
    provenance_run_id: str = Field(min_length=1, max_length=256)


class MemoryWriteReceipt(_FrozenModel):
    disposition: Literal["stored", "rejected", "outcome_unknown"]
    memory_ref: str | None = Field(default=None, min_length=1, max_length=256)


class ProductMemoryPort(Protocol):
    async def search(self, request: MemorySearch) -> tuple[MemoryItem, ...]: ...

    async def remember(self, request: MemoryWrite) -> MemoryWriteReceipt: ...
