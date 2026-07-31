"""Hexagonal ports for the dormant ADR-014 persistence foundation."""

from __future__ import annotations

from typing import Protocol

from kokoro_agent.interaction.domain.models import (
    GroupRevisionCandidate,
    OriginCandidate,
    PublishedFrame,
    RunWriteFence,
)


class InteractionOriginJournal(Protocol):
    async def prepare_origin(
        self, candidate: OriginCandidate, fence: RunWriteFence
    ) -> OriginCandidate: ...


class InteractionGroupRepository(Protocol):
    async def publish_whole_frame(
        self, candidate: GroupRevisionCandidate, fence: RunWriteFence
    ) -> PublishedFrame: ...
