from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import runtime_checkable

from typing_extensions import Protocol

from kokoro_agent.infrastructure.json_types import JsonObject, JsonValue


@dataclass(frozen=True, slots=True)
class StreamItem:
    cursor: str
    event: JsonObject


@runtime_checkable
class StreamPort(Protocol):
    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem: ...

    async def read_all(self, stream: str) -> list[StreamItem]: ...

    def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]: ...
