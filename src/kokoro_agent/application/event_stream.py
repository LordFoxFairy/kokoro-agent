"""应用层抽象：与后端无关的事件流契约（实现见 infrastructure/transport）。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import runtime_checkable

from pydantic import JsonValue
from typing_extensions import Protocol


@dataclass(frozen=True, slots=True)
class StreamItem:
    cursor: str
    event: dict[str, JsonValue]


@runtime_checkable
class StreamProtocol(Protocol):
    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem: ...

    async def read_all(self, stream: str) -> list[StreamItem]: ...

    def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]: ...
