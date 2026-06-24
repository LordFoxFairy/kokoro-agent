"""应用层抽象：与后端无关的事件流契约（实现见 infrastructure/transport）。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, JsonValue


class StreamItem(BaseModel):
    # JSON 边界构造期即校验，frozen 保持原 dataclass 的不可变语义。
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    cursor: str
    event: dict[str, JsonValue]


@runtime_checkable
class StreamProtocol(Protocol):
    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem: ...

    async def read_all(self, stream: str) -> list[StreamItem]: ...

    def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]: ...
