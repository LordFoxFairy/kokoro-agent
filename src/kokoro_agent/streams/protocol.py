"""与后端无关的事件流契约：publish 带保留上限、consumer-group 订阅、cursor 不透明。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter

# 边界洗净器：外部 JSON 在此一次性校验为强类型，非法输入抛 ValidationError。
_EVENT_ADAPTER: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])


def validate_event(event: object) -> dict[str, JsonValue]:
    return _EVENT_ADAPTER.validate_python(event)


class StreamItem(BaseModel):
    # JSON 边界构造期即校验；frozen 保证跨消费者不可变。
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    cursor: str
    event: dict[str, JsonValue]


@runtime_checkable
class StreamProtocol(Protocol):
    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem: ...

    async def read_all(self, stream: str) -> list[StreamItem]: ...

    def subscribe(
        self, stream: str, *, group: str, consumer: str
    ) -> AsyncIterator[StreamItem]: ...

    async def ack(self, stream: str, group: str, cursor: str) -> None: ...

    async def delete(self, stream: str) -> None: ...
