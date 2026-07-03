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
    """传输后端（redis/memory）的互换契约；cursor 不透明——上层只能原样回传，禁止解析。"""

    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        """追加事件并按 maxlen 修剪旧条目；返回带 cursor 的条目供发布方定位。"""
        ...

    async def read_all(self, stream: str) -> list[StreamItem]:
        """全量回放整条流：attach/断线重建状态的入口，不参与分摊消费。"""
        ...

    def subscribe(
        self, stream: str, *, group: str, consumer: str
    ) -> AsyncIterator[StreamItem]:
        """consumer-group 消费：同 group 的多 consumer 分摊消息（一条只投一个）；
        未 ack 条目挂在待确认账本，消费者崩溃后可被同组他人收养（至少一次投递）。"""
        ...

    async def ack(self, stream: str, group: str, cursor: str) -> None:
        """确认消费完成；不 ack 即视为未处理，会被重投/收养——下游靠幂等吸收重放。"""
        ...

    async def delete(self, stream: str) -> None:
        """整流删除：run 终态后清理其 per-run 流。"""
        ...
