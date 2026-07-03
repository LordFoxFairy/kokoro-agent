"""内存事件流：单进程默认后端，publish 即裁剪，group 订阅与 ack 给 redis 等价语义。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping

from pydantic import JsonValue

from kokoro_agent.streams.protocol import StreamItem, validate_event

_CURSOR_WIDTH = 20


class MemoryStream:
    def __init__(self) -> None:
        self._streams: dict[str, list[StreamItem]] = {}
        self._counters: dict[str, int] = {}
        self._signals: dict[str, asyncio.Event] = {}
        # group 语义等价 redis：同 group 内每条消息恰好投递一次（共享游标），ack 只做记账。
        self._group_cursors: dict[tuple[str, str], str] = {}
        self._acked: dict[tuple[str, str], set[str]] = {}

    def _signal_for(self, stream: str) -> asyncio.Event:
        signal = self._signals.get(stream)
        if signal is None:
            signal = asyncio.Event()
            self._signals[stream] = signal
        return signal

    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        index = self._counters.get(stream, 0)
        self._counters[stream] = index + 1
        cursor = str(index).zfill(_CURSOR_WIDTH)
        # validate 重建容器 → 存量与返回各持独立副本，跨 item 不共享嵌套引用。
        payload = validate_event(dict(event))
        items = self._streams.setdefault(stream, [])
        items.append(StreamItem(cursor=cursor, event=payload))
        if len(items) > maxlen:
            del items[: len(items) - maxlen]
        self._signal_for(stream).set()
        return StreamItem(cursor=cursor, event=payload)

    async def read_all(self, stream: str) -> list[StreamItem]:
        return [
            StreamItem(cursor=item.cursor, event=item.event)
            for item in self._streams.get(stream, ())
        ]

    async def subscribe(
        self, stream: str, *, group: str, consumer: str
    ) -> AsyncIterator[StreamItem]:
        key = (stream, group)
        while True:
            delivered = self._group_cursors.get(key)
            nxt = next(
                (
                    item
                    for item in self._streams.get(stream, ())
                    if delivered is None or item.cursor > delivered
                ),
                None,
            )
            if nxt is not None:
                self._group_cursors[key] = nxt.cursor
                yield StreamItem(cursor=nxt.cursor, event=nxt.event)
                continue
            # 检查与 clear/wait 之间无 await 点，asyncio 单线程下不存在丢唤醒。
            signal = self._signal_for(stream)
            signal.clear()
            await signal.wait()

    async def ack(self, stream: str, group: str, cursor: str) -> None:
        self._acked.setdefault((stream, group), set()).add(cursor)

    async def delete(self, stream: str) -> None:
        # 终态 control 流清理：连带 group 游标/信号，避免 stale 键累积。
        self._streams.pop(stream, None)
        self._counters.pop(stream, None)
        self._signals.pop(stream, None)
        for key in [k for k in self._group_cursors if k[0] == stream]:
            del self._group_cursors[key]

    def acked(self, stream: str, group: str) -> frozenset[str]:
        return frozenset(self._acked.get((stream, group), ()))
