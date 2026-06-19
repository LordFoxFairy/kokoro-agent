"""内存事件流：单进程默认后端，发布即深拷贝隔离，订阅靠 asyncio.Event 唤醒。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping

from kokoro_agent.application.event_stream import StreamItem
from kokoro_agent.infrastructure.json_types import JsonValue, clone_event, validate_event

_CURSOR_WIDTH = 20


class MemoryStream:
    def __init__(self, cursor_width: int = _CURSOR_WIDTH) -> None:
        self._streams: dict[str, list[StreamItem]] = {}
        self._counters: dict[str, int] = {}
        self._signals: dict[str, asyncio.Event] = {}
        self._cursor_width = cursor_width

    def _signal_for(self, stream: str) -> asyncio.Event:
        signal = self._signals.get(stream)
        if signal is None:
            signal = asyncio.Event()
            self._signals[stream] = signal
        return signal

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        seq = self._counters.get(stream, 0)
        self._counters[stream] = seq + 1
        cursor = str(seq).zfill(self._cursor_width)
        payload = clone_event(validate_event(dict(event)))
        item = StreamItem(cursor=cursor, event=payload)
        self._streams.setdefault(stream, []).append(item)
        signal = self._signal_for(stream)
        signal.set()
        return StreamItem(cursor=cursor, event=clone_event(payload))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return [
            StreamItem(cursor=item.cursor, event=clone_event(item.event))
            for item in self._streams.get(stream, ())
        ]

    async def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
        index = 0
        while True:
            items = self._streams.get(stream, ())
            while index < len(items):
                item = items[index]
                index += 1
                if from_cursor is not None and item.cursor <= from_cursor:
                    continue
                yield StreamItem(cursor=item.cursor, event=clone_event(item.event))

            signal = self._signal_for(stream)
            if index >= len(self._streams.get(stream, ())):
                signal.clear()
                await signal.wait()
