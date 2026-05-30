from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

_CURSOR_WIDTH = 20


@dataclass(frozen=True, slots=True)
class StreamItem:
    """A single delivered stream entry: an opaque cursor plus the raw event dict."""

    cursor: str
    event: dict[str, object]


@runtime_checkable
class StreamPort(Protocol):
    """Pluggable append-only event transport.

    Implementations must preserve publish order per stream and assign a
    monotonically increasing, comparable cursor to every published item.
    """

    async def publish(self, stream: str, event: dict[str, object]) -> StreamItem: ...

    async def read_all(self, stream: str) -> list[StreamItem]: ...

    def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]: ...


class MemoryStreamPort:
    """In-process append-only StreamPort.

    Single-process only (cannot bridge Python<->TS). Cursors are zero-padded
    monotonic integers so lexical ordering matches insertion order. Subscribers
    block on an :class:`asyncio.Event` instead of busy-waiting.
    """

    def __init__(self) -> None:
        self._streams: dict[str, list[StreamItem]] = {}
        self._counters: dict[str, int] = {}
        self._signals: dict[str, asyncio.Event] = {}

    def _signal_for(self, stream: str) -> asyncio.Event:
        signal = self._signals.get(stream)
        if signal is None:
            signal = asyncio.Event()
            self._signals[stream] = signal
        return signal

    async def publish(self, stream: str, event: dict[str, object]) -> StreamItem:
        seq = self._counters.get(stream, 0)
        self._counters[stream] = seq + 1
        cursor = str(seq).zfill(_CURSOR_WIDTH)
        item = StreamItem(cursor=cursor, event=dict(event))
        self._streams.setdefault(stream, []).append(item)
        signal = self._signal_for(stream)
        signal.set()
        return item

    async def read_all(self, stream: str) -> list[StreamItem]:
        return list(self._streams.get(stream, ()))

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
                yield item

            signal = self._signal_for(stream)
            if index >= len(self._streams.get(stream, ())):
                signal.clear()
                await signal.wait()
