from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from typing import Protocol, cast, runtime_checkable

from redis.asyncio import Redis, from_url

_CURSOR_WIDTH = 20
_REDIS_FIELD = "data"
_BLOCK_MS = 1000

# Concrete shapes of the redis-py stream responses we consume. xread's stub
# return is a broad union (incl. list[list[Any]]) that erases entry shapes; this
# alias documents the [(stream, [(id, fields)])] form we pin at that one call.
_Fields = dict[bytes | str, bytes | str] | None
_Entry = tuple[bytes | str | None, _Fields]
_ReadResponse = list[tuple[bytes | str | None, list[_Entry]]]


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
    monotonic integers so lexical ordering matches insertion order. Cursor width
    is a cross-language contract default, but may be narrowed in tests via the
    constructor for explicit contract checks. Subscribers block on an
    :class:`asyncio.Event` instead of busy-waiting.
    """

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

    async def publish(self, stream: str, event: dict[str, object]) -> StreamItem:
        seq = self._counters.get(stream, 0)
        self._counters[stream] = seq + 1
        cursor = str(seq).zfill(self._cursor_width)
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


def _decode(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class RedisStreamPort:
    """Redis Streams StreamPort.

    Each event is XADD'ed as a single JSON blob under the ``data`` field; the
    Redis entry id is the natural cursor. ``read_all`` uses XRANGE - +;
    ``subscribe`` uses XREAD BLOCK looping from the last seen cursor. The Redis
    field name and default block interval are the Python side of a shared
    Python/TypeScript transport contract, while tests may override block_ms via
    the constructor for focused polling behavior checks. Works across processes
    and languages (Python <-> TS).
    """

    def __init__(self, url: str = "redis://127.0.0.1:6379/0", block_ms: int = _BLOCK_MS) -> None:
        self._redis: Redis = from_url(url)
        self._block_ms = block_ms

    async def aclose(self) -> None:
        await self._redis.aclose()

    def _to_item(self, entry_id: bytes | str | None, fields: _Fields) -> StreamItem:
        raw = fields.get(_REDIS_FIELD.encode()) if fields is not None else None
        event: dict[str, object] = json.loads(_decode(raw)) if raw is not None else {}
        return StreamItem(cursor=_decode(entry_id), event=event)

    async def publish(self, stream: str, event: dict[str, object]) -> StreamItem:
        payload = json.dumps(event, ensure_ascii=False)
        entry_id = await self._redis.xadd(stream, {_REDIS_FIELD: payload})
        return StreamItem(cursor=_decode(entry_id), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        entries = await self._redis.xrange(stream, min="-", max="+")
        if not entries:
            return []
        return [self._to_item(entry_id, fields) for entry_id, fields in entries]

    async def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
        last = from_cursor if from_cursor is not None else "0-0"
        while True:
            # redis-py types xread's response as a broad union (incl. list[list[Any]])
            # that erases entry shapes; pin the documented [(stream, [(id, fields)])].
            response = await cast(
                "Awaitable[_ReadResponse | None]",
                self._redis.xread({stream: last}, block=self._block_ms),
            )
            if not response:
                continue
            for _stream_name, entries in response:
                for entry_id, fields in entries:
                    item = self._to_item(entry_id, fields)
                    last = item.cursor
                    yield item


def make_stream_port() -> StreamPort:
    """Construct a StreamPort from ``KOKORO_STREAM_BACKEND`` (memory|redis)."""
    backend = os.environ.get("KOKORO_STREAM_BACKEND", "memory").lower()
    if backend == "redis":
        url = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
        return RedisStreamPort(url)
    if backend == "memory":
        return MemoryStreamPort()
    raise ValueError(f"unknown KOKORO_STREAM_BACKEND: {backend!r}")
