from __future__ import annotations

import asyncio
import copy
import json
import os
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias, TypeGuard, runtime_checkable

from typing_extensions import Protocol

if TYPE_CHECKING:
    from redis.asyncio import Redis

_CURSOR_WIDTH = 20
_REDIS_FIELD = "data"
_BLOCK_MS = 1000

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]

_Fields = dict[bytes | str, bytes | str] | None
_Entry = tuple[bytes | str | None, _Fields]
_ReadResponse = list[tuple[bytes | str | None, list[_Entry]]]
_ObjectMapping = Mapping[object, object]
_ObjectDict = dict[object, object]
_ObjectList = list[object]
_ObjectTuple = tuple[object, ...]


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


def _is_object_mapping(value: object) -> TypeGuard[_ObjectMapping]:
    return isinstance(value, Mapping)


def _is_object_dict(value: object) -> TypeGuard[_ObjectDict]:
    return isinstance(value, dict)


def _is_object_list(value: object) -> TypeGuard[_ObjectList]:
    return isinstance(value, list)


def _is_object_tuple(value: object) -> TypeGuard[_ObjectTuple]:
    return isinstance(value, tuple)


def _pair_parts(value: object) -> tuple[object, object] | None:
    if _is_object_list(value):
        if len(value) != 2:
            return None
        left = value[0]
        right = value[1]
        return left, right
    if _is_object_tuple(value):
        if len(value) != 2:
            return None
        left = value[0]
        right = value[1]
        return left, right
    return None


def _coerce_json_value(value: object) -> JsonValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if _is_object_list(value):
        return [_coerce_json_value(item) for item in value]
    if _is_object_dict(value):
        result: JsonObject = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("stream event object keys must be strings")
            result[key] = _coerce_json_value(item)
        return result
    raise ValueError("stream event values must be JSON-serializable")


def validate_event(event: object) -> JsonObject:
    if not _is_object_dict(event):
        raise ValueError("stream event must be a JSON object")
    result: JsonObject = {}
    for key, item in event.items():
        if not isinstance(key, str):
            raise ValueError("stream event keys must be strings")
        result[key] = _coerce_json_value(item)
    return result


def _clone_event(event: JsonObject) -> JsonObject:
    return copy.deepcopy(event)


class MemoryStreamPort:
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
        payload = _clone_event(validate_event(dict(event)))
        item = StreamItem(cursor=cursor, event=payload)
        self._streams.setdefault(stream, []).append(item)
        signal = self._signal_for(stream)
        signal.set()
        return StreamItem(cursor=cursor, event=_clone_event(payload))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return [
            StreamItem(cursor=item.cursor, event=_clone_event(item.event))
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
                yield StreamItem(cursor=item.cursor, event=_clone_event(item.event))

            signal = self._signal_for(stream)
            if index >= len(self._streams.get(stream, ())):
                signal.clear()
                await signal.wait()


def _decode(value: bytes | str | None) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _expect_pair(value: object, error: str) -> tuple[object, object]:
    pair = _pair_parts(value)
    if pair is None:
        raise ValueError(error)
    return pair


def _parse_fields(value: object) -> _Fields:
    if value is None:
        return None
    if not _is_object_dict(value):
        raise ValueError("xread fields must be a dict or None")
    parsed: dict[bytes | str, bytes | str] = {}
    for key, item in value.items():
        if not isinstance(key, (bytes, str)) or not isinstance(item, (bytes, str)):
            raise ValueError("xread fields must use bytes/str keys and values")
        parsed[key] = item
    return parsed


def _parse_entries(value: object) -> list[_Entry]:
    if not _is_object_list(value):
        raise ValueError("xread entries must be a list")
    entries: list[object] = list(value)
    if entries and _is_object_list(entries[0]):
        if len(entries) != 1:
            raise ValueError("xread RESP3 wrapper must contain exactly one entry list")
        entries = list(entries[0])
    parsed: list[_Entry] = []
    for entry in entries:
        entry_id_obj, fields_obj = _expect_pair(entry, "xread item must be an (id, fields) pair")
        if entry_id_obj is not None and not isinstance(entry_id_obj, (bytes, str)):
            raise ValueError("xread id must be bytes, str, or None")
        entry_id: bytes | str | None = entry_id_obj
        parsed.append((entry_id, _parse_fields(fields_obj)))
    return parsed


def parse_xread_response(raw: object) -> _ReadResponse | None:
    if raw is None:
        return None

    stream_entries: list[tuple[object, object]] = []
    if _is_object_mapping(raw):
        stream_entries = list(raw.items())
    elif _is_object_list(raw):
        stream_entries = [
            _expect_pair(item, "xread stream entry must be a (stream, entries) pair")
            for item in raw
        ]
    else:
        raise ValueError("xread response must be a list or mapping")

    parsed: _ReadResponse = []
    for stream_name_obj, entries_obj in stream_entries:
        if stream_name_obj is not None and not isinstance(stream_name_obj, (bytes, str)):
            raise ValueError("xread stream name must be bytes, str, or None")
        stream_name: bytes | str | None = stream_name_obj
        parsed.append((stream_name, _parse_entries(entries_obj)))
    return parsed


class RedisStreamPort:
    def __init__(self, url: str = "redis://127.0.0.1:6379/0", block_ms: int = _BLOCK_MS) -> None:
        from redis.asyncio import from_url

        self._redis: Redis = from_url(url)
        self._block_ms = block_ms

    async def aclose(self) -> None:
        await self._redis.aclose()

    def _to_item(self, entry_id: bytes | str | None, fields: _Fields) -> StreamItem:
        raw = fields.get(_REDIS_FIELD.encode()) if fields is not None else None
        payload: object = json.loads(_decode(raw)) if raw is not None else {}
        event = _clone_event(validate_event(payload))
        return StreamItem(cursor=_decode(entry_id), event=event)

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        payload = _clone_event(validate_event(dict(event)))
        entry_id = await self._redis.xadd(
            stream,
            {_REDIS_FIELD: json.dumps(payload, ensure_ascii=False)},
        )
        return StreamItem(cursor=_decode(entry_id), event=_clone_event(payload))

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
            raw = await self._redis.xread({stream: last}, block=self._block_ms)
            response = parse_xread_response(raw)
            if not response:
                continue
            for _stream_name, entries in response:
                for entry_id, fields in entries:
                    item = self._to_item(entry_id, fields)
                    last = item.cursor
                    yield StreamItem(cursor=item.cursor, event=_clone_event(item.event))


def make_stream_port() -> StreamPort:
    backend = os.environ.get("KOKORO_STREAM_BACKEND", "memory").lower()
    if backend == "redis":
        url = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")
        return RedisStreamPort(url)
    if backend == "memory":
        return MemoryStreamPort()
    raise ValueError(f"unknown KOKORO_STREAM_BACKEND: {backend!r}")
