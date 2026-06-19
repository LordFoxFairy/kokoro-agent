"""Redis Streams 传输实现：XREAD/XRANGE 的线格式在此防御性解析（兼容 RESP2/3）。"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from typing import TypeAlias, TypeGuard

from redis.asyncio import Redis, from_url

from kokoro_agent.application.event_stream import StreamItem
from kokoro_agent.infrastructure.json_types import JsonValue, clone_event, validate_event

_REDIS_FIELD = "data"
_BLOCK_MS = 1000

_Fields = dict[bytes | str, bytes | str] | None
_Entry = tuple[bytes | str | None, _Fields]
_ReadResponse = list[tuple[bytes | str | None, list[_Entry]]]
# redis-py 无类型存根，XREAD/XRANGE 返回 bytes/嵌套 list/tuple 的松散结构；
# 以 object 为边界逐层收窄，屏蔽 RESP2/3 协议差异，未校验数据不进入内层。
_ObjectMapping: TypeAlias = Mapping[object, object]
_ObjectDict: TypeAlias = dict[object, object]
_ObjectList: TypeAlias = list[object]
_ObjectTuple: TypeAlias = tuple[object, ...]


def _is_object_mapping(value: object) -> TypeGuard[_ObjectMapping]:
    return isinstance(value, Mapping)


def _is_object_dict(value: object) -> TypeGuard[_ObjectDict]:
    return isinstance(value, dict)


def _is_object_list(value: object) -> TypeGuard[_ObjectList]:
    return isinstance(value, list)


def _is_object_tuple(value: object) -> TypeGuard[_ObjectTuple]:
    return isinstance(value, tuple)


def _decode(value: bytes | str | None) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _pair_parts(value: object) -> tuple[object, object] | None:
    if _is_object_list(value):
        if len(value) != 2:
            return None
        return value[0], value[1]
    if _is_object_tuple(value):
        if len(value) != 2:
            return None
        return value[0], value[1]
    return None


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
    # RESP3 可能把条目多包一层 list；若如此则解开这单层包装。
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


class RedisStream:
    def __init__(self, url: str = "redis://127.0.0.1:6379/0", block_ms: int = _BLOCK_MS) -> None:
        self._redis: Redis = from_url(url)
        self._block_ms = block_ms

    async def aclose(self) -> None:
        await self._redis.aclose()

    def _to_item(self, entry_id: bytes | str | None, fields: _Fields) -> StreamItem:
        raw = fields.get(_REDIS_FIELD.encode()) if fields is not None else None
        payload: object = json.loads(_decode(raw)) if raw is not None else {}
        event = clone_event(validate_event(payload))
        return StreamItem(cursor=_decode(entry_id), event=event)

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        payload = clone_event(validate_event(dict(event)))
        entry_id = await self._redis.xadd(
            stream,
            {_REDIS_FIELD: json.dumps(payload, ensure_ascii=False)},
        )
        return StreamItem(cursor=_decode(entry_id), event=clone_event(payload))

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
                    yield item
