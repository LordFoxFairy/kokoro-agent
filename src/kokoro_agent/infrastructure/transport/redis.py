from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING

from kokoro_agent.infrastructure.json_types import JsonValue, clone_event, validate_event
from kokoro_agent.infrastructure.transport.port import StreamItem

if TYPE_CHECKING:
    from redis.asyncio import Redis

_REDIS_FIELD = "data"
_BLOCK_MS = 1000

_Fields = dict[bytes | str, bytes | str] | None
_Entry = tuple[bytes | str | None, _Fields]
_ReadResponse = list[tuple[bytes | str | None, list[_Entry]]]


def _decode(value: bytes | str | None) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _expect_pair(value: object, error: str) -> tuple[object, object]:
    match value:
        case [left, right]:
            return left, right
        case _:
            raise ValueError(error)


def _parse_fields(value: object) -> _Fields:
    match value:
        case None:
            return None
        case dict():
            parsed: dict[bytes | str, bytes | str] = {}
            for key, item in value.items():
                if not isinstance(key, (bytes, str)) or not isinstance(item, (bytes, str)):
                    raise ValueError("xread fields must use bytes/str keys and values")
                parsed[key] = item
            return parsed
        case _:
            raise ValueError("xread fields must be a dict or None")


def _parse_entries(value: object) -> list[_Entry]:
    match value:
        case list():
            entries: list[object] = list(value)
        case _:
            raise ValueError("xread entries must be a list")
    # RESP3 may wrap the entries in one extra list layer; unwrap that single layer.
    match entries:
        case [first, *rest] if isinstance(first, list):
            if rest:
                raise ValueError("xread RESP3 wrapper must contain exactly one entry list")
            entries = list(first)
        case _:
            pass
    parsed: list[_Entry] = []
    for entry in entries:
        entry_id_obj, fields_obj = _expect_pair(entry, "xread item must be an (id, fields) pair")
        match entry_id_obj:
            case bytes() | str() | None as entry_id:
                parsed.append((entry_id, _parse_fields(fields_obj)))
            case _:
                raise ValueError("xread id must be bytes, str, or None")
    return parsed


def parse_xread_response(raw: object) -> _ReadResponse | None:
    stream_entries: list[tuple[object, object]]
    match raw:
        case None:
            return None
        case Mapping():
            stream_entries = list(raw.items())
        case list():
            stream_entries = [
                _expect_pair(item, "xread stream entry must be a (stream, entries) pair")
                for item in raw
            ]
        case _:
            raise ValueError("xread response must be a list or mapping")
    parsed: _ReadResponse = []
    for stream_name_obj, entries_obj in stream_entries:
        match stream_name_obj:
            case bytes() | str() | None as stream_name:
                parsed.append((stream_name, _parse_entries(entries_obj)))
            case _:
                raise ValueError("xread stream name must be bytes, str, or None")
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
                    yield StreamItem(cursor=item.cursor, event=clone_event(item.event))
