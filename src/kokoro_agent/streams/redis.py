"""Redis Streams 传输：固定 RESP2+decode_responses=True，xread 真实形状为 list[list]。"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Mapping
from typing import TypeAlias, TypeGuard

from redis.asyncio import Redis, from_url
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.streams.json_types import JsonValue, clone_event, validate_event

LOGGER = logging.getLogger(__name__)

_REDIS_FIELD = "data"
_BLOCK_MS = 1000
# 重连退避边界（秒）：宏观分布式层韧性——抖动快恢复，持续断线不 busy-loop 打爆 redis。
_RECONNECT_BACKOFF_MIN = 0.1
_RECONNECT_BACKOFF_MAX = 5.0

# redis-py 无类型存根，xread/xrange 返回 object；逐层收窄到下列别名。
_Fields: TypeAlias = dict[bytes | str, bytes | str] | None
_Entry: TypeAlias = tuple[bytes | str | None, _Fields]
_ReadResponse: TypeAlias = list[tuple[bytes | str | None, list[_Entry]]]
_StrLike: TypeAlias = bytes | str


def _is_seq(value: object) -> TypeGuard[list[object] | tuple[object, ...]]:
    # TypeGuard 把 redis-py 的 Unknown 容器收窄成 object 元素，纯 isinstance 无法做到。
    return isinstance(value, (list, tuple))


def _is_obj_dict(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def _decode(value: _StrLike) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _decode_cursor(value: _StrLike | None) -> str:
    # entry_id 是 redis 流游标，绝不能落成字面 'None'；缺失即数据破坏，显性抛错。
    if value is None:
        raise ValueError("redis stream entry id must not be None")
    return _decode(value)


def _expect_pair(value: object, error: str) -> tuple[object, object]:
    if _is_seq(value) and len(value) == 2:
        return value[0], value[1]
    raise ValueError(error)


def _strlike_or_none(value: object, error: str) -> _StrLike | None:
    if value is None or isinstance(value, (bytes, str)):
        return value
    raise ValueError(error)


def _parse_fields(value: object) -> _Fields:
    if value is None:
        return None
    if not _is_obj_dict(value):
        raise ValueError("xread fields must be a dict or None")
    parsed: dict[bytes | str, bytes | str] = {}
    for key, item in value.items():
        if not isinstance(key, (bytes, str)) or not isinstance(item, (bytes, str)):
            raise ValueError("xread fields must use bytes/str keys and values")
        parsed[key] = item
    return parsed


def _parse_entries(value: object) -> list[_Entry]:
    if not _is_seq(value):
        raise ValueError("xread entries must be a list")
    parsed: list[_Entry] = []
    for entry in value:
        entry_id, fields = _expect_pair(entry, "xread item must be an (id, fields) pair")
        parsed.append(
            (
                _strlike_or_none(entry_id, "xread id must be bytes, str, or None"),
                _parse_fields(fields),
            )
        )
    return parsed


def parse_xread_response(raw: object) -> _ReadResponse | None:
    if raw is None:
        return None
    if not _is_seq(raw):
        raise ValueError("xread response must be a list")
    parsed: _ReadResponse = []
    for item in raw:
        stream_name, entries = _expect_pair(
            item, "xread stream entry must be a (stream, entries) pair"
        )
        parsed.append(
            (
                _strlike_or_none(stream_name, "xread stream name must be bytes, str, or None"),
                _parse_entries(entries),
            )
        )
    return parsed


class RedisStream:
    def __init__(self, url: str = "redis://127.0.0.1:6379/0", block_ms: int = _BLOCK_MS) -> None:
        # 固定 RESP2+decode_responses：xread/xrange 全返回 str，无 bytes 解码开销
        self._redis: Redis = from_url(url, protocol=2, decode_responses=True)
        self._block_ms = block_ms

    async def aclose(self) -> None:
        await self._redis.aclose()

    def _to_item(self, entry_id: bytes | str | None, fields: _Fields) -> StreamItem:
        # decode_responses=True 下 key 是 str，直接用 _REDIS_FIELD 字符串查找
        raw = fields.get(_REDIS_FIELD) if fields is not None else None
        payload: object = json.loads(_decode(raw)) if raw is not None else {}
        event = clone_event(validate_event(payload))
        return StreamItem(cursor=_decode_cursor(entry_id), event=event)

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        payload = clone_event(validate_event(dict(event)))
        entry_id = await self._redis.xadd(
            stream,
            {_REDIS_FIELD: json.dumps(payload, ensure_ascii=False)},
        )
        return StreamItem(cursor=_decode_cursor(entry_id), event=clone_event(payload))

    async def read_all(self, stream: str) -> list[StreamItem]:
        entries = await self._redis.xrange(stream, min="-", max="+")
        if not entries:
            return []
        return [self._to_item(entry_id, fields) for entry_id, fields in entries]

    async def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
        last = from_cursor if from_cursor is not None else "0-0"
        backoff = _RECONNECT_BACKOFF_MIN
        while True:
            try:
                raw = await self._redis.xread({stream: last}, block=self._block_ms)
            except (RedisConnectionError, RedisTimeoutError) as error:
                # 宏观分布式层韧性：redis 断线/抖动绝不冒泡杀死订阅流（否则上游 serve 退出、整个
                # worker 罢工）。last 游标在生成器内跨重连存活 → 重连从断点续读、不重放整条流；
                # 指数退避防 busy-loop。非瞬态 RedisError（如命令错误）仍上抛，不掩盖真 bug。
                LOGGER.warning("redis xread on %s failed, reconnect in %.1fs: %s", stream, backoff, error)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX)
                continue
            backoff = _RECONNECT_BACKOFF_MIN  # 成功读一次即重置退避
            response = parse_xread_response(raw)
            if not response:
                continue
            for _stream_name, entries in response:
                for entry_id, fields in entries:
                    item = self._to_item(entry_id, fields)
                    last = item.cursor
                    yield item
