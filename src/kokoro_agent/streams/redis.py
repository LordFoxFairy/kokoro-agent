"""Redis Streams 传输：XADD maxlen 裁剪 + XREADGROUP/XACK 消费，断线指数退避。"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Mapping
from typing import TypeAlias, TypeGuard

from pydantic import JsonValue
from redis.asyncio import Redis, from_url
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from kokoro_agent.streams.protocol import StreamItem, validate_event

LOGGER = logging.getLogger(__name__)

_REDIS_FIELD = "data"
_BLOCK_MS = 1000
# 重连退避边界（秒）：抖动快恢复，持续断线不 busy-loop 打爆 redis。
_RECONNECT_BACKOFF_MIN = 0.1
_AUTOCLAIM_IDLE_MS = 60_000
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


def parse_xautoclaim_response(raw: object) -> list[_Entry]:
    # xautoclaim 返回 (next_cursor, entries, deleted_ids)：只消费 entries，沿用 xread 的窄化器。
    if not _is_seq(raw) or len(raw) < 2:
        raise ValueError("xautoclaim response must be (cursor, entries, ...)")
    return _parse_entries(raw[1])


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
    def __init__(
        self, url: str, block_ms: int = _BLOCK_MS, autoclaim_idle_ms: int = _AUTOCLAIM_IDLE_MS
    ) -> None:
        # 固定 RESP2+decode_responses：xread/xrange 全返回 str，无 bytes 解码开销。
        self._redis: Redis = from_url(url, protocol=2, decode_responses=True)
        self._block_ms = block_ms
        self._autoclaim_idle_ms = autoclaim_idle_ms

    async def aclose(self) -> None:
        await self._redis.aclose()

    def _to_item(self, entry_id: bytes | str | None, fields: _Fields) -> StreamItem:
        raw = fields.get(_REDIS_FIELD) if fields is not None else None
        payload: object = json.loads(_decode(raw)) if raw is not None else {}
        return StreamItem(cursor=_decode_cursor(entry_id), event=validate_event(payload))

    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        payload = validate_event(dict(event))
        entry_id = await self._redis.xadd(
            stream,
            {_REDIS_FIELD: json.dumps(payload, ensure_ascii=False)},
            maxlen=maxlen,
            approximate=True,
        )
        return StreamItem(cursor=_decode_cursor(entry_id), event=payload)

    async def read_all(self, stream: str) -> list[StreamItem]:
        entries = await self._redis.xrange(stream, min="-", max="+")
        if not entries:
            return []
        return [self._to_item(entry_id, fields) for entry_id, fields in entries]

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(stream, group, id="0", mkstream=True)
        except ResponseError as error:
            # 组已存在是幂等常态；其余 ResponseError 是真 bug，照常上抛。
            if "BUSYGROUP" not in str(error):
                raise

    async def _autoclaim_stale(
        self, stream: str, group: str, consumer: str
    ) -> list[StreamItem]:
        # 死信收养：崩溃消费者 ack 前的 PEL 条目不会被 XREADGROUP ">" 重投，
        # 空转间隙按 idle 阈值认领到本消费者重放（下游幂等去重兜正确性）。
        raw = await self._redis.xautoclaim(
            stream, group, consumer, min_idle_time=self._autoclaim_idle_ms, count=16
        )
        return [
            self._to_item(entry_id, fields)
            for entry_id, fields in parse_xautoclaim_response(raw)
        ]

    async def subscribe(
        self, stream: str, *, group: str, consumer: str
    ) -> AsyncIterator[StreamItem]:
        group_ready = False
        backoff = _RECONNECT_BACKOFF_MIN
        while True:
            try:
                if not group_ready:
                    await self._ensure_group(stream, group)
                    group_ready = True
                raw = await self._redis.xreadgroup(
                    group, consumer, {stream: ">"}, block=self._block_ms
                )
                response = parse_xread_response(raw) or []
                items = [
                    self._to_item(entry_id, fields)
                    for _stream_name, entries in response
                    for entry_id, fields in entries
                ]
                if not items:
                    # 空转间隙才做死信收养：不占热路径。
                    items = await self._autoclaim_stale(stream, group, consumer)
            except (RedisConnectionError, RedisTimeoutError) as error:
                # 断线/抖动绝不冒泡杀死订阅流；group 游标在 redis 侧存活，重连不重放已投递消息。
                LOGGER.warning(
                    "redis xreadgroup on %s failed, reconnect in %.1fs: %s", stream, backoff, error
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX)
                continue
            backoff = _RECONNECT_BACKOFF_MIN
            for item in items:
                yield item

    async def ack(self, stream: str, group: str, cursor: str) -> None:
        await self._redis.xack(stream, group, cursor)

    async def delete(self, stream: str) -> None:
        await self._redis.delete(stream)
