from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from kokoro_agent.infrastructure.stream_port import (  # noqa: E402
    RedisStreamPort,
    StreamItem,
)

REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")


async def _redis_available() -> bool:
    client = redis_asyncio.from_url(REDIS_URL)
    try:
        await asyncio.wait_for(client.ping(), timeout=0.5)
        return True
    except (OSError, asyncio.TimeoutError, redis_asyncio.RedisError):
        return False
    finally:
        await client.aclose()


@pytest.fixture
async def port() -> AsyncIterator[RedisStreamPort]:
    if not await _redis_available():
        pytest.skip(f"no redis reachable at {REDIS_URL}")
    p = RedisStreamPort(REDIS_URL)
    yield p
    await p.aclose()


def _stream() -> str:
    return f"kokoro:test:{uuid.uuid4().hex}"


async def test_publish_then_read_all_preserves_order(port: RedisStreamPort) -> None:
    stream = _stream()
    for i in range(3):
        await port.publish(stream, {"seq": i})

    items = await port.read_all(stream)
    assert [item.event["seq"] for item in items] == [0, 1, 2]
    cursors = [item.cursor for item in items]
    assert len(set(cursors)) == 3


async def test_subscribe_from_cursor_skips_earlier(port: RedisStreamPort) -> None:
    stream = _stream()
    await port.publish(stream, {"seq": 0})
    await port.publish(stream, {"seq": 1})
    existing = await port.read_all(stream)
    first_cursor = existing[0].cursor

    received: list[StreamItem] = []

    async def consume() -> None:
        async with asyncio.timeout(3):
            async for item in port.subscribe(stream, from_cursor=first_cursor):
                received.append(item)
                if item.event["seq"] == 2:
                    return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)
    await port.publish(stream, {"seq": 2})
    await task

    assert [item.event["seq"] for item in received] == [1, 2]


async def test_redis_port_allows_custom_block_ms() -> None:
    stream = _stream()
    port = RedisStreamPort(REDIS_URL, block_ms=25)
    try:
        await port.publish(stream, {"seq": 1})
        items = await port.read_all(stream)
        assert items[0].event["seq"] == 1
    finally:
        await port.aclose()
