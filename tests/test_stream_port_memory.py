from __future__ import annotations

import asyncio
from typing import cast

from kokoro_agent.infrastructure.stream_port import MemoryStreamPort, StreamItem

STREAM = "kokoro:test:stream"


async def test_publish_then_read_all_preserves_order_and_unique_cursors() -> None:
    port = MemoryStreamPort()
    for i in range(3):
        await port.publish(STREAM, {"seq": i})

    items = await port.read_all(STREAM)
    assert [item.event["seq"] for item in items] == [0, 1, 2]

    cursors = [item.cursor for item in items]
    assert len(set(cursors)) == 3
    # cursors are monotonically increasing zero-padded strings
    assert cursors == sorted(cursors)


async def test_read_all_empty_stream_returns_empty() -> None:
    port = MemoryStreamPort()
    assert await port.read_all("nope") == []


async def test_subscribe_yields_published_items() -> None:
    port = MemoryStreamPort()
    received: list[StreamItem] = []

    async def consume() -> None:
        async with asyncio.timeout(2):
            async for item in port.subscribe(STREAM):
                received.append(item)
                if len(received) == 2:
                    return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    await port.publish(STREAM, {"seq": 0})
    await port.publish(STREAM, {"seq": 1})
    await task

    assert [item.event["seq"] for item in received] == [0, 1]


async def test_subscribe_from_cursor_skips_earlier() -> None:
    port = MemoryStreamPort()
    await port.publish(STREAM, {"seq": 0})
    await port.publish(STREAM, {"seq": 1})
    existing = await port.read_all(STREAM)
    first_cursor = existing[0].cursor

    received: list[int] = []

    async def consume() -> None:
        async with asyncio.timeout(2):
            async for item in port.subscribe(STREAM, from_cursor=first_cursor):
                received.append(cast(int, item.event["seq"]))
                if received and received[-1] == 2:
                    return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    await port.publish(STREAM, {"seq": 2})
    await task

    # cursor of seq=0 is skipped; we start after it
    assert received == [1, 2]


async def test_subscribe_blocks_without_busy_wait() -> None:
    port = MemoryStreamPort()

    async def consume() -> StreamItem:
        async for item in port.subscribe(STREAM):
            return item
        raise AssertionError("no item")

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    assert not task.done()
    await port.publish(STREAM, {"seq": 42})
    item = await asyncio.wait_for(task, timeout=2)
    assert item.event["seq"] == 42
