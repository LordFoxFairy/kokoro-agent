from __future__ import annotations

import asyncio
from typing import cast

import pytest

from kokoro_agent.application.event_stream import StreamItem
from kokoro_agent.domain.json_payload import JsonObject
from kokoro_agent.infrastructure.json_types import validate_event
from kokoro_agent.infrastructure.transport import MemoryStream

STREAM = "kokoro:test:stream"


async def test_publish_then_read_all_preserves_order_and_unique_cursors() -> None:
    port = MemoryStream()
    for i in range(3):
        await port.publish(STREAM, {"seq": i})

    items = await port.read_all(STREAM)
    assert [item.event["seq"] for item in items] == [0, 1, 2]

    cursors = [item.cursor for item in items]
    assert len(set(cursors)) == 3
    assert cursors == sorted(cursors)


async def test_read_all_empty_stream_returns_empty() -> None:
    port = MemoryStream()
    assert await port.read_all("nope") == []


async def test_subscribe_yields_published_items() -> None:
    port = MemoryStream()
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
    port = MemoryStream()
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

    assert received == [1, 2]


async def test_subscribe_blocks_without_busy_wait() -> None:
    port = MemoryStream()

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


async def test_memory_port_allows_custom_cursor_width() -> None:
    port = MemoryStream(cursor_width=6)
    await port.publish(STREAM, {"seq": 1})
    item = (await port.read_all(STREAM))[0]
    assert item.cursor == "000000"


def test_validate_event_rejects_non_object_top_level_payloads() -> None:
    with pytest.raises(ValueError):
        validate_event([])
    with pytest.raises(ValueError):
        validate_event("bad")


def test_validate_event_rejects_non_json_nested_values() -> None:
    with pytest.raises(ValueError):
        validate_event({"payload": {"x": complex(1, 2)}})


async def test_memory_port_round_trips_nested_json_object() -> None:
    port = MemoryStream()
    payload: JsonObject = {
        "kind": "run.request",
        "payload": {"items": [1, True, None], "text": "你好"},
    }
    await port.publish(STREAM, payload)
    items = await port.read_all(STREAM)
    assert items[0].event == payload
