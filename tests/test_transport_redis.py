from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest

redis_asyncio = pytest.importorskip("redis.asyncio")

from kokoro_agent.application.protocols.stream import StreamItem  # noqa: E402
from kokoro_agent.domain.json_payload import JsonObject  # noqa: E402
from kokoro_agent.infrastructure.transport import RedisStream, parse_xread_response  # noqa: E402

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
async def port() -> AsyncIterator[RedisStream]:
    if not await _redis_available():
        pytest.skip(f"no redis reachable at {REDIS_URL}")
    p = RedisStream(REDIS_URL)
    yield p
    await p.aclose()


def _stream() -> str:
    return f"kokoro:test:{uuid.uuid4().hex}"


async def test_publish_then_read_all_preserves_order(port: RedisStream) -> None:
    stream = _stream()
    for i in range(3):
        await port.publish(stream, {"seq": i})

    items = await port.read_all(stream)
    assert [item.event["seq"] for item in items] == [0, 1, 2]
    cursors = [item.cursor for item in items]
    assert len(set(cursors)) == 3


async def test_subscribe_from_cursor_skips_earlier(port: RedisStream) -> None:
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
    port = RedisStream(REDIS_URL, block_ms=25)
    try:
        await port.publish(stream, {"seq": 1})
        items = await port.read_all(stream)
        assert items[0].event["seq"] == 1
    finally:
        await port.aclose()


def test_parse_xread_response_rejects_malformed_shapes() -> None:
    with pytest.raises(ValueError):
        parse_xread_response(["bad"])
    with pytest.raises(ValueError):
        parse_xread_response([("stream", ["bad-entry"])])


# Characterisation net for parse_xread_response: pin the RESP2/RESP3 parsing
# behaviour (no redis needed) so the planned stream_port split cannot silently
# regress the response shapes a real redis server returns.
def test_parse_xread_response_list_of_pairs_resp2() -> None:
    raw = [(b"stream", [(b"1-0", {b"data": b"{}"})])]
    assert parse_xread_response(raw) == [(b"stream", [(b"1-0", {b"data": b"{}"})])]


def test_parse_xread_response_mapping_resp3() -> None:
    raw = {b"stream": [(b"1-0", {b"data": b"{}"})]}
    assert parse_xread_response(raw) == [(b"stream", [(b"1-0", {b"data": b"{}"})])]


def test_parse_xread_response_unwraps_resp3_entry_wrapper() -> None:
    # RESP3 can wrap the entries in one extra list layer; it is unwrapped.
    raw = [(b"stream", [[(b"1-0", {b"data": b"{}"})]])]
    assert parse_xread_response(raw) == [(b"stream", [(b"1-0", {b"data": b"{}"})])]


def test_parse_xread_response_rejects_multi_list_resp3_wrapper() -> None:
    raw = [(b"stream", [[(b"1-0", {b"data": b"{}"})], [(b"2-0", {b"data": b"{}"})]])]
    with pytest.raises(ValueError):
        parse_xread_response(raw)


def test_parse_xread_response_preserves_none_id_and_fields() -> None:
    raw = [(b"stream", [(None, None)])]
    assert parse_xread_response(raw) == [(b"stream", [(None, None)])]


def test_parse_xread_response_none_is_none() -> None:
    assert parse_xread_response(None) is None


def test_parse_xread_response_rejects_non_strlike_fields() -> None:
    raw = [(b"stream", [(b"1-0", {b"data": 123})])]
    with pytest.raises(ValueError):
        parse_xread_response(raw)


async def test_read_all_rejects_non_object_json(port: RedisStream) -> None:
    stream = _stream()
    client = redis_asyncio.from_url(REDIS_URL)
    try:
        await client.xadd(stream, {"data": "[]"})
        with pytest.raises(ValueError):
            await port.read_all(stream)
    finally:
        await client.aclose()


async def test_redis_port_round_trips_nested_json_object(port: RedisStream) -> None:
    stream = _stream()
    payload: JsonObject = cast(
        "JsonObject",
        {
            "kind": "run.request",
            "payload": {"items": [1, True, None], "text": "你好"},
        },
    )
    await port.publish(stream, payload)
    items = await port.read_all(stream)
    assert items[0].event == payload
