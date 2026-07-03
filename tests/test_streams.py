"""传输层规格：memory/redis 同语义——publish 裁剪、group 单投递、ack、断线退避、边界洗净。"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
from pydantic import JsonValue, ValidationError

from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.streams.protocol import StreamItem, StreamProtocol, validate_event
from kokoro_agent.streams.redis import RedisStream, parse_xread_response

REDIS_URL = os.environ.get("KOKORO_REDIS_URL", "redis://127.0.0.1:6379/0")


async def _collect(
    iterator: AsyncIterator[StreamItem], count: int, timeout: float = 2.0
) -> list[StreamItem]:
    items: list[StreamItem] = []

    async def _pull() -> None:
        async for item in iterator:
            items.append(item)
            if len(items) >= count:
                return

    await asyncio.wait_for(_pull(), timeout=timeout)
    return items


# --- memory ---


async def test_memory_publish_read_all_round_trip() -> None:
    stream = MemoryStream()
    await stream.publish("s", {"a": 1}, maxlen=10)
    await stream.publish("s", {"b": {"nested": [1, 2]}}, maxlen=10)
    items = await stream.read_all("s")
    assert [item.event for item in items] == [{"a": 1}, {"b": {"nested": [1, 2]}}]
    assert items[0].cursor < items[1].cursor


async def test_memory_maxlen_trims_oldest() -> None:
    stream = MemoryStream()
    for i in range(5):
        await stream.publish("s", {"i": i}, maxlen=3)
    items = await stream.read_all("s")
    assert [item.event["i"] for item in items] == [2, 3, 4]


async def test_memory_group_delivers_each_message_once() -> None:
    stream = MemoryStream()
    for i in range(4):
        await stream.publish("s", {"i": i}, maxlen=10)
    # 同 group 两个 consumer 共享游标：合计恰好各消息一次。
    sub_a = stream.subscribe("s", group="g", consumer="a")
    got_a = await _collect(sub_a, 2)
    sub_b = stream.subscribe("s", group="g", consumer="b")
    got_b = await _collect(sub_b, 2)
    seen = [item.event["i"] for item in got_a + got_b]
    assert sorted(seen, key=lambda value: str(value)) == [0, 1, 2, 3]


async def test_memory_two_groups_each_get_all() -> None:
    stream = MemoryStream()
    await stream.publish("s", {"x": 1}, maxlen=10)
    got_g1 = await _collect(stream.subscribe("s", group="g1", consumer="a"), 1)
    got_g2 = await _collect(stream.subscribe("s", group="g2", consumer="a"), 1)
    assert got_g1[0].event == got_g2[0].event == {"x": 1}


async def test_memory_subscribe_wakes_on_late_publish() -> None:
    stream = MemoryStream()

    async def _late() -> None:
        await asyncio.sleep(0.01)
        await stream.publish("s", {"late": True}, maxlen=10)

    task = asyncio.create_task(_late())
    got = await _collect(stream.subscribe("s", group="g", consumer="a"), 1)
    await task
    assert got[0].event == {"late": True}


async def test_memory_ack_recorded() -> None:
    stream = MemoryStream()
    item = await stream.publish("s", {"x": 1}, maxlen=10)
    await stream.ack("s", "g", item.cursor)
    assert stream.acked("s", "g") == frozenset({item.cursor})


async def test_memory_events_isolated_between_items() -> None:
    stream = MemoryStream()
    payload: dict[str, JsonValue] = {"nested": {"n": 1}}
    returned = await stream.publish("s", payload, maxlen=10)
    nested = returned.event["nested"]
    assert isinstance(nested, dict)
    nested["n"] = 999
    stored = (await stream.read_all("s"))[0]
    assert stored.event == {"nested": {"n": 1}}


def test_memory_satisfies_protocol() -> None:
    assert isinstance(MemoryStream(), StreamProtocol)


@pytest.mark.parametrize("bad", [object(), {"k": object()}, {"k": {1, 2}}, [1, 2], "str", None])
def test_event_boundary_rejects_non_json(bad: object) -> None:
    # publish 的入参洗净单点：非 JSON 载荷在边界即 ValidationError，绝不入流。
    with pytest.raises(ValidationError):
        validate_event(bad)


@pytest.mark.parametrize(
    "empty", [{}, {"empty_str": ""}, {"empty_list": []}, {"empty_dict": {}}, {"null": None}],
)
async def test_publish_accepts_json_edge_values(empty: dict[str, JsonValue]) -> None:
    stream = MemoryStream()
    item = await stream.publish("s", empty, maxlen=10)
    assert item.event == empty


# --- parse_xread_response（无需 redis） ---


def test_parse_xread_live_shape() -> None:
    # live redis 实测形状：外层 list、stream-entry 是 list、内层条目是 tuple。
    raw: object = [["s", [("1-0", {"data": "{}"}), ("1-1", None)]]]
    parsed = parse_xread_response(raw)
    assert parsed == [("s", [("1-0", {"data": "{}"}), ("1-1", None)])]


def test_parse_xread_none_passthrough() -> None:
    assert parse_xread_response(None) is None


@pytest.mark.parametrize(
    "raw",
    [
        "nonsense",
        [["s"]],
        [["s", [("1-0",)]]],
        [["s", [(object(), None)]]],
        [["s", [("1-0", {"data": 42})]]],
    ],
)
def test_parse_xread_malformed_rejected(raw: object) -> None:
    with pytest.raises(ValueError):
        parse_xread_response(raw)


# --- redis（不可达即显式 skip） ---


async def _redis_or_skip() -> RedisStream:
    port = RedisStream(REDIS_URL, block_ms=100)
    try:
        await asyncio.wait_for(port.read_all("kokoro-test-ping"), timeout=1.0)
    except Exception:  # noqa: BLE001 — 网络/服务不可达统一视为 skip 条件
        await port.aclose()
        pytest.skip(f"no redis reachable at {REDIS_URL}")
    return port


async def test_redis_round_trip_and_group_ack() -> None:
    port = await _redis_or_skip()
    stream = f"kokoro-test:{uuid.uuid4().hex}"
    try:
        published = await port.publish(stream, {"n": 1, "nested": {"深": "值"}}, maxlen=100)
        items = await port.read_all(stream)
        assert [item.event for item in items] == [{"n": 1, "nested": {"深": "值"}}]
        assert items[0].cursor == published.cursor

        got = await _collect(port.subscribe(stream, group="g", consumer="c1"), 1)
        assert got[0].event == {"n": 1, "nested": {"深": "值"}}
        await port.ack(stream, "g", got[0].cursor)

        # ack 后同 group 不再重投：新消息才被消费。
        await port.publish(stream, {"n": 2}, maxlen=100)
        got2 = await _collect(port.subscribe(stream, group="g", consumer="c1"), 1)
        assert got2[0].event == {"n": 2}
    finally:
        await port.aclose()


async def test_redis_publish_respects_maxlen() -> None:
    port = await _redis_or_skip()
    stream = f"kokoro-test:{uuid.uuid4().hex}"
    try:
        # approximate trim 不保证精确长度，但公开 API 必须可用且不增长到无限。
        for i in range(200):
            await port.publish(stream, {"i": i}, maxlen=10)
        items = await port.read_all(stream)
        assert 0 < len(items) < 200
    finally:
        await port.aclose()


async def test_redis_subscribe_survives_connection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # 断线退避：xreadgroup 先抛 ConnectionError，再恢复返回数据；订阅流不死、从断点续读。
    from redis.exceptions import ConnectionError as RedisConnectionError

    class _FlakyRedis:
        def __init__(self) -> None:
            self.calls = 0

        async def xgroup_create(self, *args: object, **kwargs: object) -> None:
            return None

        async def xreadgroup(self, *args: object, **kwargs: object) -> object:
            self.calls += 1
            if self.calls == 1:
                raise RedisConnectionError("down")
            return [["s", [(f"1-{self.calls}", {"data": '{"ok": true}'})]]]

    fake = _FlakyRedis()

    def _fake_from_url(url: str, **kwargs: object) -> _FlakyRedis:
        return fake

    monkeypatch.setattr("kokoro_agent.streams.redis.from_url", _fake_from_url)
    port = RedisStream("redis://ignored", block_ms=10)
    got = await _collect(port.subscribe("s", group="g", consumer="c"), 1, timeout=5.0)
    assert got[0].event == {"ok": True}
    assert fake.calls >= 2
