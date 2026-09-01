"""传输层规格：redis（唯一真后端）——publish 裁剪、group 单投递、ack、断线退避、边界洗净。"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
from pydantic import JsonValue, ValidationError

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


async def _redis_required() -> RedisStream:
    """真 redis 前置：不可达即 fail-loud（不 skip、不灌绿数）。"""
    port = RedisStream(REDIS_URL, block_ms=100)
    try:
        await asyncio.wait_for(port.read_all("kokoro-test-ping"), timeout=1.0)
    except Exception as exc:  # noqa: BLE001 — 服务缺失显式炸
        await port.aclose()
        raise RuntimeError(f"redis required but unreachable at {REDIS_URL}: {exc}") from exc
    return port


# --- 边界洗净（无需 redis） ---


@pytest.mark.parametrize("bad", [object(), {"k": object()}, {"k": {1, 2}}, [1, 2], "str", None])
def test_event_boundary_rejects_non_json(bad: object) -> None:
    # publish 的入参洗净单点：非 JSON 载荷在边界即 ValidationError，绝不入流。
    with pytest.raises(ValidationError):
        validate_event(bad)


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


# --- redis（不可达即 fail-loud） ---


def test_redis_satisfies_protocol() -> None:
    assert isinstance(RedisStream(REDIS_URL), StreamProtocol)


@pytest.mark.parametrize(
    "empty", [{}, {"empty_str": ""}, {"empty_list": []}, {"empty_dict": {}}, {"null": None}],
)
async def test_publish_accepts_json_edge_values(empty: dict[str, JsonValue]) -> None:
    port = await _redis_required()
    stream = f"kokoro-test:{uuid.uuid4().hex}"
    try:
        item = await port.publish(stream, empty, maxlen=10)
        assert item.event == empty
    finally:
        await port.aclose()


async def test_redis_round_trip_and_group_ack() -> None:
    port = await _redis_required()
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


async def test_redis_autoclaim_adopts_stale_pending() -> None:
    # 死信收养：消费者 A 读到未 ack 即"崩溃"，空转的消费者 B 按 idle 阈值收养该 PEL 条目。
    port_a = await _redis_required()
    port_b = RedisStream(REDIS_URL, block_ms=100, autoclaim_idle_ms=50)
    stream = f"kokoro-test:{uuid.uuid4().hex}"
    try:
        await port_a.publish(stream, {"n": "orphan"}, maxlen=100)
        got_a = await _collect(port_a.subscribe(stream, group="g", consumer="crashed"), 1)
        assert got_a[0].event == {"n": "orphan"}  # 未 ack：留在 crashed 的 PEL
        await asyncio.sleep(0.1)  # 超过 idle 阈值
        got_b = await _collect(port_b.subscribe(stream, group="g", consumer="survivor"), 1)
        assert got_b[0].event == {"n": "orphan"}
        await port_b.ack(stream, "g", got_b[0].cursor)
    finally:
        await port_a.aclose()
        await port_b.aclose()


async def test_redis_publish_respects_maxlen() -> None:
    port = await _redis_required()
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
