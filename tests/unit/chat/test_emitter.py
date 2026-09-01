"""RunEmitter persists GA chat facts without writing Session's browser stream."""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import JsonValue
from support.chat import FakeChatStore
from support.fakes import FakeBus

from kokoro_agent.contract import live_stream
from kokoro_agent.execution.events import RunEmitter, message_delta_payload
from kokoro_agent.streams.protocol import StreamItem


class _OrderedBus(FakeBus):
    def __init__(self, order: list[str]) -> None:
        super().__init__()
        self._order = order

    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        self._order.append("raw")
        return await super().publish(stream, event, maxlen=maxlen)


async def test_chat_fact_is_durable_before_raw_agent_event() -> None:
    order: list[str] = []
    bus = _OrderedBus(order)
    store = FakeChatStore(order)
    emitter = await RunEmitter.attach(
        bus,
        "run-1",
        namespace="ns",
        session_id="session-1",
        chat_store=store,
    )
    payload = message_delta_payload("hello", segment_id="native-segment")
    assert payload is not None

    await emitter.emit(payload)

    assert order == ["chat", "raw"]
    assert store.records[0].event_type == "assistant.delta"
    assert "native-segment" not in store.records[0].payload_json
    assert all(stream != live_stream("session-1") for stream, _event, _maxlen in bus.published)
