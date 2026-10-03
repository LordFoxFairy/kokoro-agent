"""RunEmitter persists GA chat facts without writing Session's browser stream."""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from pydantic import JsonValue
from support.chat import FakeChatRepository
from support.fakes import FakeBus

from kokoro_agent.protocol import live_stream
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
    store = FakeChatRepository(order)
    emitter = await RunEmitter.attach(
        bus,
        "run-1",
        tenant_id="tenant",
        namespace="ns",
        session_id="session-1",
        chat_repository=store,
    )
    payload = message_delta_payload("hello", segment_id="native-segment")
    assert payload is not None

    await emitter.emit(payload)

    assert order == ["chat", "raw"]
    assert store.records[0].event_type == "assistant.delta"
    assert "native-segment" not in store.records[0].payload_json
    assert all(
        stream != live_stream("session-1") for stream, _event, _maxlen in bus.published
    )


async def test_r91_explicit_empty_todo_is_durable_before_live() -> None:
    import json
    from kokoro_agent.protocol import TodoUpdatedPayload

    order: list[str] = []
    bus = _OrderedBus(order)
    store = FakeChatRepository(order)
    emitter = await RunEmitter.attach(
        bus,
        "run-todo",
        tenant_id="tenant",
        namespace="ns",
        session_id="session",
        chat_repository=store,
    )
    await emitter.emit(TodoUpdatedPayload(todos=[]))
    assert order == ["chat", "raw"], "explicit clear must persist before publishing"
    assert len(store.records) == 1
    assert store.records[0].event_type == "todo.updated"
    assert json.loads(store.records[0].payload_json) == {"todos": []}


def test_r91_todo_producer_preserves_explicit_empty_control() -> None:
    from support.fakes import FakeToolCall
    from kokoro_agent.execution.events import todo_payload

    payload = todo_payload(FakeToolCall("todo", "write_todos", input={"todos": []}))
    assert payload.model_dump() == {"todos": []}


@pytest.mark.parametrize("arguments", [None, {}, {"unrelated": "SENTINEL_PRIVATE"}])
def test_r91_todo_producer_rejects_missing_table_instead_of_clearing(
    arguments: dict[str, object] | None,
) -> None:
    from support.fakes import FakeToolCall
    from kokoro_agent.execution.events import todo_payload

    with pytest.raises(ValueError):
        todo_payload(FakeToolCall("todo", "write_todos", input=arguments))


async def test_r93_skill_round_anchor_and_live_failure_replay_identity() -> None:
    import json
    from support.fakes import FakeRunRepository, request
    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.protocol import SkillPhase

    class OfflineBus(FakeBus):
        async def publish(
            self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
        ) -> StreamItem:
            raise OSError("live unavailable")

    store = FakeRunRepository()
    req = request("phase-anchor")
    lease = await store.try_claim(req)
    assert lease is not None
    emitter = await RunEmitter.attach(
        OfflineBus(),
        req.run_id,
        outbox=store,
        lease=lease,
        tenant_id=req.execution_identity.tenant_ref,
        namespace=RunScope.of(req).namespace,
        session_id=req.session_id,
        chat_repository=store.chat_repository,
    )
    first = emitter.skill_progress(("skill:exact-revision",))
    receipts = [
        await first(SkillPhase(phase=phase))
        for phase in ("resolving", "loading", "ready")
    ]
    frozen = [row.model_dump_json() for row in store.chat_repository.records]
    payloads = [json.loads(row.payload_json) for row in store.chat_repository.records]
    assert len({item["preflight_id"] for item in payloads}) == 1
    assert [item.source_index for item in receipts] == [0, 1, 2]
    assert all(item["source_refs"] == ["skill:exact-revision"] for item in payloads)
    second = emitter.skill_progress(("skill:exact-revision",))
    await second(SkillPhase(phase="resolving"))
    latest = json.loads(store.chat_repository.records[-1].payload_json)
    assert latest["preflight_id"] != payloads[0]["preflight_id"]
    assert latest["activity_id"] == payloads[0]["activity_id"]
    assert frozen == [
        row.model_dump_json() for row in store.chat_repository.records[:3]
    ]


async def test_r93_skill_progress_old_lease_stops_after_reclaim() -> None:
    from support.fakes import FakeRunRepository, request
    from kokoro_agent.protocol import SkillPhase
    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.execution.events import ProgressAuthorityLost

    store = FakeRunRepository()
    req = request("phase-takeover")
    lease = await store.try_claim(req)
    assert lease is not None
    emitter = await RunEmitter.attach(
        FakeBus(),
        req.run_id,
        outbox=store,
        lease=lease,
        tenant_id=req.execution_identity.tenant_ref,
        namespace=RunScope.of(req).namespace,
        session_id=req.session_id,
        chat_repository=store.chat_repository,
    )
    callback = emitter.skill_progress(("skill:one",))
    await callback(SkillPhase(phase="resolving"))
    store.expired = [req]
    reclaimed = await store.reclaim_expired("new-owner")
    assert reclaimed[0].lease.generation > lease.generation
    with pytest.raises(ProgressAuthorityLost):
        await callback(SkillPhase(phase="loading"))
    assert len(store.chat_repository.records) == 1


@pytest.mark.parametrize("phase", ["started", "finished"])
def test_r93_subagent_requires_real_call_identity(phase: str) -> None:
    from support.fakes import FakeSubagentRun
    from kokoro_agent.execution.events import (
        subagent_started_payload,
        subagent_finished_payload,
    )

    producer = (
        subagent_started_payload if phase == "started" else subagent_finished_payload
    )
    with pytest.raises(ValueError, match="identity"):
        producer(FakeSubagentRun(trigger_call_id=None), source="runtime-custom")
