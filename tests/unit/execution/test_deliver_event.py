"""delivery.created 追发规格：投影在 tool.returned 之后经同一 emitter 追发，序号统一。"""

from __future__ import annotations

from support.fakes import settled_state_callback

from support.fakes import terminal_emitter

from collections.abc import Mapping
from datetime import datetime, timezone
import json
from typing import cast

from pydantic import JsonValue

from support.fakes import (
    FakeAgent,
    FakeBus,
    FakeRunStream,
    FakeToolCall,
    FakeRunRepository,
    request,
    usage_recorder,
)
from support.chat import FakeChatRepository

from kokoro_agent.protocol import (
    ArtifactKind,
    DeliveryCreated,
    DeliveryCreatedPayload,
    SubagentSource,
)
from kokoro_agent.domain.chat.projection import project_chat_fact
from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.execution.events import RunEmitter, delivery_created_payload
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.tools.deliver import DeliverResult


def _runtime_custom(_name: str) -> SubagentSource:
    return "runtime-custom"


async def _always_claim() -> bool:
    return True


async def _invoke(bus: FakeBus, run: FakeRunStream) -> None:
    emitter = await RunEmitter.attach(bus, "r1")
    await invoke_once(
        emitter,
        (settlement_agent := FakeAgent(run=run)),
        "c1",
        {"messages": []},
        initial=True,
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(emitter, _always_claim, usage_recorder()[0]),
        record_usage=usage_recorder()[0],
        on_native_settled=settled_state_callback(settlement_agent, "c1"),
    )


def _delivered_json(*, note: str = "", artifact_kind: ArtifactKind = "document") -> str:
    return DeliverResult(
        status="delivered",
        artifact_id="artifact-abc123",
        asset_id="asset-abc123",
        artifact_kind=artifact_kind,
        path="/report.pdf",
        title="Report",
        mime="application/pdf",
        size=12,
        content_hash="abc123",
        note=note,
    ).model_dump_json()


def _deliver_call(output: object) -> FakeToolCall:
    return FakeToolCall(
        tool_call_id="t1",
        tool_name="deliver",
        input={"path": "/report.pdf", "title": "Report"},
        output=output,
    )


# --- 投影函数单测 ---


def test_payload_built_from_delivered_result() -> None:
    payload = delivery_created_payload(_deliver_call(_delivered_json(note="v1")))
    assert payload is not None
    assert payload.path == "/report.pdf"
    assert payload.tool_call_id == "t1"
    assert payload.content_hash == "abc123"
    assert payload.artifact_kind == "document"
    assert payload.mime == "application/pdf"
    assert payload.size == 12
    assert payload.note == "v1"


def test_delivery_chat_projection_keeps_owner_kind() -> None:
    payload = delivery_created_payload(_deliver_call(_delivered_json()))
    assert payload is not None
    projected = project_chat_fact(
        tenant_id="tenant-1",
        namespace="namespace-1",
        session_id="conversation-1",
        run_id="r1",
        source_index=1,
        created_at=datetime.now(timezone.utc),
        payload=payload,
    )
    assert projected is not None
    assert json.loads(projected.event.payload_json)["artifact_kind"] == "document"


def test_empty_note_omitted() -> None:
    payload = delivery_created_payload(_deliver_call(_delivered_json(note="")))
    assert payload is not None
    assert payload.note is None  # 空串省略（exclude_none 落地即缺席）。


def test_non_deliver_tool_never_follows() -> None:
    call = FakeToolCall(tool_call_id="t1", tool_name="lookup", output=_delivered_json())
    assert delivery_created_payload(call) is None


def test_degraded_error_text_never_follows() -> None:
    # 降级 error 文本（非 JSON）不追发。
    assert delivery_created_payload(_deliver_call("error: 文件不存在")) is None


def test_tool_exception_never_follows() -> None:
    call = FakeToolCall(tool_call_id="t1", tool_name="deliver", error="boom")
    assert delivery_created_payload(call) is None


# --- 端到端投影：追发紧随 tool.returned，序号连续 ---


async def test_delivery_follows_tool_returned_via_same_emitter() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeRunStream(tool_views=(_deliver_call(_delivered_json()),)))

    kinds = bus.kinds("r1")
    assert (
        kinds.index("delivery.created") == kinds.index("tool.returned") + 1
    )  # 紧随，序号连续。
    events = bus.run_events("r1")
    returned = next(e for e in events if e.kind == "tool.returned")
    delivery = next(e for e in events if e.kind == "delivery.created")
    assert delivery.index == returned.index + 1  # 序号由 emitter 统一，不旁路。
    assert isinstance(delivery, DeliveryCreated)
    assert delivery.payload.content_hash == "abc123"
    assert delivery.payload.artifact_id == "artifact-abc123"
    assert delivery.payload.asset_id == "asset-abc123"
    assert delivery.payload.artifact_kind == "document"
    assert delivery.payload.tool_call_id == "t1"


async def test_non_deliver_result_emits_no_delivery() -> None:
    bus = FakeBus()
    call = FakeToolCall(tool_call_id="t1", tool_name="lookup", output="found")
    await _invoke(bus, FakeRunStream(tool_views=(call,)))
    assert "delivery.created" not in bus.kinds("r1")


async def test_delivery_is_critical_and_deduplicates_same_tool_call() -> None:
    bus, store = FakeBus(), FakeRunRepository()
    lease = await store.try_claim(request("r1"))
    assert lease is not None
    emitter = await RunEmitter.attach(bus, "r1", outbox=store, lease=lease)
    payload = delivery_created_payload(_deliver_call(_delivered_json()))
    assert payload is not None

    await emitter.emit(payload)
    await emitter.emit(payload)

    rows = [row for row in store.outbox["r1"] if row["kind"] == "delivery.created"]
    assert len(rows) == 1
    assert rows[0]["status"] == "published"
    assert cast(str, rows[0]["event_id"]).startswith("evt_")


async def test_terminal_barrier_recovers_journal_success_after_stream_loss() -> None:
    bus, store = FakeBus(), FakeRunRepository()
    chat = FakeChatRepository()
    lease = await store.try_claim(request("r1"))
    assert lease is not None
    store.tool_journal[("r1", "t1")] = {
        "name": "deliver",
        "status": "succeeded",
        "result": _delivered_json(artifact_kind="code"),
        "is_error": False,
    }
    emitter = await RunEmitter.attach(
        bus,
        "r1",
        outbox=store,
        lease=lease,
        tenant_id="tenant-1",
        namespace="namespace-1",
        session_id="conversation-1",
        chat_repository=chat,
    )
    await emitter.ensure_delivery_events()
    await emitter.ensure_delivery_events()
    assert bus.kinds("r1").count("delivery.created") == 1  # published is not sent twice
    published = bus.run_events("r1")[0]
    assert isinstance(published.payload, DeliveryCreatedPayload)
    assert published.payload.artifact_kind == "code"
    assert len(chat.records) == 1
    assert json.loads(chat.records[0].payload_json)["artifact_kind"] == "code"
    assert (
        len([row for row in store.outbox["r1"] if row["kind"] == "delivery.created"])
        == 1
    )


async def test_terminal_barrier_rejects_incomplete_delivery_intent() -> None:
    import pytest

    bus, store = FakeBus(), FakeRunRepository()
    lease = await store.try_claim(request("r1"))
    assert lease is not None
    store.tool_journal[("r1", "t1")] = {
        "name": "deliver",
        "status": "started",
        "result": '{"version":1}',
        "is_error": False,
    }
    emitter = await RunEmitter.attach(bus, "r1", outbox=store, lease=lease)
    with pytest.raises(RuntimeError, match="delivery"):
        await emitter.ensure_delivery_events()
    assert store.outbox.get("r1") is None


async def test_terminal_barrier_rejects_corrupt_successful_delivery_result() -> None:
    import pytest

    bus, store = FakeBus(), FakeRunRepository()
    lease = await store.try_claim(request("r1"))
    assert lease is not None
    store.tool_journal[("r1", "t1")] = {
        "name": "deliver",
        "status": "succeeded",
        "result": "not-a-final-receipt",
        "is_error": False,
    }
    emitter = await RunEmitter.attach(bus, "r1", outbox=store, lease=lease)
    with pytest.raises(RuntimeError, match="valid final receipt"):
        await emitter.ensure_delivery_events()
    assert store.outbox.get("r1") is None


async def test_terminal_barrier_rejects_success_without_owner_kind() -> None:
    import json
    import pytest

    bus, store = FakeBus(), FakeRunRepository()
    lease = await store.try_claim(request("r1"))
    assert lease is not None
    old_result = json.loads(_delivered_json())
    del old_result["artifact_kind"]
    store.tool_journal[("r1", "t1")] = {
        "name": "deliver",
        "status": "succeeded",
        "result": json.dumps(old_result),
        "is_error": False,
    }
    emitter = await RunEmitter.attach(bus, "r1", outbox=store, lease=lease)
    with pytest.raises(RuntimeError, match="valid final receipt"):
        await emitter.ensure_delivery_events()
    assert store.outbox.get("r1") is None


async def test_durable_delivery_does_not_wait_for_redis_live_publish() -> None:
    class BrokenLiveBus(FakeBus):
        async def publish(
            self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
        ) -> StreamItem:
            raise ConnectionError("redis down")

    bus, store = BrokenLiveBus(), FakeRunRepository()
    lease = await store.try_claim(request("r1"))
    assert lease is not None
    emitter = await RunEmitter.attach(bus, "r1", outbox=store, lease=lease)
    payload = delivery_created_payload(_deliver_call(_delivered_json()))
    assert payload is not None

    await emitter.emit(payload)

    assert store.outbox["r1"][0]["kind"] == "delivery.created"
    assert store.outbox["r1"][0]["status"] == "queued"
