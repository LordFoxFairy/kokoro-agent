"""delivery.created 追发规格：投影在 tool.returned 之后经同一 emitter 追发，序号统一。"""

from __future__ import annotations

from fakes import (
    FakeAgent,
    FakeBus,
    FakeLedger,
    FakeRunStream,
    FakeToolCall,
    completed_execution_context,
    request,
    usage_recorder,
)

from kokoro_agent.contract import DeliveryCreated, SubagentSource
from kokoro_agent.execution.events import RunEmitter, delivery_created_payload
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.tools.deliver import DeliverResult


def _runtime_custom(_name: str) -> SubagentSource:
    return "runtime-custom"


async def _invoke(bus: FakeBus, run: FakeRunStream) -> None:
    ledger = FakeLedger()
    await ledger.try_claim(request("r1"))
    emitter = await RunEmitter.attach(bus, "r1", outbox=ledger)
    await invoke_once(
        emitter,
        FakeAgent(run=run),
        {"configurable": {"thread_id": "c1"}, "metadata": {"kokoro_run_id": "r1"}},
        {"messages": []},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        prepare_completed=lambda: completed_execution_context("r1"),
        record_usage=usage_recorder()[0],
    )


def _delivered_json(*, note: str = "") -> str:
    return DeliverResult(
        status="delivered",
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
    assert payload.content_hash == "abc123"
    assert payload.mime == "application/pdf"
    assert payload.size == 12
    assert payload.note == "v1"


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
    assert kinds.index("delivery.created") == kinds.index("tool.returned") + 1  # 紧随，序号连续。
    events = bus.run_events("r1")
    returned = next(e for e in events if e.kind == "tool.returned")
    delivery = next(e for e in events if e.kind == "delivery.created")
    assert delivery.index == returned.index + 1  # 序号由 emitter 统一，不旁路。
    assert isinstance(delivery, DeliveryCreated)
    assert delivery.payload.content_hash == "abc123"


async def test_non_deliver_result_emits_no_delivery() -> None:
    bus = FakeBus()
    call = FakeToolCall(tool_call_id="t1", tool_name="lookup", output="found")
    await _invoke(bus, FakeRunStream(tool_views=(call,)))
    assert "delivery.created" not in bus.kinds("r1")
