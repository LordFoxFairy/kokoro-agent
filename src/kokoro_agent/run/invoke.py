"""一次 graph invoke：astream_events→project→publish，遇 interrupt 暂停退出否则终态收口。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeGuard

from langchain_core.messages import BaseMessage
from pydantic import JsonValue

from kokoro_agent.application.protocols.agent import InvokableAgent
from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.events.agent_event import AgentEvent
from kokoro_agent.events.attribution import SubagentAttribution
from kokoro_agent.events.awaiting import awaiting_approval_events
from kokoro_agent.events.project import project

__all__ = ["InvokableAgent", "events_stream", "invoke_once"]


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


async def invoke_once(
    bus: StreamProtocol,
    agent: InvokableAgent,
    run_id: str,
    conversation_id: str,
    payload: object,
    interrupt_on_names: frozenset[str] = frozenset(),
) -> None:
    stream = events_stream(run_id)
    config: dict[str, JsonValue] = {"configurable": {"thread_id": conversation_id}}
    attribution = SubagentAttribution()
    await _publish(bus, stream, run_id, "run.started", {})
    try:
        # interrupt 前最后一次 on_chat_model_* 的 run_id 即 awaiting 的 segment_id。
        segment_id = ""
        async for event in agent.astream_events(payload, version="v2", config=config):
            if str(event["event"]).startswith("on_chat_model_"):
                segment_id = event["run_id"]
            for ev in project(event, attribution, run_id):
                await bus.publish(stream, ev.model_dump())
        snapshot = await agent.aget_state(config)
        if _first_interrupt_value(snapshot) is not None:
            # interrupt 暂停退出：逐 pending tool_call 发审批信号后返回，不发 run.completed。
            for ev in _awaiting_events(snapshot, interrupt_on_names, segment_id, run_id):
                await bus.publish(stream, ev.model_dump())
            return
        await _publish(bus, stream, run_id, "run.completed", {"status": "completed"})
    except Exception as error:  # noqa: BLE001 — 顶层兜底：任何异常统一收口为 run.failed
        await _publish(
            bus,
            stream,
            run_id,
            "run.failed",
            {"error_kind": type(error).__name__, "message": str(error)},
        )


async def _publish(
    bus: StreamProtocol, stream: str, run_id: str, kind: str, payload: dict[str, JsonValue]
) -> None:
    event = AgentEvent.model_validate({"kind": kind, "run_id": run_id, "payload": payload})
    await bus.publish(stream, event.model_dump())


def _is_object_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _first_interrupt_value(snapshot: object) -> Mapping[object, object] | None:
    # StateSnapshot.tasks[].interrupts[].value 松类型：经 object 边界 + isinstance 收窄。
    tasks: object = getattr(snapshot, "tasks", None)
    if not _is_object_sequence(tasks):
        return None
    for task in tasks:
        interrupts: object = getattr(task, "interrupts", None)
        if not _is_object_sequence(interrupts) or not interrupts:
            continue
        value: object = getattr(interrupts[0], "value", None)
        if _is_object_mapping(value):
            return value
    return None


def _is_object_sequence(value: object) -> TypeGuard[tuple[object, ...] | list[object]]:
    # list/tuple 收口到 Sequence[object]，避免迭代得到 Unknown 元素。
    return isinstance(value, (list, tuple))


def _awaiting_events(
    snapshot: object, interrupt_on_names: frozenset[str], segment_id: str, run_id: str
) -> list[AgentEvent]:
    value = _first_interrupt_value(snapshot)
    if value is None:
        return []
    requests: object = value.get("action_requests")
    action_requests = requests if _is_object_list(requests) else []
    return awaiting_approval_events(
        _snapshot_messages(snapshot),
        action_requests,
        interrupt_on_names,
        segment_id=segment_id,
        run_id=run_id,
    )


def _snapshot_messages(snapshot: object) -> list[BaseMessage]:
    # StateSnapshot.values 松类型 dict[str, Any]：经 object 边界收窄 messages 为 BaseMessage 序列。
    values: object = getattr(snapshot, "values", None)
    if not _is_object_mapping(values):
        return []
    raw: object = values.get("messages")
    if not _is_object_list(raw):
        return []
    return [m for m in raw if isinstance(m, BaseMessage)]
