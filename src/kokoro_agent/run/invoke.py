"""一次 graph invoke：astream_events→project→publish，遇 interrupt 暂停退出否则终态收口。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Protocol, TypeGuard, runtime_checkable

from langchain_core.runnables.schema import StreamEvent
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.events.agent_event import AgentEvent
from kokoro_agent.events.attribution import SubagentAttribution
from kokoro_agent.events.project import project


@runtime_checkable
class InvokableAgent(Protocol):
    """编译后 langgraph 图的窄契约：仅 invoke 路径需要的两个方法。"""

    def astream_events(
        self, payload: object, *, version: str, config: dict[str, JsonValue]
    ) -> AsyncIterator[StreamEvent]: ...

    async def aget_state(self, config: dict[str, JsonValue]) -> object: ...


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


async def invoke_once(
    bus: StreamProtocol,
    agent: InvokableAgent,
    run_id: str,
    conversation_id: str,
    payload: object,
) -> None:
    stream = events_stream(run_id)
    config: dict[str, JsonValue] = {"configurable": {"thread_id": conversation_id}}
    attribution = SubagentAttribution()
    await _publish(bus, stream, run_id, "run.started", {})
    try:
        async for event in agent.astream_events(payload, version="v2", config=config):
            for ev in project(event, attribution):
                await bus.publish(stream, ev.model_dump())
        snapshot = await agent.aget_state(config)
        approval = _awaiting_approval_payload(snapshot)
        if approval is not None:
            # interrupt 暂停退出：发审批信号后返回，不发 run.completed（run 留在 checkpoint）。
            await _publish(bus, stream, run_id, "tool.awaiting_approval", approval)
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


def _awaiting_approval_payload(snapshot: object) -> dict[str, JsonValue] | None:
    value = _first_interrupt_value(snapshot)
    if value is None:
        return None
    requests: object = value.get("action_requests")
    if not _is_object_list(requests) or not requests:
        return None
    request = requests[0]
    if not _is_object_mapping(request):
        return None
    return {
        "name": _str(request.get("name")),
        "args": _json_mapping(request.get("args")),
        "description": _str(request.get("description")),
        "allowed_decisions": _allowed_decisions(value),
    }


def _allowed_decisions(value: Mapping[object, object]) -> list[JsonValue]:
    configs: object = value.get("review_configs")
    if not _is_object_list(configs) or not configs:
        return []
    config = configs[0]
    if not _is_object_mapping(config):
        return []
    decisions: object = config.get("allowed_decisions")
    if not _is_object_list(decisions):
        return []
    return [d for d in decisions if isinstance(d, str)]


def _str(value: object) -> str:
    return value if isinstance(value, str) else ""


def _json_mapping(value: object) -> dict[str, JsonValue]:
    # interrupt action_requests[].args 是工具入参字典，逐键收窄为 JSON 标量。
    if not _is_object_mapping(value):
        return {}
    args: dict[str, JsonValue] = {}
    for key, item in value.items():
        if isinstance(key, str) and (item is None or isinstance(item, (str, int, float, bool))):
            args[key] = item
    return args
