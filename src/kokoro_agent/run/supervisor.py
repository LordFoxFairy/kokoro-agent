"""调度：订阅请求流，按 kind 派发 run.request/resume/cancel，含 resume 幂等护栏。"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Mapping
from typing import TypeGuard

from langchain_core.messages import HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.events.agent_event import AgentEvent
from kokoro_agent.infrastructure.observability import trace_config
from kokoro_agent.run.admission import ProcessedRunIds
from kokoro_agent.run.invoke import InvokableAgent, events_stream, invoke_once
from kokoro_agent.wire.run_request import (
    InboundMessage,
    ResumeDecision,
    RunCancel,
    RunRequest,
    RunResume,
    parse_inbound,
)

LOGGER = logging.getLogger(__name__)

REQUESTS_STREAM = "kokoro:runs:requests"
MAX_CONCURRENT_RUNS = 8

AgentBuilder = Callable[[RunRequest], InvokableAgent]


class RunSupervisor:
    """注入 agent_builder 构建 run 级 agent；进程内 map 关联 resume 到原 RunRequest。"""

    def __init__(
        self,
        agent_builder: AgentBuilder,
        checkpointer: BaseCheckpointSaver[str] | None = None,
        max_concurrent: int = MAX_CONCURRENT_RUNS,
    ) -> None:
        self._build = agent_builder
        # R1 dev 占位：与 InMemorySaver 同 dev-only，真·无状态 wire 留 R-approval。
        self._checkpointer = checkpointer if checkpointer is not None else InMemorySaver()
        self._sem = asyncio.Semaphore(max_concurrent)
        self._processed = ProcessedRunIds()
        # run_id→原 RunRequest：resume 据此取 conversation_id + 重建 agent（R1 占位，内存增长见 report）。
        self._runs: dict[str, RunRequest] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        # 已发终态的 run_id 集合：防止 cancel 在自然完成后重复补发 cancelled。
        self._terminal: set[str] = set()

    @property
    def tasks(self) -> Mapping[str, asyncio.Task[None]]:
        return self._tasks

    async def serve(self, bus: StreamProtocol) -> None:
        async for item in bus.subscribe(REQUESTS_STREAM):
            msg = parse_inbound(item.event)
            if msg is None:
                LOGGER.warning("dropping unparseable inbound on %s", REQUESTS_STREAM)
                continue
            await self.dispatch(bus, msg)

    async def dispatch(self, bus: StreamProtocol, msg: InboundMessage) -> None:
        if isinstance(msg, RunRequest):
            await self._on_request(bus, msg)
        elif isinstance(msg, RunResume):
            await self._on_resume(bus, msg)
        else:
            await self._on_cancel(bus, msg)

    async def _on_request(self, bus: StreamProtocol, request: RunRequest) -> None:
        if request.run_id in self._processed:
            LOGGER.debug("skipping already-processed run_id=%s", request.run_id)
            return
        self._processed.add(request.run_id)
        self._runs[request.run_id] = request
        payload = {"messages": [HumanMessage(content=request.input)]}
        self._spawn(bus, request, request.run_id, request.conversation_id, payload)

    async def _on_resume(self, bus: StreamProtocol, msg: RunResume) -> None:
        request = self._runs.get(msg.run_id)
        if request is None:
            LOGGER.warning("dropping resume for unknown run_id=%s", msg.run_id)
            return
        try:
            agent = self._build(request)
        except Exception as error:  # noqa: BLE001 — 构建失败收口为 run.failed
            await self._emit_failed(bus, msg.run_id, error)
            return
        config: RunnableConfig = {"configurable": {"thread_id": request.conversation_id}}
        snapshot = await agent.aget_state(config)
        # 幂等护栏（spec §9.1）：无 pending interrupt 的 resume 是重复/过期帧，丢弃不重跑。
        if not _has_pending_interrupt(snapshot):
            LOGGER.warning("dropping resume without pending interrupt for run_id=%s", msg.run_id)
            return
        command: Command[object] = Command(resume={"decisions": [_decision_dict(msg.decision)]})
        trace = trace_config(request)
        self._spawn_agent(bus, agent, msg.run_id, request.conversation_id, command, trace=trace)

    async def _on_cancel(self, bus: StreamProtocol, msg: RunCancel) -> None:
        # 已自然终态：invoke_once 返回 True 后记录，跳过补发避免双终态。
        if msg.run_id in self._terminal:
            return
        task = self._tasks.get(msg.run_id)
        if task is not None and not task.done():
            # 运行中：被 cancel 的 invoke task 不自发终态，统一由此分支补发 cancelled。
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._emit_cancelled(bus, msg.run_id)

    def _spawn(
        self,
        bus: StreamProtocol,
        request: RunRequest,
        run_id: str,
        conversation_id: str,
        payload: object,
    ) -> None:
        try:
            agent = self._build(request)
        except Exception as error:  # noqa: BLE001 — model 解析等构建失败收口为 run.failed
            self._tasks[run_id] = asyncio.create_task(self._emit_failed(bus, run_id, error))
            self._tasks[run_id].add_done_callback(lambda _t: self._tasks.pop(run_id, None))
            return
        trace = trace_config(request)
        self._spawn_agent(bus, agent, run_id, conversation_id, payload, trace=trace)

    def _spawn_agent(
        self,
        bus: StreamProtocol,
        agent: InvokableAgent,
        run_id: str,
        conversation_id: str,
        payload: object,
        trace: RunnableConfig | None = None,
    ) -> None:
        task = asyncio.create_task(
            self._guarded(bus, agent, run_id, conversation_id, payload, trace)
        )
        self._tasks[run_id] = task
        task.add_done_callback(lambda _t: self._tasks.pop(run_id, None))

    async def _guarded(
        self,
        bus: StreamProtocol,
        agent: InvokableAgent,
        run_id: str,
        conversation_id: str,
        payload: object,
        trace: RunnableConfig | None = None,
    ) -> None:
        # Semaphore 仅限活跃 invoke：暂停态不持有，故 resume 重新竞争额度。
        async with self._sem:
            emitted = await invoke_once(bus, agent, run_id, conversation_id, payload, trace=trace)
        # asyncio 单线程：invoke_once return 与此行之间无 await，原子写入，无竞态窗口。
        if emitted:
            self._terminal.add(run_id)

    async def _emit_cancelled(self, bus: StreamProtocol, run_id: str) -> None:
        # 契约无 run.cancelled kind：cancel 终态即 run.completed + status=cancelled。
        await self._publish(bus, run_id, "run.completed", {"status": "cancelled"})

    async def _emit_failed(self, bus: StreamProtocol, run_id: str, error: Exception) -> None:
        await self._publish(
            bus, run_id, "run.failed", {"error_kind": type(error).__name__, "message": str(error)}
        )

    @staticmethod
    async def _publish(
        bus: StreamProtocol, run_id: str, kind: str, payload: dict[str, JsonValue]
    ) -> None:
        event = AgentEvent.model_validate({"kind": kind, "run_id": run_id, "payload": payload})
        await bus.publish(events_stream(run_id), event.model_dump())


def _decision_dict(decision: ResumeDecision) -> dict[str, JsonValue]:
    out: dict[str, JsonValue] = {"type": decision.type}
    if decision.type == "edit" and decision.edited_action is not None:
        out["edited_action"] = decision.edited_action
    if decision.type in ("reject", "respond") and decision.message is not None:
        out["message"] = decision.message
    return out


def _is_seq(value: object) -> TypeGuard[tuple[object, ...] | list[object]]:
    return isinstance(value, (list, tuple))


def _has_pending_interrupt(snapshot: object) -> bool:
    # StateSnapshot.tasks[].interrupts 松类型：经 object 边界 + isinstance 收窄。
    tasks: object = getattr(snapshot, "tasks", None)
    if not _is_seq(tasks):
        return False
    for task in tasks:
        interrupts: object = getattr(task, "interrupts", None)
        if _is_seq(interrupts) and interrupts:
            return True
    return False
