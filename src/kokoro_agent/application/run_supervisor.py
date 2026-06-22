"""应用层：run 执行编排——单条 run 的执行、并发信号量、task 集与 cancel 接管。"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver

from kokoro_agent.application.protocols.stream import StreamProtocol
from kokoro_agent.application.request_admission import RequestAdmission
from kokoro_agent.application.run_agent import run_agent
from kokoro_agent.application.run_terminal_events import run_cancelled_event, run_failed_event
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.control import wait_for_cancel
from kokoro_agent.infrastructure.model import make_chat_model
from kokoro_agent.infrastructure.subagent import RuntimeSubagentRegistry

LOGGER = logging.getLogger(__name__)

REQUESTS_STREAM = "kokoro:runs:requests"
MAX_CONCURRENT_RUNS = 8
_CHECKPOINTER: BaseCheckpointSaver[str] = InMemorySaver()


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


async def execute_run(
    bus: StreamProtocol,
    request: RunRequest,
    model: BaseChatModel | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> None:
    """跑一轮 run 并把事件流发布回事件流；模型解析失败显式收口为 run.failed。"""
    stream = events_stream(request.run_id)
    try:
        resolved_model = model if model is not None else make_chat_model(request.execution_style)
    except Exception as error:  # noqa: BLE001
        LOGGER.exception("model resolution failed for run_id=%s", request.run_id)
        failed = run_failed_event(request.run_id, type(error).__name__, str(error))
        await bus.publish(stream, failed.model_dump())
        return

    # 每个 run 新建 registry：运行时自定义子代理生命周期为 run 级，不跨 run/session 共享。
    runtime_registry = RuntimeSubagentRegistry()
    async for event in run_agent(
        request,
        resolved_model,
        control_bus=bus,
        runtime_registry=runtime_registry,
        checkpointer=checkpointer,
    ):
        await bus.publish(stream, event.model_dump())


async def run_once(
    bus: StreamProtocol,
    admission: RequestAdmission,
    model: BaseChatModel | None = None,
) -> None:
    """一次性排空请求流并就地执行（无并发/无 cancel 编排）：供测试与单步驱动。"""
    for item in await bus.read_all(REQUESTS_STREAM):
        request = await admission.admit(bus, item.event)
        if request is not None:
            await execute_run(bus, request, model, _CHECKPOINTER)


class RunSupervisor:
    """订阅请求流，对每条准入请求并发起一轮 run，并在 control cancel 时接管取消。"""

    def __init__(self, admission: RequestAdmission) -> None:
        self._admission = admission
        self._sem = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
        self._tasks: set[asyncio.Task[None]] = set()

    async def serve(self, bus: StreamProtocol) -> None:
        async for item in bus.subscribe(REQUESTS_STREAM):
            request = await self._admission.admit(bus, item.event)
            if request is None:
                continue
            task = asyncio.create_task(self._run_with_cancel(bus, request))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _run_guarded(self, bus: StreamProtocol, request: RunRequest) -> None:
        async with self._sem:
            try:
                await execute_run(bus, request, checkpointer=_CHECKPOINTER)
            except Exception:  # noqa: BLE001
                LOGGER.exception("run task crashed; worker loop continues")

    async def _run_with_cancel(self, bus: StreamProtocol, request: RunRequest) -> None:
        run_task = asyncio.create_task(self._run_guarded(bus, request))
        canceller = asyncio.create_task(self._cancel_on_signal(bus, request.run_id, run_task))
        try:
            await run_task
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await bus.publish(
                    events_stream(request.run_id),
                    run_cancelled_event(request.run_id).model_dump(),
                )
        finally:
            canceller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await canceller

    @staticmethod
    async def _cancel_on_signal(
        bus: StreamProtocol, run_id: str, run_task: asyncio.Task[None]
    ) -> None:
        await wait_for_cancel(bus, run_id)
        run_task.cancel()
