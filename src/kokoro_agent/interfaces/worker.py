"""Worker 入口：消费请求流，每个 run.request 并发跑一轮并把事件发布回事件流。"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from dotenv import load_dotenv
from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import ValidationError

from kokoro_agent.application.event_stream import StreamProtocol
from kokoro_agent.application.run_agent import run_agent
from kokoro_agent.domain.agent_event import AgentEvent
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.model import make_chat_model
from kokoro_agent.infrastructure.control import wait_for_cancel
from kokoro_agent.infrastructure.json_types import JsonObject
from kokoro_agent.infrastructure.transport import make_stream
from kokoro_agent.infrastructure.subagent import RuntimeSubagentRegistry

LOGGER = logging.getLogger(__name__)

REQUESTS_STREAM = "kokoro:runs:requests"
_CHECKPOINTER: BaseCheckpointSaver[str] = InMemorySaver()

MAX_CONCURRENT_RUNS = 8
MAX_PROCESSED_RUN_IDS = 4096


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


def _run_id_of(raw: JsonObject) -> str | None:
    run_id = raw.get("run_id")
    return run_id if isinstance(run_id, str) else None


def _parse_request(raw: JsonObject) -> RunRequest | None:
    try:
        return RunRequest.model_validate(raw)
    except ValidationError as error:
        run_id = _run_id_of(raw)
        if run_id is not None:
            LOGGER.warning("invalid run.request for run_id=%s: %s", run_id, error)
        else:
            LOGGER.warning("dropping malformed run.request without usable run_id: %s", error)
        return None


class ProcessedRunIds:
    """有界、按插入序的已处理 run id 集合：超过上限即逐出最旧项，限制长驻 worker 的内存增长。"""

    def __init__(self, max_size: int = MAX_PROCESSED_RUN_IDS) -> None:
        self._ids: dict[str, None] = {}
        self._max_size = max_size

    def __contains__(self, run_id: str) -> bool:
        return run_id in self._ids

    def __len__(self) -> int:
        return len(self._ids)

    def add(self, run_id: str) -> None:
        self._ids[run_id] = None
        if len(self._ids) > self._max_size:
            oldest = next(iter(self._ids))
            del self._ids[oldest]


async def _publish_run_failed(
    bus: StreamProtocol, run_id: str, error_kind: str, message: str
) -> None:
    failed = AgentEvent(
        kind="run.failed",
        run_id=run_id,
        seq=1,
        payload={"error_kind": error_kind, "message": message},
    )
    await bus.publish(events_stream(run_id), failed.model_dump())


async def _run_request(
    bus: StreamProtocol,
    request: RunRequest,
    model: BaseChatModel | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> None:
    stream = events_stream(request.run_id)
    try:
        resolved_model = model if model is not None else make_chat_model(request.execution_style)
    except Exception as error:  # noqa: BLE001
        LOGGER.exception("model resolution failed for run_id=%s", request.run_id)
        await _publish_run_failed(bus, request.run_id, type(error).__name__, str(error))
        return

    # 每个 run 新建 registry：运行时自定义子代理在每次工具调用时完整提供，
    # 生命周期为 run 级，不跨 run/session 共享。
    runtime_registry = RuntimeSubagentRegistry()
    async for event in run_agent(
        request,
        resolved_model,
        control_bus=bus,
        runtime_registry=runtime_registry,
        checkpointer=checkpointer,
    ):
        await bus.publish(stream, event.model_dump())


async def _admit_request(
    bus: StreamProtocol, raw: JsonObject, processed: ProcessedRunIds
) -> RunRequest | None:
    """校验并去重一条 run.request：非法发布 run.failed，已处理跳过，否则登记并返回待执行请求。"""
    request = _parse_request(raw)
    if request is None:
        run_id = _run_id_of(raw)
        if run_id is not None:
            await _publish_run_failed(bus, run_id, "ValidationError", "invalid run.request")
        return None
    if request.run_id in processed:
        LOGGER.debug("skipping already-processed run_id=%s", request.run_id)
        return None
    processed.add(request.run_id)
    return request


async def _handle_request(
    bus: StreamProtocol,
    raw: JsonObject,
    processed: ProcessedRunIds,
    model: BaseChatModel | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> None:
    request = await _admit_request(bus, raw, processed)
    if request is not None:
        await _run_request(bus, request, model, checkpointer)


async def run_once(
    bus: StreamProtocol, processed: ProcessedRunIds, model: BaseChatModel | None = None
) -> None:
    for item in await bus.read_all(REQUESTS_STREAM):
        await _handle_request(
            bus,
            item.event,
            processed,
            model,
            _CHECKPOINTER,
        )


async def _run_guarded(
    bus: StreamProtocol, request: RunRequest, sem: asyncio.Semaphore
) -> None:
    async with sem:
        try:
            await _run_request(
                bus,
                request,
                checkpointer=_CHECKPOINTER,
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception("run task crashed; worker loop continues")


async def _cancel_on_signal(
    bus: StreamProtocol, run_id: str, run_task: asyncio.Task[None]
) -> None:
    await wait_for_cancel(bus, run_id)
    run_task.cancel()


async def _run_with_cancel(
    bus: StreamProtocol, request: RunRequest, sem: asyncio.Semaphore
) -> None:
    run_task = asyncio.create_task(_run_guarded(bus, request, sem))
    canceller = asyncio.create_task(_cancel_on_signal(bus, request.run_id, run_task))
    try:
        await run_task
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await bus.publish(
                events_stream(request.run_id),
                AgentEvent(
                    kind="run.completed",
                    run_id=request.run_id,
                    seq=0,
                    payload={"status": "cancelled"},
                ).model_dump(),
            )
    finally:
        canceller.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await canceller


async def serve(bus: StreamProtocol) -> None:
    processed = ProcessedRunIds()
    sem = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
    tasks: set[asyncio.Task[None]] = set()
    async for item in bus.subscribe(REQUESTS_STREAM):
        request = await _admit_request(bus, item.event, processed)
        if request is None:
            continue
        task = asyncio.create_task(_run_with_cancel(bus, request, sem))
        tasks.add(task)
        task.add_done_callback(tasks.discard)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    bus = make_stream()
    LOGGER.info("kokoro-agent worker starting on stream %s", REQUESTS_STREAM)
    asyncio.run(serve(bus))


if __name__ == "__main__":
    main()
