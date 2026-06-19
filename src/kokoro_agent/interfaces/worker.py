from __future__ import annotations

import asyncio
import contextlib
import logging

from dotenv import load_dotenv
from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import ValidationError

from kokoro_agent.application.run_agent import run_agent
from kokoro_agent.domain.agent_event import AgentEvent
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.model import make_chat_model
from kokoro_agent.infrastructure.control import wait_for_cancel
from kokoro_agent.infrastructure.json_types import JsonObject
from kokoro_agent.infrastructure.transport import StreamPort, make_stream_port
from kokoro_agent.infrastructure.subagent_registry import RuntimeSubagentRegistry

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
    """Bounded, insertion-ordered set of handled run ids; evicts the oldest once
    past the cap so a long-lived worker never grows without bound."""

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
    port: StreamPort, run_id: str, error_kind: str, message: str
) -> None:
    failed = AgentEvent(
        kind="run.failed",
        run_id=run_id,
        seq=1,
        payload={"error_kind": error_kind, "message": message},
    )
    await port.publish(events_stream(run_id), failed.model_dump())


async def _run_request(
    port: StreamPort,
    request: RunRequest,
    model: BaseChatModel | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> None:
    stream = events_stream(request.run_id)
    try:
        resolved_model = model if model is not None else make_chat_model(request.execution_style)
    except Exception as error:  # noqa: BLE001
        LOGGER.exception("model resolution failed for run_id=%s", request.run_id)
        await _publish_run_failed(port, request.run_id, type(error).__name__, str(error))
        return

    # Fresh registry per run: runtime-custom subagents are supplied in full on every
    # tool call, so their lifecycle is run-scoped — never shared across runs/sessions.
    runtime_registry = RuntimeSubagentRegistry()
    async for event in run_agent(
        request,
        resolved_model,
        control_port=port,
        runtime_registry=runtime_registry,
        checkpointer=checkpointer,
    ):
        await port.publish(stream, event.model_dump())


async def _handle_request(
    port: StreamPort,
    raw: JsonObject,
    processed: ProcessedRunIds,
    model: BaseChatModel | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> None:
    request = _parse_request(raw)
    if request is None:
        run_id = _run_id_of(raw)
        if run_id is not None:
            await _publish_run_failed(port, run_id, "ValidationError", "invalid run.request")
        return
    if request.run_id in processed:
        LOGGER.debug("skipping already-processed run_id=%s", request.run_id)
        return
    processed.add(request.run_id)
    await _run_request(port, request, model, checkpointer)


async def run_once(
    port: StreamPort, processed: ProcessedRunIds, model: BaseChatModel | None = None
) -> None:
    for item in await port.read_all(REQUESTS_STREAM):
        await _handle_request(
            port,
            item.event,
            processed,
            model,
            _CHECKPOINTER,
        )


async def _run_guarded(
    port: StreamPort, request: RunRequest, sem: asyncio.Semaphore
) -> None:
    async with sem:
        try:
            await _run_request(
                port,
                request,
                checkpointer=_CHECKPOINTER,
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception("run task crashed; worker loop continues")


async def _cancel_on_signal(
    port: StreamPort, run_id: str, run_task: asyncio.Task[None]
) -> None:
    await wait_for_cancel(port, run_id)
    run_task.cancel()


async def _run_with_cancel(
    port: StreamPort, request: RunRequest, sem: asyncio.Semaphore
) -> None:
    run_task = asyncio.create_task(_run_guarded(port, request, sem))
    canceller = asyncio.create_task(_cancel_on_signal(port, request.run_id, run_task))
    try:
        await run_task
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await port.publish(
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


async def serve(port: StreamPort) -> None:
    processed = ProcessedRunIds()
    sem = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
    tasks: set[asyncio.Task[None]] = set()
    async for item in port.subscribe(REQUESTS_STREAM):
        request = _parse_request(item.event)
        if request is None:
            run_id = _run_id_of(item.event)
            if run_id is not None:
                await _publish_run_failed(port, run_id, "ValidationError", "invalid run.request")
            continue
        if request.run_id in processed:
            continue
        processed.add(request.run_id)
        task = asyncio.create_task(_run_with_cancel(port, request, sem))
        tasks.add(task)
        task.add_done_callback(tasks.discard)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    load_dotenv()
    port = make_stream_port()
    LOGGER.info("kokoro-agent worker starting on stream %s", REQUESTS_STREAM)
    asyncio.run(serve(port))


if __name__ == "__main__":
    main()
