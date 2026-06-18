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
from kokoro_agent.infrastructure.chat_model import make_chat_model
from kokoro_agent.infrastructure.control import wait_for_cancel
from kokoro_agent.infrastructure.stream_port import JsonObject, StreamPort, make_stream_port
from kokoro_agent.infrastructure.subagent_registry import RuntimeSubagentRegistry

LOGGER = logging.getLogger(__name__)

REQUESTS_STREAM = "kokoro:runs:requests"
_CHECKPOINTER: BaseCheckpointSaver[str] = InMemorySaver()
_RUNTIME_SUBAGENT_REGISTRY = RuntimeSubagentRegistry()

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


def has_processed(processed: dict[str, None], run_id: str) -> bool:
    return run_id in processed


def mark_processed(processed: dict[str, None], run_id: str) -> None:
    processed[run_id] = None
    if len(processed) > MAX_PROCESSED_RUN_IDS:
        oldest = next(iter(processed))
        del processed[oldest]


async def _run_request(
    port: StreamPort,
    request: RunRequest,
    model: BaseChatModel | None = None,
    runtime_registry: RuntimeSubagentRegistry | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> None:
    stream = events_stream(request.run_id)
    try:
        resolved_model = model if model is not None else make_chat_model(request.execution_style)
    except Exception as error:  # noqa: BLE001
        LOGGER.exception("model resolution failed for run_id=%s", request.run_id)
        failed = AgentEvent(
            kind="run.failed",
            run_id=request.run_id,
            seq=1,
            payload={"error_kind": type(error).__name__, "message": str(error)},
        )
        await port.publish(stream, failed.model_dump())
        return

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
    processed: dict[str, None],
    model: BaseChatModel | None = None,
    runtime_registry: RuntimeSubagentRegistry | None = None,
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> None:
    request = _parse_request(raw)
    if request is None:
        run_id = _run_id_of(raw)
        if run_id is not None:
            failed = AgentEvent(
                kind="run.failed",
                run_id=run_id,
                seq=1,
                payload={"error_kind": "ValidationError", "message": "invalid run.request"},
            )
            await port.publish(events_stream(run_id), failed.model_dump())
        return
    if has_processed(processed, request.run_id):
        LOGGER.debug("skipping already-processed run_id=%s", request.run_id)
        return
    mark_processed(processed, request.run_id)
    await _run_request(port, request, model, runtime_registry, checkpointer)


async def run_once(
    port: StreamPort, processed: dict[str, None], model: BaseChatModel | None = None
) -> None:
    for item in await port.read_all(REQUESTS_STREAM):
        await _handle_request(
            port,
            item.event,
            processed,
            model,
            _RUNTIME_SUBAGENT_REGISTRY,
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
                runtime_registry=_RUNTIME_SUBAGENT_REGISTRY,
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
    processed: dict[str, None] = {}
    sem = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
    tasks: set[asyncio.Task[None]] = set()
    async for item in port.subscribe(REQUESTS_STREAM):
        request = _parse_request(item.event)
        if request is None:
            run_id = _run_id_of(item.event)
            if run_id is not None:
                failed = AgentEvent(
                    kind="run.failed",
                    run_id=run_id,
                    seq=1,
                    payload={"error_kind": "ValidationError", "message": "invalid run.request"},
                )
                await port.publish(events_stream(run_id), failed.model_dump())
            continue
        if has_processed(processed, request.run_id):
            continue
        mark_processed(processed, request.run_id)
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
