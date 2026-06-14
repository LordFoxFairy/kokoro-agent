from __future__ import annotations

import asyncio
import contextlib
import logging

from dotenv import load_dotenv
from langchain_core.language_models import BaseChatModel
from pydantic import ValidationError

from kokoro_agent.domain.agent_event import AgentEvent
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.chat_model import make_chat_model
from kokoro_agent.infrastructure.control import wait_for_cancel
from kokoro_agent.infrastructure.stream_port import StreamPort, make_stream_port
from kokoro_agent.application.run_agent import run_agent

LOGGER = logging.getLogger(__name__)

REQUESTS_STREAM = "kokoro:runs:requests"


def events_stream(run_id: str) -> str:
    return f"kokoro:run:{run_id}:events"


# 上限并发 run 数：每个 run 一个 asyncio task，所以一个待批(HITL)或慢 run 不再独占 worker、
# 冻结其它会话。上限防失控的并行 LLM 调用打爆网关/内存。
MAX_CONCURRENT_RUNS = 8


def _run_id_of(raw: dict[str, object]) -> str | None:
    rid = raw.get("run_id")
    return rid if isinstance(rid, str) else None


async def _run_request(
    port: StreamPort,
    raw: dict[str, object],
    model: BaseChatModel | None = None,
) -> None:
    """Validate one run.request and stream its events (no dedup — callers own that)."""
    try:
        request = RunRequest.model_validate(raw)
    except ValidationError as error:
        LOGGER.warning("dropping malformed run.request: %s", error)
        return

    stream = events_stream(request.run_id)
    try:
        resolved_model = model if model is not None else make_chat_model(request.execution_style)
    except Exception as error:  # noqa: BLE001 — boundary: bad model config -> terminal run.failed, loop stays alive
        LOGGER.exception("model resolution failed for run_id=%s", request.run_id)
        failed = AgentEvent(
            kind="run.failed",
            run_id=request.run_id,
            seq=1,
            payload={"error_kind": type(error).__name__, "message": str(error)},
        )
        await port.publish(stream, failed.model_dump())
        return
    # 同一 StreamPort 兼作 control 通道：被门控工具经它读 kokoro:run:<id>:control 等审批。
    async for event in run_agent(request, resolved_model, control_port=port):
        await port.publish(stream, event.model_dump())


async def _handle_request(
    port: StreamPort,
    raw: dict[str, object],
    processed: set[str],
    model: BaseChatModel | None = None,
) -> None:
    """Dedup-by-run_id then run (sequential helper used by run_once)."""
    rid = _run_id_of(raw)
    if rid is not None:
        if rid in processed:
            LOGGER.debug("skipping already-processed run_id=%s", rid)
            return
        processed.add(rid)
    await _run_request(port, raw, model)


async def run_once(
    port: StreamPort, processed: set[str], model: BaseChatModel | None = None
) -> None:
    """Drain currently-pending run requests once and emit their event streams.

    Idempotent per ``run_id`` via the caller-owned ``processed`` set, so a
    duplicate request for a run that already ran is ignored.
    """
    for item in await port.read_all(REQUESTS_STREAM):
        await _handle_request(port, item.event, processed, model)


async def _run_guarded(
    port: StreamPort, raw: dict[str, object], sem: asyncio.Semaphore
) -> None:
    # 单个 run task：信号量限并发；异常吞在此处，绝不让一个 run 崩溃带垮 serve 主循环。
    async with sem:
        try:
            await _run_request(port, raw)
        except Exception:  # noqa: BLE001 — boundary: isolate a crashing run; the loop must survive
            LOGGER.exception("run task crashed; worker loop continues")


async def _cancel_on_signal(
    port: StreamPort, run_id: str, run_task: asyncio.Task[None]
) -> None:
    # 守候该 run 的 control 流：一旦收到 cancel 就取消 run task（连同其内阻塞的待批门）。
    await wait_for_cancel(port, run_id)
    run_task.cancel()


async def _run_with_cancel(
    port: StreamPort, raw: dict[str, object], sem: asyncio.Semaphore
) -> None:
    """跑一个 run，并挂一个 cancel 守候：用户放弃时取消整个 run（解阻塞所有待批门），补发 cancelled 终态。"""
    run_task = asyncio.create_task(_run_guarded(port, raw, sem))
    run_id = _run_id_of(raw)
    if run_id is None:
        await run_task
        return
    canceller = asyncio.create_task(_cancel_on_signal(port, run_id, run_task))
    try:
        await run_task
    except asyncio.CancelledError:
        # 被 cancel 守候取消（非 serve 关停）：补发 cancelled 终态收口，让 relay/web 收束。
        with contextlib.suppress(Exception):
            await port.publish(
                events_stream(run_id),
                AgentEvent(
                    kind="run.completed",
                    run_id=run_id,
                    seq=0,
                    payload={"status": "cancelled"},
                ).model_dump(),
            )
    finally:
        canceller.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await canceller


async def serve(port: StreamPort) -> None:
    # 每个 run.request 起一个独立 task（不 await），所以待批/慢 run 不阻塞后续 run。
    # 去重同步发生在 spawn 前，杜绝同 run_id 并发双跑。
    processed: set[str] = set()
    sem = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
    tasks: set[asyncio.Task[None]] = set()
    async for item in port.subscribe(REQUESTS_STREAM):
        rid = _run_id_of(item.event)
        if rid is not None:
            if rid in processed:
                continue
            processed.add(rid)
        task = asyncio.create_task(_run_with_cancel(port, item.event, sem))
        tasks.add(task)
        task.add_done_callback(tasks.discard)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    # 本地开发把 .env（gitignored）载入环境供 make_chat_model 读取；生产无 .env 时空操作。
    load_dotenv()
    port = make_stream_port()
    LOGGER.info("kokoro-agent worker starting on stream %s", REQUESTS_STREAM)
    asyncio.run(serve(port))


if __name__ == "__main__":
    main()
