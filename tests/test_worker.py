from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime
from typing import TypeGuard, TypedDict, cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from langchain_core.language_models import BaseChatModel
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from kokoro_agent.domain.agent_event import AgentEvent
from kokoro_agent.infrastructure.local_fake_model import make_local_fake_chat_model
from kokoro_agent.infrastructure.chat_model import LOCAL_FAKE_MODEL_FLAG, make_chat_model
from kokoro_agent.infrastructure.stream_port import MemoryStreamPort
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.interfaces.worker import (
    REQUESTS_STREAM,
    events_stream,
    run_once,
    serve,
)


class _TextPayload(TypedDict):
    text: str


def _is_text_payload(value: object) -> TypeGuard[_TextPayload]:
    if not isinstance(value, Mapping):
        return False

    text = cast("Mapping[str, object]", value).get("text")
    return isinstance(text, str)


def _payload_text(value: object) -> str:
    assert _is_text_payload(value)
    return value["text"]


def _payload_field(event: dict[str, object], key: str) -> str:
    payload = event["payload"]
    assert isinstance(payload, Mapping)
    value = cast("Mapping[str, object]", payload).get(key)
    assert isinstance(value, str)
    return value


def _fake_model(*replies: str) -> BaseChatModel:
    return GenericFakeChatModel(
        messages=iter([AIMessage(content=text) for text in replies])
    )


def _request(run_id: str = "run_01") -> dict[str, object]:
    return {
        "kind": "run.request",
        "run_id": run_id,
        "session_id": "ses_01",
        "conversation_id": "conv_01",
        "input": "hello kokoro",
    }


async def test_run_once_streams_with_injected_model() -> None:
    # The worker drives the real DeepAgents loop; the scripted local-fake model
    # makes that hermetic. Assert the WORKER's plumbing (request → run_agent →
    # ordered events on the run's stream), not specific model wording.
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request())

    processed: set[str] = set()
    await run_once(port, processed, make_local_fake_chat_model())

    items = await port.read_all(events_stream("run_01"))
    kinds = [item.event["kind"] for item in items]
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    # the deep-agent activity surfaces: a CC-style todo and a final answer.
    assert "todo.updated" in kinds
    assert "text.completed" in kinds

    # seq is monotonic from 1, no gaps.
    seqs = [item.event["seq"] for item in items]
    assert seqs == list(range(1, len(seqs) + 1))


async def test_run_once_executes_the_built_in_now_tool() -> None:
    # X1 最小闭环：注册的内置工具被真实 DeepAgents 循环执行，
    # 并以通用 tool.invoked/tool.returned 事件浮出（结果是可解析的本地时间）。
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request("run_tool"))

    processed: set[str] = set()
    await run_once(port, processed, make_local_fake_chat_model())

    items = await port.read_all(events_stream("run_tool"))
    kinds = [item.event["kind"] for item in items]
    assert "tool.invoked" in kinds
    invoked = next(item.event for item in items if item.event["kind"] == "tool.invoked")
    returned = next(item.event for item in items if item.event["kind"] == "tool.returned")
    assert _payload_field(invoked, "name") == "now"
    assert _payload_field(returned, "name") == "now"
    assert datetime.fromisoformat(_payload_field(returned, "result")).tzinfo is not None


async def test_run_once_is_idempotent_per_run_id() -> None:
    port = MemoryStreamPort()
    model = make_local_fake_chat_model()
    processed: set[str] = set()

    await port.publish(REQUESTS_STREAM, _request())
    await run_once(port, processed, model)
    await port.publish(REQUESTS_STREAM, _request())  # duplicate run_id
    await run_once(port, processed, model)

    items = await port.read_all(events_stream("run_01"))
    kinds = [item.event["kind"] for item in items]
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert kinds.count("run.started") == 1  # processed exactly once


async def test_run_once_rejects_malformed_request() -> None:
    port = MemoryStreamPort()
    # missing required "input"
    await port.publish(
        REQUESTS_STREAM,
        {
            "kind": "run.request",
            "run_id": "run_bad",
            "session_id": "ses_01",
            "conversation_id": "conv_01",
        },
    )

    processed: set[str] = set()
    await run_once(port, processed, _fake_model("unused"))

    # malformed request produces no events and does not crash the loop
    assert await port.read_all(events_stream("run_bad")) == []


@pytest.mark.asyncio
async def test_run_once_streams_with_local_fake_model(
    monkeypatch: MonkeyPatch,
) -> None:
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request("run_local_fake"))

    monkeypatch.setenv(LOCAL_FAKE_MODEL_FLAG, "1")
    model = make_chat_model()

    processed: set[str] = set()
    await run_once(port, processed, model)

    items = await port.read_all(events_stream("run_local_fake"))
    kinds = [item.event["kind"] for item in items]
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert "todo.updated" in kinds  # CC-style planning surfaces
    assert "text.completed" in kinds

    completed = next(item for item in items if item.event["kind"] == "text.completed")
    assert "本地预览" in _payload_text(completed.event["payload"])


@pytest.mark.asyncio
async def test_model_resolution_failure_emits_run_failed_and_loop_survives(
    monkeypatch: MonkeyPatch,
) -> None:
    port = MemoryStreamPort()
    processed: set[str] = set()

    def broken_make_chat_model(execution_style: str = "fast") -> BaseChatModel:
        raise ValueError("Invalid KOKORO_MODEL spec: 'plainstring'")

    monkeypatch.setattr(
        "kokoro_agent.interfaces.worker.make_chat_model", broken_make_chat_model
    )

    # 坏模型配置必须落终态 run.failed，而不是崩掉 worker 留下永远悬挂的 run。
    await port.publish(REQUESTS_STREAM, _request("run_broken"))
    await run_once(port, processed, None)

    items = await port.read_all(events_stream("run_broken"))
    assert [item.event["kind"] for item in items] == ["run.failed"]
    payload = items[-1].event["payload"]
    assert isinstance(payload, Mapping)
    assert payload["error_kind"] == "ValueError"

    # 循环存活：下一条请求（注入健康模型）照常处理到 run.completed。
    await port.publish(REQUESTS_STREAM, _request("run_after"))
    await run_once(port, processed, make_local_fake_chat_model())
    after = await port.read_all(events_stream("run_after"))
    assert [item.event["kind"] for item in after][-1] == "run.completed"


@pytest.mark.asyncio
async def test_run_once_resolves_model_from_request_execution_style(
    monkeypatch: MonkeyPatch,
) -> None:
    port = MemoryStreamPort()
    processed: set[str] = set()
    seen_styles: list[str] = []
    await port.publish(
        REQUESTS_STREAM,
        {
            **_request(),
            "execution_style": "thinking",
        },
    )

    def fake_make_chat_model(execution_style: str = "fast") -> BaseChatModel:
        seen_styles.append(execution_style)
        return make_local_fake_chat_model()

    async def fake_run_agent(
        request: object, model: BaseChatModel, control_port: object = None
    ):
        yield AgentEvent(
            kind="run.completed",
            run_id="run_01",
            seq=1,
            payload={"status": "completed"},
        )

    monkeypatch.setattr("kokoro_agent.interfaces.worker.make_chat_model", fake_make_chat_model)
    monkeypatch.setattr("kokoro_agent.interfaces.worker.run_agent", fake_run_agent)

    await run_once(port, processed, None)

    assert seen_styles == ["thinking"]


@pytest.mark.asyncio
async def test_serve_runs_concurrently_a_blocked_run_does_not_freeze_others(
    monkeypatch: MonkeyPatch,
) -> None:
    # 一个待批(HITL)/慢 run 阻塞时,_serve 仍能并发跑完其它 run——修「awaiting 冻结全局」。
    port = MemoryStreamPort()
    release = asyncio.Event()

    async def fake_run_agent(
        request: RunRequest, model: BaseChatModel, control_port: object = None
    ):
        if request.run_id == "run_block":
            await release.wait()
        yield AgentEvent(kind="run.started", run_id=request.run_id, seq=1, payload={})
        yield AgentEvent(
            kind="run.completed",
            run_id=request.run_id,
            seq=2,
            payload={"status": "completed"},
        )

    def fake_make_chat_model(execution_style: str = "fast") -> BaseChatModel:
        return make_local_fake_chat_model()

    monkeypatch.setattr(
        "kokoro_agent.interfaces.worker.make_chat_model", fake_make_chat_model
    )
    monkeypatch.setattr("kokoro_agent.interfaces.worker.run_agent", fake_run_agent)

    async def completed(run_id: str) -> bool:
        items = await port.read_all(events_stream(run_id))
        return any(i.event.get("kind") == "run.completed" for i in items)

    serve_task = asyncio.create_task(serve(port))
    try:
        await port.publish(REQUESTS_STREAM, _request("run_block"))
        await port.publish(REQUESTS_STREAM, _request("run_fast"))
        async with asyncio.timeout(2):
            while not await completed("run_fast"):
                await asyncio.sleep(0.01)
        # run_block 仍卡着,但 run_fast 已跑完 → worker 不再串行冻结。
        assert not await completed("run_block")
        release.set()
        async with asyncio.timeout(2):
            while not await completed("run_block"):
                await asyncio.sleep(0.01)
    finally:
        serve_task.cancel()
