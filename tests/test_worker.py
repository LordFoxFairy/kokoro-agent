from __future__ import annotations

from collections.abc import Mapping
from typing import TypeGuard, TypedDict, cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from langchain_core.language_models import BaseChatModel
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from kokoro_agent.domain.events import AgentEvent
from kokoro_agent.infrastructure.local_fake_model import make_local_fake_chat_model
from kokoro_agent.infrastructure.chat_model import LOCAL_FAKE_MODEL_FLAG, make_chat_model
from kokoro_agent.infrastructure.stream_port import MemoryStreamPort
from kokoro_agent.worker import REQUESTS_STREAM, events_stream, run_once


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

    async def fake_run_agent(request: object, model: BaseChatModel):
        yield AgentEvent(
            kind="run.completed",
            run_id="run_01",
            seq=1,
            payload={"status": "completed"},
        )

    monkeypatch.setattr("kokoro_agent.worker.make_chat_model", fake_make_chat_model)
    monkeypatch.setattr("kokoro_agent.worker.run_agent", fake_run_agent)

    await run_once(port, processed, None)

    assert seen_styles == ["thinking"]
