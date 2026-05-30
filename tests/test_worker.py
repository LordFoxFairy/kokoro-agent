from __future__ import annotations

import os

import pytest

from kokoro_agent.infrastructure.model import make_agent
from kokoro_agent.infrastructure.stream_port import MemoryStreamPort
from kokoro_agent.worker import REQUESTS_STREAM, events_stream, run_once


def _request(run_id: str = "run_01") -> dict[str, object]:
    return {
        "kind": "run.request",
        "run_id": run_id,
        "session_id": "ses_01",
        "conversation_id": "conv_01",
        "input": "plan and search for kokoro",
    }


@pytest.mark.asyncio
async def test_run_once_streams_with_deep_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KOKORO_MODEL", "scripted")
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request())

    agent = make_agent()
    processed: set[str] = set()
    await run_once(port, processed, agent)

    items = await port.read_all(events_stream("run_01"))
    kinds = [item.event["kind"] for item in items]
    assert kinds[0] == "run.started"
    assert "tool.invoked" in kinds
    assert "text.completed" in kinds
    assert kinds[-1] == "run.completed"

    # seq is monotonic from 1.
    seqs = [item.event["seq"] for item in items]
    assert seqs == list(range(1, len(seqs) + 1))


@pytest.mark.asyncio
async def test_run_once_is_idempotent_per_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KOKORO_MODEL", "scripted")
    port = MemoryStreamPort()
    processed: set[str] = set()

    agent = make_agent()
    await port.publish(REQUESTS_STREAM, _request())
    await run_once(port, processed, agent)
    await port.publish(REQUESTS_STREAM, _request())  # duplicate run_id
    await run_once(port, processed, agent)

    items = await port.read_all(events_stream("run_01"))
    kinds = [item.event["kind"] for item in items]
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert kinds.count("run.started") == 1  # processed exactly once


@pytest.mark.asyncio
async def test_run_once_rejects_malformed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KOKORO_MODEL", "scripted")
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

    agent = make_agent()
    processed: set[str] = set()
    await run_once(port, processed, agent)

    # malformed request produces no events and does not crash the loop
    assert await port.read_all(events_stream("run_bad")) == []
