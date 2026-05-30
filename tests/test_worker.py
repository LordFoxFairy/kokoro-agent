from __future__ import annotations

from kokoro_agent.infrastructure.stream_port import MemoryStreamPort
from kokoro_agent.worker import REQUESTS_STREAM, events_stream, run_once

EXPECTED_KINDS = [
    "run.started",
    "text.delta",
    "text.completed",
    "run.completed",
]


def _request(run_id: str = "run_01") -> dict[str, object]:
    return {
        "kind": "run.request",
        "run_id": run_id,
        "session_id": "ses_01",
        "conversation_id": "conv_01",
        "input": "hello kokoro",
    }


async def test_run_once_consumes_request_and_emits_full_sequence() -> None:
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request())

    processed: set[str] = set()
    await run_once(port, processed)

    items = await port.read_all(events_stream("run_01"))
    kinds = [item.event["kind"] for item in items]
    assert kinds == EXPECTED_KINDS

    seqs = [item.event["seq"] for item in items]
    assert seqs == [1, 2, 3, 4]

    delta = items[1].event["payload"]
    assert isinstance(delta, dict)
    assert delta["text"] == "Kokoro received: hello kokoro"


async def test_run_once_is_idempotent_per_run_id() -> None:
    port = MemoryStreamPort()
    await port.publish(REQUESTS_STREAM, _request())
    await port.publish(REQUESTS_STREAM, _request())

    processed: set[str] = set()
    await run_once(port, processed)

    items = await port.read_all(events_stream("run_01"))
    kinds = [item.event["kind"] for item in items]
    assert kinds == EXPECTED_KINDS


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
    await run_once(port, processed)

    # malformed request produces no events and does not crash the loop
    assert await port.read_all(events_stream("run_bad")) == []
