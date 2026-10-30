"""Public text must use the SDK text projection, not the assembled tool blocks."""

import json

import pytest
from langchain_core.language_models.chat_model_stream import AsyncChatModelStream
from support.chat import FakeChatRepository
from support.fakes import FakeBus, FakeRunStream, find_events

from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.protocols import AgentRunStream
from kokoro_agent.execution.publish_agent_events import pump_run
from kokoro_agent.protocol import MessageCompleted


@pytest.mark.parametrize("case", ["mixed", "revised", "tool-only"])
async def test_native_stream_text_survives_tool_block_completion(case: str) -> None:
    stream = AsyncChatModelStream(node="model", message_id="stage-one")
    stream.dispatch({"event": "message-start", "id": "stage-one"})
    if case != "tool-only":
        for text in ("先读取", "项目清单。"):
            stream.dispatch(
                {
                    "event": "content-block-delta",
                    "index": 0,
                    "delta": {"type": "text-delta", "text": text},
                }
            )
    if case == "revised":
        stream.dispatch(
            {
                "event": "content-block-finish",
                "index": 0,
                "content": {"type": "text", "text": "先检查项目清单。"},
            }
        )
    stream.dispatch(
        {
            "event": "content-block-finish",
            "index": 1,
            "content": {
                "type": "tool_call",
                "id": "read-one",
                "name": "read_file",
                "args": {"path": "/checklist.md"},
            },
        }
    )
    stream.dispatch({"event": "message-finish"})
    expected = {
        "mixed": "先读取项目清单。",
        "revised": "先检查项目清单。",
        "tool-only": "",
    }[case]
    assert await stream.text == expected
    assert stream.output_message is not None
    if case == "mixed":
        assert stream.output_message.text == ""  # Reproduces the live loss shape.
    bus, chat = FakeBus(), FakeChatRepository()
    emitter = await RunEmitter.attach(
        bus,
        "run-text",
        tenant_id="tenant",
        namespace="tenant:subject",
        session_id="session-text",
        chat_repository=chat,
    )
    run = FakeRunStream(models=(stream,))
    assert isinstance(run, AgentRunStream)
    await pump_run(emitter, run, source_for=lambda _: "runtime-custom")
    completed = find_events(bus.run_events("run-text"), MessageCompleted)
    assert [
        (event.payload.segment_id, event.payload.content) for event in completed
    ] == [("stage-one", expected)]
    replay = await chat.replay("tenant", "tenant:subject", "session-text")
    assert [
        json.loads(event.payload_json)["content"]
        for event in replay
        if event.event_type == "assistant.completed"
    ] == [expected]
    history = await chat.history("tenant", "tenant:subject", "session-text")
    assert [message.content for message in history] == [expected]
