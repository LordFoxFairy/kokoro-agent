"""Script-output journal recovery and visibility through the real native SDK."""

from pathlib import Path
from typing import Any

import pytest
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolRuntime

from kokoro_agent.domain.run.repository import LeaseFence
from kokoro_agent.tools.middleware import (
    RunSupersededError,
    ToolEffectJournalMiddleware,
)
from support.fakes import FakeRunRepository, request


async def test_isolated_execute_recovers_started_output_without_running_again(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from unittest.mock import AsyncMock

    from deepagents.middleware.filesystem import ExecuteSchema
    from langchain_core.tools import StructuredTool

    from kokoro_agent.domain.run.workspace_output import WorkspaceExecuteIntent
    from kokoro_agent.sandbox.docker_backend import DockerShellBackend

    store = FakeRunRepository()
    first = await store.try_claim(request("rn"))
    assert first is not None
    backend = DockerShellBackend(
        root=tmp_path, container_id="not-started", timeout=2, max_output_bytes=2048
    )
    identity = AsyncMock(return_value="a" * 64)
    capture = AsyncMock(side_effect=[RuntimeError("worker interrupted"), b"original"])
    cleanup = AsyncMock()
    monkeypatch.setattr(backend, "aworkspace_output_identity", identity)
    monkeypatch.setattr(backend, "aexecute_workspace_output", capture)
    monkeypatch.setattr(backend, "acleanup_workspace_output", cleanup)
    ordinary_calls = 0

    async def handler(call: ToolCallRequest) -> ToolMessage:
        nonlocal ordinary_calls
        args = call.tool_call["args"]
        if args["command"] == "pwd":
            ordinary_calls += 1
            return ToolMessage(
                content="ordinary stdout", name="execute", tool_call_id="ordinary"
            )
        assert set(args) == {"command", "timeout"}
        native = await backend.aexecute(args["command"], timeout=args["timeout"])
        return ToolMessage(
            content=native.output, tool_call_id="c1", name="execute", status="success"
        )

    def echo(command: str) -> str:
        return command

    tool = StructuredTool(
        func=echo,
        name="execute",
        description="Run a command",
        args_schema=ExecuteSchema,
    )
    command = "echo private-input"
    runtime: ToolRuntime[Any, Any] = ToolRuntime(
        state=None,
        context=None,
        config={"configurable": {"thread_id": "thread"}},
        stream_writer=lambda _: None,
        tool_call_id="c1",
        store=None,
    )
    call = ToolCallRequest(
        tool=tool,
        state=None,
        tool_call={
            "name": "execute",
            "id": "c1",
            "args": {"command": command, "output_path": "/outputs/report.md"},
            "type": "tool_call",
        },
        runtime=runtime,
    )

    def middleware(lease: LeaseFence) -> ToolEffectJournalMiddleware:
        return ToolEffectJournalMiddleware(
            run_repository=store, run_id="rn", lease=lease
        ).for_workspace_output("thread", None, script=backend)

    with pytest.raises(RuntimeError, match="worker interrupted"):
        await middleware(first).awrap_tool_call(call, handler)
    record = await store.get_tool_journal("rn", "c1")
    assert record is not None and record.status == "started"
    intent = WorkspaceExecuteIntent.from_journal(record.result)
    assert intent is not None and command not in record.result
    assert len(store.sandbox_cleanups) == 1
    cleanup.assert_not_awaited()

    assert await store.pause("rn", first)
    replacement = await store.adopt("rn", "replacement")
    assert replacement is not None
    with pytest.raises(RunSupersededError):
        await middleware(first).awrap_tool_call(call, handler)
    wrong = call.override(
        tool_call={
            **call.tool_call,
            "args": {"command": "other", "output_path": intent.path},
        }
    )
    rejected = await middleware(replacement).awrap_tool_call(wrong, handler)
    assert isinstance(rejected, ToolMessage) and rejected.status == "error"
    assert capture.await_count == 1

    recovered = await middleware(replacement).awrap_tool_call(call, handler)
    replay = await middleware(replacement).awrap_tool_call(call, handler)
    assert isinstance(recovered, ToolMessage) and isinstance(replay, ToolMessage)
    assert (
        recovered.text == replay.text == "Captured workspace output /outputs/report.md"
    )
    assert capture.await_count == 2
    assert [entry.kwargs["recover"] for entry in capture.await_args_list] == [
        False,
        True,
    ]
    cleanup.assert_awaited_once_with(intent.key)
    assert ordinary_calls == 0

    plain = ToolCallRequest(
        tool=tool,
        state=None,
        runtime=runtime,
        tool_call={
            "name": "execute",
            "id": "ordinary",
            "args": {"command": "pwd"},
            "type": "tool_call",
        },
    )
    result = await middleware(replacement).awrap_tool_call(plain, handler)
    assert isinstance(result, ToolMessage) and result.text == "ordinary stdout"
    assert ordinary_calls == 1 and capture.await_count == 2


async def test_script_output_emits_the_original_sdk_execute_activity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from unittest.mock import AsyncMock

    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.checkpoint.memory import InMemorySaver

    from kokoro_agent.domain.run.scope import RunScope
    from kokoro_agent.sandbox.docker_backend import DockerShellBackend
    from support.deepagents import create_test_deep_agent
    from support.local_fake import LocalFakeChatModel

    store = FakeRunRepository()
    run = request("script-activity")
    lease = await store.try_claim(run)
    assert lease is not None
    thread = RunScope.of(run).scoped_thread_id
    backend = DockerShellBackend(
        root=tmp_path, container_id="not-started", timeout=2, max_output_bytes=2048
    )
    capture = AsyncMock(return_value=b"stopped script bytes")
    monkeypatch.setattr(backend, "aexecute_workspace_output", capture)
    monkeypatch.setattr(
        backend, "aworkspace_output_identity", AsyncMock(return_value="a" * 64)
    )
    monkeypatch.setattr(backend, "acleanup_workspace_output", AsyncMock())
    journal = ToolEffectJournalMiddleware(
        run_repository=store, run_id=run.run_id, lease=lease
    ).for_workspace_output(thread, None, script=backend)
    runnable = create_test_deep_agent(
        model=LocalFakeChatModel.with_script(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "execute",
                            "id": "original-execute",
                            "args": {
                                "command": "one script",
                                "output_path": "/outputs/report.md",
                            },
                        }
                    ],
                ),
                AIMessage(content="done"),
            ]
        ),
        backend=backend,
        checkpointer=InMemorySaver(),
        middleware=[journal],
    )
    stream = await runnable.astream_events(
        {"messages": [HumanMessage(content="Write one script output")]},
        version="v3",
        config={"configurable": {"thread_id": thread}},
        transformers=[],
    )
    async with stream:
        calls = [
            (call.tool_name, call.tool_call_id) async for call in stream.tool_calls
        ]
        assert not await stream.interrupted()
    assert calls == [("execute", "original-execute")]
    assert store.tool_journal[(run.run_id, "original-execute")]["result"] == (
        "Captured workspace output /outputs/report.md"
    )
    assert capture.await_count == 1
