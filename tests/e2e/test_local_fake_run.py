"""LocalFake 全链路 e2e：真实 DeepAgents 循环 + memory 传输 + sqlite 状态，wire 全程契约校验。"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from pathlib import Path

import aiosqlite
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import JsonValue

from kokoro_agent.contract import (
    CONSUMER_GROUP,
    REQUESTS_MAXLEN,
    REQUESTS_STREAM,
    RUN_CONTROL_MAXLEN,
    FilesystemPerm,
    ModelConfig,
    Permissions,
    RunInput,
    RunRequest,
    RuntimeConfig,
    RuntimeContext,
    agent_event_adapter,
    run_control_stream,
    run_events_stream,
)
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.prompts import SYSTEM_PROMPT
from kokoro_agent.execution.protocols import InvokableAgent
from langchain.agents.middleware import AgentMiddleware

from kokoro_agent.model.local_fake import hitl_script, make_local_fake_chat_model
from kokoro_agent.tools.middleware import ToolResultReviewMiddleware
from kokoro_agent.sandbox import build_filesystem_permissions
from kokoro_agent.storage.sqlite import SqliteRunStateStore
from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.subagents import build_catalog
from kokoro_agent.tools.ask_user import ASK_USER_TOOL_NAME
from kokoro_agent.tools.permissions import build_interrupt_on
from kokoro_agent.tools.registry import resolve_tools
from kokoro_agent.worker.supervisor import RunSupervisor


def _request(
    run_id: str,
    *,
    approval_tools: tuple[str, ...] = (),
    review_tools: tuple[str, ...] = (),
    filesystem: FilesystemPerm = "read_only",
    content: str = "你好，帮我规划一下",
) -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        thread_id=f"{run_id}-thread",
        input=RunInput(message_id=f"{run_id}-m", content=content),
        runtime=RuntimeConfig(
            model=ModelConfig(provider="anthropic", name="claude"),
            tools=[],
            skills=[],
            mcp=[],
            subagents=[],
            backend="state",
            permissions=Permissions(
                approval_tools=list(approval_tools),
                review_tools=list(review_tools),
                subagent_create="deny",
                filesystem=filesystem,
            ),
        ),
        context=RuntimeContext(namespace="e2e", session_id="e2e-session"),
    )


def _build_supervisor(
    store: SqliteRunStateStore, script: Sequence[AIMessage] | None = None
) -> RunSupervisor:
    saver = InMemorySaver()
    catalog = build_catalog(None)

    async def build(request: RunRequest) -> InvokableAgent:
        runtime = request.runtime
        review = frozenset(runtime.permissions.review_tools)
        middleware: list[AgentMiddleware] = []
        if review:
            middleware.append(ToolResultReviewMiddleware(review, store, request.run_id))
        return build_agent(
            model=make_local_fake_chat_model(script),
            tools=resolve_tools(runtime.tools),
            system_prompt=SYSTEM_PROMPT,
            subagents=catalog.definitions(),
            checkpointer=saver,
            permissions=build_filesystem_permissions(runtime.permissions.filesystem),
            interrupt_on=build_interrupt_on(frozenset(runtime.permissions.approval_tools)),
            middleware=middleware,
        )

    def approval_names(request: RunRequest) -> frozenset[str]:
        return frozenset(request.runtime.permissions.approval_tools) | {ASK_USER_TOOL_NAME}

    return RunSupervisor(
        agent_builder=build,
        store=store,
        approval_tool_names=approval_names,
        trace_factory=lambda _request: None,
        source_for=catalog.source_for,
        consumer="e2e-consumer",
        heartbeat_s=60.0,
    )


async def _read_kinds(bus: MemoryStream, run_id: str) -> list[str]:
    items = await bus.read_all(run_events_stream(run_id))
    return [str(item.event.get("kind")) for item in items]


async def _wait_for(bus: MemoryStream, run_id: str, kinds: set[str], timeout: float = 30.0) -> None:
    async def _poll() -> None:
        while not (set(await _read_kinds(bus, run_id)) & kinds):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def test_local_fake_full_run_over_serve_loop(tmp_path: Path) -> None:
    bus = MemoryStream()
    run = _request("e2e-run-1")
    async with aiosqlite.connect(str(tmp_path / "e2e.db")) as db:
        store = SqliteRunStateStore(db, ttl_ms=90_000)
        await store.setup()
        supervisor = _build_supervisor(store)

        # 走真实 serve 循环：请求经 consumer-group 消费 + ack，非直调 dispatch。
        serve_task = asyncio.create_task(supervisor.serve(bus))
        await bus.publish(REQUESTS_STREAM, dict(run.model_dump()), maxlen=REQUESTS_MAXLEN)

        await _wait_for(bus, run.run_id, {"run.completed", "run.failed"})
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass

        items = await bus.read_all(run_events_stream(run.run_id))
        # 每帧 wire 都过契约门（strict 判别联合）：非法帧根本发不出来。
        events = [agent_event_adapter.validate_python(item.event) for item in items]
        kinds = [event.kind for event in events]

        assert kinds[0] == "run.started"
        assert kinds[-1] == "run.completed"
        assert "todo.updated" in kinds
        assert "message.delta" in kinds
        assert "message.completed" in kinds
        # per-run index 从 0 起严格连续单调：event_id 幂等链的根基。
        assert [event.index for event in events] == list(range(len(events)))
        assert all(event.run_id == run.run_id for event in events)

        # 请求已被 ack（consumer-group 语义），终态已被认领。
        assert len(bus.acked(REQUESTS_STREAM, CONSUMER_GROUP)) == 1
        assert await store.is_terminal(run.run_id) is True

        # 幂等：同一 run.request 再来一遍，不产生任何新事件。
        before = len(items)
        await supervisor.dispatch(bus, run)
        for task in tuple(supervisor.tasks.values()):
            await task
        assert len(await bus.read_all(run_events_stream(run.run_id))) == before


async def _awaiting_frames(bus: MemoryStream, run_id: str) -> list[dict[str, JsonValue]]:
    out: list[dict[str, JsonValue]] = []
    for item in await bus.read_all(run_events_stream(run_id)):
        if item.event.get("kind") == "tool.awaiting_approval":
            payload = item.event.get("payload")
            if isinstance(payload, dict):
                out.append(payload)
    return out


async def _wait_awaiting(
    bus: MemoryStream, run_id: str, kind: str, timeout: float = 30.0
) -> dict[str, JsonValue]:
    async def _poll() -> dict[str, JsonValue]:
        while True:
            frame = next(
                (p for p in await _awaiting_frames(bus, run_id) if p.get("kind") == kind), None
            )
            if frame is not None:
                return frame
            if "run.failed" in await _read_kinds(bus, run_id):
                raise AssertionError(f"run failed before {kind} pause")
            await asyncio.sleep(0.01)

    return await asyncio.wait_for(_poll(), timeout=timeout)


async def _publish_resume(
    bus: MemoryStream, run: RunRequest, decision: dict[str, JsonValue]
) -> None:
    event: dict[str, JsonValue] = {
        "kind": "run.resume",
        "run_id": run.run_id,
        "thread_id": run.thread_id,
        "decisions": [decision],
    }
    await bus.publish(run_control_stream(run.run_id), event, maxlen=RUN_CONTROL_MAXLEN)


async def test_local_fake_hitl_ask_user_then_approval_over_control_stream(tmp_path: Path) -> None:
    bus = MemoryStream()
    run = _request("e2e-hitl-1", approval_tools=("write_file",), filesystem="workspace_write")
    async with aiosqlite.connect(str(tmp_path / "hitl.db")) as db:
        store = SqliteRunStateStore(db, ttl_ms=90_000)
        await store.setup()
        supervisor = _build_supervisor(store, script=hitl_script())
        serve_task = asyncio.create_task(supervisor.serve(bus))
        await bus.publish(REQUESTS_STREAM, dict(run.model_dump()), maxlen=REQUESTS_MAXLEN)

        # ① ask_user 暂停 → 人工作答（respond）从 control 流回灌。
        ask = await _wait_awaiting(bus, run.run_id, "ask_user")
        respond: dict[str, JsonValue] = {
            "type": "respond",
            "tool_id": str(ask["tool_id"]),
            "response": "中文",
        }
        await _publish_resume(bus, run, respond)

        # ② write_file 审批暂停 → approve 从 control 流回灌。
        approval = await _wait_awaiting(bus, run.run_id, "tool_approval")
        approve: dict[str, JsonValue] = {"type": "approve", "tool_id": str(approval["tool_id"])}
        await _publish_resume(bus, run, approve)

        # ③ 正常文本流收束到终态。
        await _wait_for(bus, run.run_id, {"run.completed", "run.failed"})
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass

        kinds = await _read_kinds(bus, run.run_id)
        assert kinds[-1] == "run.completed"
        # 两次暂停都发过 awaiting；write_file 审批后真实执行并返回。
        assert kinds.count("tool.awaiting_approval") == 2
        assert "tool.returned" in kinds
        assert await store.is_terminal(run.run_id) is True


async def test_local_fake_result_review_over_control_stream(tmp_path: Path) -> None:
    """结果审核全链：write_file 配 review → 执行后暂停带结果 → respond 替换 → 回流为替换文本。"""
    bus = MemoryStream()
    run = _request("e2e-review-1", review_tools=("write_file",), filesystem="workspace_write")
    async with aiosqlite.connect(str(tmp_path / "review.db")) as db:
        store = SqliteRunStateStore(db, ttl_ms=90_000)
        await store.setup()
        # 脚本去掉 ask_user 帧：只走 write_file（审核）+ 终帧文本。
        script = [turn for turn in hitl_script() if not _calls_tool(turn, "ask_user")]
        supervisor = _build_supervisor(store, script=script)
        serve_task = asyncio.create_task(supervisor.serve(bus))
        await bus.publish(REQUESTS_STREAM, dict(run.model_dump()), maxlen=REQUESTS_MAXLEN)

        # ① 工具执行后暂停：awaiting 带真实结果，决策集为审核三元组。
        awaiting = await _wait_awaiting(bus, run.run_id, "result_review")
        assert awaiting["name"] == "write_file"
        assert isinstance(awaiting["result"], str) and awaiting["result"]
        assert awaiting["allowed_decisions"] == ["approve", "respond", "reject"]
        # 双执行防护实证：首跑结果 keep-first 已落盘。
        cached = await store.get_tool_result(run.run_id, str(awaiting["tool_id"]))
        assert cached is not None and cached[0] == awaiting["result"]

        # ② respond 替换结果 → 重入命中缓存不重跑工具 → 回流为替换文本。
        respond: dict[str, JsonValue] = {
            "type": "respond",
            "tool_id": str(awaiting["tool_id"]),
            "response": "curated result",
        }
        await _publish_resume(bus, run, respond)
        await _wait_for(bus, run.run_id, {"run.completed", "run.failed"})
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass

        kinds = await _read_kinds(bus, run.run_id)
        assert kinds[-1] == "run.completed"
        # raw returned 被抑制：wire 上唯一的 returned 是裁决直发，且在 awaiting 之后。
        assert kinds.count("tool.returned") == 1
        assert kinds.index("tool.awaiting_approval") < kinds.index("tool.returned")
        returned = await _tool_returned_payload(bus, run.run_id, str(awaiting["tool_id"]))
        assert returned["result"] == "curated result"
        assert returned["responded"] is True


def _calls_tool(turn: AIMessage, name: str) -> bool:
    return any(tc["name"] == name for tc in turn.tool_calls)


async def _tool_returned_payload(
    bus: MemoryStream, run_id: str, tool_id: str
) -> dict[str, JsonValue]:
    for item in await bus.read_all(run_events_stream(run_id)):
        if item.event.get("kind") != "tool.returned":
            continue
        payload = item.event.get("payload")
        assert isinstance(payload, dict)
        if payload.get("tool_id") == tool_id:
            return payload
    raise AssertionError(f"no tool.returned for {tool_id!r}")
