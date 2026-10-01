"""invoke/pump/emit 规格：三路终态、投影合流顺序、index 单调、usage 聚合、哨兵收束。"""

from __future__ import annotations

from collections.abc import Iterator
from support.fakes import isolated_native_registry

from support.fakes import settled_state_callback

from support.fakes import terminal_emitter

import asyncio
import pytest
import ast
import json
from dataclasses import dataclass
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from langchain_core.callbacks import BaseCallbackHandler, CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import StructuredTool
from langgraph.types import Interrupt
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import BaseModel as PydanticBaseModel

from support.fakes import (
    FakeAgent,
    FakeBus,
    FakeModel,
    FakeRunStream,
    FakeState,
    FakeSubagentRun,
    FakeToolCall,
    find_event,
    find_events,
    text_model,
    text_run,
    usage_recorder,
)
from support.chat import FakeChatRepository
from kokoro_agent.protocol import (
    RUN_EVENTS_MAXLEN,
    MessageCompleted,
    RunCompleted,
    RunCompletedPayload,
    RunFailedPayload,
    RunFailed,
    RunStartedPayload,
    SubagentFinished,
    SubagentSource,
    SubagentStarted,
    SubagentToolInvoked,
    SubagentToolReturned,
    ToolAwaitingApprovalPayload,
    ToolInvoked,
    ToolInvokedPayload,
    ToolReturned,
    ToolReturnedPayload,
)
from support.deepagents import create_test_deep_agent
from support.local_fake import LocalFakeChatModel
from kokoro_agent.execution.events import RunEmitter, clip_result, tool_returned_payload
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.protocol.streams import run_events_stream
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.streams.redis import RedisStream


def _runtime_custom(_name: str) -> SubagentSource:
    return "runtime-custom"


async def _always_claim() -> bool:
    return True


async def _invoke(
    bus: FakeBus,
    agent: FakeAgent,
    run_id: str = "r1",
    *,
    approval_tool_names: frozenset[str] = frozenset(),
    claim: Callable[[], Awaitable[bool]] = _always_claim,
    trace: RunnableConfig | None = None,
) -> bool:
    emitter = await RunEmitter.attach(bus, run_id)
    return await invoke_once(
        emitter,
        agent,
        "c1",
        {"messages": []},
        approval_tool_names=approval_tool_names,
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(emitter, claim, usage_recorder()[0]),
        record_usage=usage_recorder()[0],
        trace=trace,
        on_native_settled=settled_state_callback(agent, "c1"),
    )


async def test_first_event_is_run_started_index_zero() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=text_run("hi")))
    first = bus.run_events("r1")[0]
    assert first.kind == "run.started"
    assert first.index == 0
    assert first.run_id == "r1"


async def test_order_started_delta_completed_terminal() -> None:
    bus = FakeBus()
    done = await _invoke(bus, FakeAgent(run=text_run("hello")))
    assert done is True
    kinds = bus.kinds("r1")
    assert kinds[0] == "run.started"
    assert kinds[-1] == "run.completed"
    assert kinds.index("message.delta") < kinds.index("message.completed")


async def test_index_strictly_monotonic() -> None:
    bus = FakeBus()
    model = FakeModel(
        text_deltas=("a", "b", "c"),
        reasoning_deltas=("think",),
        output_message=AIMessage(content="abc", id="seg"),
    )
    await _invoke(bus, FakeAgent(run=FakeRunStream(models=(model,))))
    indexes = [e.index for e in bus.run_events("r1")]
    assert indexes == list(range(len(indexes)))


async def test_resumed_segment_does_not_repeat_run_started(stream: RedisStream) -> None:
    # resume/重拾续段：index>0 时不再宣告 run.started（wire 噪音，真栈 dump 抓获）。
    run_id = f"rn-{uuid4().hex}"
    first = RunEmitter(stream, run_id)
    assert first.at_start is True
    await first.emit(RunStartedPayload())
    resumed = await RunEmitter.attach(stream, run_id)
    assert resumed.at_start is False


def test_clip_result_boundary_matrix() -> None:
    # wire 轻事件护栏：4000 内原样（truncated=False）；超限截断标注省略量（truncated=True）；空串安全。
    assert clip_result("") == ("", False)
    assert clip_result("a" * 4000) == ("a" * 4000, False)
    clipped, truncated = clip_result("b" * 4100)
    assert truncated is True
    assert clipped.startswith("b" * 4000) and clipped.endswith("…[truncated 100 chars]")


@dataclass
class _StubToolCall:
    tool_call_id: str = "t1"
    tool_name: str = "probe"
    input: dict[str, object] | None = None
    output: object = None
    error: str | None = None


def _tool_call_info(output: object) -> _StubToolCall:
    return _StubToolCall(output=output)


def test_tool_returned_truncated_absent_when_complete() -> None:
    # 契约语义：truncated 缺席 = 结果完整；截断时 = True（exclude_none 上 wire）。
    short = tool_returned_payload(_tool_call_info(output="ok"))
    assert short is not None  # 非 interrupt 工具：恒有 returned 载荷。
    assert short.truncated is None
    assert "truncated" not in short.model_dump(exclude_none=True)
    long = tool_returned_payload(_tool_call_info(output="c" * 5000))
    assert long is not None
    assert long.truncated is True
    assert long.model_dump(exclude_none=True)["truncated"] is True


async def test_tool_events_inherit_awaiting_segment(stream: RedisStream) -> None:
    # resume 后 invoked/returned 只有 tool_call_id 兜底段：必须继承 awaiting 的 AIMessage 段（真栈走查根治）。
    run_id = f"rn-{uuid4().hex}"
    emitter = RunEmitter(stream, run_id)
    await emitter.emit(
        ToolAwaitingApprovalPayload(
            segment_id="seg_msg",
            tool_id="t1",
            name="write_file",
            args={},
            description="d",
            allowed_decisions=["approve"],
            kind="tool_approval",
            risk=None,
            editable=False,
            input_schema=None,
            pending_tool_ids=["t1"],
        )
    )
    # 模拟 resume：从流重建发射器（归属映射经历史回放恢复）。
    resumed = await RunEmitter.attach(stream, run_id)
    await resumed.emit(
        ToolInvokedPayload(segment_id="t1", tool_id="t1", name="write_file", args={})
    )
    await resumed.emit(
        ToolReturnedPayload(
            segment_id="t1",
            tool_id="t1",
            name="write_file",
            result="ok",
            is_error=False,
            rejected=None,
            reject_reason=None,
            responded=None,
            summary=None,
        )
    )
    items = await stream.read_all(run_events_stream(run_id))
    segs: list[str] = []
    for item in items[-2:]:
        payload = item.event["payload"]
        assert isinstance(payload, dict)
        seg = payload["segment_id"]
        assert isinstance(seg, str)
        segs.append(seg)
    assert segs == ["seg_msg", "seg_msg"]


async def test_optional_none_fields_never_serialize_as_null(
    stream: RedisStream,
) -> None:
    # 契约 optional 字段的 None 即缺席：null 上 wire 会被 session zod .optional() 拒收（跨栈 e2e 抓获的真实缺陷）。
    run_id = f"rn-{uuid4().hex}"
    emitter = RunEmitter(stream, run_id)
    await emitter.emit(
        ToolAwaitingApprovalPayload(
            segment_id="s1",
            tool_id="t1",
            name="write_file",
            args={},
            description="d",
            allowed_decisions=["approve"],
            kind="tool_approval",
            risk=None,
            editable=False,
            input_schema=None,
            pending_tool_ids=["t1"],
        )
    )
    items = await stream.read_all(run_events_stream(run_id))
    payload = items[-1].event["payload"]
    assert isinstance(payload, dict)
    assert "risk" not in payload and "input_schema" not in payload


async def test_thinking_channel_and_final_frame() -> None:
    model = FakeModel(
        text_deltas=("hel", "lo"),
        reasoning_deltas=("mm",),
        output_message=AIMessage(content="hello", id="seg"),
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(models=(model,))))
    events = bus.run_events("r1")
    assert bus.kinds("r1").count("thinking.delta") == 1
    completed = find_event(events, MessageCompleted)
    assert completed.payload.content == "hello"


async def test_empty_text_segment_completes_without_empty_delta() -> None:
    # tool-only 段的终态必须可见，不能让前一段正文继续冒充最终答案。
    model = FakeModel(text_deltas=(), output_message=AIMessage(content="", id="seg"))
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(models=(model,))))
    kinds = bus.kinds("r1")
    assert "message.delta" not in kinds
    completed = find_events(bus.run_events("r1"), MessageCompleted)
    assert [
        (event.payload.segment_id, event.payload.content) for event in completed
    ] == [("seg", "")]
    assert kinds.index("message.completed") < kinds.index("run.completed")


async def test_no_final_message_or_text_does_not_invent_empty_completion() -> None:
    model = FakeModel(text_deltas=(), output_message=None, message_id="no-output")
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(models=(model,))))
    assert "message.completed" not in bus.kinds("r1")


async def test_text_tool_and_empty_final_segment_are_replayable_in_order() -> None:
    bus = FakeBus()
    chat = FakeChatRepository()
    run_id = "text-tool-empty-final"
    emitter = await RunEmitter.attach(
        bus,
        run_id,
        tenant_id="tenant",
        namespace="tenant:subject",
        session_id="session-1",
        chat_repository=chat,
    )
    models = (
        text_model("draft", msg_id="draft-segment"),
        FakeModel(
            message_id="tool-segment",
            output_message=AIMessage(
                content="",
                id="tool-segment",
                tool_calls=[{"name": "lookup", "args": {"q": "x"}, "id": "tool-1"}],
            ),
        ),
        FakeModel(
            message_id="final-segment",
            output_message=AIMessage(content="", id="final-segment"),
        ),
    )
    agent = FakeAgent(
        run=FakeRunStream(
            models=models,
            tool_views=(
                FakeToolCall(
                    tool_call_id="tool-1",
                    tool_name="lookup",
                    input={"q": "x"},
                    output="found",
                ),
            ),
        )
    )
    done = await invoke_once(
        emitter,
        agent,
        "thread-1",
        {"messages": []},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(emitter, _always_claim, usage_recorder()[0]),
        record_usage=usage_recorder()[0],
        on_native_settled=settled_state_callback(agent, "thread-1"),
    )
    assert done is True
    events = bus.run_events(run_id)
    completed = find_events(events, MessageCompleted)
    assert [
        (event.payload.segment_id, event.payload.content) for event in completed
    ] == [
        ("draft-segment", "draft"),
        ("tool-segment", ""),
        ("final-segment", ""),
    ]
    assert [event.index for event in events] == list(range(len(events)))
    assert events[-1].kind == "run.completed"
    assert [event.kind for event in events].count("message.delta") == 1
    assert [event.kind for event in events].count("tool.invoked") == 1
    replay = await chat.replay("tenant", "tenant:subject", "session-1")
    completed_replay = [
        json.loads(event.payload_json)["content"]
        for event in replay
        if event.event_type == "assistant.completed"
    ]
    assert completed_replay == ["draft", "", ""]
    history = await chat.history("tenant", "tenant:subject", "session-1")
    assert [message.content for message in history] == ["draft", "", ""]


async def test_tool_invoked_and_returned() -> None:
    tool = FakeToolCall(
        tool_call_id="t1", tool_name="lookup", input={"q": "x"}, output="found"
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tool,))))
    events = bus.run_events("r1")
    invoked = find_event(events, ToolInvoked)
    returned = find_event(events, ToolReturned)
    assert invoked.payload.args == {"q": "x"}
    assert returned.payload.result == "found"
    assert returned.payload.is_error is False
    assert returned.payload.rejected is None


async def test_tool_error_marks_is_error() -> None:
    tool = FakeToolCall(
        tool_call_id="t1", tool_name="lookup", input={}, error="exploded"
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tool,))))
    returned = find_event(bus.run_events("r1"), ToolReturned)
    assert returned.payload.is_error is True
    assert returned.payload.result == "exploded"


async def test_todo_tool_becomes_todo_updated() -> None:
    todos: list[dict[str, object]] = [{"content": "plan", "status": "pending"}]
    tool = FakeToolCall(
        tool_call_id="t1", tool_name="write_todos", input={"todos": todos}
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tool,))))
    kinds = bus.kinds("r1")
    assert "todo.updated" in kinds
    assert "tool.invoked" not in kinds


async def test_malformed_todos_fail_loud_as_run_failed() -> None:
    # LLM 产出的 todos 是不可信载荷：strict 洗净失败收口为 run.failed，不发脏 wire。
    tool = FakeToolCall(
        tool_call_id="t1",
        tool_name="write_todos",
        input={"todos": [{"content": "", "status": "?"}]},
    )
    bus = FakeBus()
    done = await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tool,))))
    assert done is True
    assert bus.kinds("r1")[-1] == "run.failed"


async def test_task_tool_not_double_emitted() -> None:
    tool = FakeToolCall(
        tool_call_id="t1", tool_name="task", input={"description": "go"}
    )
    sub = FakeSubagentRun(trigger_call_id="t1")
    bus = FakeBus()
    await _invoke(
        bus, FakeAgent(run=FakeRunStream(tool_views=(tool,), subagent_runs=(sub,)))
    )
    kinds = bus.kinds("r1")
    assert "tool.invoked" not in kinds
    assert kinds.count("subagent.started") == 1
    assert kinds.count("subagent.finished") == 1


async def test_subagent_text_and_inner_tools_routing() -> None:
    inner_tool = FakeToolCall(tool_call_id="inner", tool_name="lookup", input={})
    sub = FakeSubagentRun(
        name="researcher",
        trigger_call_id="sub-1",
        models=(text_model("sub answer", msg_id="sub-seg"),),
        tool_views=(inner_tool,),
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    events = bus.run_events("r1")
    kinds = [e.kind for e in events]
    assert "subagent.text.delta" in kinds
    assert "subagent.text.completed" in kinds
    # 子代理内工具走 subagent.tool.* 可见性通道，不冒充顶层工具。
    assert "tool.invoked" not in kinds and "tool.returned" not in kinds
    invoked = find_event(events, SubagentToolInvoked)
    assert invoked.payload.subagent_id == "sub-1"
    assert invoked.payload.tool_id == "inner"
    assert invoked.payload.name == "lookup"
    returned = find_event(events, SubagentToolReturned)
    assert returned.payload.subagent_id == "sub-1"
    assert returned.payload.is_error is False
    started = find_event(events, SubagentStarted)
    assert started.payload.subagent_id == "sub-1"
    assert started.payload.source == "runtime-custom"


async def test_subagent_inner_tool_error_and_clip() -> None:
    giant = "x" * 5000
    bad = FakeToolCall(tool_call_id="e1", tool_name="lookup", input={}, error="boom")
    big = FakeToolCall(tool_call_id="b1", tool_name="lookup", input={}, output=giant)
    sub = FakeSubagentRun(trigger_call_id="sub-1", tool_views=(bad, big))
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    events = find_events(bus.run_events("r1"), SubagentToolReturned)
    by_tool = {e.payload.tool_id: e.payload for e in events}
    assert by_tool["e1"].is_error is True and by_tool["e1"].result == "boom"
    assert by_tool["b1"].truncated is True and len(by_tool["b1"].result) < 5000


async def test_subagent_todo_stays_in_subagent_channel() -> None:
    # 子代理自己的 todo 不得覆盖主面板：走 subagent.tool.* 而非 todo.updated。
    todo = FakeToolCall(
        tool_call_id="td1",
        tool_name="write_todos",
        input={"todos": [{"content": "step", "status": "pending"}]},
    )
    sub = FakeSubagentRun(trigger_call_id="sub-1", tool_views=(todo,))
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    kinds = bus.kinds("r1")
    assert "todo.updated" not in kinds
    assert "subagent.tool.invoked" in kinds and "subagent.tool.returned" in kinds


async def test_nested_task_inside_subagent_not_double_emitted() -> None:
    inner_task = FakeToolCall(
        tool_call_id="nt1", tool_name="task", input={"description": "go"}
    )
    sub = FakeSubagentRun(trigger_call_id="sub-1", tool_views=(inner_task,))
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    kinds = bus.kinds("r1")
    assert (
        "subagent.tool.invoked" not in kinds and "subagent.tool.returned" not in kinds
    )


async def test_failed_subagent_flagged() -> None:
    sub = FakeSubagentRun(status="failed")
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    finished = find_event(bus.run_events("r1"), SubagentFinished)
    assert finished.payload.failed is True


async def test_custom_channel_drained_without_wire_events() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(custom_items=({"telemetry": 1},))))
    assert bus.kinds("r1") == ["run.started", "run.completed"]


class _UsageFake(BaseChatModel):
    tokens: tuple[int, int, int] = (0, 0, 0)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        i, o, t = self.tokens
        msg = AIMessage(
            content="u",
            usage_metadata={"input_tokens": i, "output_tokens": o, "total_tokens": t},
            response_metadata={"model_name": "fake-model"},
        )
        return ChatResult(generations=[ChatGeneration(message=msg)])

    @property
    def _llm_type(self) -> str:
        return "usage-fake"


class _UsageAgent(FakeAgent):
    def __init__(self, usages: Sequence[tuple[int, int, int]]) -> None:
        super().__init__(run=text_run("x"))
        self._usages = usages

    async def astream_events(
        self,
        payload: object,
        *,
        version: str,
        context: object | None = None,
        config: RunnableConfig,
        transformers: Sequence[object],
    ) -> FakeRunStream:
        for tokens in self._usages:
            await _UsageFake(tokens=tokens).ainvoke("x")
        return self.run


async def test_usage_aggregated_into_run_completed() -> None:
    bus = FakeBus()
    await _invoke(bus, _UsageAgent(usages=((3, 5, 8), (1, 2, 3))))
    completed = find_event(bus.run_events("r1"), RunCompleted)
    assert completed.payload.token_usage is not None
    assert completed.payload.token_usage.input_tokens == 4
    assert completed.payload.token_usage.output_tokens == 7


async def test_no_usage_is_null() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=text_run("hi")))
    completed = find_event(bus.run_events("r1"), RunCompleted)
    assert completed.payload.token_usage is None


def _interrupt_state() -> FakeState:
    ai = AIMessage(
        content="",
        id="seg-1",
        tool_calls=[{"name": "danger", "args": {"x": 1}, "id": "call-A"}],
    )
    interrupt = Interrupt(
        value={
            "action_requests": [
                {"name": "danger", "args": {"x": 1}, "description": "do danger"}
            ],
            "review_configs": [
                {
                    "action_name": "danger",
                    "allowed_decisions": ["approve", "edit", "reject"],
                }
            ],
        }
    )
    return FakeState(
        interrupts=(interrupt,), values={"messages": [HumanMessage(content="go"), ai]}
    )


async def test_pending_interrupt_defers_durable_waiting_to_callback_no_terminal() -> (
    None
):
    bus = FakeBus()
    agent = FakeAgent(run=FakeRunStream(is_interrupted=True), state=_interrupt_state())
    done = await _invoke(bus, agent, approval_tool_names=frozenset({"danger"}))
    assert done is False
    kinds = bus.kinds("r1")
    assert "tool.awaiting_approval" not in kinds
    assert "run.completed" not in kinds
    assert agent.run.context_exited
    # Full safe pending groups now belong to the repository/reader transaction;
    # this invocation layer must not synthesize old partial activity frames.
    assert agent.state.interrupts == _interrupt_state().interrupts


async def test_exception_emits_run_failed() -> None:
    bus = FakeBus()
    done = await _invoke(bus, FakeAgent(raise_on_stream=ValueError("boom")))
    assert done is True
    assert bus.kinds("r1") == ["run.started", "run.failed"]
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert failed.payload.model_dump() == {"code": "internal_error", "retryable": False}
    assert "boom" not in failed.payload.model_dump_json()
    assert failed.payload.code == "internal_error"  # 未归类异常兜底码


async def test_empty_exception_message_still_emits_only_safe_failure() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(raise_on_stream=RuntimeError()))
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert failed.payload.model_dump() == {"code": "internal_error", "retryable": False}


async def test_claim_denied_suppresses_terminal() -> None:
    async def deny() -> bool:
        return False

    bus = FakeBus()
    done = await _invoke(bus, FakeAgent(run=text_run("hi")), claim=deny)
    assert done is True
    kinds = bus.kinds("r1")
    assert "run.completed" not in kinds
    assert "run.failed" not in kinds


async def test_consume_exception_no_drainer_leak() -> None:
    # 哨兵必达：投影消费抛错时 drainer 收束、invoke 不挂起，顶层收口为 run.failed。
    bus = FakeBus()
    agent = FakeAgent(run=FakeRunStream(raise_on_messages=True))
    done = await asyncio.wait_for(_invoke(bus, agent), timeout=2.0)
    assert done is True
    assert bus.kinds("r1")[-1] == "run.failed"


async def test_trace_config_merged() -> None:
    class _Handler(BaseCallbackHandler):
        pass

    handler = _Handler()
    trace: RunnableConfig = {
        "callbacks": [handler],
        "metadata": {"langfuse_session_id": "s1", "kokoro_run_id": "r1"},
    }
    bus = FakeBus()
    agent = FakeAgent(run=text_run("hi"))
    await _invoke(bus, agent, trace=trace)
    assert agent.seen_config.get("configurable") == {"thread_id": "c1"}
    assert agent.seen_config.get("callbacks") == [handler]
    assert agent.seen_config.get("metadata") == {
        "langfuse_session_id": "s1",
        "kokoro_run_id": "r1",
    }


async def test_trace_none_config_only_configurable() -> None:
    bus = FakeBus()
    agent = FakeAgent(run=text_run("hi"))
    await _invoke(bus, agent, trace=None)
    assert agent.seen_config.get("configurable") == {"thread_id": "c1"}
    assert "callbacks" not in agent.seen_config
    assert "metadata" not in agent.seen_config


async def test_emitter_attach_continues_after_existing_events() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=text_run("first")))
    count = len(bus.run_events("r1"))
    # 新 emitter（模拟 resume/重启/重拾）从既有最大 index 之后续接，不回卷。
    await _invoke(bus, FakeAgent(run=text_run("second")))
    indexes = [e.index for e in bus.run_events("r1")]
    assert indexes == list(range(len(indexes)))
    assert len(indexes) > count


def test_fake_bus_satisfies_stream_protocol() -> None:
    assert isinstance(FakeBus(), StreamProtocol)


async def test_events_published_with_run_events_maxlen() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=text_run("hi")))
    assert all(maxlen == RUN_EVENTS_MAXLEN for _stream, _event, maxlen in bus.published)


class _NoopArgs(PydanticBaseModel):
    pass


async def test_native_v3_draft_tool_then_empty_final_segment_order() -> None:
    noop = StructuredTool(
        name="noop", description="no-op", args_schema=_NoopArgs, func=lambda: "ok"
    )
    agent = create_test_deep_agent(
        model=LocalFakeChatModel.with_script(
            [
                AIMessage(
                    content="draft",
                    id="native-draft",
                    tool_calls=[
                        {
                            "name": "noop",
                            "args": {},
                            "id": "native-tool",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="", id="native-empty-final"),
            ]
        ),
        tools=[noop],
        system_prompt="x",
        subagents=[],
        checkpointer=InMemorySaver(),
        permissions=[],
        interrupt_on={},
    )
    bus = FakeBus()
    done = await invoke_once(
        (terminal_test_emitter_1 := RunEmitter(bus, "native-empty-final")),
        agent,
        "native-thread",
        {"messages": [HumanMessage(content="go")]},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(
            terminal_test_emitter_1, _always_claim, usage_recorder()[0]
        ),
        record_usage=usage_recorder()[0],
        on_native_settled=settled_state_callback(agent, "native-thread"),
    )
    assert done is True
    events = bus.run_events("native-empty-final")
    significant = [
        event.kind
        for event in events
        if event.kind in {"message.completed", "tool.invoked", "tool.returned"}
    ]
    assert significant == [
        "message.completed",
        "tool.invoked",
        "tool.returned",
        "message.completed",
    ]
    assert [
        event.payload.content for event in find_events(events, MessageCompleted)
    ] == ["draft", ""]


class _LoopingModel(LocalFakeChatModel):
    """每轮都发同一工具调用：制造无限图循环，验证 recursion_limit 熔断。"""

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        calls = sum(1 for m in messages if isinstance(m, AIMessage))
        message = AIMessage(
            content="",
            tool_calls=[
                {"name": "noop", "args": {}, "id": f"loop{calls}", "type": "tool_call"}
            ],
        )
        return ChatResult(generations=[ChatGeneration(message=message)])


async def test_runaway_loop_hits_recursion_limit_and_fails_loud(
    stream: RedisStream,
) -> None:
    noop = StructuredTool(
        name="noop", description="no-op", args_schema=_NoopArgs, func=lambda: "ok"
    )
    agent = create_test_deep_agent(
        model=_LoopingModel.with_script([]),
        tools=[noop],
        system_prompt="x",
        subagents=[],
        checkpointer=InMemorySaver(),
        permissions=[],
        interrupt_on={},
    )
    run_id = f"rloop-{uuid4().hex}"

    async def claim() -> bool:
        return True

    terminal = await invoke_once(
        (terminal_test_emitter_2 := RunEmitter(stream, run_id)),
        agent,
        "tloop",
        {"messages": [HumanMessage(content="go")]},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(
            terminal_test_emitter_2, claim, usage_recorder()[0]
        ),
        record_usage=usage_recorder()[0],
        recursion_limit=8,
        on_native_settled=settled_state_callback(agent, "tloop"),
    )
    assert terminal is True
    events = [item.event for item in await stream.read_all(run_events_stream(run_id))]
    assert events[-1]["kind"] == "run.failed"
    payload = events[-1]["payload"]
    assert isinstance(payload, dict)
    assert payload == {"code": "recursion_limit_exceeded", "retryable": False}
    # 失败码闭集：熔断失败在 wire 上是稳定码（web 本地化键），不是异常类名。
    assert payload.get("code") == "recursion_limit_exceeded"


def test_tool_returned_renders_content_blocks_readably() -> None:
    # MCP 工具返回标准 content blocks（list[dict]）：wire 上应是文本拼接，不是 Python repr。
    blocks = [
        {"type": "text", "text": "第一段"},
        {"type": "text", "text": "第二段"},
        {"type": "image", "data": "..."},
    ]
    payload = tool_returned_payload(_tool_call_info(output=blocks))
    assert payload is not None
    assert payload.result == "第一段\n第二段\n[1 non-text block(s) omitted]"
    assert "{'type'" not in payload.result


async def test_tool_output_streams_between_invoked_and_returned() -> None:
    tc = FakeToolCall(
        tool_call_id="t1",
        tool_name="execute",
        output="done",
        deltas=("line1\n", "line2\n"),
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tc,))))
    kinds = bus.kinds("r1")
    assert (
        kinds.index("tool.invoked")
        < kinds.index("tool.output.delta")
        < kinds.index("tool.returned")
    )
    deltas = [
        e.payload.delta for e in bus.run_events("r1") if e.kind == "tool.output.delta"
    ]
    assert deltas == ["line1\n", "line2\n"]


async def test_tool_output_stream_budget_clips_silently() -> None:
    tc = FakeToolCall(
        tool_call_id="t1",
        tool_name="execute",
        output="done",
        deltas=("a" * 3000, "b" * 3000, "c" * 3000),
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tc,))))
    deltas = [
        e.payload.delta for e in bus.run_events("r1") if e.kind == "tool.output.delta"
    ]
    assert sum(len(d) for d in deltas) == 4000  # TOOL_RESULT_MAX_CHARS 预算截停
    assert bus.kinds("r1")[-1] == "run.completed"


async def test_subagent_thinking_streams_on_wire() -> None:
    sub = FakeSubagentRun(
        models=(
            FakeModel(
                text_deltas=("hi",),
                reasoning_deltas=("hmm",),
                output_message=AIMessage(content="hi", id="s"),
            ),
        ),
        name="poet",
        trigger_call_id="sub1",
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    kinds = bus.kinds("r1")
    assert "subagent.thinking.delta" in kinds
    deltas = [
        e.payload.delta
        for e in bus.run_events("r1")
        if e.kind == "subagent.thinking.delta"
    ]
    assert deltas == ["hmm"]


async def test_run_completed_reports_cumulative_usage_not_segment() -> None:
    # 多段 run 少报回归钉：终态 token_usage 取 record_usage 返回的跨段累计，而非本段测量。
    async def preloaded_recorder(
        input_tokens: int, output_tokens: int
    ) -> tuple[int, int]:
        return (30 + input_tokens, 3 + output_tokens)  # 模拟前段已入账 30/3

    bus = FakeBus()
    emitter = await RunEmitter.attach(bus, "racc")
    await invoke_once(
        emitter,
        (settlement_agent := FakeAgent(run=text_run("hi"))),
        "c1",
        {"messages": []},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(emitter, _always_claim, preloaded_recorder),
        record_usage=preloaded_recorder,
        on_native_settled=settled_state_callback(settlement_agent, "c1"),
    )
    completed = find_event(bus.run_events("racc"), RunCompleted)
    assert completed.payload.token_usage is not None
    assert completed.payload.token_usage.input_tokens == 30
    assert completed.payload.token_usage.output_tokens == 3


async def test_pause_segment_records_usage_too() -> None:
    # 暂停段也入账：不然恢复后终态只累计后段。
    calls: list[tuple[int, int]] = []

    async def recorder(input_tokens: int, output_tokens: int) -> tuple[int, int]:
        calls.append((input_tokens, output_tokens))
        return (input_tokens, output_tokens)

    bus = FakeBus()
    agent = FakeAgent(
        run=FakeRunStream(is_interrupted=True),
        state=FakeState(interrupts=()),
    )
    emitter = await RunEmitter.attach(bus, "rpause")
    terminal = await invoke_once(
        emitter,
        agent,
        "c1",
        {"messages": []},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(emitter, _always_claim, recorder),
        record_usage=recorder,
        on_native_settled=settled_state_callback(agent, "c1"),
    )
    assert terminal is False
    assert len(calls) == 1  # 暂停路径恰好入账一次


def test_failure_code_classification_matrix() -> None:
    # 归码闭集：预算/熔断各归其码，其余兜底 internal_error；装配码由调用点显式指定。
    from langgraph.errors import GraphRecursionError

    from kokoro_agent.execution.failures import failure_code, run_failed_payload
    from kokoro_agent.tools.middleware import TokenBudgetExceeded

    assert failure_code(TokenBudgetExceeded("over budget")) == "token_budget_exceeded"
    assert failure_code(GraphRecursionError("loop")) == "recursion_limit_exceeded"
    assert failure_code(ValueError("x")) == "internal_error"
    assert (
        run_failed_payload(ValueError("x"), code="assembly_failed").code
        == "assembly_failed"
    )


def test_failure_classification_has_a_dedicated_execution_module_owner() -> None:
    root = Path(__file__).resolve().parents[3]
    failures_path = root / "src" / "kokoro_agent" / "execution" / "failures.py"
    events_path = root / "src" / "kokoro_agent" / "execution" / "events.py"
    assert failures_path.is_file(), "failure classification must move out of events.py"

    def module_structure(
        path: Path,
    ) -> tuple[set[str], set[tuple[str, str, str]]]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names: set[str] = set()
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(node.name)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names.add(node.target.id)
            elif isinstance(node, ast.Assign):
                names.update(
                    target.id for target in node.targets if isinstance(target, ast.Name)
                )
        imports: set[tuple[str, str, str]] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = "." * node.level + (node.module or "")
                imports.update(
                    (module, alias.name, alias.asname or alias.name)
                    for alias in node.names
                )
            elif isinstance(node, ast.Import):
                imports.update(
                    (
                        alias.name,
                        alias.name.rsplit(".", 1)[-1],
                        alias.asname or alias.name.split(".", 1)[0],
                    )
                    for alias in node.names
                )
        return names, imports

    owned = {"_MODEL_FAILURE_CODES", "failure_code", "run_failed_payload"}
    failure_definitions, _ = module_structure(failures_path)
    event_definitions, event_imports = module_structure(events_path)
    assert owned <= failure_definitions
    assert owned.isdisjoint(event_definitions)
    assert not any(
        module.endswith("failures") or source in owned or binding in owned
        for module, source, binding in event_imports
    ), "events.py must not retain compatibility aliases or a second failure owner"


@pytest.mark.parametrize("phase", ["active", "waiting", "unknown"])
async def test_native_settlement_is_after_context_drain_before_terminal(
    phase: Literal["active", "waiting", "unknown"],
) -> None:
    order: list[str] = []
    agent = FakeAgent(run=text_run("done"))
    bus = FakeBus()
    emitter = await RunEmitter.attach(bus, "settled-order")

    async def settle(
        seal_usage: Callable[[], Awaitable[tuple[int, int]]],
    ) -> Literal["active", "waiting", "unknown"]:
        assert agent.run.context_exited
        order.append("settled")
        if phase != "active":
            await seal_usage()
        return phase

    async def finalize(
        payload: RunCompletedPayload | RunFailedPayload, usage: tuple[int, int]
    ) -> bool:
        assert order == ["settled"]
        order.append("terminal")
        return await terminal_emitter(emitter, _always_claim, usage_recorder()[0])(
            payload, usage
        )

    completed = await invoke_once(
        emitter,
        agent,
        "c1",
        {},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=finalize,
        record_usage=usage_recorder()[0],
        on_native_settled=settle,
    )
    assert completed is (phase == "active")
    assert order == (["settled", "terminal"] if phase == "active" else ["settled"])
    assert ("run.completed" in bus.kinds("settled-order")) is (phase == "active")
    assert "run.failed" not in bus.kinds("settled-order")


async def test_settlement_persistence_failure_propagates_without_second_terminal() -> (
    None
):
    agent = FakeAgent(run=text_run("done"))
    bus = FakeBus()
    emitter = await RunEmitter.attach(bus, "settlement-failed")
    failure = RuntimeError("repository unavailable")

    async def settle(
        seal_usage: Callable[[], Awaitable[tuple[int, int]]],
    ) -> Literal["active", "waiting", "unknown"]:
        assert agent.run.context_exited
        await seal_usage()
        raise failure

    with pytest.raises(RuntimeError) as caught:
        await invoke_once(
            emitter,
            agent,
            "c1",
            {},
            approval_tool_names=frozenset(),
            source_for=_runtime_custom,
            finalize_terminal=terminal_emitter(
                emitter, _always_claim, usage_recorder()[0]
            ),
            record_usage=usage_recorder()[0],
            on_native_settled=settle,
        )
    assert caught.value is failure
    assert "run.failed" not in bus.kinds("settlement-failed")
    assert "run.completed" not in bus.kinds("settlement-failed")


@pytest.mark.parametrize("native_interrupted", [True, False])
@pytest.mark.parametrize("phase", ["waiting", "unknown"])
async def test_waiting_settlement_seals_usage_before_releasing_lease(
    native_interrupted: bool,
    phase: Literal["waiting", "unknown"],
) -> None:
    agent = FakeAgent(run=FakeRunStream(is_interrupted=native_interrupted))
    bus = FakeBus()
    emitter = await RunEmitter.attach(bus, "waiting-usage-order")
    writes: list[tuple[int, int]] = []
    released = False

    async def record_usage(input_tokens: int, output_tokens: int) -> tuple[int, int]:
        assert not released, "the real repository rejects usage after lease release"
        writes.append((input_tokens, output_tokens))
        return input_tokens, output_tokens

    async def settle(
        seal_usage: Callable[[], Awaitable[tuple[int, int]]],
    ) -> Literal["active", "waiting", "unknown"]:
        nonlocal released
        assert agent.run.context_exited
        assert await asyncio.gather(seal_usage(), seal_usage()) == [(0, 0), (0, 0)]
        assert await seal_usage() == (0, 0)
        assert writes == [(0, 0)], "seal this segment before committing waiting"
        released = True
        return phase

    terminal = await invoke_once(
        emitter,
        agent,
        "c1",
        {},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(emitter, _always_claim, record_usage),
        record_usage=record_usage,
        on_native_settled=settle,
    )
    assert terminal is False and released
    assert writes == [(0, 0)]
    assert "run.completed" not in bus.kinds("waiting-usage-order")
    assert "run.failed" not in bus.kinds("waiting-usage-order")


async def test_usage_seal_failure_propagates_without_settlement_or_terminal() -> None:
    agent = FakeAgent(run=FakeRunStream(is_interrupted=False))
    bus = FakeBus()
    emitter = await RunEmitter.attach(bus, "usage-seal-failed")
    failure = RuntimeError("usage transaction failed")
    settled = False
    attempts = 0

    async def record_usage(_input: int, _output: int) -> tuple[int, int]:
        nonlocal attempts
        attempts += 1
        raise failure

    async def settle(
        seal_usage: Callable[[], Awaitable[tuple[int, int]]],
    ) -> Literal["active", "waiting", "unknown"]:
        nonlocal settled
        assert agent.run.context_exited
        await seal_usage()
        settled = True
        return "waiting"

    with pytest.raises(RuntimeError) as caught:
        await invoke_once(
            emitter,
            agent,
            "c1",
            {},
            approval_tool_names=frozenset(),
            source_for=_runtime_custom,
            finalize_terminal=terminal_emitter(emitter, _always_claim, record_usage),
            record_usage=record_usage,
            on_native_settled=settle,
        )
    assert caught.value is failure
    assert attempts == 1 and not settled
    assert "run.completed" not in bus.kinds("usage-seal-failed")
    assert "run.failed" not in bus.kinds("usage-seal-failed")


async def test_interrupted_active_reader_keeps_usage_without_terminal() -> None:
    agent = FakeAgent(run=FakeRunStream(is_interrupted=True))
    bus = FakeBus()
    emitter = await RunEmitter.attach(bus, "interrupted-active")
    totals: list[tuple[int, int]] = []

    async def record_usage(input_tokens: int, output_tokens: int) -> tuple[int, int]:
        totals.append((input_tokens, output_tokens))
        return input_tokens, output_tokens

    async def settle(
        seal_usage: Callable[[], Awaitable[tuple[int, int]]],
    ) -> Literal["active", "waiting", "unknown"]:
        assert agent.run.context_exited
        assert totals == []
        return "active"

    terminal = await invoke_once(
        emitter,
        agent,
        "c1",
        {},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        finalize_terminal=terminal_emitter(emitter, _always_claim, record_usage),
        record_usage=record_usage,
        on_native_settled=settle,
    )
    assert terminal is False
    assert totals == [(0, 0)]
    assert "run.completed" not in bus.kinds("interrupted-active")
    assert "run.failed" not in bus.kinds("interrupted-active")


@pytest.fixture(autouse=True)
def isolate_sdk_profile_side_effects() -> Iterator[None]:
    with isolated_native_registry():
        yield
