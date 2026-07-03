"""invoke/pump/emit 规格：三路终态、投影合流顺序、index 单调、usage 聚合、哨兵收束。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler, CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Interrupt

from fakes import (
    FakeAgent,
    FakeBus,
    FakeModel,
    FakeRunStream,
    FakeState,
    FakeSubagentRun,
    FakeToolCall,
    find_event,
    text_model,
    text_run,
)
from kokoro_agent.contract import (
    RUN_EVENTS_MAXLEN,
    MessageCompleted,
    RunCompleted,
    RunFailed,
    SubagentFinished,
    SubagentSource,
    SubagentStarted,
    ToolAwaitingApproval,
    ToolAwaitingApprovalPayload,
    ToolInvoked,
    ToolInvokedPayload,
    ToolReturned,
    ToolReturnedPayload,
)
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.streams.protocol import StreamProtocol


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
        claim_terminal=claim,
        trace=trace,
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


async def test_tool_events_inherit_awaiting_segment() -> None:
    # resume 后 invoked/returned 只有 tool_call_id 兜底段：必须继承 awaiting 的 AIMessage 段（真栈走查根治）。
    bus = MemoryStream()
    emitter = RunEmitter(bus, "rn")
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
    resumed = await RunEmitter.attach(bus, "rn")
    await resumed.emit(ToolInvokedPayload(segment_id="t1", tool_id="t1", name="write_file", args={}))
    await resumed.emit(
        ToolReturnedPayload(
            segment_id="t1", tool_id="t1", name="write_file", result="ok", is_error=False,
            rejected=None, reject_reason=None, responded=None, artifact_ref=None, summary=None,
        )
    )
    items = await bus.read_all("kokoro:run:rn:events")
    segs: list[str] = []
    for item in items[-2:]:
        payload = item.event["payload"]
        assert isinstance(payload, dict)
        seg = payload["segment_id"]
        assert isinstance(seg, str)
        segs.append(seg)
    assert segs == ["seg_msg", "seg_msg"]


async def test_optional_none_fields_never_serialize_as_null() -> None:
    # 契约 optional 字段的 None 即缺席：null 上 wire 会被 session zod .optional() 拒收（跨栈 e2e 抓获的真实缺陷）。
    bus = MemoryStream()
    emitter = RunEmitter(bus, "rn")
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
    items = await bus.read_all("kokoro:run:rn:events")
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


async def test_empty_text_frames_skipped() -> None:
    # tool-only 段：output_message.text==""，不发空 message.delta / message.completed。
    model = FakeModel(text_deltas=(), output_message=AIMessage(content="", id="seg"))
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(models=(model,))))
    kinds = bus.kinds("r1")
    assert "message.delta" not in kinds
    assert "message.completed" not in kinds


async def test_tool_invoked_and_returned() -> None:
    tool = FakeToolCall(tool_call_id="t1", tool_name="lookup", input={"q": "x"}, output="found")
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
    tool = FakeToolCall(tool_call_id="t1", tool_name="lookup", input={}, error="exploded")
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tool,))))
    returned = find_event(bus.run_events("r1"), ToolReturned)
    assert returned.payload.is_error is True
    assert returned.payload.result == "exploded"


async def test_todo_tool_becomes_todo_updated() -> None:
    todos: list[dict[str, object]] = [{"content": "plan", "status": "pending"}]
    tool = FakeToolCall(tool_call_id="t1", tool_name="write_todos", input={"todos": todos})
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tool,))))
    kinds = bus.kinds("r1")
    assert "todo.updated" in kinds
    assert "tool.invoked" not in kinds


async def test_malformed_todos_fail_loud_as_run_failed() -> None:
    # LLM 产出的 todos 是不可信载荷：strict 洗净失败收口为 run.failed，不发脏 wire。
    tool = FakeToolCall(
        tool_call_id="t1", tool_name="write_todos", input={"todos": [{"content": "", "status": "?"}]}
    )
    bus = FakeBus()
    done = await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tool,))))
    assert done is True
    assert bus.kinds("r1")[-1] == "run.failed"


async def test_task_tool_not_double_emitted() -> None:
    tool = FakeToolCall(tool_call_id="t1", tool_name="task", input={"description": "go"})
    sub = FakeSubagentRun(trigger_call_id="t1")
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tool,), subagent_runs=(sub,))))
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
    # 契约无子代理内工具通道：抽干弃置，不冒充顶层工具。
    assert "tool.invoked" not in kinds
    started = find_event(events, SubagentStarted)
    assert started.payload.subagent_id == "sub-1"
    assert started.payload.source == "runtime-custom"


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
        content="", id="seg-1", tool_calls=[{"name": "danger", "args": {"x": 1}, "id": "call-A"}]
    )
    interrupt = Interrupt(
        value={
            "action_requests": [{"name": "danger", "args": {"x": 1}, "description": "do danger"}],
            "review_configs": [
                {"action_name": "danger", "allowed_decisions": ["approve", "edit", "reject"]}
            ],
        }
    )
    return FakeState(interrupts=(interrupt,), values={"messages": [HumanMessage(content="go"), ai]})


async def test_pending_interrupt_emits_awaiting_no_terminal() -> None:
    bus = FakeBus()
    agent = FakeAgent(run=FakeRunStream(is_interrupted=True), state=_interrupt_state())
    done = await _invoke(bus, agent, approval_tool_names=frozenset({"danger"}))
    assert done is False
    kinds = bus.kinds("r1")
    assert kinds[-1] == "tool.awaiting_approval"
    assert "run.completed" not in kinds
    awaiting = find_event(bus.run_events("r1"), ToolAwaitingApproval)
    assert awaiting.payload.tool_id == "call-A"
    assert awaiting.payload.pending_tool_ids == ["call-A"]


async def test_exception_emits_run_failed() -> None:
    bus = FakeBus()
    done = await _invoke(bus, FakeAgent(raise_on_stream=ValueError("boom")))
    assert done is True
    assert bus.kinds("r1") == ["run.started", "run.failed"]
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert failed.payload.error_kind == "ValueError"
    assert failed.payload.message == "boom"


async def test_empty_exception_message_falls_back_to_kind() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(raise_on_stream=RuntimeError()))
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert failed.payload.message == "RuntimeError"


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
    assert agent.seen_config.get("metadata") == {"langfuse_session_id": "s1", "kokoro_run_id": "r1"}


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
