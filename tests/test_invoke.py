"""invoke/pump/emit 规格：三路终态、投影合流顺序、index 单调、usage 聚合、哨兵收束。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any
from uuid import uuid4

import pytest
from langchain_core.callbacks import BaseCallbackHandler, CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import StructuredTool
from langgraph.types import Interrupt
from pydantic import BaseModel as PydanticBaseModel, JsonValue

from fakes import (
    FakeAgent,
    FakeBus,
    FakeLedger,
    FakeModel,
    FakeRunStream,
    FakeState,
    FakeSubagentRun,
    FakeToolCall,
    find_event,
    find_events,
    text_model,
    text_run,
    completed_execution_context,
    usage_recorder,
    request,
)
from kokoro_agent.contract import (
    RUN_EVENTS_MAXLEN,
    MessageCompleted,
    PlanProposed,
    PlanProposal,
    PlanProposedPayload,
    PlanStep,
    RunCompleted,
    RunFailed,
    RunStartedPayload,
    SubagentFinished,
    SubagentSource,
    SubagentStarted,
    SubagentToolInvoked,
    SubagentToolReturned,
    ToolAwaitingApproval,
    ToolAwaitingApprovalPayload,
    ToolInvoked,
    ToolInvokedPayload,
    ToolReturned,
    ToolReturnedPayload,
)
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.execution.events import RunEmitter, clip_result, tool_returned_payload
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.contract.streams import run_events_stream
from kokoro_agent.streams.protocol import StreamItem, StreamProtocol
from kokoro_agent.streams.redis import RedisStream


def _runtime_custom(_name: str) -> SubagentSource:
    return "runtime-custom"


async def _always_claim() -> bool:
    return True


async def _invoke(
    bus: FakeBus,
    agent: InvokableAgent,
    run_id: str = "r1",
    *,
    approval_tool_names: frozenset[str] = frozenset(),
    claim: Callable[[], Awaitable[bool]] | None = None,
    trace: RunnableConfig | None = None,
    ledger: FakeLedger | None = None,
) -> bool:
    ledger = ledger or FakeLedger()
    await ledger.try_claim(request(run_id))
    emitter = await RunEmitter.attach(bus, run_id, outbox=ledger)
    return await invoke_once(
        emitter,
        agent,
        {
            "configurable": {"thread_id": "c1"},
            "metadata": {"kokoro_run_id": run_id},
        },
        {"messages": []},
        approval_tool_names=approval_tool_names,
        source_for=_runtime_custom,
        claim_terminal=claim or (lambda: ledger.try_mark_terminal(run_id)),
        prepare_completed=lambda: completed_execution_context(run_id),
        record_usage=usage_recorder()[0],
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
    assert kinds[-2:] == ["run.owner.completed", "run.completed"]


async def test_owner_publish_loss_keeps_both_slots_queued_for_causal_replay() -> None:
    class OwnerFailingBus(FakeBus):
        async def publish(
            self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
        ) -> StreamItem:
            if event.get("kind") == "run.owner.completed":
                raise ConnectionError("owner publish lost")
            return await super().publish(stream, event, maxlen=maxlen)

    bus = OwnerFailingBus()
    ledger = FakeLedger()
    done = await _invoke(bus, FakeAgent(run=text_run("hello")), ledger=ledger)

    assert done is True
    assert "run.completed" not in bus.kinds("r1")
    owner = next(row for row in ledger.outbox["r1"] if row["kind"] == "run.owner.completed")
    terminal = next(row for row in ledger.outbox["r1"] if row["kind"] == "run.completed")
    assert owner["status"] == terminal["status"] == "queued"
    owner_seq = owner["durable_seq"]
    terminal_seq = terminal["durable_seq"]
    assert isinstance(owner_seq, int) and isinstance(terminal_seq, int)
    assert owner_seq + 1 == terminal_seq


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
    await resumed.emit(ToolInvokedPayload(segment_id="t1", tool_id="t1", name="write_file", args={}))
    await resumed.emit(
        ToolReturnedPayload(
            segment_id="t1", tool_id="t1", name="write_file", result="ok", is_error=False,
            rejected=None, reject_reason=None, responded=None, summary=None,
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


async def test_optional_none_fields_never_serialize_as_null(stream: RedisStream) -> None:
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
    inner_task = FakeToolCall(tool_call_id="nt1", tool_name="task", input={"description": "go"})
    sub = FakeSubagentRun(trigger_call_id="sub-1", tool_views=(inner_task,))
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    kinds = bus.kinds("r1")
    assert "subagent.tool.invoked" not in kinds and "subagent.tool.returned" not in kinds


async def test_failed_subagent_flagged() -> None:
    sub = FakeSubagentRun(status="failed")
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    finished = find_event(bus.run_events("r1"), SubagentFinished)
    assert finished.payload.failed is True


async def test_custom_channel_drained_without_wire_events() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(custom_items=({"telemetry": 1},))))
    assert bus.kinds("r1") == ["run.started", "run.owner.completed", "run.completed"]


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


def _plan_interrupt_state(*, mixed: bool = False) -> FakeState:
    tool_calls: list[dict[str, Any]] = [
        {
            "name": "propose_plan",
            "args": {
                "summary": "Ship the audited change",
                "steps": [{"label": "Inspect"}, {"label": "Implement"}],
            },
            "id": "call-plan-A",
        }
    ]
    actions: list[dict[str, Any]] = [
        {
            "name": "propose_plan",
            "args": tool_calls[0]["args"],
            "description": "propose a plan",
        }
    ]
    configs: list[dict[str, Any]] = [
        {"action_name": "propose_plan", "allowed_decisions": ["approve", "reject"]}
    ]
    if mixed:
        tool_calls.append({"name": "danger", "args": {"x": 1}, "id": "call-danger"})
        actions.append({"name": "danger", "args": {"x": 1}, "description": "do danger"})
        configs.append(
            {
                "action_name": "danger",
                "allowed_decisions": ["approve", "edit", "reject"],
            }
        )
    ai = AIMessage(content="", id="seg-plan", tool_calls=tool_calls)
    interrupt = Interrupt(value={"action_requests": actions, "review_configs": configs})
    return FakeState(interrupts=(interrupt,), values={"messages": [HumanMessage(content="go"), ai]})


async def test_plan_interrupt_emits_dedicated_owner_without_generic_awaiting() -> None:
    bus = FakeBus()
    agent = FakeAgent(run=FakeRunStream(is_interrupted=True), state=_plan_interrupt_state())
    done = await _invoke(bus, agent, approval_tool_names=frozenset({"propose_plan"}))

    assert done is False
    assert bus.kinds("r1") == ["run.started", "plan.proposed"]
    proposed = find_event(bus.run_events("r1"), PlanProposed)
    assert proposed.payload.segment_id == "seg-plan"
    assert proposed.payload.owner_ref == "call-plan-A"
    assert proposed.payload.owner_version == 1
    assert proposed.payload.proposal.summary == "Ship the audited change"
    assert [step.label for step in proposed.payload.proposal.steps] == ["Inspect", "Implement"]
    assert len({step.step_ref for step in proposed.payload.proposal.steps}) == 2
    assert {step.status for step in proposed.payload.proposal.steps} == {"pending"}
    assert proposed.payload.proposal.allowed_actions == ["accept", "reject"]


async def test_plan_interrupt_mixed_with_another_tool_fails_loud() -> None:
    bus = FakeBus()
    agent = FakeAgent(run=FakeRunStream(is_interrupted=True), state=_plan_interrupt_state(mixed=True))
    done = await _invoke(
        bus,
        agent,
        approval_tool_names=frozenset({"propose_plan", "danger"}),
    )

    assert done is True
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert "only tool call" in failed.payload.message


async def test_plan_model_guard_blocks_nonapproval_side_effect_in_same_frame() -> None:
    from kokoro_agent.tools.permissions import build_interrupt_on
    from kokoro_agent.tools.propose_plan import PROPOSE_PLAN_TOOL, PROPOSE_PLAN_TOOL_NAME
    from kokoro_agent.tools.middleware import PlanProposalCallGuardMiddleware

    called = 0

    class SideEffectArgs(PydanticBaseModel):
        value: str

    def side_effect(value: str) -> str:
        nonlocal called
        called += 1
        return value

    side_effect_tool = StructuredTool(
        name="side_effect",
        description="mutates external state",
        args_schema=SideEffectArgs,
        func=side_effect,
    )
    model = LocalFakeChatModel.with_script(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": PROPOSE_PLAN_TOOL_NAME,
                        "args": {"summary": "Ship", "steps": [{"label": "Implement"}]},
                        "id": "call-plan",
                        "type": "tool_call",
                    },
                    {
                        "name": "side_effect",
                        "args": {"value": "mutated"},
                        "id": "call-side",
                        "type": "tool_call",
                    },
                ],
            )
        ]
    )
    agent = build_agent(
        model=model,
        tools=[PROPOSE_PLAN_TOOL, side_effect_tool],
        system_prompt="x",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on=build_interrupt_on(
            frozenset(), plan_tools=frozenset({PROPOSE_PLAN_TOOL_NAME})
        ),
        middleware=[PlanProposalCallGuardMiddleware()],
    )
    bus = FakeBus()

    done = await _invoke(
        bus,
        agent,
        approval_tool_names=frozenset({PROPOSE_PLAN_TOOL_NAME}),
    )

    assert done is True
    assert called == 0
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert "only tool call" in failed.payload.message


async def test_nested_plan_interrupt_cannot_mint_a_synthetic_owner() -> None:
    task = AIMessage(
        content="",
        id="seg-task",
        tool_calls=[
            {
                "name": "task",
                "args": {"description": "delegate", "subagent_type": "general-purpose"},
                "id": "call-task",
            }
        ],
    )
    interrupt = Interrupt(
        value={
            "action_requests": [
                {
                    "name": "propose_plan",
                    "args": {"summary": "Nested", "steps": [{"label": "Do it"}]},
                    "description": "nested plan",
                }
            ],
            "review_configs": [
                {
                    "action_name": "propose_plan",
                    "allowed_decisions": ["approve", "reject"],
                }
            ],
        }
    )
    state = FakeState(
        interrupts=(interrupt,), values={"messages": [HumanMessage(content="go"), task]}
    )
    bus = FakeBus()
    agent = FakeAgent(run=FakeRunStream(is_interrupted=True), state=state)

    done = await _invoke(bus, agent, approval_tool_names=frozenset({"propose_plan"}))

    assert done is True
    assert "plan.proposed" not in bus.kinds("r1")
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert "main agent" in failed.payload.message


async def test_plan_proposal_critical_identity_is_semantically_idempotent() -> None:
    bus = FakeBus()
    ledger = FakeLedger()
    await ledger.try_claim(request("r-plan"))
    emitter = RunEmitter(bus, "r-plan", outbox=ledger)
    payload = PlanProposedPayload(
        segment_id="seg-plan",
        owner_ref="call-plan-A",
        owner_version=1,
        proposal=PlanProposal(
            summary="Ship it",
            steps=[PlanStep(step_ref="pstep_1", label="Implement", status="pending")],
            allowed_actions=["accept", "reject"],
        ),
    )

    await emitter.emit(payload)
    await emitter.emit(payload)

    proposed = find_events(bus.run_events("r-plan"), PlanProposed)
    assert len(proposed) == 1
    assert proposed[0].durable_seq == 1
    assert proposed[0].event_id == "evt_fake_r-plan_1"
    assert ledger.durable_counter["r-plan"] == 1

    with pytest.raises(ValueError, match="semantic critical frame conflict"):
        await emitter.emit(
            payload.model_copy(
                update={
                    "proposal": payload.proposal.model_copy(update={"summary": "Changed"})
                }
            )
        )


async def test_exception_emits_run_failed() -> None:
    bus = FakeBus()
    done = await _invoke(bus, FakeAgent(raise_on_stream=ValueError("boom")))
    assert done is True
    assert bus.kinds("r1") == ["run.started", "run.failed"]
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert failed.payload.error_kind == "ValueError"
    assert failed.payload.message == "boom"
    assert failed.payload.code == "internal_error"  # 未归类异常兜底码


async def test_empty_exception_message_falls_back_to_kind() -> None:
    bus = FakeBus()
    await _invoke(bus, FakeAgent(raise_on_stream=RuntimeError()))
    failed = find_event(bus.run_events("r1"), RunFailed)
    assert failed.payload.message == "RuntimeError"


async def test_completion_claim_lost_suppresses_terminal() -> None:
    bus = FakeBus()
    ledger = FakeLedger()
    await ledger.try_mark_terminal("r1")
    done = await _invoke(bus, FakeAgent(run=text_run("hi")), ledger=ledger)
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
    assert agent.seen_config.get("metadata") == {"kokoro_run_id": "r1"}


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
            tool_calls=[{"name": "noop", "args": {}, "id": f"loop{calls}", "type": "tool_call"}],
        )
        return ChatResult(generations=[ChatGeneration(message=message)])


async def test_runaway_loop_hits_recursion_limit_and_fails_loud(stream: RedisStream) -> None:
    noop = StructuredTool(
        name="noop", description="no-op", args_schema=_NoopArgs, func=lambda: "ok"
    )
    agent = build_agent(
        model=_LoopingModel.with_script([]),
        tools=[noop],
        system_prompt="x",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on={},
    )
    run_id = f"rloop-{uuid4().hex}"

    async def claim() -> bool:
        return True

    terminal = await invoke_once(
        RunEmitter(stream, run_id),
        agent,
        {"configurable": {"thread_id": "tloop"}, "metadata": {"kokoro_run_id": run_id}},
        {"messages": [HumanMessage(content="go")]},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        claim_terminal=claim,
        prepare_completed=lambda: completed_execution_context(run_id),
        record_usage=usage_recorder()[0],
        recursion_limit=8,
    )
    assert terminal is True
    events = [item.event for item in await stream.read_all(run_events_stream(run_id))]
    assert events[-1]["kind"] == "run.failed"
    payload = events[-1]["payload"]
    assert isinstance(payload, dict)
    assert "Recursion" in str(payload.get("error_kind"))
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
    tc = FakeToolCall(tool_call_id="t1", tool_name="execute", output="done", deltas=("line1\n", "line2\n"))
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tc,))))
    kinds = bus.kinds("r1")
    assert kinds.index("tool.invoked") < kinds.index("tool.output.delta") < kinds.index("tool.returned")
    deltas = [e.payload.delta for e in bus.run_events("r1") if e.kind == "tool.output.delta"]
    assert deltas == ["line1\n", "line2\n"]


async def test_tool_output_stream_budget_clips_silently() -> None:
    tc = FakeToolCall(
        tool_call_id="t1", tool_name="execute", output="done",
        deltas=("a" * 3000, "b" * 3000, "c" * 3000),
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(tool_views=(tc,))))
    deltas = [e.payload.delta for e in bus.run_events("r1") if e.kind == "tool.output.delta"]
    assert sum(len(d) for d in deltas) == 4000  # TOOL_RESULT_MAX_CHARS 预算截停
    assert bus.kinds("r1")[-1] == "run.completed"


async def test_subagent_thinking_streams_on_wire() -> None:
    sub = FakeSubagentRun(
        models=(FakeModel(text_deltas=("hi",), reasoning_deltas=("hmm",), output_message=AIMessage(content="hi", id="s")),),
        name="poet", trigger_call_id="sub1",
    )
    bus = FakeBus()
    await _invoke(bus, FakeAgent(run=FakeRunStream(subagent_runs=(sub,))))
    kinds = bus.kinds("r1")
    assert "subagent.thinking.delta" in kinds
    deltas = [e.payload.delta for e in bus.run_events("r1") if e.kind == "subagent.thinking.delta"]
    assert deltas == ["hmm"]


async def test_run_completed_reports_cumulative_usage_not_segment() -> None:
    # 多段 run 少报回归钉：终态 token_usage 取 record_usage 返回的跨段累计，而非本段测量。
    async def preloaded_recorder(input_tokens: int, output_tokens: int) -> tuple[int, int]:
        return (30 + input_tokens, 3 + output_tokens)  # 模拟前段已入账 30/3

    bus = FakeBus()
    ledger = FakeLedger()
    emitter = await RunEmitter.attach(bus, "racc", outbox=ledger)
    await invoke_once(
        emitter,
        FakeAgent(run=text_run("hi")),
        {"configurable": {"thread_id": "c1"}, "metadata": {"kokoro_run_id": "racc"}},
        {"messages": []},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        claim_terminal=_always_claim,
        prepare_completed=lambda: completed_execution_context("racc"),
        record_usage=preloaded_recorder,
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
        {"configurable": {"thread_id": "c1"}, "metadata": {"kokoro_run_id": "rpause"}},
        {"messages": []},
        approval_tool_names=frozenset(),
        source_for=_runtime_custom,
        claim_terminal=_always_claim,
        prepare_completed=lambda: completed_execution_context("rpause"),
        record_usage=recorder,
    )
    assert terminal is False
    assert len(calls) == 1  # 暂停路径恰好入账一次


def test_failure_code_classification_matrix() -> None:
    # 归码闭集：预算/熔断各归其码，其余兜底 internal_error；装配码由调用点显式指定。
    from langgraph.errors import GraphRecursionError

    from kokoro_agent.execution.events import failure_code, run_failed_payload
    from kokoro_agent.tools.middleware import TokenBudgetExceeded

    assert failure_code(TokenBudgetExceeded("over budget")) == "token_budget_exceeded"
    assert failure_code(GraphRecursionError("loop")) == "recursion_limit_exceeded"
    assert failure_code(ValueError("x")) == "internal_error"
    assert run_failed_payload(ValueError("x"), code="assembly_failed").code == "assembly_failed"
