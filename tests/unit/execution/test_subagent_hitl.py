"""子代理内 HITL 规格：deepagents 把 interrupt_on 下发子图；本仓帧构造须回退嵌套帧
（合成稳定 tool_id + task 段归属）而非 fail-loud——修复真缺陷 C′ 的回归钉。"""

from __future__ import annotations

from uuid import uuid4

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.types import Command
from pydantic import BaseModel, ConfigDict

from support.fakes import usage_recorder
from support.deepagents import create_test_deep_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.contract.streams import run_events_stream
from support.local_fake import LocalFakeChatModel
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.tools.permissions import build_interrupt_on

_EXECUTED = {"n": 0}


class _NoArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


def _gated() -> str:
    _EXECUTED["n"] += 1
    return "gated-ok"


def _build(saver: BaseCheckpointSaver[str]):
    gate_tool = StructuredTool(name="gated", description="d", args_schema=_NoArgs, func=_gated)
    main_model = LocalFakeChatModel.with_script([
        AIMessage(content="", tool_calls=[{
            "name": "task", "args": {"description": "do", "subagent_type": "helper"},
            "id": "t1", "type": "tool_call"}]),
        AIMessage(content="main done"),
    ])
    sub_model = LocalFakeChatModel.with_script([
        AIMessage(content="", tool_calls=[{"name": "gated", "args": {}, "id": "g1", "type": "tool_call"}]),
        AIMessage(content="sub done"),
    ])
    return create_test_deep_agent(
        model=main_model,
        tools=[gate_tool],
        system_prompt="x",
        subagents=[{"name": "helper", "description": "h", "system_prompt": "s",
                    "model": sub_model, "tools": [gate_tool]}],
        checkpointer=saver,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset({"gated"})),
    )


async def test_subagent_approval_pauses_then_approve_completes(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    _EXECUTED["n"] = 0
    saver = checkpointer
    sfx = uuid4().hex
    run1, run2 = f"rsub-{sfx}", f"rsub2-{sfx}"
    names = frozenset({"gated", "ask_user_question"})

    async def claim() -> bool:
        return True

    recorder, _seen = usage_recorder()
    first = await invoke_once(
        RunEmitter(stream, run1),
        _build(saver),
        "tsub",
        {"messages": [HumanMessage(content="go", id="m1")]},
        approval_tool_names=names,
        source_for=lambda _n: "built-in",
        claim_terminal=claim,
        record_usage=recorder,
    )
    assert first is False  # 暂停成卡，而非 run.failed
    assert _EXECUTED["n"] == 0  # 审批前工具未执行（无旁路）
    events = [item.event for item in await stream.read_all(run_events_stream(run1))]
    awaiting = [e for e in events if e["kind"] == "tool.awaiting_approval"]
    assert len(awaiting) == 1
    payload = awaiting[0]["payload"]
    assert isinstance(payload, dict)
    assert payload["name"] == "gated"
    assert payload["kind"] == "tool_approval"
    assert payload["segment_id"] == "t1"  # 归属触发委派的 task 调用
    tool_id = payload["tool_id"]
    assert isinstance(tool_id, str) and ":" in tool_id  # interrupt.id 派生的合成稳定 id

    # 重读快照重建帧：合成 id 必须稳定（resume 对齐依据）。
    agent2 = _build(saver)
    second = await invoke_once(
        RunEmitter(stream, run2),
        agent2,
        "tsub",
        Command(resume={"decisions": [{"type": "approve"}]}),
        approval_tool_names=names,
        source_for=lambda _n: "built-in",
        claim_terminal=claim,
        record_usage=recorder,
    )
    assert second is True
    assert _EXECUTED["n"] == 1  # 批准后在子代理内执行恰一次
    events2 = [item.event for item in await stream.read_all(run_events_stream(run2))]
    assert events2[-1]["kind"] == "run.completed"


async def test_general_purpose_delegation_runs_inside_guards(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    # 内生 GP 旁路收口回归钉：唯一的 TerminalGuard 只挂在我们覆盖的 general-purpose spec 上，
    # 账本已终态 → 委派进 GP 的首个模型轮必被熔断（RunSupersededError 出自 GP 子图内）。
    from support.fakes import FakeRunRepository
    from kokoro_agent.agents.subagents import general_purpose_subagent
    from kokoro_agent.tools.middleware import TerminalGuardMiddleware

    run_id = f"rgp-{uuid4().hex}"
    run_repository = FakeRunRepository()
    assert await run_repository.try_mark_terminal(run_id)
    guard = TerminalGuardMiddleware(run_repository=run_repository, run_id=run_id)
    main_model = LocalFakeChatModel.with_script([
        AIMessage(content="", tool_calls=[{
            "name": "task", "args": {"description": "do", "subagent_type": "general-purpose"},
            "id": "t1", "type": "tool_call"}]),
        AIMessage(content="main done"),
    ])
    agent = create_test_deep_agent(
        model=main_model,
        tools=[],
        system_prompt="x",
        subagents=[general_purpose_subagent([guard])],
        checkpointer=checkpointer,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
    )

    async def claim() -> bool:
        return True

    recorder, _seen = usage_recorder()
    terminal = await invoke_once(
        RunEmitter(stream, run_id),
        agent,
        "tgp",
        {"messages": [HumanMessage(content="go", id="m1")]},
        approval_tool_names=frozenset({"ask_user_question"}),
        source_for=lambda _n: "built-in",
        claim_terminal=claim,
        record_usage=recorder,
    )
    assert terminal is True
    events = [item.event for item in await stream.read_all(run_events_stream(run_id))]
    kinds = [e["kind"] for e in events]
    # 委派真的进了 GP 子图，且首个模型轮即被守卫熔断。
    assert "subagent.started" in kinds
    failed = [e for e in events if e["kind"] == "run.failed"]
    assert len(failed) == 1
    payload = failed[0]["payload"]
    assert isinstance(payload, dict)
    # 异常跨子图边界被 LangGraph 重建（类型折叠为 RuntimeError），message 保留守卫原文。
    assert "terminated elsewhere" in str(payload["message"])


async def test_subagent_review_pauses_with_cached_result(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    # 审核政策同样不可被委派旁路：子代理内 review 工具执行后暂停，结果进卡（keep-first 缓存）。
    from support.fakes import FakeRunRepository
    from kokoro_agent.tools.middleware import ToolResultReviewMiddleware

    _EXECUTED["n"] = 0
    saver = checkpointer
    run_id = f"rrev-{uuid4().hex}"
    store = FakeRunRepository()
    gate_tool = StructuredTool(name="gated", description="d", args_schema=_NoArgs, func=_gated)
    review = ToolResultReviewMiddleware(frozenset({"gated"}), store, run_id)
    main_model = LocalFakeChatModel.with_script([
        AIMessage(content="", tool_calls=[{
            "name": "task", "args": {"description": "do", "subagent_type": "helper"},
            "id": "t1", "type": "tool_call"}]),
        AIMessage(content="main done"),
    ])
    sub_model = LocalFakeChatModel.with_script([
        AIMessage(content="", tool_calls=[{"name": "gated", "args": {}, "id": "g1", "type": "tool_call"}]),
        AIMessage(content="sub done"),
    ])
    agent = create_test_deep_agent(
        model=main_model,
        tools=[gate_tool],
        system_prompt="x",
        subagents=[{"name": "helper", "description": "h", "system_prompt": "s",
                    "model": sub_model, "tools": [gate_tool], "middleware": [review]}],
        checkpointer=saver,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
    )

    async def claim() -> bool:
        return True

    recorder, _seen = usage_recorder()
    paused = await invoke_once(
        RunEmitter(stream, run_id, review_tool_names=frozenset({"gated"})),
        agent,
        "trev",
        {"messages": [HumanMessage(content="go", id="m1")]},
        approval_tool_names=frozenset({"ask_user_question"}),
        source_for=lambda _n: "built-in",
        claim_terminal=claim,
        record_usage=recorder,
    )
    assert paused is False
    assert _EXECUTED["n"] == 1  # 审核语义：先执行后审
    events = [item.event for item in await stream.read_all(run_events_stream(run_id))]
    awaiting = [e for e in events if e["kind"] == "tool.awaiting_approval"]
    assert len(awaiting) == 1
    payload = awaiting[0]["payload"]
    assert isinstance(payload, dict)
    assert payload["kind"] == "result_review"
    assert payload["result"] == "gated-ok"
    assert payload["tool_id"] == "g1"  # review 载荷自带子图内真实 tool_id
