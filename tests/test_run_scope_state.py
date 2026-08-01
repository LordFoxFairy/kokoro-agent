"""RunScope 乘 State 轴规格：真图内工具经 ToolRuntime.state 读 scope；续跑不重供仍保持。"""

# StructuredTool.from_function 含未解 Runnable 泛型（ToolRuntime 注入仅走此构造路径）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

from langchain.tools import ToolRuntime
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver

from fakes import FakeLedger, completed_execution_context, usage_recorder
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.state import KokoroAgentState, RunScope
from kokoro_agent.streams.redis import RedisStream

_SEEN: dict[str, object] = {}


async def _probe_scope(runtime: ToolRuntime[None, KokoroAgentState]) -> str:
    """读取本次 run 的 scope。"""
    _SEEN["scope"] = runtime.state.get("scope")
    return "ok"


PROBE_TOOL = StructuredTool.from_function(coroutine=_probe_scope, name="probe_scope")

_SCRIPT = [
    AIMessage(
        content="",
        tool_calls=[{"name": "probe_scope", "args": {}, "id": "p1", "type": "tool_call"}],
    ),
    AIMessage(content="done"),
]


def _scope() -> RunScope:
    return RunScope(namespace="tenant-a", session_id="s1", run_id="rn", thread_id="s1")


async def _invoke(
    agent: InvokableAgent, payload: object, run_id: str, bus: RedisStream
) -> bool:
    ledger = FakeLedger()
    return await invoke_once(
        RunEmitter(bus, run_id, outbox=ledger),
        agent,
        {
            "configurable": {"thread_id": _scope().scoped_thread_id},
            "metadata": {"kokoro_run_id": run_id},
        },
        payload,
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        prepare_completed=lambda: completed_execution_context(run_id),
        record_usage=usage_recorder()[0],
    )


async def test_tool_reads_scope_from_state(stream: RedisStream) -> None:
    _SEEN.clear()
    agent = build_agent(
        model=LocalFakeChatModel.with_script(_SCRIPT),
        tools=[PROBE_TOOL],
        system_prompt="x",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on={},
    )
    scope = _scope()
    terminal = await _invoke(
        agent,
        {"messages": [HumanMessage(content="hi")], "scope": scope.as_state()},
        "rn",
        stream,
    )
    assert terminal is True
    assert _SEEN["scope"] == scope.as_state()
    assert RunScope.from_state(scope.as_state()) == scope


async def test_scope_survives_second_segment_without_resupply(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    # 法则：scope 落 checkpoint，续段（resume 式二跑）不重供仍可读——图节点不得改写。
    _SEEN.clear()
    saver = checkpointer
    scope = _scope()
    first = build_agent(
        model=LocalFakeChatModel.with_script(_SCRIPT),
        tools=[PROBE_TOOL],
        system_prompt="x",
        subagents=[],
        checkpointer=saver,
        permissions=[],
        interrupt_on={},
    )
    await _invoke(
        first, {"messages": [HumanMessage(content="hi")], "scope": scope.as_state()}, "rn", stream
    )
    _SEEN.clear()
    second_script = [
        *_SCRIPT,  # 占位：同线程历史已含首段两条 AIMessage（LocalFake 轮次=历史 AIMessage 数）
        AIMessage(
            content="",
            tool_calls=[{"name": "probe_scope", "args": {}, "id": "p2", "type": "tool_call"}],
        ),
        AIMessage(content="done2"),
    ]
    second = build_agent(
        model=LocalFakeChatModel.with_script(second_script),
        tools=[PROBE_TOOL],
        subagents=[],
        system_prompt="x",
        checkpointer=saver,
        permissions=[],
        interrupt_on={},
    )
    await _invoke(second, {"messages": [HumanMessage(content="again")]}, "rn2", stream)
    assert _SEEN["scope"] == scope.as_state()
