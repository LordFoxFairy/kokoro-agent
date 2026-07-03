"""RuntimeContext 注入规格：真 deepagents 图内，工具经 get_runtime 读到本次 run 身份。"""

# @tool 装饰器重载含未解 Runnable 泛型（第三方边界）：沿 build_agent.py 先例最窄豁免。
# pyright: reportUnknownVariableType=false

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.runtime import get_runtime

from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.run.context import RunContext
from kokoro_agent.streams.memory import MemoryStream
from kokoro_agent.subagents import build_catalog

_SEEN: dict[str, str] = {}


@tool
def probe_identity() -> str:
    """读取本次 run 的注入身份。"""
    runtime = get_runtime(RunContext)
    _SEEN["namespace"] = runtime.context.namespace
    _SEEN["session_id"] = runtime.context.session_id
    return "ok"


async def test_tool_reads_injected_run_context() -> None:
    _SEEN.clear()
    script = [
        AIMessage(
            content="",
            tool_calls=[{"name": "probe_identity", "args": {}, "id": "p1", "type": "tool_call"}],
        ),
        AIMessage(content="done"),
    ]
    agent = build_agent(
        model=LocalFakeChatModel.with_script(script),
        tools=[probe_identity],
        system_prompt="x",
        subagents=build_catalog(None).definitions(),
        checkpointer=None,
        permissions=[],
        interrupt_on={},
        context_schema=RunContext,
    )
    bus = MemoryStream()
    context = RunContext(namespace="tenant-a", session_id="s1", run_id="rn", thread_id="s1")

    async def claim() -> bool:
        return True

    terminal = await invoke_once(
        RunEmitter(bus, "rn"),
        agent,
        context.scoped_thread_id,
        {"messages": [HumanMessage(content="hi")]},
        approval_tool_names=frozenset(),
        source_for=lambda _name: "built-in",
        claim_terminal=claim,
        context=context,
    )
    assert terminal is True
    assert _SEEN == {"namespace": "tenant-a", "session_id": "s1"}
