"""steering 规格：信箱在模型轮前排空注入（稳定 message_id），不打断进行中的图。"""

from __future__ import annotations

from typing import Any

import pytest
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatResult

from langchain.agents.middleware.types import AgentState
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.runtime import Runtime

from support.fakes import FakeBus, FakeLedger, request, usage_recorder
from support.deepagents import create_test_deep_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.contract.streams import run_events_stream
from kokoro_agent.execution.run_agent import invoke_once
from support.local_fake import LocalFakeChatModel
from kokoro_agent.tools.middleware import SteeringMiddleware
from kokoro_agent.tools.permissions import build_interrupt_on


async def test_before_model_injects_mailbox_in_order() -> None:
    ledger = FakeLedger()
    await ledger.try_claim(request("r-steer"))
    await ledger.add_steer("r-steer", "m1", "改成国内市场")
    await ledger.add_steer("r-steer", "m2", "语气正式一点")
    middleware = SteeringMiddleware(store=ledger, run_id="r-steer")
    update = await middleware.abefore_model({"messages": []}, Runtime(context=None))
    assert update is not None
    messages = update["messages"]
    assert [m.content for m in messages] == ["改成国内市场", "语气正式一点"]
    assert [m.id for m in messages] == ["m1", "m2"]  # 稳定 id：checkpoint 重放幂等
    assert all(isinstance(m, HumanMessage) for m in messages)


async def test_before_model_empty_mailbox_is_noop() -> None:
    ledger = FakeLedger()
    await ledger.try_claim(request("r-steer"))
    middleware = SteeringMiddleware(store=ledger, run_id="r-steer")
    assert await middleware.abefore_model({"messages": []}, Runtime(context=None)) is None


async def test_steer_reaches_model_in_real_graph(checkpointer: BaseCheckpointSaver[str]) -> None:
    # 真图：invoke 前信箱有插话 → 首个模型轮的 messages 里可见该 HumanMessage。
    captured: list[list[BaseMessage]] = []

    class Recorder(LocalFakeChatModel):
        def _generate(
            self,
            messages: list[BaseMessage],
            stop: list[str] | None = None,
            run_manager: CallbackManagerForLLMRun | None = None,
            **kwargs: Any,
        ) -> ChatResult:
            captured.append(list(messages))
            return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)

    ledger = FakeLedger()
    await ledger.try_claim(request("r-graph"))
    await ledger.add_steer("r-graph", "steer-1", "重点只看国内市场")
    agent = create_test_deep_agent(
        model=Recorder.with_script([AIMessage(content="ok")]),
        tools=[],
        system_prompt="base",
        subagents=[],
        checkpointer=checkpointer,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
        middleware=[SteeringMiddleware(store=ledger, run_id="r-graph")],
    )

    async def claim() -> bool:
        return True

    bus = FakeBus()
    terminal = await invoke_once(
        RunEmitter(bus, "r-graph"),
        agent,
        "t-graph",
        {"messages": [HumanMessage(content="写调研报告", id="m0")]},
        approval_tool_names=frozenset(),
        source_for=lambda _n: "built-in",
        claim_terminal=claim,
        record_usage=usage_recorder()[0],
    )
    assert terminal is True
    humans = [m.text for m in captured[-1] if m.type == "human"]
    assert humans == ["写调研报告", "重点只看国内市场"]
    # peek+下一轮见证语义：run 在注入轮后即终态、无下一轮，信箱残留是设计内
    # （随 run TTL 清扫，绝不丢插话）。手动再走一轮 before_model 验证见证机制：
    # 插话已在消息史（=已随 checkpoint 落定）→ 本轮 ack 清箱、且不重复注入。
    assert await ledger.peek_steers("r-graph") == [("steer-1", "重点只看国内市场")]
    witness = SteeringMiddleware(store=ledger, run_id="r-graph")
    # "已落定"的最小见证态：插话以稳定 id 存在于消息史（即已进 checkpoint）。
    landed_state: AgentState[Any] = {
        "messages": [HumanMessage(content="重点只看国内市场", id="steer-1")]
    }
    update = await witness.abefore_model(landed_state, Runtime(context=None))
    assert update is None  # 已落定：不重复注入
    assert await ledger.peek_steers("r-graph") == []  # 见证后 ack 清箱
    # 注入项经 before_model 节点出现在消息投影里：绝不冒充 assistant 正文上 wire。
    events = [item.event for item in await bus.read_all(run_events_stream("r-graph"))]
    texts: list[str] = []
    for e in events:
        payload = e["payload"]
        if e["kind"] in {"message.delta", "message.completed"} and isinstance(payload, dict):
            texts.append(f"{payload.get('delta', '')}{payload.get('content', '')}")
    assert all("重点只看国内市场" not in t for t in texts), texts
    assert any("ok" in t for t in texts)  # 真模型输出仍正常上 wire


async def test_steer_content_never_empty_fail_loud() -> None:
    # 契约 NonEmptyStr 在入口拦截；middleware 侧对空内容 fail-loud 兜底（绝不注入空消息）。
    ledger = FakeLedger()
    await ledger.try_claim(request("r-empty"))
    ledger.steers["r-empty"] = [("mx", "")]
    middleware = SteeringMiddleware(store=ledger, run_id="r-empty")
    with pytest.raises(ValueError, match="empty steer"):
        await middleware.abefore_model({"messages": []}, Runtime(context=None))
