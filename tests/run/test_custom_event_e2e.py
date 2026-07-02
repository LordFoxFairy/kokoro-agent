"""守则D 端到端：工具内 get_stream_writer() 派发的纯业务事件，经 CustomTransformer→run.custom
→ ACL 投影，浮现为对外 agent_status{status:custom}。用真实 deepagents agent 全链路验证。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

import pytest
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable
from langchain_core.tools import StructuredTool
from langgraph.config import get_stream_writer
from pydantic import BaseModel, Field, JsonValue, PrivateAttr

from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.execution.build_agent import build_deep_agent


async def _emit_billing(amount: int) -> str:
    get_stream_writer()({"kind": "billing", "amount": amount, "unit": "credit"})
    return f"charged {amount}"


class _BillingArgs(BaseModel):
    amount: int = Field(description="计费金额")


# 守则D：工具内 get_stream_writer() 派发纯业务事件，零全局/单例/硬编码回调。
emit_billing = StructuredTool(
    name="emit_billing",
    description="记一笔计费埋点",
    args_schema=_BillingArgs,
    coroutine=_emit_billing,
)


_SCRIPT: list[AIMessage] = [
    AIMessage(
        content="",
        tool_calls=[{"name": "emit_billing", "args": {"amount": 7}, "id": "c1", "type": "tool_call"}],
    ),
    AIMessage(content="done"),
]


class _Scripted(BaseChatModel):
    _cursor: int = PrivateAttr(default=0)

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools: Any, **kwargs: Any) -> Runnable[LanguageModelInput, AIMessage]:
        return self.with_types(output_type=AIMessage)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        if not any(isinstance(m, AIMessage) for m in messages):
            self._cursor = 0
        reply = _SCRIPT[self._cursor] if self._cursor < len(_SCRIPT) else AIMessage(content="")
        self._cursor += 1
        return ChatResult(generations=[ChatGeneration(message=reply)])


class _Bus:
    def __init__(self) -> None:
        self.events: list[dict[str, JsonValue]] = []

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        self.events.append(dict(event))
        return StreamItem(cursor=str(len(self.events)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return []

    def subscribe(self, stream: str, from_cursor: str | None = None) -> AsyncIterator[StreamItem]:
        return _empty()


async def _empty() -> AsyncIterator[StreamItem]:
    return
    yield  # pragma: no cover - 使函数成为 async generator


@pytest.mark.asyncio
async def test_get_stream_writer_event_surfaces_as_agent_status_custom() -> None:
    agent = build_deep_agent(
        model=_Scripted(),
        tools=[emit_billing],
        system_prompt="x",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on={},
    )
    bus = _Bus()
    emitted = await invoke_once(
        bus, agent, "run-1", "conv-1", {"messages": [HumanMessage(content="hi")]}
    )
    assert emitted is True

    customs = [
        e
        for e in bus.events
        if e["event"] == "agent_status" and _status(e) == "custom"
    ]
    assert len(customs) == 1
    assert _custom_payload(customs[0]) == {"kind": "billing", "amount": 7, "unit": "credit"}


@pytest.mark.asyncio
async def test_text_chunk_carries_string_text_e2e() -> None:
    # 分通道：真实 agent 的助手文本经原生 .text projection → text_chunk{text:str}，无旧 content 块数组。
    agent = build_deep_agent(
        model=_Scripted(),
        tools=[emit_billing],
        system_prompt="x",
        subagents=[],
        checkpointer=None,
        permissions=[],
        interrupt_on={},
    )
    bus = _Bus()
    await invoke_once(bus, agent, "run-2", "conv-2", {"messages": [HumanMessage(content="hi")]})
    text_finals = [
        e["data"]
        for e in bus.events
        if e["event"] == "text_chunk" and isinstance(e["data"], Mapping) and e["data"].get("final")
    ]
    assert text_finals
    last = text_finals[-1]
    assert last.get("text") == "done"
    assert "content" not in last


def _status(event: dict[str, JsonValue]) -> object:
    data = event["data"]
    return data.get("status") if isinstance(data, Mapping) else None


def _custom_payload(event: dict[str, JsonValue]) -> object:
    data = event["data"]
    return data.get("custom") if isinstance(data, Mapping) else None
