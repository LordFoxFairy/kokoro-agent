"""request_human 原语规格：HumanRequest 信封 round-trip + 任意工具执行中途暂停→resume 回值→续跑。"""

from __future__ import annotations

from deepagents.backends.state import StateBackend

from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.stream import CustomTransformer
from langgraph.types import Command
from pydantic import BaseModel, ConfigDict, ValidationError

from kokoro_agent.contract import ToolReturnedPayload, agent_event_adapter, run_events_stream
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.publish_agent_events import pump_run
from kokoro_agent.hitl import HumanRequest, request_human
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.tools.permissions import build_interrupt_on


def test_human_request_envelope_roundtrip() -> None:
    hr = HumanRequest(
        request_id="r1",
        kind="input",
        response_schema={"type": "string"},
        context={"otp": "?"},
    )
    value = hr.to_interrupt_value()
    assert value == {
        "kokoro_human_request": {
            "request_id": "r1",
            "kind": "input",
            "response_schema": {"type": "string"},
            "context": {"otp": "?"},
        }
    }
    assert HumanRequest.from_interrupt_value(value) == hr


def test_from_interrupt_value_none_for_foreign_shapes() -> None:
    # langchain approval 形态（action_requests/review_configs）与非 dict 均非本信封：返回 None。
    assert HumanRequest.from_interrupt_value({"action_requests": [], "review_configs": []}) is None
    assert HumanRequest.from_interrupt_value("nope") is None


def test_from_interrupt_value_fail_loud_on_malformed_envelope() -> None:
    # 信封键在但体不合法（缺 request_id）：strict 校验 fail-loud，绝不静默发错帧。
    with pytest.raises(ValidationError):
        HumanRequest.from_interrupt_value({"kokoro_human_request": {"kind": "input"}})


class _ProbeArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    topic: str


def _probe(topic: str) -> str:
    # 任意执行点：工具执行中途请求人，resume 回值原地返回后拼进产物续跑。
    answer = request_human(
        kind="input",
        request_id="probe-1",
        schema={"type": "string"},
        context={"topic": topic},
    )
    return f"otp:{answer}"


async def _drive(
    agent: InvokableAgent, payload: object, config: RunnableConfig, emitter: RunEmitter
) -> bool:
    # 精简版 invoke_once：抽干投影推进整图，返回是否暂停（不经 wire 投影——kind=input 无 wire 面）。
    run = await agent.astream_events(
        payload, version="v3", config=config, transformers=[CustomTransformer]
    )
    async with run:
        await pump_run(emitter, run, source_for=lambda _n: "built-in")
        return await run.interrupted()


async def test_request_human_pauses_mid_tool_then_resumes_value(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    probe_tool = StructuredTool(
        name="probe", description="d", args_schema=_ProbeArgs, func=_probe
    )
    model = LocalFakeChatModel.with_script(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "probe", "args": {"topic": "login"}, "id": "p1", "type": "tool_call"}
                ],
            ),
            AIMessage(content="done"),
        ]
    )
    agent = build_agent(
        model=model,
        tools=[probe_tool],
        system_prompt="x",
        subagents=[],
        checkpointer=checkpointer,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
        backend=StateBackend(),
    )
    run_id = f"rrh-{uuid4().hex}"
    emitter = RunEmitter(stream, run_id)
    config: RunnableConfig = {"configurable": {"thread_id": f"trh-{uuid4().hex}"}}

    # ① 首跑：工具执行中途 request_human 挂起，信封由 checkpoint 承载。
    paused = await _drive(agent, {"messages": [HumanMessage(content="go", id="m1")]}, config, emitter)
    assert paused is True
    snapshot = await agent.aget_state(config)
    hr = HumanRequest.from_interrupt_value(snapshot.interrupts[0].value)
    assert hr is not None
    assert hr.kind == "input"
    assert hr.request_id == "probe-1"
    assert hr.response_schema == {"type": "string"}
    assert hr.context == {"topic": "login"}

    # ② resume 回值原地返回工具调用点 → 工具续跑把人给的值拼进结果 → run 收束。
    done = await _drive(agent, Command(resume="123456"), config, emitter)
    assert done is False

    events = [
        agent_event_adapter.validate_python(item.event)
        for item in await stream.read_all(run_events_stream(run_id))
    ]
    returned = [
        event.payload.result
        for event in events
        if isinstance(event.payload, ToolReturnedPayload)
    ]
    assert "otp:123456" in returned
