"""request_input 规格：工具执行中途结构化请求 → submit 回灌 value 续跑 / schema 校验失败重问 / reject。

低层原语 request_human 已有 test_request_human 覆盖；本测覆盖其上层包装：resume 载荷解码、
jsonschema 校验、不合法重新 interrupt（同 request_id 附 validation_error）、reject 决策。
"""

from __future__ import annotations

from uuid import uuid4

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.stream import CustomTransformer
from langgraph.types import Command
from pydantic import BaseModel, ConfigDict, JsonValue

from kokoro_agent.contract import ToolReturnedPayload, agent_event_adapter, run_events_stream
from kokoro_agent.execution.build_agent import build_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.protocols import InvokableAgent
from kokoro_agent.execution.publish_agent_events import pump_run
from kokoro_agent.hitl import HumanRequest, InputRejected, InputSubmitted, request_input
from kokoro_agent.model.local_fake import LocalFakeChatModel
from kokoro_agent.streams.redis import RedisStream
from kokoro_agent.tools.permissions import build_interrupt_on

_OTP_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "properties": {"otp": {"type": "string"}},
    "required": ["otp"],
    "additionalProperties": False,
}


class _ProbeArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    topic: str


def _probe(topic: str) -> str:
    # 工具执行中途：按 schema 请求一份结构化回应，续跑把结果拼进产物。
    result = request_input(
        request_id="probe-1",
        schema=_OTP_SCHEMA,
        context={"name": "probe", "args": {"topic": topic}},
    )
    if isinstance(result, InputRejected):
        return f"rejected:{result.reason}"
    assert isinstance(result, InputSubmitted)
    return f"otp:{result.value['otp']}"


def _build(checkpointer: BaseCheckpointSaver[str]) -> InvokableAgent:
    probe_tool = StructuredTool(name="probe", description="d", args_schema=_ProbeArgs, func=_probe)
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
    return build_agent(
        model=model,
        tools=[probe_tool],
        system_prompt="x",
        subagents=[],
        checkpointer=checkpointer,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
    )


async def _drive(
    agent: InvokableAgent, payload: object, config: RunnableConfig, emitter: RunEmitter
) -> bool:
    run = await agent.astream_events(
        payload, version="v3", config=config, transformers=[CustomTransformer]
    )
    async with run:
        await pump_run(emitter, run, source_for=lambda _n: "built-in")
        return await run.interrupted()


async def _returned(stream: RedisStream, run_id: str) -> list[str]:
    events = [
        agent_event_adapter.validate_python(item.event)
        for item in await stream.read_all(run_events_stream(run_id))
    ]
    return [e.payload.result for e in events if isinstance(e.payload, ToolReturnedPayload)]


async def test_submit_valid_value_resumes(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    agent = _build(checkpointer)
    run_id = f"ri-{uuid4().hex}"
    emitter = RunEmitter(stream, run_id)
    config: RunnableConfig = {"configurable": {"thread_id": f"tri-{uuid4().hex}"}}

    paused = await _drive(agent, {"messages": [HumanMessage(content="go", id="m1")]}, config, emitter)
    assert paused is True

    resume: list[dict[str, JsonValue]] = [
        {"request_id": "probe-1", "type": "submit", "value": {"otp": "123456"}}
    ]
    done = await _drive(agent, Command(resume=resume), config, emitter)
    assert done is False
    assert "otp:123456" in await _returned(stream, run_id)


async def test_invalid_value_reprompts_with_validation_error_then_submits(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    agent = _build(checkpointer)
    run_id = f"ri-{uuid4().hex}"
    emitter = RunEmitter(stream, run_id)
    config: RunnableConfig = {"configurable": {"thread_id": f"tri-{uuid4().hex}"}}

    await _drive(agent, {"messages": [HumanMessage(content="go", id="m1")]}, config, emitter)

    # ① 不合法回灌（缺必填 otp）：不炸 run，原地重新 interrupt 且 context 附 validation_error。
    bad: list[dict[str, JsonValue]] = [{"request_id": "probe-1", "type": "submit", "value": {}}]
    still_paused = await _drive(agent, Command(resume=bad), config, emitter)
    assert still_paused is True
    snapshot = await agent.aget_state(config)
    hr = HumanRequest.from_interrupt_value(snapshot.interrupts[0].value)
    assert hr is not None and hr.request_id == "probe-1"  # 幂等锚不变。
    assert isinstance(hr.context.get("validation_error"), str)

    # ② 重填合法值：通过校验，续跑。
    good: list[dict[str, JsonValue]] = [
        {"request_id": "probe-1", "type": "submit", "value": {"otp": "999"}}
    ]
    done = await _drive(agent, Command(resume=good), config, emitter)
    assert done is False
    assert "otp:999" in await _returned(stream, run_id)


async def test_reject_returns_rejected(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    agent = _build(checkpointer)
    run_id = f"ri-{uuid4().hex}"
    emitter = RunEmitter(stream, run_id)
    config: RunnableConfig = {"configurable": {"thread_id": f"tri-{uuid4().hex}"}}

    await _drive(agent, {"messages": [HumanMessage(content="go", id="m1")]}, config, emitter)
    resume: list[dict[str, JsonValue]] = [
        {"request_id": "probe-1", "type": "reject", "reason": "用户拒绝"}
    ]
    done = await _drive(agent, Command(resume=resume), config, emitter)
    assert done is False
    assert "rejected:用户拒绝" in await _returned(stream, run_id)
