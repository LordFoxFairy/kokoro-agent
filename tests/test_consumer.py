"""consume_run/drain 的工具·子代理·容错分支：subagent-launch/todo 排空、subagent 事件、publish 失败。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeVar

from pydantic import JsonValue

from kokoro_agent.application.protocols.agent import (
    AgentRunStream,
    ModelStream,
    SubagentRunStream,
    ToolCallView,
)
from kokoro_agent.application.protocols.stream import StreamItem
from kokoro_agent.application.projection.transformer import (
    SUBAGENT_LAUNCH_NAMES,
    tool_start_event,
)
from kokoro_agent.application.run.consumer import EventQueue, consume_run, drain
from kokoro_agent.infrastructure.constants import TODO_TOOL_NAME
from kokoro_agent.interfaces.envelope import AgentEvent

_T = TypeVar("_T")


async def _aiter(items: Sequence[_T] = ()) -> AsyncIterator[_T]:
    for item in items:
        yield item


@dataclass
class _FakeTool:
    tool_call_id: str
    tool_name: str
    input: dict[str, object] | None = None
    output: object = "ok"
    error: str | None = None
    completed: bool = True
    deltas: tuple[object, ...] = ()
    # ToolCallView 把 output_deltas 声明为可写裸属性（非 property）且类型恰为 AsyncIterable[object]
    # （可变属性按不变量匹配）→ fake 字段须同型，不能用子类型 AsyncIterator，否则 pyright 判不满足。
    output_deltas: AsyncIterable[object] = field(init=False)

    def __post_init__(self) -> None:
        self.output_deltas = _aiter(self.deltas)


@dataclass
class _FakeSub:
    name: str | None = "researcher"
    trigger_call_id: str | None = "call-sub"
    task_input: str | None = "go research"
    status: str = "success"
    tools: tuple[_FakeTool, ...] = ()

    @property
    def messages(self) -> AsyncIterator[ModelStream]:
        return _aiter([])

    @property
    def tool_calls(self) -> AsyncIterator[ToolCallView]:
        return _aiter(self.tools)

    @property
    def subagents(self) -> AsyncIterator[SubagentRunStream]:
        return _aiter([])

    @property
    def custom(self) -> AsyncIterator[object]:
        return _aiter([])


@dataclass
class _FakeRun:
    tools: tuple[_FakeTool, ...] = ()
    subs: tuple[_FakeSub, ...] = ()
    customs: tuple[object, ...] = ()

    @property
    def messages(self) -> AsyncIterator[ModelStream]:
        return _aiter([])

    @property
    def tool_calls(self) -> AsyncIterator[ToolCallView]:
        return _aiter(self.tools)

    @property
    def subagents(self) -> AsyncIterator[SubagentRunStream]:
        return _aiter(self.subs)

    @property
    def custom(self) -> AsyncIterator[object]:
        return _aiter(self.customs)

    async def interrupted(self) -> bool:
        return False

    async def __aenter__(self) -> AgentRunStream:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool | None:
        return None


@dataclass
class _FailBus:
    """publish 必抛：验证 drain 局部容错——单事件发布失败不毁整条流、哨兵照常收束。"""

    attempts: list[str] = field(default_factory=lambda: [])

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        self.attempts.append(stream)
        raise RuntimeError("publish boom")

    async def read_all(self, stream: str) -> list[StreamItem]:
        return []

    def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
        return _aiter([])


async def _names(queue: EventQueue) -> list[str]:
    out: list[str] = []
    while not queue.empty():
        ev = queue.get_nowait()
        if ev is not None:
            out.append(ev.event)
    return out


async def test_normal_tool_emits_start_end_and_drains_output() -> None:
    queue: EventQueue = asyncio.Queue()
    tool = _FakeTool("c1", "fetch_url", input={"url": "x"}, deltas=("a", "b"))
    await consume_run(_FakeRun(tools=(tool,)), "r", queue, subagent_id=None)
    assert await _names(queue) == ["tool_call_start", "tool_call_end"]


async def test_todo_tool_emits_single_todo_event_no_tool_frames() -> None:
    queue: EventQueue = asyncio.Queue()
    tool = _FakeTool("c2", TODO_TOOL_NAME, input={"todos": []}, deltas=("x",))
    await consume_run(_FakeRun(tools=(tool,)), "r", queue, subagent_id=None)
    names = await _names(queue)
    # todo 工具：发一条 todo 帧，不发 tool_call_start/end。
    assert len(names) == 1
    assert "tool_call_start" not in names


async def test_subagent_launch_tool_drained_not_emitted() -> None:
    queue: EventQueue = asyncio.Queue()
    launch_name = sorted(SUBAGENT_LAUNCH_NAMES)[0]
    tool = _FakeTool("c3", launch_name, deltas=("y",))
    await consume_run(_FakeRun(tools=(tool,)), "r", queue, subagent_id=None)
    # 子代理启动工具由 run.subagents 投影处理 → 此处仅排空输出、不发任何 tool_call_* 帧。
    assert await _names(queue) == []


async def test_subagent_started_and_finished_events() -> None:
    queue: EventQueue = asyncio.Queue()
    await consume_run(_FakeRun(subs=(_FakeSub(),)), "r", queue, subagent_id=None)
    names = await _names(queue)
    assert names == ["agent_status", "agent_status"]  # started + finished（叶子子代理无内层）


async def test_custom_channel_emits_custom_event() -> None:
    queue: EventQueue = asyncio.Queue()
    await consume_run(_FakeRun(customs=({"k": "v"},)), "r", queue, subagent_id=None)
    ev = queue.get_nowait()
    assert ev is not None
    # 业务遥测原样挂 agent_status.data.custom（status="custom"）。
    assert ev.event == "agent_status"
    assert queue.empty()


async def test_drain_tolerates_publish_failure_and_stops_on_sentinel() -> None:
    queue: EventQueue = asyncio.Queue()
    ev: AgentEvent = tool_start_event(
        _FakeTool("c", "fetch_url", input={}), request_id="r"
    )
    await queue.put(ev)
    await queue.put(None)
    bus = _FailBus()
    # publish 抛错被局部吞掉、不向上冒泡；收到 None 哨兵后正常返回（不挂起）。
    await asyncio.wait_for(drain(bus, "s", queue), timeout=1.0)
    assert bus.attempts == ["s"]


async def test_tool_with_none_output_yields_empty_result_text() -> None:
    # output 为 None 且无 error → 结果文本为空串（_result_text 的 None 分支）。
    queue: EventQueue = asyncio.Queue()
    tool = _FakeTool("c", "fetch_url", output=None, error=None)
    await consume_run(_FakeRun(tools=(tool,)), "r", queue, subagent_id=None)
    queue.get_nowait()  # tool_call_start
    end = queue.get_nowait()
    assert end is not None
    assert end.model_dump()["data"]["result"] == ""


def test_fake_tool_satisfies_toolcallview() -> None:
    # 结构契约自检：fakes 必须真的满足 protocol，否则测的是别的东西。
    assert isinstance(_FakeTool("x", "y"), ToolCallView)
