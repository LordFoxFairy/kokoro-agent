"""HITL 端到端：fake agent 真走 interrupt→resume，四档决策经 supervisor 全链路。

每档：dispatch RunRequest → 收 awaiting_approval(tool_id 对齐) →
dispatch RunResume(对应决定) → fake 收 Command(resume) 续跑出 tool_call_end → 收终态。
fake 是真 fake（自带 interrupt/resume 状态机），非 mock。
"""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeGuard, TypeVar

import pytest
from langchain.agents.middleware import InterruptOnConfig
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable
from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command, Interrupt
from pydantic import BaseModel, Field, JsonValue, PrivateAttr

from kokoro_agent.application.protocols.agent import InvokableAgent
from kokoro_agent.application.protocols.stream import StreamItem
from kokoro_agent.application.run.invoke import events_stream
from kokoro_agent.application.run.supervisor import RunSupervisor
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.agent_builder import make_deep_agent
from kokoro_agent.interfaces.inbound import InboundMessage, parse_inbound
from .fakes import FakeRunStateStore

# 与 AppConfig.approval 默认 requires_approval_tools 同名：确保 supervisor 计算的
# interrupt_on_names 真命中本工具，端到端验证同源对齐而非旁路。
_TOOL_NAME = "web_fetch"
_TOOL_ID = "call-A"

_T = TypeVar("_T")


async def _aiter(items: Sequence[_T]) -> AsyncIterator[_T]:
    for item in items:
        yield item


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, JsonValue]]] = []

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        self.published.append((stream, dict(event)))
        return StreamItem(cursor=str(len(self.published)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return []

    def subscribe(self, stream: str, from_cursor: str | None = None) -> AsyncIterator[StreamItem]:
        return _aiter([])


@dataclass
class _ToolView:
    output: object
    tool_call_id: str = _TOOL_ID
    tool_name: str = _TOOL_NAME
    input: dict[str, object] | None = field(default_factory=lambda: {})
    error: str | None = None
    completed: bool = True
    output_deltas: AsyncIterable[object] = field(default_factory=lambda: _aiter([]))


@dataclass
class _RunStream:
    tools: Sequence[_ToolView] = ()
    is_interrupted: bool = False

    @property
    def messages(self) -> AsyncIterator[object]:
        return _aiter([])

    @property
    def tool_calls(self) -> AsyncIterator[_ToolView]:
        return _aiter(self.tools)

    @property
    def subagents(self) -> AsyncIterator[object]:
        return _aiter([])

    @property
    def custom(self) -> AsyncIterator[object]:
        return _aiter([])

    async def interrupted(self) -> bool:
        return self.is_interrupted

    async def __aenter__(self) -> "_RunStream":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


@dataclass
class _State:
    interrupts: tuple[Interrupt, ...] = ()
    values: Mapping[str, object] = field(default_factory=lambda: {})


@dataclass
class _FakeHitlAgent:
    """interrupt→resume 状态机：首轮 interrupted 带 pending；resume 据决策发 tool_call_end 后转终态。"""

    args: Mapping[str, JsonValue] = field(default_factory=lambda: {"x": 1})
    resumed: bool = False
    seen_resume: object = None

    async def astream_events(
        self,
        payload: object,
        *,
        version: str,
        config: RunnableConfig,
        transformers: Sequence[object],
    ) -> _RunStream:
        if isinstance(payload, Command):
            self.resumed = True
            self.seen_resume = payload.resume
            dtype = _decision_type(_first_decision(payload.resume))
            # 真 deepagents：reject/respond 生成 synthetic ToolMessage 跳过 tool 节点 → 工具不经 v3
            # projection 浮现（仅 supervisor 据 snapshot 直发终态）；approve/edit 真跑工具、经 projection。
            if dtype in ("reject", "respond"):
                return _RunStream()
            return _RunStream(tools=(_ToolView(output=self._result(payload.resume)),))
        return _RunStream(is_interrupted=True)

    async def aget_state(self, config: RunnableConfig) -> _State:
        if self.resumed:
            return _State()
        messages = [
            HumanMessage(content="go"),
            AIMessage(
                content="",
                id="seg",
                tool_calls=[{"name": _TOOL_NAME, "args": dict(self.args), "id": _TOOL_ID}],
            ),
        ]
        return _State(
            interrupts=(Interrupt(value=self._interrupt_value()),),
            values={"messages": messages},
        )

    def _interrupt_value(self) -> dict[str, JsonValue]:
        return {
            "action_requests": [
                {"name": _TOOL_NAME, "args": dict(self.args), "description": "do danger"}
            ]
        }

    def _result(self, resume: object) -> str:
        # 仅 approve/edit 经此（真跑工具）；reject/respond 不经 projection。
        decision = _first_decision(resume)
        if _decision_type(decision) == "edit":
            return f"ran with {_edited_args(decision)}"
        return f"ran with {dict(self.args)}"


def _is_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def _is_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _first_decision(resume: object) -> Mapping[object, object]:
    if not _is_mapping(resume):
        return {}
    decisions: object = resume.get("decisions")
    if not _is_list(decisions) or not decisions:
        return {}
    first = decisions[0]
    return first if _is_mapping(first) else {}


def _decision_type(decision: Mapping[object, object]) -> str:
    dtype: object = decision.get("type")
    return dtype if isinstance(dtype, str) else ""


def _edited_args(decision: Mapping[object, object]) -> Mapping[object, object]:
    action: object = decision.get("edited_action")
    if not _is_mapping(action):
        return {}
    args: object = action.get("args")
    return args if _is_mapping(args) else {}


def _builder(agent: _FakeHitlAgent) -> Callable[[RunRequest], _FakeHitlAgent]:
    def build(request: RunRequest) -> _FakeHitlAgent:
        return agent

    return build


def _supervisor(
    agent_builder: Callable[[RunRequest], InvokableAgent],
) -> RunSupervisor:
    return RunSupervisor(agent_builder=agent_builder, store=FakeRunStateStore())


def _request(run_id: str) -> RunRequest:
    # permission_mode=default 让 supervisor 计算非空 interrupt_on_names。
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        session_id="s1",
        conversation_id="c1",
        input="hello",
        permission_mode="default",
    )


def _inbound(raw: dict[str, JsonValue]) -> InboundMessage:
    parsed = parse_inbound(raw)
    assert parsed is not None
    return parsed


async def _drain(sup: RunSupervisor) -> None:
    for task in tuple(sup.tasks.values()):
        await task


def _events_of(bus: _FakeBus, run_id: str, event_name: str) -> list[dict[str, JsonValue]]:
    return [
        e for s, e in bus.published if s == events_stream(run_id) and e.get("event") == event_name
    ]


def _data(event: dict[str, JsonValue]) -> Mapping[str, JsonValue]:
    data = event["data"]
    assert isinstance(data, Mapping)
    return data


async def _run_until_awaiting(sup: RunSupervisor, bus: _FakeBus, run_id: str) -> None:
    await sup.dispatch(bus, _request(run_id))
    await _drain(sup)
    # 逐工具顶层 tool_call_awaiting（不再是 agent_status{awaiting_approval} 打包 pending 数组）。
    awaiting = _events_of(bus, run_id, "tool_call_awaiting")
    assert len(awaiting) == 1
    assert _data(awaiting[0]).get("tool_id") == _TOOL_ID
    assert "agent_done" not in [e.get("event") for _, e in bus.published]


async def _resume(
    sup: RunSupervisor, bus: _FakeBus, run_id: str, decision: dict[str, JsonValue]
) -> None:
    # 单工具便捷封装：注入唯一 pending 的 tool_id + 包成 decisions 列表（多工具见 _resume_many）。
    resume = _inbound(
        {"kind": "run.resume", "run_id": run_id, "decisions": [{**decision, "tool_id": _TOOL_ID}]}
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)


def _tool_result(bus: _FakeBus, run_id: str) -> str:
    returned = _events_of(bus, run_id, "tool_call_end")
    assert len(returned) == 1
    result: object = _data(returned[0]).get("result")
    assert isinstance(result, str)
    return result


def _assert_completed(bus: _FakeBus, run_id: str) -> None:
    done = _events_of(bus, run_id, "agent_done")
    assert len(done) == 1
    assert _data(done[0]).get("status") == "completed"


# ① approve → 工具据原 args 真跑出结果。
@pytest.mark.asyncio
async def test_approve_tool_actually_runs() -> None:
    agent = _FakeHitlAgent()
    bus = _FakeBus()
    sup = _supervisor(_builder(agent))
    await _run_until_awaiting(sup, bus, "ra")
    await _resume(sup, bus, "ra", {"type": "approve"})

    assert agent.seen_resume == {"decisions": [{"type": "approve"}]}
    assert _tool_result(bus, "ra") == "ran with {'x': 1}"
    _assert_completed(bus, "ra")


# ② edit → 新 args 生效，工具据编辑参数跑。
@pytest.mark.asyncio
async def test_edit_new_args_take_effect() -> None:
    agent = _FakeHitlAgent()
    bus = _FakeBus()
    sup = _supervisor(_builder(agent))
    await _run_until_awaiting(sup, bus, "re")
    await _resume(
        sup, bus, "re", {"type": "edit", "edited_action": {"name": _TOOL_NAME, "args": {"x": 99}}}
    )

    assert agent.seen_resume == {
        "decisions": [{"type": "edit", "edited_action": {"name": _TOOL_NAME, "args": {"x": 99}}}]
    }
    assert _tool_result(bus, "re") == "ran with {'x': 99}"
    _assert_completed(bus, "re")


# ③ reject → 工具不真跑，拒绝语义结果。
@pytest.mark.asyncio
async def test_reject_does_not_run_tool() -> None:
    agent = _FakeHitlAgent()
    bus = _FakeBus()
    sup = _supervisor(_builder(agent))
    await _run_until_awaiting(sup, bus, "rr")
    await _resume(sup, bus, "rr", {"type": "reject", "message": "no"})

    assert agent.seen_resume == {"decisions": [{"type": "reject", "message": "no"}]}
    # 机制B replay 安全：被拒工具权威 rejected=True、is_error=False、result/reject_reason=理由，
    # 重放不退化为绿勾 done 或真 error。
    end = _data(_events_of(bus, "rr", "tool_call_end")[0])
    assert end["rejected"] is True
    assert end["is_error"] is False
    assert end["result"] == "no"
    assert end["reject_reason"] == "no"
    _assert_completed(bus, "rr")


# ④ respond → 合成结果回填。
@pytest.mark.asyncio
async def test_respond_synthesizes_result() -> None:
    agent = _FakeHitlAgent()
    bus = _FakeBus()
    sup = _supervisor(_builder(agent))
    await _run_until_awaiting(sup, bus, "rs")
    await _resume(sup, bus, "rs", {"type": "respond", "message": "use cache"})

    assert agent.seen_resume == {"decisions": [{"type": "respond", "message": "use cache"}]}
    # respond：工具不经 projection，supervisor 据 snapshot 直发 done 终态，result=合成回复（非 rejected）。
    end = _data(_events_of(bus, "rs", "tool_call_end")[0])
    assert end["result"] == "use cache"
    assert end["rejected"] is False
    assert end["is_error"] is False
    _assert_completed(bus, "rs")


# --------------------------------------------------------------------------- #
# 真 deepagents 回归：reject/respond 工具不经 v3 projection 浮现（probe 实证），
# 故 supervisor 据 snapshot 直发终态。fake 曾把 reject 脚本成 projection 事件、掩盖此 bug。
# --------------------------------------------------------------------------- #


async def _real_web_fetch(url: str) -> str:
    return f"web_fetched {url}"


class _RealArgs(BaseModel):
    url: str = Field(description="u")


_real_tool = StructuredTool(
    name=_TOOL_NAME, description="d", args_schema=_RealArgs, coroutine=_real_web_fetch
)

_REAL_SCRIPT: list[AIMessage] = [
    AIMessage(
        content="",
        tool_calls=[{"name": _TOOL_NAME, "args": {"url": "x"}, "id": _TOOL_ID, "type": "tool_call"}],
    ),
    AIMessage(content="done"),
]


class _RealHitlModel(BaseChatModel):
    _cursor: int = PrivateAttr(default=0)

    @property
    def _llm_type(self) -> str:
        return "real-hitl"

    def bind_tools(self, tools: Any, **kwargs: Any) -> Runnable[LanguageModelInput, AIMessage]:
        return self.with_types(output_type=AIMessage)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        msg = _REAL_SCRIPT[min(self._cursor, len(_REAL_SCRIPT) - 1)]
        self._cursor += 1
        return ChatResult(generations=[ChatGeneration(message=msg)])


def _real_agent() -> InvokableAgent:
    return make_deep_agent(
        model=_RealHitlModel(),
        tools=[_real_tool],
        system_prompt="s",
        subagents=[],
        checkpointer=InMemorySaver(),
        permissions=[],
        interrupt_on={
            _TOOL_NAME: InterruptOnConfig(allowed_decisions=["approve", "edit", "reject", "respond"])
        },
    )


@pytest.mark.asyncio
async def test_real_deepagents_reject_emits_authoritative_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KOKORO_APPROVAL_TOOLS", _TOOL_NAME)
    bus = _FakeBus()
    agent = _real_agent()
    sup = _supervisor(lambda _req: agent)
    await _run_until_awaiting(sup, bus, "rr")
    await _resume(sup, bus, "rr", {"type": "reject", "message": "no"})
    ends = _events_of(bus, "rr", "tool_call_end")
    assert len(ends) == 1, f"恰好一个权威终态(supervisor 直发，projection 不发): {ends}"
    end = _data(ends[0])
    assert end["rejected"] is True
    assert end["is_error"] is False
    assert end["reject_reason"] == "no"


@pytest.mark.asyncio
async def test_real_deepagents_respond_emits_done(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOKORO_APPROVAL_TOOLS", _TOOL_NAME)
    bus = _FakeBus()
    agent = _real_agent()
    sup = _supervisor(lambda _req: agent)
    await _run_until_awaiting(sup, bus, "rs")
    await _resume(sup, bus, "rs", {"type": "respond", "message": "use cache"})
    ends = _events_of(bus, "rs", "tool_call_end")
    assert len(ends) == 1
    end = _data(ends[0])
    assert end["rejected"] is False
    assert end["is_error"] is False
    assert end["responded"] is True
    assert end["result"] == "use cache"


# --------------------------------------------------------------------------- #
# ① 真 deepagents 回归：同帧多个被门控工具 → resume 不再崩（旧单决策 wire 会触发 langchain
# 「decisions 数≠工具数」ValueError）；逐工具决策、支持部分审批（批一个、拒一个）。
# --------------------------------------------------------------------------- #

_MULTI_SCRIPT: list[AIMessage] = [
    AIMessage(
        content="",
        tool_calls=[
            {"name": _TOOL_NAME, "args": {"url": "a"}, "id": "call-A", "type": "tool_call"},
            {"name": _TOOL_NAME, "args": {"url": "b"}, "id": "call-B", "type": "tool_call"},
        ],
    ),
    AIMessage(content="done"),
]


class _RealMultiModel(BaseChatModel):
    _cursor: int = PrivateAttr(default=0)

    @property
    def _llm_type(self) -> str:
        return "real-multi"

    def bind_tools(self, tools: Any, **kwargs: Any) -> Runnable[LanguageModelInput, AIMessage]:
        return self.with_types(output_type=AIMessage)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        msg = _MULTI_SCRIPT[min(self._cursor, len(_MULTI_SCRIPT) - 1)]
        self._cursor += 1
        return ChatResult(generations=[ChatGeneration(message=msg)])


def _real_multi_agent() -> InvokableAgent:
    return make_deep_agent(
        model=_RealMultiModel(),
        tools=[_real_tool],
        system_prompt="s",
        subagents=[],
        checkpointer=InMemorySaver(),
        permissions=[],
        interrupt_on={
            _TOOL_NAME: InterruptOnConfig(allowed_decisions=["approve", "edit", "reject", "respond"])
        },
    )


@pytest.mark.asyncio
async def test_real_deepagents_multi_tool_partial_approval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KOKORO_REQUIRES_APPROVAL_TOOLS", _TOOL_NAME)
    bus = _FakeBus()
    agent = _real_multi_agent()
    sup = _supervisor(lambda _req: agent)
    await sup.dispatch(bus, _request("rm"))
    await _drain(sup)
    awaiting_ids = sorted(str(_data(a).get("tool_id")) for a in _events_of(bus, "rm", "tool_call_awaiting"))
    assert awaiting_ids == ["call-A", "call-B"]

    # 部分审批：批准 call-A、拒绝 call-B —— 旧单决策 wire 在此必崩。
    resume = _inbound(
        {
            "kind": "run.resume",
            "run_id": "rm",
            "decisions": [
                {"type": "approve", "tool_id": "call-A"},
                {"type": "reject", "tool_id": "call-B", "message": "no"},
            ],
        }
    )
    await sup.dispatch(bus, resume)
    await _drain(sup)

    assert _events_of(bus, "rm", "agent_error") == []  # 不崩
    ends = {_data(e).get("tool_id"): _data(e) for e in _events_of(bus, "rm", "tool_call_end")}
    assert ends["call-B"]["rejected"] is True  # 被拒工具权威 rejected
    assert ends["call-A"]["rejected"] is False  # 批准工具真跑、非 rejected
