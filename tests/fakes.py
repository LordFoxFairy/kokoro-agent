"""测试共用的强类型 fake：总线、run 状态存储、v3 投影流与 agent。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeVar

from langchain_core.messages import AIMessage
from langchain_core.runnables.config import RunnableConfig
from langgraph.types import Interrupt
from pydantic import JsonValue

from kokoro_agent.contract import (
    AgentEvent,
    ModelConfig,
    Permissions,
    RunInput,
    RunRequest,
    RuntimeConfig,
    RuntimeContext,
    agent_event_adapter,
    run_events_stream,
)
from kokoro_agent.contract import REQUESTS_STREAM
from kokoro_agent.streams.protocol import StreamItem

_T = TypeVar("_T")
_E = TypeVar("_E")


async def aiter_items(items: Sequence[_T]) -> AsyncIterator[_T]:
    for item in items:
        yield item


def find_events(events: Sequence[AgentEvent], cls: type[_E]) -> list[_E]:
    return [event for event in events if isinstance(event, cls)]


def find_event(events: Sequence[AgentEvent], cls: type[_E]) -> _E:
    matched = find_events(events, cls)
    assert matched, f"no {cls.__name__} in {[e.kind for e in events]}"
    return matched[0]


class FakeBus:
    """内存 fake：publish 落地即可 read_all（供 RunEmitter.attach 续接），ack 记账。"""

    def __init__(
        self,
        inbound: Sequence[StreamItem] = (),
        control: Mapping[str, Sequence[StreamItem]] | None = None,
    ) -> None:
        self.published: list[tuple[str, dict[str, JsonValue], int]] = []
        self.acked: list[str] = []
        self.deleted: list[str] = []
        self.expired_streams: list[tuple[str, int]] = []
        self._inbound = tuple(inbound)
        # per-run control 流独立化：请求流投 _inbound，各 control 流投各自项。
        self._control = {stream: tuple(items) for stream, items in (control or {}).items()}

    async def publish(
        self, stream: str, event: Mapping[str, JsonValue], *, maxlen: int
    ) -> StreamItem:
        self.published.append((stream, dict(event), maxlen))
        return StreamItem(cursor=str(len(self.published)), event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return [
            StreamItem(cursor=str(i), event=event)
            for i, (name, event, _maxlen) in enumerate(self.published)
            if name == stream
        ]

    async def subscribe(
        self, stream: str, *, group: str, consumer: str
    ) -> AsyncIterator[StreamItem]:
        items = self._inbound if stream == REQUESTS_STREAM else self._control.get(stream, ())
        for item in items:
            yield item

    async def ack(self, stream: str, group: str, cursor: str) -> None:
        self.acked.append(cursor)

    async def delete(self, stream: str) -> None:
        self.deleted.append(stream)

    async def expire(self, stream: str, ttl_s: int) -> None:
        self.expired_streams.append((stream, ttl_s))

    def run_events(self, run_id: str) -> list[AgentEvent]:
        return [
            agent_event_adapter.validate_python(event)
            for name, event, _maxlen in self.published
            if name == run_events_stream(run_id)
        ]

    def kinds(self, run_id: str) -> list[str]:
        return [event.kind for event in self.run_events(run_id)]


class FakeLedger:
    """协议等价的内存 store：租约以 leases dict 表达，None=暂停哨兵。"""

    def __init__(self) -> None:
        self.requests: dict[str, RunRequest] = {}
        self.terminals: set[str] = set()
        self.leases: dict[str, int | None] = {}
        self.owners: dict[str, str] = {}
        self.renewed: list[str] = []
        self.paused_runs: list[str] = []
        self.expired: list[RunRequest] = []
        self.tool_results: dict[tuple[str, str], tuple[str, bool]] = {}
        self.token_totals: dict[str, int] = {}
        self.usage_totals: dict[str, tuple[int, int]] = {}
        self.steers: dict[str, list[tuple[str, str]]] = {}
        self.sandbox_ids: dict[str, str] = {}
        self.terminal_at: dict[str, int] = {}
        self.clock_ms = 0

    async def try_claim(self, request: RunRequest, owner: str = "test-consumer") -> bool:
        if request.run_id in self.requests:
            return False
        self.requests[request.run_id] = request
        self.leases[request.run_id] = 1
        self.owners[request.run_id] = owner
        return True

    async def renew(self, run_id: str, owner: str = "test-consumer") -> bool:
        self.renewed.append(run_id)
        if run_id in self.terminals:
            return False
        if self.owners.get(run_id, owner) != owner:
            return False
        self.leases[run_id] = 1
        self.owners.setdefault(run_id, owner)
        return True

    async def adopt(self, run_id: str, owner: str = "test-consumer") -> None:
        if run_id not in self.terminals:
            self.leases[run_id] = 1
            self.owners[run_id] = owner

    async def pause(self, run_id: str) -> None:
        self.paused_runs.append(run_id)
        if run_id not in self.terminals:
            self.leases[run_id] = None

    async def reclaim_expired(self, owner: str = "test-consumer") -> list[RunRequest]:
        out = self.expired
        self.expired = []
        for req in out:
            self.owners[req.run_id] = owner
        return out

    async def list_paused(self) -> list[str]:
        return sorted(
            run_id
            for run_id, lease in self.leases.items()
            if lease is None and run_id not in self.terminals
        )

    async def get_request(self, run_id: str) -> RunRequest | None:
        return self.requests.get(run_id)

    async def add_tokens(self, run_id: str, count: int) -> int:
        self.token_totals[run_id] = self.token_totals.get(run_id, 0) + count
        return self.token_totals[run_id]

    async def add_usage(self, run_id: str, input_tokens: int, output_tokens: int) -> tuple[int, int]:
        cur_in, cur_out = self.usage_totals.get(run_id, (0, 0))
        self.usage_totals[run_id] = (cur_in + input_tokens, cur_out + output_tokens)
        return self.usage_totals[run_id]

    async def try_mark_terminal(self, run_id: str) -> bool:
        if run_id in self.terminals:
            return False
        self.terminals.add(run_id)
        self.terminal_at[run_id] = self.clock_ms
        return True

    async def purge_terminal(self, max_age_ms: int) -> int:
        cutoff = self.clock_ms - max_age_ms
        stale = [r for r in self.terminals if self.terminal_at.get(r, 0) <= cutoff]
        for run_id in stale:
            self.terminals.discard(run_id)
            self.terminal_at.pop(run_id, None)
            self.requests.pop(run_id, None)
            self.leases.pop(run_id, None)
            self.token_totals.pop(run_id, None)
            self.usage_totals.pop(run_id, None)
            self.steers.pop(run_id, None)
            self.tool_results = {k: v for k, v in self.tool_results.items() if k[0] != run_id}
        return len(stale)

    async def is_terminal(self, run_id: str) -> bool:
        return run_id in self.terminals

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        if run_id not in self.requests:
            return
        box = self.steers.setdefault(run_id, [])
        if all(mid != message_id for mid, _ in box):
            box.append((message_id, content))

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        return list(self.steers.get(run_id, []))

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None:
        box = self.steers.get(run_id)
        if box is None:
            return
        self.steers[run_id] = [(mid, c) for mid, c in box if mid not in set(message_ids)]

    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None:
        self.tool_results.setdefault((run_id, tool_id), (result, is_error))

    async def get_tool_result(self, run_id: str, tool_id: str) -> tuple[str, bool] | None:
        return self.tool_results.get((run_id, tool_id))

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None:
        self.sandbox_ids.setdefault(run_id, sandbox_id)

    async def get_sandbox_id(self, run_id: str) -> str | None:
        return self.sandbox_ids.get(run_id)


@dataclass
class FakeToolCall:
    tool_call_id: str
    tool_name: str
    input: dict[str, object] | None = None
    output: object = None
    error: str | None = None
    completed: bool = True
    deltas: Sequence[object] = ()

    @property
    def output_deltas(self) -> AsyncIterator[object]:
        return aiter_items(tuple(self.deltas))


@dataclass
class FakeModel:
    text_deltas: Sequence[str] = ()
    reasoning_deltas: Sequence[str] = ()
    output_message: AIMessage | None = None
    message_id: str | None = "seg"
    namespace: list[str] = field(default_factory=list[str])
    node: str | None = "model"

    @property
    def text(self) -> AsyncIterator[str]:
        return aiter_items(self.text_deltas)

    @property
    def reasoning(self) -> AsyncIterator[str]:
        return aiter_items(self.reasoning_deltas)


@dataclass
class FakeSubagentRun:
    name: str | None = "researcher"
    trigger_call_id: str | None = "sub-call-1"
    task_input: str | None = "investigate"
    status: str = "success"
    models: Sequence[FakeModel] = ()
    tool_views: Sequence[FakeToolCall] = ()

    @property
    def messages(self) -> AsyncIterator[FakeModel]:
        return aiter_items(self.models)

    @property
    def tool_calls(self) -> AsyncIterator[FakeToolCall]:
        return aiter_items(self.tool_views)

    @property
    def subagents(self) -> AsyncIterator["FakeSubagentRun"]:
        return aiter_items(())

    @property
    def custom(self) -> AsyncIterator[object]:
        return aiter_items(())


@dataclass
class FakeRunStream:
    models: Sequence[FakeModel] = ()
    tool_views: Sequence[FakeToolCall] = ()
    subagent_runs: Sequence[FakeSubagentRun] = ()
    custom_items: Sequence[object] = ()
    is_interrupted: bool = False
    raise_on_messages: bool = False

    @property
    def messages(self) -> AsyncIterator[FakeModel]:
        if self.raise_on_messages:
            raise RuntimeError("boom")
        return aiter_items(self.models)

    @property
    def tool_calls(self) -> AsyncIterator[FakeToolCall]:
        return aiter_items(self.tool_views)

    @property
    def subagents(self) -> AsyncIterator[FakeSubagentRun]:
        return aiter_items(self.subagent_runs)

    @property
    def custom(self) -> AsyncIterator[object]:
        return aiter_items(self.custom_items)

    async def interrupted(self) -> bool:
        return self.is_interrupted

    async def __aenter__(self) -> "FakeRunStream":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


@dataclass
class FakeState:
    interrupts: tuple[Interrupt, ...] = ()
    values: Mapping[str, object] = field(default_factory=dict[str, object])


@dataclass
class FakeAgent:
    run: FakeRunStream = field(default_factory=FakeRunStream)
    state: FakeState = field(default_factory=FakeState)
    raise_on_stream: Exception | None = None
    seen_payloads: list[object] = field(default_factory=list[object])
    seen_config: dict[str, object] = field(default_factory=dict[str, object])
    # 每次 astream_events 依序消费一个 gate（不足则不阻塞）：模拟长时运行与任务竞态。
    gates: list[asyncio.Event] = field(default_factory=list[asyncio.Event])
    seen_contexts: list[object] = field(default_factory=list[object])
    _calls: int = 0

    async def astream_events(
        self,
        payload: object,
        *,
        version: str,
        config: RunnableConfig,
        transformers: Sequence[object],
        context: object | None = None,
    ) -> FakeRunStream:
        self.seen_payloads.append(payload)
        self.seen_contexts.append(context)
        self.seen_config.update(config)
        call = self._calls
        self._calls += 1
        if call < len(self.gates):
            await self.gates[call].wait()
        if self.raise_on_stream is not None:
            raise self.raise_on_stream
        return self.run

    async def aget_state(self, config: RunnableConfig) -> FakeState:
        return self.state


def text_model(text: str, *, msg_id: str = "seg") -> FakeModel:
    return FakeModel(
        text_deltas=(text,), output_message=AIMessage(content=text, id=msg_id), message_id=msg_id
    )


def text_run(text: str = "done") -> FakeRunStream:
    return FakeRunStream(models=(text_model(text),))


def request(
    run_id: str,
    *,
    session_id: str = "s1",
    thread_id: str = "c1",
    namespace: str = "local:s1",
    content: str = "hello",
    approval_tools: tuple[str, ...] = (),
    review_tools: tuple[str, ...] = (),
) -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id=run_id,
        thread_id=thread_id,
        input=RunInput(message_id=f"{run_id}-m", content=content),
        runtime=RuntimeConfig(
        agent_type="general",
            model=ModelConfig(provider="anthropic", name="claude"),
            tools=[],
            skills=[],
            mcp_servers=[],
            subagents=[],
            backend="state",
            permissions=Permissions(
                approval_tools=list(approval_tools),
                review_tools=list(review_tools),
                subagent_create="deny",
                filesystem="read_only",
            ),
        ),
        context=RuntimeContext(namespace=namespace, session_id=session_id),
    )


def usage_recorder() -> tuple[Callable[[int, int], Awaitable[tuple[int, int]]], dict[str, int]]:
    """invoke_once 用量入账的测试替身：返回 (recorder, 累计观测)。"""
    seen = {"input": 0, "output": 0}

    async def record(input_tokens: int, output_tokens: int) -> tuple[int, int]:
        seen["input"] += input_tokens
        seen["output"] += output_tokens
        return (seen["input"], seen["output"])

    return record, seen
