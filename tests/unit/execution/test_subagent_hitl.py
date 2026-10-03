"""真实子代理 checkpoint 的完整 pending、审批与审核恢复，以及 GP 守卫。

此 invoke 层测试读取 native 状态，不伪造 durable interaction.state；持久 Run/Chat 桥另由 PG 测试证明。
"""

from __future__ import annotations

import pytest
import json
from typing import Literal
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.outputs import ChatResult
from langchain_core.runnables.config import RunnableConfig
from collections.abc import Iterator
from support.fakes import isolated_native_registry

from support.fakes import settled_state_callback

from support.fakes import finish_run

from support.fakes import terminal_emitter

from uuid import uuid4

from langchain_core.messages import AIMessage, HumanMessage, BaseMessage
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.types import Command, StateSnapshot, Interrupt
from pydantic import BaseModel, ConfigDict, TypeAdapter
from langchain.agents.middleware.human_in_the_loop import HITLRequest

from support.fakes import request, usage_recorder
from support.deepagents import create_test_deep_agent
from kokoro_agent.execution.events import RunEmitter
from kokoro_agent.execution.run_agent import invoke_once
from kokoro_agent.protocol.streams import run_events_stream
from kokoro_agent.protocol import RunFailedPayload
from kokoro_agent.protocol.events import SubagentStartedPayload
from kokoro_agent.hitl import HumanRequest
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
    gate_tool = StructuredTool(
        name="gated", description="d", args_schema=_NoArgs, func=_gated
    )
    main_model = LocalFakeChatModel.with_script(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "task",
                        "args": {"description": "do", "subagent_type": "helper"},
                        "id": "t1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="main done"),
        ]
    )
    sub_model = LocalFakeChatModel.with_script(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "gated", "args": {}, "id": "g1", "type": "tool_call"}
                ],
            ),
            AIMessage(content="sub done"),
        ]
    )
    return create_test_deep_agent(
        model=main_model,
        tools=[gate_tool],
        system_prompt="x",
        subagents=[
            {
                "name": "helper",
                "description": "h",
                "system_prompt": "s",
                "model": sub_model,
                "tools": [gate_tool],
            }
        ],
        checkpointer=saver,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset({"gated"})),
    )


async def test_subagent_approval_pauses_then_approve_completes(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    _EXECUTED["n"] = 0
    saver = checkpointer
    # This direct-invoke fixture proves native causality, not a Run attempt/fence.
    assert [
        row async for row in saver.alist({"configurable": {"thread_id": "tsub"}})
    ] == []
    sfx = uuid4().hex
    run1, run2 = f"rsub-{sfx}", f"rsub2-{sfx}"
    names = frozenset({"gated", "ask_user_question"})

    async def claim() -> bool:
        return True

    recorder, _seen = usage_recorder()
    first = await invoke_once(
        (terminal_test_emitter_1 := RunEmitter(stream, run1)),
        (settlement_agent := _build(saver)),
        "tsub",
        {"messages": [HumanMessage(content="go", id="m1")]},
        initial=True,
        approval_tool_names=names,
        source_for=lambda _n: "built-in",
        finalize_terminal=terminal_emitter(terminal_test_emitter_1, claim, recorder),
        record_usage=recorder,
        on_native_settled=settled_state_callback(settlement_agent, "tsub"),
    )
    assert first is False  # 暂停成卡，而非 run.failed
    assert _EXECUTED["n"] == 0  # 审批前工具未执行（无旁路）
    events = [item.event for item in await stream.read_all(run_events_stream(run1))]
    config: RunnableConfig = {"configurable": {"thread_id": "tsub"}}
    snapshot = await settlement_agent.aget_state(config, subgraphs=True)
    assert isinstance(snapshot, StateSnapshot)
    from langgraph.checkpoint.base import CheckpointTuple

    root_locator = snapshot.config.get("configurable", {})
    assert root_locator.get("thread_id") == "tsub"
    assert root_locator.get("checkpoint_ns") == ""
    root_id = root_locator.get("checkpoint_id")
    assert isinstance(root_id, str) and root_id
    assert len(snapshot.tasks) == 1
    root_task = snapshot.tasks[0]
    assert root_task.id and root_task.state is None
    assert len(root_task.interrupts) == 1
    pending = root_task.interrupts[0]
    assert pending.id and snapshot.interrupts == (pending,)
    root_exact: RunnableConfig = {
        "configurable": {
            "thread_id": "tsub",
            "checkpoint_ns": "",
            "checkpoint_id": root_id,
        }
    }
    root_tuple = await saver.aget_tuple(root_exact)
    assert root_tuple is not None
    assert root_tuple.config.get("configurable", {}) == root_exact.get("configurable")
    assert [
        (task, tuple(TypeAdapter(list[Interrupt]).validate_python(value, strict=True)))
        for task, channel, value in root_tuple.pending_writes or ()
        if channel == "__interrupt__"
    ] == [(root_task.id, (pending,))]

    # Dynamic task-tool subgraphs are not discoverable through task.state.
    # Fixed LangGraph records the dispatch checkpoint in metadata.parents; its
    # interrupt exit preserves that checkpoint ID (_loop._put_checkpoint).
    # Enumerate the same thread, never guess a namespace or select latest.
    children: list[tuple[CheckpointTuple, str, tuple[Interrupt, ...]]] = []
    async for candidate in saver.alist({"configurable": {"thread_id": "tsub"}}):
        locator = candidate.config.get("configurable", {})
        assert locator.get("thread_id") == "tsub"
        namespace = locator.get("checkpoint_ns")
        assert isinstance(namespace, str)
        if namespace == "":
            continue
        parents = candidate.metadata.get("parents", {})
        if parents.get("") != root_id:
            continue
        for task_id, channel, value in candidate.pending_writes or ():
            if channel == "__interrupt__":
                interrupts = tuple(
                    TypeAdapter(list[Interrupt]).validate_python(value, strict=True)
                )
                children.append((candidate, task_id, interrupts))
    # Zero/multiple matches remain a hard failure, not a broader ancestry guess.
    assert len(children) == 1
    child, child_task_id, child_interrupts = children[0]
    assert child_task_id and child_task_id != root_task.id
    assert child_interrupts == snapshot.interrupts == (pending,)
    child_locator = child.config.get("configurable", {})
    child_ns, child_id = (
        child_locator.get("checkpoint_ns"),
        child_locator.get("checkpoint_id"),
    )
    assert isinstance(child_ns, str) and child_ns
    assert isinstance(child_id, str) and child_id
    child_exact: RunnableConfig = {
        "configurable": {
            "thread_id": "tsub",
            "checkpoint_ns": child_ns,
            "checkpoint_id": child_id,
        }
    }
    persisted = await saver.aget_tuple(child_exact)
    assert persisted is not None
    assert persisted.config.get("configurable", {}) == child_exact.get("configurable")
    assert persisted.metadata.get("parents", {}).get("") == root_id
    assert persisted.checkpoint == child.checkpoint
    assert persisted.parent_config == child.parent_config
    assert persisted.pending_writes == child.pending_writes
    assert [
        (task, tuple(TypeAdapter(list[Interrupt]).validate_python(value, strict=True)))
        for task, channel, value in persisted.pending_writes or ()
        if channel == "__interrupt__"
    ] == [(child_task_id, (pending,))]
    reread = await settlement_agent.aget_state(config, subgraphs=True)
    assert isinstance(reread, StateSnapshot)
    assert reread.config == snapshot.config and reread.interrupts == snapshot.interrupts
    assert reread.tasks == snapshot.tasks
    assert not [
        event
        for event in events
        if event["kind"] in {"run.completed", "run.failed", "tool.awaiting_approval"}
    ]
    starts = [
        SubagentStartedPayload.model_validate(event["payload"])
        for event in events
        if event["kind"] == "subagent.started"
    ]
    assert len(starts) == 1 and starts[0].segment_id == starts[0].subagent_id == "t1"
    value = TypeAdapter(HITLRequest).validate_python(pending.value, strict=True)
    assert pending.value == value
    assert set(value) == {
        "action_requests",
        "review_configs",
    }
    actions, reviews = value["action_requests"], value["review_configs"]
    assert isinstance(actions, list) and len(actions) == 1
    assert actions[0]["name"] == "gated" and actions[0]["args"] == {}
    assert isinstance(reviews, list) and len(reviews) == 1
    assert reviews[0]["action_name"] == "gated"
    assert reviews[0]["allowed_decisions"] == ["approve", "edit", "reject"]

    # Rebuild the real graph against the same persisted pause; route by native ID.
    agent2 = _build(saver)
    second = await invoke_once(
        (terminal_test_emitter_2 := RunEmitter(stream, run2)),
        agent2,
        "tsub",
        Command(resume={pending.id: {"decisions": [{"type": "approve"}]}}),
        initial=False,
        approval_tool_names=names,
        source_for=lambda _n: "built-in",
        finalize_terminal=terminal_emitter(terminal_test_emitter_2, claim, recorder),
        record_usage=recorder,
        on_native_settled=settled_state_callback(agent2, "tsub"),
    )
    assert second is True
    assert _EXECUTED["n"] == 1  # 批准后在子代理内执行恰一次
    events2 = [item.event for item in await stream.read_all(run_events_stream(run2))]
    assert events2[-1]["kind"] == "run.completed"
    assert [
        event["kind"]
        for event in events2
        if event["kind"] in {"run.completed", "run.failed"}
    ] == ["run.completed"]
    settled = await agent2.aget_state(config, subgraphs=True)
    assert (
        isinstance(settled, StateSnapshot)
        and not settled.interrupts
        and not settled.tasks
    )


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
    lease = await run_repository.try_claim(request(run_id))
    assert lease is not None
    assert await finish_run(run_repository, run_id, lease)
    guard = TerminalGuardMiddleware(
        run_repository=run_repository, run_id=run_id, lease=lease
    )
    calls = {"main": 0, "gp": 0}

    class CountedModel(LocalFakeChatModel):
        role: Literal["main", "gp"] = "main"

        def _generate(
            self,
            messages: list[BaseMessage],
            stop: list[str] | None = None,
            run_manager: CallbackManagerForLLMRun | None = None,
            **kwargs: object,
        ) -> ChatResult:
            calls[self.role] += 1
            return super()._generate(
                messages, stop=stop, run_manager=run_manager, **kwargs
            )

    main_model = CountedModel.with_script(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "task",
                        "args": {
                            "description": "do",
                            "subagent_type": "general-purpose",
                        },
                        "id": "t1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="main done"),
        ]
    )
    gp_model = CountedModel.with_script(
        [AIMessage(content="guard-bypass-secret-r42")]
    ).model_copy(update={"role": "gp"})
    gp = general_purpose_subagent([guard])
    gp["model"] = gp_model
    agent = create_test_deep_agent(
        model=main_model,
        tools=[],
        system_prompt="x",
        subagents=[gp],
        checkpointer=checkpointer,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
    )

    async def claim() -> bool:
        return True

    recorder, _seen = usage_recorder()
    terminal = await invoke_once(
        (terminal_test_emitter_3 := RunEmitter(stream, run_id)),
        agent,
        "tgp",
        {"messages": [HumanMessage(content="go", id="m1")]},
        initial=True,
        approval_tool_names=frozenset({"ask_user_question"}),
        source_for=lambda _n: "built-in",
        finalize_terminal=terminal_emitter(terminal_test_emitter_3, claim, recorder),
        record_usage=recorder,
        on_native_settled=settled_state_callback(agent, "tgp"),
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
    assert calls == {"main": 1, "gp": 0}
    assert RunFailedPayload.model_validate(payload).model_dump(mode="json") == {
        "code": "internal_error",
        "retryable": False,
    }
    assert payload == {"code": "internal_error", "retryable": False}
    assert "run.completed" not in kinds
    serialized = json.dumps(payload)
    for forbidden in (
        "message",
        "lease generation was superseded",
        "RunSupersededError",
        "Traceback",
        "guard-bypass-secret-r42",
    ):
        assert forbidden not in serialized


async def test_subagent_review_pauses_with_cached_result(
    stream: RedisStream, checkpointer: BaseCheckpointSaver[str]
) -> None:
    # 审核政策同样不可被委派旁路：子代理内 review 工具执行后暂停，结果进卡（keep-first 缓存）。
    from support.fakes import FakeRunRepository
    from kokoro_agent.tools.middleware import ToolResultReviewMiddleware

    _EXECUTED["n"] = 0
    saver = checkpointer
    # This direct-invoke fixture proves native causality, not a Run attempt/fence.
    assert [
        row async for row in saver.alist({"configurable": {"thread_id": "trev"}})
    ] == []
    run_id = f"rrev-{uuid4().hex}"
    store = FakeRunRepository()
    lease = await store.try_claim(request(run_id))
    assert lease is not None
    gate_tool = StructuredTool(
        name="gated", description="d", args_schema=_NoArgs, func=_gated
    )
    review = ToolResultReviewMiddleware(frozenset({"gated"}), store, run_id, lease)
    main_model = LocalFakeChatModel.with_script(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "task",
                        "args": {"description": "do", "subagent_type": "helper"},
                        "id": "t1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="main done"),
        ]
    )
    sub_model = LocalFakeChatModel.with_script(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "gated", "args": {}, "id": "g1", "type": "tool_call"}
                ],
            ),
            AIMessage(content="sub done"),
        ]
    )
    agent = create_test_deep_agent(
        model=main_model,
        tools=[gate_tool],
        system_prompt="x",
        subagents=[
            {
                "name": "helper",
                "description": "h",
                "system_prompt": "s",
                "model": sub_model,
                "tools": [gate_tool],
                "middleware": [review],
            }
        ],
        checkpointer=saver,
        permissions=[],
        interrupt_on=build_interrupt_on(frozenset()),
    )

    async def claim() -> bool:
        return True

    recorder, _seen = usage_recorder()
    paused = await invoke_once(
        (
            terminal_test_emitter_4 := RunEmitter(
                stream, run_id, review_tool_names=frozenset({"gated"})
            )
        ),
        agent,
        "trev",
        {"messages": [HumanMessage(content="go", id="m1")]},
        initial=True,
        approval_tool_names=frozenset({"ask_user_question"}),
        source_for=lambda _n: "built-in",
        finalize_terminal=terminal_emitter(terminal_test_emitter_4, claim, recorder),
        record_usage=recorder,
        on_native_settled=settled_state_callback(agent, "trev"),
    )
    assert paused is False
    assert _EXECUTED["n"] == 1  # 审核语义：先执行后审
    events = [item.event for item in await stream.read_all(run_events_stream(run_id))]
    config: RunnableConfig = {"configurable": {"thread_id": "trev"}}
    snapshot = await agent.aget_state(config, subgraphs=True)
    assert isinstance(snapshot, StateSnapshot)
    from langgraph.checkpoint.base import CheckpointTuple

    root_locator = snapshot.config.get("configurable", {})
    assert root_locator.get("thread_id") == "trev"
    assert root_locator.get("checkpoint_ns") == ""
    root_id = root_locator.get("checkpoint_id")
    assert isinstance(root_id, str) and root_id
    assert len(snapshot.tasks) == 1
    root_task = snapshot.tasks[0]
    assert root_task.id and root_task.state is None
    assert len(root_task.interrupts) == 1
    pending = root_task.interrupts[0]
    assert pending.id and snapshot.interrupts == (pending,)
    root_exact: RunnableConfig = {
        "configurable": {
            "thread_id": "trev",
            "checkpoint_ns": "",
            "checkpoint_id": root_id,
        }
    }
    root_tuple = await saver.aget_tuple(root_exact)
    assert root_tuple is not None
    assert root_tuple.config.get("configurable", {}) == root_exact.get("configurable")
    assert [
        (task, tuple(TypeAdapter(list[Interrupt]).validate_python(value, strict=True)))
        for task, channel, value in root_tuple.pending_writes or ()
        if channel == "__interrupt__"
    ] == [(root_task.id, (pending,))]

    # Dynamic task-tool subgraphs are not discoverable through task.state.
    # Fixed LangGraph records the dispatch checkpoint in metadata.parents; its
    # interrupt exit preserves that checkpoint ID (_loop._put_checkpoint).
    # Enumerate the same thread, never guess a namespace or select latest.
    children: list[tuple[CheckpointTuple, str, tuple[Interrupt, ...]]] = []
    async for candidate in saver.alist({"configurable": {"thread_id": "trev"}}):
        locator = candidate.config.get("configurable", {})
        assert locator.get("thread_id") == "trev"
        namespace = locator.get("checkpoint_ns")
        assert isinstance(namespace, str)
        if namespace == "":
            continue
        parents = candidate.metadata.get("parents", {})
        if parents.get("") != root_id:
            continue
        for task_id, channel, value in candidate.pending_writes or ():
            if channel == "__interrupt__":
                interrupts = tuple(
                    TypeAdapter(list[Interrupt]).validate_python(value, strict=True)
                )
                children.append((candidate, task_id, interrupts))
    # Zero/multiple matches remain a hard failure, not a broader ancestry guess.
    assert len(children) == 1
    child, child_task_id, child_interrupts = children[0]
    assert child_task_id and child_task_id != root_task.id
    assert child_interrupts == snapshot.interrupts == (pending,)
    child_locator = child.config.get("configurable", {})
    child_ns, child_id = (
        child_locator.get("checkpoint_ns"),
        child_locator.get("checkpoint_id"),
    )
    assert isinstance(child_ns, str) and child_ns
    assert isinstance(child_id, str) and child_id
    child_exact: RunnableConfig = {
        "configurable": {
            "thread_id": "trev",
            "checkpoint_ns": child_ns,
            "checkpoint_id": child_id,
        }
    }
    persisted = await saver.aget_tuple(child_exact)
    assert persisted is not None
    assert persisted.config.get("configurable", {}) == child_exact.get("configurable")
    assert persisted.metadata.get("parents", {}).get("") == root_id
    assert persisted.checkpoint == child.checkpoint
    assert persisted.parent_config == child.parent_config
    assert persisted.pending_writes == child.pending_writes
    assert [
        (task, tuple(TypeAdapter(list[Interrupt]).validate_python(value, strict=True)))
        for task, channel, value in persisted.pending_writes or ()
        if channel == "__interrupt__"
    ] == [(child_task_id, (pending,))]
    reread = await agent.aget_state(config, subgraphs=True)
    assert isinstance(reread, StateSnapshot)
    assert reread.config == snapshot.config and reread.interrupts == snapshot.interrupts
    assert reread.tasks == snapshot.tasks
    assert not [
        event
        for event in events
        if event["kind"] in {"run.completed", "run.failed", "tool.awaiting_approval"}
    ]
    starts = [
        SubagentStartedPayload.model_validate(event["payload"])
        for event in events
        if event["kind"] == "subagent.started"
    ]
    assert len(starts) == 1 and starts[0].segment_id == starts[0].subagent_id == "t1"
    human = HumanRequest.from_interrupt_value(pending.value)
    assert human is not None and human.kind == "review" and human.request_id == "g1"
    assert human.context == {
        "name": "gated",
        "args": {},
        "result": "gated-ok",
        "is_error": False,
    }
    cached = await store.get_tool_result(run_id, "g1")
    assert cached == ("gated-ok", False)
    assert await invoke_once(
        terminal_test_emitter_4,
        agent,
        "trev",
        Command(resume={pending.id: [{"request_id": "g1", "type": "approve"}]}),
        initial=False,
        approval_tool_names=frozenset({"ask_user_question"}),
        source_for=lambda _n: "built-in",
        finalize_terminal=terminal_emitter(terminal_test_emitter_4, claim, recorder),
        record_usage=recorder,
        on_native_settled=settled_state_callback(agent, "trev"),
    )
    assert _EXECUTED["n"] == 1
    assert await store.get_tool_result(run_id, "g1") == cached
    settled = await agent.aget_state(config, subgraphs=True)
    assert (
        isinstance(settled, StateSnapshot)
        and not settled.interrupts
        and not settled.tasks
    )
    resumed_events = [
        item.event for item in await stream.read_all(run_events_stream(run_id))
    ]
    assert [
        event["kind"]
        for event in resumed_events
        if event["kind"] in {"run.completed", "run.failed"}
    ] == ["run.completed"]
    assert resumed_events[-1]["kind"] == "run.completed"


@pytest.fixture(autouse=True)
def isolate_sdk_profile_side_effects() -> Iterator[None]:
    with isolated_native_registry():
        yield
