# pyright: reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
"""Direct-lock preflight for the official LangGraph Swarm API."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import re
from typing import Any

from langchain.agents import create_agent
from langchain.agents.middleware import (
    SummarizationMiddleware,
    wrap_model_call,
    wrap_tool_call,
)
from langchain.tools import tool
from langchain_core.language_models.fake_chat_models import (
    FakeListChatModel,
    GenericFakeChatModel,
)
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_swarm import SwarmState, create_handoff_tool, create_swarm
from pydantic import PrivateAttr


def _peer(name: str, handoff_target: str):
    return create_agent(
        FakeListChatModel(responses=["done"]),
        tools=[create_handoff_tool(agent_name=handoff_target)],
        name=name,
    )


def test_official_swarm_dependency_constructs_checkpointed_outer_graph() -> None:
    """The future adapter is anchored to public official APIs, not a custom router."""
    alpha = _peer("alpha", "beta")
    beta = _peer("beta", "alpha")

    swarm = create_swarm(
        [alpha, beta],
        default_active_agent="alpha",
        state_schema=SwarmState,
    )

    assert swarm.compile(checkpointer=InMemorySaver()) is not None


def test_handoff_tool_exposes_agent_name_directly_so_agent_keys_must_be_safe() -> None:
    """The GA compiler must accept only direct lower-snake AgentKeys for Swarm peers.

    ``langgraph-swarm`` retains dots and hyphens when it creates the model-visible
    default handoff tool name.  A display slug or a Workflow-local alias therefore
    cannot be used as an Agent identity and then repaired at tool-bind time.
    """
    assert re.fullmatch(r"[a-z][a-z0-9_]{0,63}", "creative_image_director")
    assert not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", "creative.image-director")

    safe = create_handoff_tool(agent_name="creative_image_director")
    dotted = create_handoff_tool(agent_name="creative.image-director")
    hyphenated = create_handoff_tool(agent_name="creative-image-director")

    assert safe.name == "transfer_to_creative_image_director"
    assert dotted.name == "transfer_to_creative.image-director"
    assert hyphenated.name == "transfer_to_creative-image-director"


def test_official_swarm_honors_active_agent_input_so_ga_must_never_forward_it() -> None:
    """Native Swarm treats ``active_agent`` as state input, not trusted routing policy."""
    alpha = create_agent(FakeListChatModel(responses=["alpha"]), name="alpha")
    beta = create_agent(FakeListChatModel(responses=["beta"]), name="beta")
    swarm = create_swarm(
        [alpha, beta],
        default_active_agent="alpha",
        state_schema=SwarmState,
    ).compile(checkpointer=InMemorySaver())

    result = swarm.invoke(
        {
            "messages": [{"role": "user", "content": "hello"}],
            "active_agent": "beta",
        },
        config={"configurable": {"thread_id": "swarm-forbidden-state-input"}},
    )

    assert result["messages"][-1].content == "beta"
    assert result["active_agent"] == "beta"


@dataclass(frozen=True)
class _SwarmRuntimeContext:
    marker: str


_seen_swarm_contexts: list[object] = []


@wrap_model_call
def _capture_swarm_runtime_context(request: Any, handler: Callable[[Any], Any]) -> Any:
    _seen_swarm_contexts.append(request.runtime.context)
    return handler(request)


def _invoke_swarm_with_runtime_context(
    compiled_swarm: Any,
    runtime_context: _SwarmRuntimeContext,
) -> None:
    """Isolate the current unparameterized ``create_swarm().compile()`` type surface."""
    compiled_swarm.invoke(
        {"messages": [{"role": "user", "content": "hello"}]},
        config={"configurable": {"thread_id": "swarm-runtime-context"}},
        context=runtime_context,
    )


def test_official_swarm_propagates_runtime_context_to_the_active_peer() -> None:
    """RuntimeContext remains an invocation input, not SwarmState or peer configuration."""
    _seen_swarm_contexts.clear()
    alpha = create_agent(
        FakeListChatModel(responses=["done"]),
        middleware=[_capture_swarm_runtime_context],
        name="alpha",
        context_schema=_SwarmRuntimeContext,
    )
    beta = create_agent(
        FakeListChatModel(responses=["done"]),
        middleware=[_capture_swarm_runtime_context],
        name="beta",
        context_schema=_SwarmRuntimeContext,
    )
    swarm = create_swarm(
        [alpha, beta],
        default_active_agent="alpha",
        state_schema=SwarmState,
        context_schema=_SwarmRuntimeContext,
    ).compile(checkpointer=InMemorySaver())

    runtime_context = _SwarmRuntimeContext(marker="same-run-scope")
    _invoke_swarm_with_runtime_context(swarm, runtime_context)

    assert _seen_swarm_contexts == [runtime_context]


def _summarizing_swarm(
    checkpointer: InMemorySaver,
    *,
    alpha_messages: list[AIMessage],
    beta_messages: list[AIMessage],
    summary: str,
) -> Any:
    """Build a fresh peer swarm with the same native summarization configuration."""
    alpha = create_agent(
        ToolCapturingFakeModel(messages=iter(alpha_messages)),
        tools=[create_handoff_tool(agent_name="beta")],
        middleware=[
            SummarizationMiddleware(
                model=FakeListChatModel(responses=[summary]),
                trigger=("messages", 3),
                keep=("messages", 2),
            )
        ],
        name="alpha",
    )
    beta = create_agent(
        ToolCapturingFakeModel(messages=iter(beta_messages)),
        middleware=[
            SummarizationMiddleware(
                model=FakeListChatModel(responses=[summary]),
                trigger=("messages", 3),
                keep=("messages", 2),
            )
        ],
        name="beta",
    )
    return create_swarm(
        [alpha, beta],
        default_active_agent="alpha",
        state_schema=SwarmState,
    ).compile(checkpointer=checkpointer)


def test_peer_summarization_does_not_compact_outer_swarm_history() -> None:
    """Peer-local summary updates append to outer Swarm messages instead of replacing them.

    GA must keep configured summarization disabled for Swarm on this framework tuple.
    A DeepAgents outer can use its native compaction, but a peer's
    ``RemoveMessage(REMOVE_ALL_MESSAGES)`` update does not become an outer Swarm
    history replacement.
    """
    checkpointer = InMemorySaver()
    first = _summarizing_swarm(
        checkpointer,
        alpha_messages=[
            AIMessage(
                content="",
                tool_calls=[{"name": "transfer_to_beta", "args": {}, "id": "to_beta"}],
            )
        ],
        beta_messages=[AIMessage(content="beta before restart")],
        summary="first summary",
    )

    first_result = first.invoke(
        {"messages": [{"role": "user", "content": "start"}]},
        config={"configurable": {"thread_id": "swarm-peer-summarization"}},
    )

    first_contents = [str(message.content) for message in first_result["messages"]]
    assert first_result["active_agent"] == "beta"
    assert "start" in first_contents
    assert "Successfully transferred to beta" in first_contents
    assert (
        "Here is a summary of the conversation to date:\n\nfirst summary"
        in first_contents
    )
    assert "beta before restart" in first_contents

    restarted = _summarizing_swarm(
        checkpointer,
        alpha_messages=[AIMessage(content="alpha must not resume")],
        beta_messages=[AIMessage(content="beta after restart")],
        summary="restart summary",
    )
    restarted_result = restarted.invoke(
        {"messages": [{"role": "user", "content": "continue"}]},
        config={"configurable": {"thread_id": "swarm-peer-summarization"}},
    )

    restarted_contents = [
        str(message.content) for message in restarted_result["messages"]
    ]
    assert restarted_result["active_agent"] == "beta"
    assert "start" in restarted_contents
    assert "Successfully transferred to beta" in restarted_contents
    assert "first summary" in "\n".join(restarted_contents)
    assert "beta after restart" in restarted_contents


@tool
def find_skills(query: str) -> str:
    """Find a GA-owned Skill candidate."""
    return query


@tool
def unsafe_tool(query: str) -> str:
    """Represents a tool that this compiled Workflow did not allow."""
    raise AssertionError("an unlisted Swarm tool must never execute")


@wrap_model_call
def _visible_swarm_tools(request: Any, handler: Callable[[Any], Any]) -> Any:
    """The model side of the frozen, compile-time ToolPolicy allow-set."""
    return handler(
        request.override(
            tools=[tool_ for tool_ in request.tools if tool_.name == "find_skills"]
        )
    )


@wrap_tool_call
def _dispatch_swarm_tools(request: Any, handler: Callable[[Any], Any]) -> Any:
    """The dispatch side must still fail closed if a model emits an unbound call."""
    if request.tool_call["name"] != "find_skills":
        return ToolMessage(
            content="blocked by static tool policy",
            name=request.tool_call["name"],
            tool_call_id=request.tool_call["id"],
            status="error",
        )
    return handler(request)


class ToolCapturingFakeModel(GenericFakeChatModel):
    """Ignores the bind result while retaining its surface for assertions."""

    _bound_tool_names: list[str] = PrivateAttr(default_factory=list)

    def bind_tools(self, tools: Sequence[Any], **kwargs: Any) -> ToolCapturingFakeModel:
        del kwargs
        self._bound_tool_names = [str(getattr(tool_, "name", tool_)) for tool_ in tools]
        return self

    @property
    def bound_tool_names(self) -> list[str]:
        """Expose a stable test observation without leaking the Pydantic private attr."""
        return list(self._bound_tool_names)


_colliding_handoff_tool_calls = 0


@tool("transfer_to_beta")
def _colliding_handoff_tool() -> str:
    """A registered business tool that illegally occupies an official handoff name."""
    global _colliding_handoff_tool_calls
    _colliding_handoff_tool_calls += 1
    return "wrong tool executed"


def test_duplicate_handoff_tool_name_silently_replaces_the_official_handoff() -> None:
    """GA must reject duplicate/reserved tool names before binding a Swarm peer."""
    global _colliding_handoff_tool_calls
    _colliding_handoff_tool_calls = 0
    alpha_model = ToolCapturingFakeModel(
        messages=iter(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "transfer_to_beta",
                            "args": {},
                            "id": "call_colliding_handoff",
                        }
                    ],
                ),
                AIMessage(content="alpha still owns the conversation"),
            ]
        )
    )
    alpha = create_agent(
        alpha_model,
        tools=[create_handoff_tool(agent_name="beta"), _colliding_handoff_tool],
        name="alpha",
    )
    beta = create_agent(FakeListChatModel(responses=["beta"]), name="beta")
    swarm = create_swarm(
        [alpha, beta],
        default_active_agent="alpha",
        state_schema=SwarmState,
    ).compile(checkpointer=InMemorySaver())

    result = swarm.invoke(
        {"messages": [{"role": "user", "content": "handoff"}]},
        config={"configurable": {"thread_id": "swarm-duplicate-tool-name"}},
    )

    assert alpha_model.bound_tool_names == ["transfer_to_beta"]
    assert _colliding_handoff_tool_calls == 1
    assert result["messages"][-1].content == "alpha still owns the conversation"
    assert result.get("active_agent") is None


@wrap_tool_call
def _reject_non_atomic_swarm_handoff_batch(
    request: Any, handler: Callable[[Any], Any]
) -> Any:
    """Test-only model for GA's future full-message handoff guard."""
    latest_message = request.state["messages"][-1]
    tool_calls = latest_message.tool_calls
    has_handoff = any(
        str(tool_call["name"]).startswith("transfer_to_") for tool_call in tool_calls
    )
    if has_handoff and len(tool_calls) != 1:
        return ToolMessage(
            content="invalid_handoff_batch",
            name=request.tool_call["name"],
            tool_call_id=request.tool_call["id"],
            status="error",
        )
    return handler(request)


def test_full_message_handoff_guard_prevents_every_batch_dispatch() -> None:
    """A middleware guard can fail the complete invalid batch before Swarm routes it."""
    alpha_model = ToolCapturingFakeModel(
        messages=iter(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "transfer_to_beta", "args": {}, "id": "to_beta"},
                        {"name": "transfer_to_gamma", "args": {}, "id": "to_gamma"},
                    ],
                ),
                AIMessage(content="alpha retained the conversation"),
            ]
        )
    )
    alpha = create_agent(
        alpha_model,
        tools=[
            create_handoff_tool(agent_name="beta"),
            create_handoff_tool(agent_name="gamma"),
        ],
        middleware=[_reject_non_atomic_swarm_handoff_batch],
        name="alpha",
    )
    beta = create_agent(
        ToolCapturingFakeModel(messages=iter([AIMessage(content="beta")])), name="beta"
    )
    gamma = create_agent(
        ToolCapturingFakeModel(messages=iter([AIMessage(content="gamma")])),
        name="gamma",
    )
    swarm = create_swarm(
        [alpha, beta, gamma],
        default_active_agent="alpha",
        state_schema=SwarmState,
    ).compile(checkpointer=InMemorySaver())

    result = swarm.invoke(
        {"messages": [{"role": "user", "content": "handoff twice"}]},
        config={"configurable": {"thread_id": "swarm-handoff-batch-guard"}},
    )

    errors = [
        message for message in result["messages"] if isinstance(message, ToolMessage)
    ]
    assert result.get("active_agent") is None
    assert [(message.name, message.status, message.content) for message in errors] == [
        ("transfer_to_beta", "error", "invalid_handoff_batch"),
        ("transfer_to_gamma", "error", "invalid_handoff_batch"),
    ]
    assert result["messages"][-1].content == "alpha retained the conversation"


def test_hybrid_handoff_batch_blocks_the_other_tool_before_dispatch() -> None:
    """A handoff cannot race a normal effect/tool inside the same model message."""
    lookup_calls: list[str] = []

    @tool("lookup")
    def lookup(query: str) -> str:
        """A normal GA tool whose handler must not run in a hybrid handoff batch."""
        lookup_calls.append(query)
        return query

    alpha_model = ToolCapturingFakeModel(
        messages=iter(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "transfer_to_beta", "args": {}, "id": "to_beta"},
                        {"name": "lookup", "args": {"query": "music"}, "id": "lookup"},
                    ],
                ),
                AIMessage(content="alpha handled the invalid batch"),
            ]
        )
    )
    alpha = create_agent(
        alpha_model,
        tools=[create_handoff_tool(agent_name="beta"), lookup],
        middleware=[_reject_non_atomic_swarm_handoff_batch],
        name="alpha",
    )
    beta = create_agent(
        ToolCapturingFakeModel(messages=iter([AIMessage(content="beta")])), name="beta"
    )
    swarm = create_swarm(
        [alpha, beta],
        default_active_agent="alpha",
        state_schema=SwarmState,
    ).compile(checkpointer=InMemorySaver())

    result = swarm.invoke(
        {"messages": [{"role": "user", "content": "handoff and lookup"}]},
        config={"configurable": {"thread_id": "swarm-hybrid-handoff-batch"}},
    )

    errors = [
        message for message in result["messages"] if isinstance(message, ToolMessage)
    ]
    assert lookup_calls == []
    assert result.get("active_agent") is None
    assert [(message.name, message.status, message.content) for message in errors] == [
        ("transfer_to_beta", "error", "invalid_handoff_batch"),
        ("lookup", "error", "invalid_handoff_batch"),
    ]
    assert result["messages"][-1].content == "alpha handled the invalid batch"


def _run_multi_handoff_swarm(
    tool_calls: list[dict[str, Any]],
) -> tuple[
    dict[str, Any],
    ToolCapturingFakeModel,
    ToolCapturingFakeModel,
    ToolCapturingFakeModel,
]:
    """Run one peer response that illegally asks for two official handoffs."""
    alpha_model = ToolCapturingFakeModel(
        messages=iter([AIMessage(content="", tool_calls=tool_calls)])
    )
    beta_model = ToolCapturingFakeModel(
        messages=iter([AIMessage(content="beta reply")])
    )
    gamma_model = ToolCapturingFakeModel(
        messages=iter([AIMessage(content="gamma reply")])
    )
    alpha = create_agent(
        alpha_model,
        tools=[
            create_handoff_tool(agent_name="beta"),
            create_handoff_tool(agent_name="gamma"),
        ],
        name="alpha",
    )
    beta = create_agent(beta_model, name="beta")
    gamma = create_agent(gamma_model, name="gamma")
    swarm = create_swarm(
        [alpha, beta, gamma],
        default_active_agent="alpha",
        state_schema=SwarmState,
    ).compile(checkpointer=InMemorySaver())

    result = swarm.invoke(
        {"messages": [{"role": "user", "content": "handoff twice"}]},
        config={"configurable": {"thread_id": "swarm-multiple-handoffs"}},
    )

    return result, alpha_model, beta_model, gamma_model


def test_multiple_official_handoffs_are_accepted_but_collapse_to_one_peer() -> None:
    """ToolNode accepts a multi-handoff message but cannot represent two transfers.

    Official handoff tools return separate ``Command.PARENT`` updates. ToolNode
    executes the calls as one tool batch; current framework scheduling collapses
    that batch to one peer, without a protocol error for the rejected transfer.
    GA must reject the complete model tool-call batch before any tool dispatch.
    """
    result, alpha_model, _, _ = _run_multi_handoff_swarm(
        [
            {"name": "transfer_to_beta", "args": {}, "id": "to_beta"},
            {"name": "transfer_to_gamma", "args": {}, "id": "to_gamma"},
        ]
    )

    assert alpha_model.bound_tool_names == ["transfer_to_beta", "transfer_to_gamma"]
    assert len(result["messages"][1].tool_calls) == 2
    assert result["active_agent"] in {"beta", "gamma"}
    handoff_messages = [
        message
        for message in result["messages"]
        if isinstance(message, ToolMessage)
        and isinstance(message.name, str)
        and message.name.startswith("transfer_to_")
    ]
    assert len(handoff_messages) == 1
    assert handoff_messages[0].name == f"transfer_to_{result['active_agent']}"
    assert result["messages"][-1].content == f"{result['active_agent']} reply"


def test_official_swarm_peer_uses_static_model_and_dispatch_tool_policy() -> None:
    """A Swarm peer uses official ``create_agent`` without reopening a generic tool surface."""
    alpha_model = ToolCapturingFakeModel(
        messages=iter(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "unsafe_tool",
                            "args": {"query": "must not run"},
                            "id": "call_unlisted_swarm_tool",
                        }
                    ],
                ),
                "done",
            ]
        )
    )
    alpha = create_agent(
        alpha_model,
        tools=[find_skills, unsafe_tool],
        middleware=[_visible_swarm_tools, _dispatch_swarm_tools],
        name="alpha",
    )
    beta = create_agent(
        FakeListChatModel(responses=["done"]),
        tools=[find_skills],
        middleware=[_visible_swarm_tools, _dispatch_swarm_tools],
        name="beta",
    )
    swarm = create_swarm(
        [alpha, beta],
        default_active_agent="alpha",
        state_schema=SwarmState,
    ).compile(checkpointer=InMemorySaver())

    result = swarm.invoke(
        {"messages": [{"role": "user", "content": "hello"}]},
        config={"configurable": {"thread_id": "swarm-tool-policy"}},
    )

    assert alpha_model.bound_tool_names == ["find_skills"]
    blocked = [
        message for message in result["messages"] if isinstance(message, ToolMessage)
    ]
    assert len(blocked) == 1
    assert blocked[0].name == "unsafe_tool"
    assert blocked[0].status == "error"
    assert blocked[0].content == "blocked by static tool policy"
