# pyright: reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false, reportAttributeAccessIssue=false, reportOptionalMemberAccess=false, reportUnknownArgumentType=false, reportIncompatibleMethodOverride=false, reportPrivateUsage=false
"""Direct-lock preflight for Kokoro's native DeepAgents construction boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextvars import ContextVar
from importlib.metadata import version
from typing import Any, cast

from deepagents import (
    GeneralPurposeSubagentProfile,
    HarnessProfile,
    create_deep_agent,
    register_harness_profile,
)
from deepagents.backends import FilesystemBackend, StateBackend
from deepagents.backends.protocol import WriteResult
from langchain.agents.middleware import wrap_model_call, wrap_tool_call
from langchain.tools import tool
from langchain_core.language_models.fake_chat_models import (
    FakeListChatModel,
    GenericFakeChatModel,
)
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.config import get_config
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from pydantic import PrivateAttr
import pytest
from typing_extensions import TypedDict
from packaging.version import Version


@tool
def catalog_lookup(query: str) -> str:
    """Look up a record in a fixed test catalog."""
    return query


@tool
def catalog_read(candidate_ref: str) -> str:
    """Read one record from a fixed test catalog."""
    return candidate_ref


class WorkbenchBackendProbe(StateBackend):
    """A direct ``BackendProtocol`` instance, not a runtime backend factory or StoreBackend."""


class OffloadUnavailableBackend(StateBackend):
    """Makes DeepAgents take its documented non-fatal offload-failure path."""

    def write(self, file_path: str, content: str) -> WriteResult:
        del file_path, content
        return WriteResult(error="workbench unavailable")


class ToolCapturingFakeModel(FakeListChatModel):
    """Captures the tool surface that DeepAgents binds to the model."""

    _bound_tool_names: list[str] = PrivateAttr(default_factory=list)

    def bind_tools(self, tools: list[Any], **kwargs: Any) -> ToolCapturingFakeModel:
        del kwargs
        self._bound_tool_names = [str(getattr(tool_, "name", tool_)) for tool_ in tools]
        return self

class SkillPromptCapturingFakeModel(ToolCapturingFakeModel):
    """Captures the normal agent request used by the native Skill middleware proof."""

    _received_message_text: list[str] = PrivateAttr(default_factory=list)

    def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        self._received_message_text = [str(message.content) for message in input]
        return super().invoke(input, config=config, **kwargs)


class InvocationTrackingFakeModel(ToolCapturingFakeModel):
    """Records direct provider calls made below LangChain model middleware."""

    _invoke_configs: list[Any] = PrivateAttr(default_factory=list)
    _async_invoke_configs: list[Any] = PrivateAttr(default_factory=list)
    _ambient_graph_configs: list[dict[str, Any]] = PrivateAttr(default_factory=list)

    def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        self._invoke_configs.append(config)
        self._ambient_graph_configs.append(dict(get_config()))
        return super().invoke(input, config=config, **kwargs)

    async def ainvoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        self._async_invoke_configs.append(config)
        self._ambient_graph_configs.append(dict(get_config()))
        return await super().ainvoke(input, config=config, **kwargs)


class BoundProfileFakeModel(ToolCapturingFakeModel):
    """A construction proxy that exposes only Kokoro's fixed profile identity.

    ``selected_provider_label`` deliberately resembles a Codex provider model,
    but it is not the object's DeepAgents identity.  A real ``BoundChatModel``
    resolves that label only inside the claimed Run's model transport scope.
    """

    model_name: str = "bound_chat"
    selected_provider_label: str = "openai:gpt-5.3-codex"
    _received_message_text: list[str] = PrivateAttr(default_factory=list)

    def _get_ls_params(
        self, stop: list[str] | None = None, **kwargs: Any
    ) -> dict[str, str]:
        del stop, kwargs
        return {"ls_provider": "kokoro", "ls_model_type": "chat"}

    def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        self._received_message_text = [str(message.content) for message in input]
        return super().invoke(input, config=config, **kwargs)

_ACTIVE_RUN_MODEL: ContextVar[ToolCapturingFakeModel | None] = ContextVar(
    "active_run_model", default=None
)


class RunBoundFakeModel(ToolCapturingFakeModel):
    """Minimal proxy proving a per-invoke model binding reaches native summaries."""

    def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        provider_model = _ACTIVE_RUN_MODEL.get()
        if provider_model is None:
            raise RuntimeError("no model bound for this run")
        return provider_model.invoke(input, config=config, **kwargs)

    async def ainvoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        provider_model = _ACTIVE_RUN_MODEL.get()
        if provider_model is None:
            raise RuntimeError("no model bound for this run")
        return await provider_model.ainvoke(input, config=config, **kwargs)


class InterruptProbeState(TypedDict, total=False):
    """The smallest native state needed to lock the LangGraph HITL boundary."""

    prompt: str
    answer: str


def _assert_native_summary_call_config(config: Any) -> None:
    """Keep the native-summary identification stable across LangGraph locks.

    DeepAgents owns the full ``RunnableConfig`` for its direct summary call.
    Newer LangGraph releases add callback, checkpoint and internal-call values to
    that config.  Kokoro only relies on the documented semantic marker, not the
    framework's private config shape.
    """
    assert isinstance(config, dict)
    metadata = config.get("metadata")
    assert isinstance(metadata, dict)
    assert metadata.get("lc_source") == "summarization"


@wrap_model_call
def _only_ga_skill_tools(request: Any, handler: Callable[[Any], Any]) -> Any:
    """The same static model-surface operation the target ToolPolicy will use."""
    allowed = {"catalog_lookup", "catalog_read"}
    return handler(
        request.override(
            tools=[tool_ for tool_ in request.tools if tool_.name in allowed]
        )
    )


def _register_test_profile() -> None:
    profile = HarnessProfile(
        general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
        excluded_tools=frozenset({"delete", "execute"}),
    )
    register_harness_profile("toolcapturingfakemodel", profile)
    register_harness_profile("invocationtrackingfakemodel", profile)
    register_harness_profile("runboundfakemodel", profile)


_bound_profile_registered = False
_BOUND_PROFILE_SUFFIX = "__kokoro_bound_profile_probe__"


def _register_bound_profile() -> None:
    """Register the target fixed identity once; profile registration is additive."""
    global _bound_profile_registered
    if _bound_profile_registered:
        return
    register_harness_profile(
        "kokoro:bound_chat",
        HarnessProfile(
            system_prompt_suffix=_BOUND_PROFILE_SUFFIX,
        general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
            excluded_tools=frozenset({"delete", "execute"}),
        ),
    )
    _bound_profile_registered = True


def test_langgraph_human_control_requires_interrupt_then_command_resume() -> None:
    """LangGraph accepts human content through a pending native interrupt only.

    This is intentionally a framework-level contract rather than a Kokoro
    controller.  ``Command(resume=...)`` is valid once the graph has durably
    paused.  It is not a general-purpose, concurrent ``steer`` or state-update
    channel for an in-flight ``invoke``; the target Root contract therefore
    exposes HITL responses and cancellation, not a fabricated mid-model-turn
    message injection API.
    """

    def ask_human(state: InterruptProbeState) -> dict[str, str]:
        prompt = state.get("prompt")
        assert isinstance(prompt, str)
        answer = interrupt({"kind": "question", "prompt": prompt})
        return {"answer": cast(str, answer)}

    graph = (
        StateGraph(InterruptProbeState)
        .add_node("ask_human", ask_human)
        .add_edge(START, "ask_human")
        .add_edge("ask_human", END)
        .compile(checkpointer=InMemorySaver())
    )
    config = cast(Any, {"configurable": {"thread_id": "native-hitl-boundary"}})

    paused = graph.invoke({"prompt": "What should I do next?"}, config=config)
    assert paused["__interrupt__"][0].value == {
        "kind": "question",
        "prompt": "What should I do next?",
    }
    assert graph.get_state(config).tasks[0].interrupts

    resumed = graph.invoke(Command(resume="continue"), config=config)
    assert resumed == {
        "prompt": "What should I do next?",
        "answer": "continue",
    }
    assert graph.get_state(config).tasks == ()


def test_deepagents_graph_uses_native_skills_without_ga_skill_tools(
    tmp_path: Any,
) -> None:
    """GA supplies a source; DeepAgents owns metadata injection and progressive reads."""
    _register_test_profile()
    skill_dir = tmp_path / ".skills" / "style"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: style\ndescription: Writing style guide\n---\nLead clearly.",
        encoding="utf-8",
    )
    model = SkillPromptCapturingFakeModel(responses=["done"])

    agent = create_deep_agent(
        model,
        tools=[],
        skills=["/.skills/"],
        memory=None,
        subagents=(),
        backend=FilesystemBackend(root_dir=tmp_path, virtual_mode=True),
    )

    graph_tools = agent.get_graph().nodes["tools"].data._tools_by_name
    assert "find_skills" not in graph_tools
    assert "load_skill" not in graph_tools
    assert "skills_metadata" not in agent.get_output_jsonschema()["properties"]

    agent.invoke({"messages": [{"role": "user", "content": "hello"}]})
    received = "\n".join(model._received_message_text)
    assert "Writing style guide" in received
    assert "/.skills/style/SKILL.md" in received
    assert "find_skills" not in model._bound_tool_names
    assert "load_skill" not in model._bound_tool_names
    # The native current-thread reader is required for DeepAgents summary
    # rehydration; generic command execution and recursive deletion remain absent.
    assert "read_file" in model._bound_tool_names
    assert "execute" not in model._bound_tool_names
    assert "delete" not in model._bound_tool_names


def test_fixed_bound_profile_identity_keeps_provider_codex_profile_out_of_graph() -> (
    None
):
    """The static graph resolves only ``kokoro:bound_chat``, never a run label.

    This is the framework proof for the production ``BoundChatModel`` contract:
    a selected ``openai:gpt-5.3-codex`` transport is intentionally carried by
    the proxy as opaque Run-scoped data, while DeepAgents sees its fixed
    ``kokoro:bound_chat`` provider/model identity at construction time.  The
    test profile's suffix proves that exact profile selection happened; absence
    of generic delegation/execute/delete proves the static model-visible surface comes
    from that profile rather than a provider/Codex profile.
    """
    _register_bound_profile()
    model = BoundProfileFakeModel(responses=["done"])

    agent = create_deep_agent(
        model,
        skills=None,
        memory=None,
        subagents=(),
        backend=WorkbenchBackendProbe(),
    )

    agent.invoke({"messages": [{"role": "user", "content": "hello"}]})

    assert model.selected_provider_label == "openai:gpt-5.3-codex"
    assert any(
        _BOUND_PROFILE_SUFFIX in message for message in model._received_message_text
    )
    assert "task" not in model._bound_tool_names
    assert "execute" not in model._bound_tool_names
    assert "delete" not in model._bound_tool_names
    # DeepAgents 0.6.6 remains the physical prototype baseline.  The clean
    # build locks 0.7.8, where write_todos became opt-in; do not mistake this
    # version delta for provider/Codex profile selection.
    if Version(version("deepagents")) >= Version("0.7.0"):
        assert "write_todos" not in model._bound_tool_names


def test_native_compaction_keeps_private_raw_history_and_restores_compact_context() -> (
    None
):
    """DeepAgents compacts the model request, not the durable message checkpoint.

    The locked DeepAgents middleware appends evicted text to the state-backed
    workbench and stores its cursor/summary in private state.  A rebuilt graph
    must use that private summary plus the retained tail, never re-send the
    old raw turn to the model.
    """
    _register_test_profile()
    checkpointer = InMemorySaver()
    config = {"configurable": {"thread_id": "native-compaction"}}
    first_model_requests: list[list[str]] = []

    @wrap_model_call
    def capture_first_compacted_request(
        request: Any, handler: Callable[[Any], Any]
    ) -> Any:
        first_model_requests.append(
            [str(message.content) for message in request.messages]
        )
        return handler(request)

    first = create_deep_agent(
        ToolCapturingFakeModel(
            responses=["native-summary-one", "first-answer"],
            profile={"max_input_tokens": 100},
        ),
        skills=None,
        memory=None,
        subagents=(),
        backend=StateBackend(),
        checkpointer=checkpointer,
        middleware=[capture_first_compacted_request],
    )
    seed_messages: list[dict[str, str]] = []
    for index in range(5):
        seed_messages.extend(
            (
                {"role": "user", "content": f"old-user-{index} " + "x " * 100},
                {
                    "role": "assistant",
                    "content": f"old-assistant-{index} " + "y " * 100,
                },
            )
        )

    first_result = first.invoke(
        cast(Any, {"messages": seed_messages}), config=cast(Any, config)
    )
    assert first_result["messages"][-1].content == "first-answer"

    first_state = first.get_state(cast(Any, config)).values
    event = first_state["_summarization_event"]
    assert event is not None
    assert "native-summary-one" in str(event["summary_message"].content)
    file_path = event["file_path"]
    assert isinstance(file_path, str)
    assert file_path.startswith("/conversation_history/")
    assert file_path.endswith(".md")
    # DeepAgents 0.7.8 derives this from a framework-private invocation id,
    # instead of exposing Kokoro's thread id as its file name.  Keep the
    # version-bound property explicit while still preserving this prototype's
    # 0.6.6 evidence until the target lock replaces it.
    if Version(version("deepagents")) >= Version("0.7.0"):
        assert file_path != "/conversation_history/native-compaction.md"
    assert "old-user-0" in "\n".join(
        str(message.content) for message in first_state["messages"]
    )
    assert "old-user-0" in first_state["files"][event["file_path"]]["content"]
    assert len(first_model_requests) == 1
    assert "native-summary-one" in "\n".join(first_model_requests[0])
    assert "old-user-0" not in "\n".join(first_model_requests[0])

    restarted_model_requests: list[list[str]] = []

    @wrap_model_call
    def capture_restarted_compacted_request(
        request: Any, handler: Callable[[Any], Any]
    ) -> Any:
        restarted_model_requests.append(
            [str(message.content) for message in request.messages]
        )
        return handler(request)

    restarted = create_deep_agent(
        ToolCapturingFakeModel(
            responses=["native-summary-two", "resumed-answer"],
            profile={"max_input_tokens": 100},
        ),
        skills=None,
        memory=None,
        subagents=(),
        backend=StateBackend(),
        checkpointer=checkpointer,
        middleware=[capture_restarted_compacted_request],
    )
    restarted_result = restarted.invoke(
        {"messages": [{"role": "user", "content": "continue from the same thread"}]},
        config=cast(Any, config),
    )

    assert restarted_result["messages"][-1].content == "resumed-answer"
    assert len(restarted_model_requests) == 1
    assert "native-summary-two" in "\n".join(restarted_model_requests[0])
    assert "old-user-0" not in "\n".join(restarted_model_requests[0])
    assert (
        "old-user-0"
        in restarted.get_state(cast(Any, config)).values["files"][event["file_path"]][
            "content"
        ]
    )


def test_native_summary_bypasses_agent_model_middleware_but_keeps_graph_context() -> (
    None
):
    """A raw DeepAgents summary call is a second provider call below ``wrap_model_call``.

    Its framework-owned config carries the semantic marker
    ``metadata.lc_source=summarization``; LangGraph can enrich the remaining
    config shape.  The ambient graph config still retains the current
    thread/run, which a GA model transport wrapper can bind to its RunLedger
    slot.
    """
    _register_test_profile()
    captured_agent_requests: list[list[str]] = []

    @wrap_model_call
    def capture_agent_model_request(request: Any, handler: Callable[[Any], Any]) -> Any:
        captured_agent_requests.append(
            [str(message.content) for message in request.messages]
        )
        return handler(request)

    model = InvocationTrackingFakeModel(
        responses=["summary-below-middleware", "answer-through-agent-middleware"],
        profile={"max_input_tokens": 100},
    )
    agent = create_deep_agent(
        model,
        skills=None,
        memory=None,
        subagents=(),
        backend=StateBackend(),
        checkpointer=InMemorySaver(),
        middleware=[capture_agent_model_request],
    )
    config = {
        "configurable": {
            "thread_id": "native-summary-model-call",
            "run_id": "run-for-native-summary",
        }
    }
    seed_messages: list[dict[str, str]] = []
    for index in range(5):
        seed_messages.extend(
            (
                {"role": "user", "content": f"meter-old-user-{index} " + "x " * 100},
                {
                    "role": "assistant",
                    "content": f"meter-old-assistant-{index} " + "y " * 100,
                },
            )
        )

    result = agent.invoke(
        cast(Any, {"messages": seed_messages}), config=cast(Any, config)
    )

    assert result["messages"][-1].content == "answer-through-agent-middleware"
    assert len(captured_agent_requests) == 1
    assert len(model._invoke_configs) == 2
    _assert_native_summary_call_config(model._invoke_configs[0])
    assert all(
        graph_config["configurable"]["run_id"] == "run-for-native-summary"
        for graph_config in model._ambient_graph_configs
    )


def test_run_bound_model_proxy_routes_native_summary_and_agent_call_to_one_label() -> (
    None
):
    """A per-invoke proxy works for both DeepAgents provider call paths.

    This is the framework-level feasibility proof for GA's ``BoundChatModel``:
    its static construction object may dispatch to the selected run model without
    recompiling the graph, and native summary does not fall back to a default.
    """
    _register_test_profile()
    default_model = InvocationTrackingFakeModel(
        responses=["default-summary", "default-answer"],
        profile={"max_input_tokens": 100},
    )
    selected_model = InvocationTrackingFakeModel(
        responses=["selected-summary", "selected-answer"],
        profile={"max_input_tokens": 100},
    )
    run_bound_model = RunBoundFakeModel(
        responses=["unreachable"], profile={"max_input_tokens": 100}
    )
    agent = create_deep_agent(
        run_bound_model,
        skills=None,
        memory=None,
        subagents=(),
        backend=StateBackend(),
        checkpointer=InMemorySaver(),
    )
    config = {"configurable": {"thread_id": "run-bound-model-proxy"}}
    seed_messages: list[dict[str, str]] = []
    for index in range(5):
        seed_messages.extend(
            (
                {"role": "user", "content": f"bound-old-user-{index} " + "x " * 100},
                {
                    "role": "assistant",
                    "content": f"bound-old-assistant-{index} " + "y " * 100,
                },
            )
        )

    token = _ACTIVE_RUN_MODEL.set(selected_model)
    try:
        result = agent.invoke(
            cast(Any, {"messages": seed_messages}), config=cast(Any, config)
        )
    finally:
        _ACTIVE_RUN_MODEL.reset(token)
    event = agent.get_state(cast(Any, config)).values["_summarization_event"]

    assert result["messages"][-1].content == "selected-answer"
    assert event is not None
    assert "selected-summary" in str(event["summary_message"].content)
    assert default_model._invoke_configs == []
    assert len(selected_model._invoke_configs) == 2
    _assert_native_summary_call_config(selected_model._invoke_configs[0])
    with pytest.raises(RuntimeError, match="no model bound for this run"):
        run_bound_model.invoke("outside a claimed run")


def test_run_bound_model_proxy_routes_async_native_summary_and_agent_call() -> None:
    """``ContextVar`` binding survives the native async summary/agent path."""
    _register_test_profile()
    default_model = InvocationTrackingFakeModel(
        responses=["async-default-summary", "async-default-answer"],
        profile={"max_input_tokens": 100},
    )
    selected_model = InvocationTrackingFakeModel(
        responses=["async-selected-summary", "async-selected-answer"],
        profile={"max_input_tokens": 100},
    )
    agent = create_deep_agent(
        RunBoundFakeModel(responses=["unreachable"], profile={"max_input_tokens": 100}),
        skills=None,
        memory=None,
        subagents=(),
        backend=StateBackend(),
        checkpointer=InMemorySaver(),
    )
    config = {"configurable": {"thread_id": "run-bound-model-proxy-async"}}
    seed_messages: list[dict[str, str]] = []
    for index in range(5):
        seed_messages.extend(
            (
                {"role": "user", "content": f"async-old-user-{index} " + "x " * 100},
                {
                    "role": "assistant",
                    "content": f"async-old-assistant-{index} " + "y " * 100,
                },
            )
        )

    token = _ACTIVE_RUN_MODEL.set(selected_model)
    try:
        result = asyncio.run(
            agent.ainvoke(
                cast(Any, {"messages": seed_messages}), config=cast(Any, config)
            )
        )
    finally:
        _ACTIVE_RUN_MODEL.reset(token)
    event = agent.get_state(cast(Any, config)).values["_summarization_event"]

    assert result["messages"][-1].content == "async-selected-answer"
    assert event is not None
    assert "async-selected-summary" in str(event["summary_message"].content)
    assert default_model._async_invoke_configs == []
    assert len(selected_model._async_invoke_configs) == 2
    _assert_native_summary_call_config(selected_model._async_invoke_configs[0])


def test_agent_model_override_does_not_select_the_native_summary_model() -> None:
    """``request.override(model=...)`` applies after native summary generation.

    The compiler must therefore pass a Run-bound model transport to
    ``create_deep_agent`` itself; a normal graph ``ModelRouterMiddleware`` alone
    cannot meter or select the summary call.
    """
    _register_test_profile()
    source_model = InvocationTrackingFakeModel(
        responses=["source-model-summary"],
        profile={"max_input_tokens": 100},
    )
    routed_model = ToolCapturingFakeModel(responses=["routed-model-answer"])

    @wrap_model_call
    def route_only_agent_node(request: Any, handler: Callable[[Any], Any]) -> Any:
        return handler(request.override(model=routed_model))

    agent = create_deep_agent(
        source_model,
        skills=None,
        memory=None,
        subagents=(),
        backend=StateBackend(),
        checkpointer=InMemorySaver(),
        middleware=[route_only_agent_node],
    )
    config = {"configurable": {"thread_id": "native-summary-model-selection"}}
    seed_messages: list[dict[str, str]] = []
    for index in range(5):
        seed_messages.extend(
            (
                {"role": "user", "content": f"router-old-user-{index} " + "x " * 100},
                {
                    "role": "assistant",
                    "content": f"router-old-assistant-{index} " + "y " * 100,
                },
            )
        )

    result = agent.invoke(
        cast(Any, {"messages": seed_messages}), config=cast(Any, config)
    )
    event = agent.get_state(cast(Any, config)).values["_summarization_event"]

    assert result["messages"][-1].content == "routed-model-answer"
    assert event is not None
    assert "source-model-summary" in str(event["summary_message"].content)
    assert len(source_model._invoke_configs) == 1
    _assert_native_summary_call_config(source_model._invoke_configs[0])


def test_native_compaction_offload_failure_keeps_raw_checkpoint_but_no_rehydration_path() -> (
    None
):
    """The framework summaries even when its backend cannot persist history.

    This proves why GA must turn ``file_path=None`` into its own private
    degradation fact rather than claiming that an old observation is readable.
    """
    _register_test_profile()
    agent = create_deep_agent(
        ToolCapturingFakeModel(
            responses=["unavailable-summary", "answer-after-offload-failure"],
            profile={"max_input_tokens": 100},
        ),
        skills=None,
        memory=None,
        subagents=(),
        backend=OffloadUnavailableBackend(),
        checkpointer=InMemorySaver(),
    )
    config = {"configurable": {"thread_id": "native-compaction-failure"}}
    seed_messages: list[dict[str, str]] = []
    for index in range(5):
        seed_messages.extend(
            (
                {"role": "user", "content": f"failed-old-user-{index} " + "x " * 100},
                {
                    "role": "assistant",
                    "content": f"failed-old-assistant-{index} " + "y " * 100,
                },
            )
        )

    with pytest.warns(UserWarning, match="Older messages will not be recoverable"):
        result = agent.invoke(
            cast(Any, {"messages": seed_messages}), config=cast(Any, config)
        )
    state = agent.get_state(cast(Any, config)).values

    assert result["messages"][-1].content == "answer-after-offload-failure"
    event = state["_summarization_event"]
    assert event is not None
    assert event["file_path"] is None
    assert "failed-old-user-0" in "\n".join(
        str(message.content) for message in state["messages"]
    )
    assert "/conversation_history/native-compaction-failure.md" not in state.get(
        "files", {}
    )


def test_official_model_middleware_can_freeze_the_visible_tool_surface() -> None:
    """A static middleware can constrain native tool visibility without graph rebuilding."""
    _register_test_profile()
    model = ToolCapturingFakeModel(responses=["done"])
    agent = create_deep_agent(
        model,
        tools=[catalog_lookup, catalog_read],
        skills=None,
        memory=None,
        subagents=(),
        backend=WorkbenchBackendProbe(),
        middleware=[_only_ga_skill_tools],
    )

    agent.invoke({"messages": [{"role": "user", "content": "hello"}]})
    assert model._bound_tool_names == ["catalog_lookup", "catalog_read"]


@wrap_tool_call
def _reject_unlisted_tool(request: Any, handler: Callable[[Any], Any]) -> Any:
    """The target ToolPolicy's dispatch half: fail closed before the native handler."""
    if request.tool_call["name"] not in {"catalog_lookup", "catalog_read"}:
        return ToolMessage(
            content="blocked by static tool policy",
            name=request.tool_call["name"],
            tool_call_id=request.tool_call["id"],
            status="error",
        )
    return handler(request)


class ToolCallingFakeModel(GenericFakeChatModel):
    """Returns an unbound tool call to prove dispatch is guarded independently."""

    def bind_tools(self, tools: list[Any], **kwargs: Any) -> ToolCallingFakeModel:
        del tools, kwargs
        return self


_UNBOUND_DESTRUCTIVE_CALLS: list[tuple[str, dict[str, str]]] = [
    ("execute", {"command": "echo should-not-run"}),
]
if Version(version("deepagents")) >= Version("0.7.0"):
    _UNBOUND_DESTRUCTIVE_CALLS.append(("delete", {"path": "/scratch.txt"}))


@pytest.mark.parametrize(("tool_name", "tool_args"), _UNBOUND_DESTRUCTIVE_CALLS)
def test_official_tool_middleware_rejects_an_unbound_destructive_tool_call(
    tool_name: str, tool_args: dict[str, str]
) -> None:
    """Tool visibility alone is insufficient: dispatch must also be statically guarded.

    DeepAgents 0.7.0 added a recursive filesystem ``delete`` tool whenever the
    backend supports it.  Kokoro never places it in a model-visible workbench
    base and the frozen dispatch allow-set rejects a malformed/raw call too.
    """
    register_harness_profile(
        "toolcallingfakemodel",
        HarnessProfile(
        general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
            excluded_tools=frozenset({"delete", "execute"}),
        ),
    )
    model = ToolCallingFakeModel(
        messages=iter(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": tool_name,
                            "args": tool_args,
                            "id": f"call_unbound_{tool_name}",
                        }
                    ],
                ),
                "done",
            ]
        )
    )
    agent = create_deep_agent(
        model,
        tools=[catalog_lookup, catalog_read],
        skills=None,
        memory=None,
        subagents=(),
        backend=WorkbenchBackendProbe(),
        middleware=[_reject_unlisted_tool],
    )

    result = agent.invoke({"messages": [{"role": "user", "content": "hello"}]})
    blocked = [
        message for message in result["messages"] if isinstance(message, ToolMessage)
    ]
    assert len(blocked) == 1
    assert blocked[0].name == tool_name
    assert blocked[0].status == "error"
    assert blocked[0].content == "blocked by static tool policy"
