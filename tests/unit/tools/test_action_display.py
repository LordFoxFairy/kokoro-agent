"""Public action intent belongs to one model call, never to execution arguments."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from langchain import agents
from langchain.agents.middleware.types import (
    ModelRequest,
    ModelResponse,
    ToolCallRequest,
)
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.utils import function_calling
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolCallTransformer, ToolNode
from langgraph.prebuilt.tool_node import ToolRuntime
from pydantic import BaseModel, ConfigDict, ValidationError

from kokoro_agent.tools.action_display import (
    ACTION_FIELD,
    ActionDisplayMiddleware,
    ActionObservation,
    model_action_tools,
)
from support.local_fake import LocalFakeChatModel


class LookupArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    path: str


def lookup_tool() -> StructuredTool:
    def lookup(path: str) -> str:
        return f"read:{path}"

    return StructuredTool(
        func=lookup,
        name="lookup",
        description="Read a virtual file",
        args_schema=LookupArgs,
    )


def request_for(tool: StructuredTool | None, args: dict[str, Any]) -> ToolCallRequest:
    runtime: ToolRuntime[Any, Any] = ToolRuntime(
        state=None,
        context=None,
        config={},
        stream_writer=lambda _: None,
        tool_call_id="call-1",
        store=None,
    )
    return ToolCallRequest(
        tool_call={"name": "lookup", "id": "call-1", "args": args, "type": "tool_call"},
        tool=tool,
        state=None,
        runtime=runtime,
    )


async def qualify(
    middleware: ActionDisplayMiddleware, request: ToolCallRequest
) -> ToolCallRequest:
    async def handler(_: ModelRequest[Any]) -> ModelResponse:
        return ModelResponse(
            result=[AIMessage(content="", tool_calls=[request.tool_call])]
        )

    assert request.tool is not None
    response = await middleware.awrap_model_call(
        ModelRequest(model=LocalFakeChatModel(), messages=[], tools=[request.tool]),
        handler,
    )
    return request.override(state={"messages": response.result})


def test_schema_copy_keeps_original_tool_and_required_business_fields() -> None:
    tool = lookup_tool()
    original = deepcopy(tool.get_input_schema().model_json_schema())
    visible = model_action_tools([tool], frozenset({"lookup"}))
    assert len(visible) == 1
    schema = visible[0]
    assert isinstance(schema, dict)
    assert schema["function"]["name"] == tool.name
    assert schema["function"]["parameters"]["required"] == ["path"]
    assert ACTION_FIELD in schema["function"]["parameters"]["properties"]
    convert: Any = getattr(function_calling, "convert_to_openai_tool")
    expected = deepcopy(convert(tool))
    del schema["function"]["parameters"]["properties"][ACTION_FIELD]
    assert schema == expected
    assert tool.get_input_schema().model_json_schema() == original


@pytest.mark.parametrize("kind", ["collision", "strict", "builtin", "unselected"])
def test_unavailable_decoration_preserves_original_definition(kind: str) -> None:
    definition: dict[str, Any] = {
        "type": "function",
        "function": {
            "name": "lookup",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
            },
        },
    }
    selected: frozenset[str] = frozenset({"lookup"})
    if kind == "collision":
        definition["function"]["parameters"]["properties"][ACTION_FIELD] = {
            "type": "integer"
        }
    elif kind == "strict":
        definition["function"]["strict"] = True
    elif kind == "builtin":
        definition = {"type": "web_search"}
    else:
        selected = frozenset()
    before = deepcopy(definition)
    assert model_action_tools([definition], selected) == [before]
    assert definition == before


async def test_model_override_is_one_call_without_mutating_the_request() -> None:
    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), lambda _: None, run_id="run-1"
    )
    tool = lookup_tool()
    original = ModelRequest(model=LocalFakeChatModel(), messages=[], tools=[tool])
    seen: list[ModelRequest[Any]] = []

    async def handler(request: ModelRequest[Any]) -> ModelResponse:
        seen.append(request)
        return ModelResponse(result=[AIMessage(content="done")])

    result = await middleware.awrap_model_call(original, handler)
    assert result.result[0].text == "done"
    assert len(seen) == 1 and seen[0] is not original
    assert original.tools == [tool]
    assert isinstance(seen[0].tools[0], dict)


async def test_strip_binds_exact_original_call_without_forwarding_or_mutating_title() -> (
    None
):
    observed: list[ActionObservation] = []
    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    )
    original = request_for(
        lookup_tool(), {"path": "/plan.md", ACTION_FIELD: "核对发布检查单"}
    )
    seen: list[ToolCallRequest] = []

    async def handler(request: ToolCallRequest) -> ToolMessage:
        seen.append(request)
        assert request.tool is not None
        invoke: Any = getattr(request.tool, "ainvoke")
        return await invoke(request.tool_call)

    original = await qualify(middleware, original)
    result = await middleware.awrap_tool_call(original, handler)
    assert isinstance(result, ToolMessage) and result.text == "read:/plan.md"
    assert seen[0].tool_call == {
        "name": "lookup",
        "id": "call-1",
        "args": {"path": "/plan.md"},
        "type": "tool_call",
    }
    assert original.tool_call["args"][ACTION_FIELD] == "核对发布检查单"
    assert len(observed) == 1
    assert observed[0].title == "核对发布检查单"
    assert observed[0].matches("call-1", "lookup", {"path": "/plan.md"})
    assert not observed[0].matches("call-2", "lookup", {"path": "/plan.md"})
    assert not observed[0].matches("call-1", "other", {"path": "/plan.md"})
    assert not observed[0].matches("call-1", "lookup", {"path": "/changed.md"})


@pytest.mark.parametrize(
    "title",
    [
        None,
        17,
        "",
        "a" * 257,
        "一" * 86,
        "read\nfile",
        "read\u202efile",
        "token=abc",
        "Bearer abc",
        "/Users/private/key",
        {"text": "read"},
    ],
)
async def test_bad_presentation_is_omitted_but_business_arguments_still_execute(
    title: Any,
) -> None:
    observed: list[ActionObservation] = []
    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    )
    request = request_for(lookup_tool(), {"path": "/plan.md", ACTION_FIELD: title})

    async def handler(clean: ToolCallRequest) -> ToolMessage:
        assert clean.tool is not None
        invoke: Any = getattr(clean.tool, "ainvoke")
        return await invoke(clean.tool_call)

    request = await qualify(middleware, request)
    result = await middleware.awrap_tool_call(request, handler)
    assert isinstance(result, ToolMessage) and result.text == "read:/plan.md"
    assert observed == []


async def test_unknown_tool_is_not_sanitized_or_granted_execution() -> None:
    observed: list[ActionObservation] = []
    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    )
    original = request_for(None, {ACTION_FIELD: "read"})

    async def handler(request: ToolCallRequest) -> ToolMessage:
        assert request is original
        return ToolMessage(
            content="unknown tool", tool_call_id="call-1", status="error"
        )

    result = await middleware.awrap_tool_call(original, handler)
    assert isinstance(result, ToolMessage) and result.status == "error"
    assert observed == []


async def test_real_sdk_preserves_title_in_checkpoint_but_not_tool_input() -> None:
    observed: list[ActionObservation] = []
    args = {"path": "/plan.md", ACTION_FIELD: "核对发布检查单"}
    create: Any = getattr(agents, "create_agent")
    graph = create(
        model=LocalFakeChatModel.with_script(
            [
                AIMessage(
                    content="先核对检查单。",
                    tool_calls=[
                        {
                            "name": "lookup",
                            "args": args,
                            "id": "call-1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="核对完成。"),
            ]
        ),
        tools=[lookup_tool()],
        middleware=[
            ActionDisplayMiddleware(
                frozenset({"lookup"}), observed.append, run_id="run-1"
            )
        ],
        checkpointer=InMemorySaver(),
    )
    config: RunnableConfig = {"configurable": {"thread_id": "action-title-sdk"}}
    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="Read the plan")]}, config
    )
    tool_result = next(
        message for message in result["messages"] if isinstance(message, ToolMessage)
    )
    assert tool_result.status == "success" and tool_result.text == "read:/plan.md"
    snapshot = await graph.aget_state(config)
    original = next(
        message
        for message in snapshot.values["messages"]
        if isinstance(message, AIMessage) and message.tool_calls
    )
    assert original.tool_calls[0]["args"] == args
    assert observed[0].matches("call-1", "lookup", {"path": "/plan.md"})


async def test_real_native_stream_has_clean_input_and_exact_observation_before_start() -> (
    None
):
    observed: list[ActionObservation] = []
    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    )
    graph = StateGraph(MessagesState)
    add_node: Any = getattr(graph, "add_node")
    add_node(
        "tools", ToolNode([lookup_tool()], awrap_tool_call=middleware.awrap_tool_call)
    )
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    compile_graph: Any = getattr(graph, "compile")
    native = compile_graph(transformers=[ToolCallTransformer])
    stream: Any = getattr(native, "astream_events")
    calls = [
        {
            "name": "lookup",
            "id": f"call-{number}",
            "type": "tool_call",
            "args": {"path": f"/file-{number}.md", ACTION_FIELD: f"核对文件 {number}"},
        }
        for number in range(2)
    ]

    async def model_handler(_: ModelRequest[Any]) -> ModelResponse:
        return ModelResponse(result=[AIMessage(content="", tool_calls=calls)])

    response = await middleware.awrap_model_call(
        ModelRequest(model=LocalFakeChatModel(), messages=[], tools=[lookup_tool()]),
        model_handler,
    )
    run = await stream({"messages": response.result}, version="v3")
    started: list[str] = []
    async with run:
        async for call in run.tool_calls:
            started.append(call.tool_call_id)
            assert ACTION_FIELD not in call.input
            matching = [
                item
                for item in observed
                if item.matches(call.tool_call_id, call.tool_name, call.input)
            ]
            assert len(matching) == 1
            async for _ in call.output_deltas:
                pass
            assert (
                isinstance(call.output, ToolMessage) and call.output.status == "success"
            )
    assert set(started) == {"call-0", "call-1"}
    assert all(ACTION_FIELD in call["args"] for call in calls)


async def test_title_does_not_hide_business_schema_errors() -> None:
    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), lambda _: None, run_id="run-1"
    )
    original = request_for(
        lookup_tool(), {"wrong_path": "/plan.md", ACTION_FIELD: "核对检查单"}
    )

    async def handler(clean: ToolCallRequest) -> ToolMessage:
        assert clean.tool is not None
        invoke: Any = getattr(clean.tool, "ainvoke")
        return await invoke(clean.tool_call)

    original = await qualify(middleware, original)
    with pytest.raises(ValidationError):
        await middleware.awrap_tool_call(original, handler)


async def test_nested_mcp_arguments_are_not_treated_as_display_metadata() -> None:
    class McpArgs(BaseModel):
        model_config = ConfigDict(extra="forbid")
        arguments: dict[str, Any]

    def remote(arguments: dict[str, Any]) -> str:
        return "ok"

    tool = StructuredTool(
        name="lookup",
        description="MCP wrapper",
        args_schema=McpArgs,
        func=remote,
    )
    original = request_for(
        tool,
        {
            "arguments": {ACTION_FIELD: "remote business value"},
            ACTION_FIELD: "查询指定资料",
        },
    )
    observed: list[ActionObservation] = []

    async def handler(clean: ToolCallRequest) -> ToolMessage:
        assert clean.tool_call["args"] == {
            "arguments": {ACTION_FIELD: "remote business value"}
        }
        return ToolMessage(
            content="remote failed", tool_call_id="call-1", status="error"
        )

    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    )
    original = await qualify(middleware, original)
    result = await middleware.awrap_tool_call(original, handler)
    assert isinstance(result, ToolMessage) and result.status == "error"
    assert observed[0].matches(
        "call-1", "lookup", {"arguments": {ACTION_FIELD: "remote business value"}}
    )


async def test_exact_utf8_boundary_and_changed_approved_arguments() -> None:
    observed: list[ActionObservation] = []
    title = "核" * 84 + "abcd"
    original = request_for(lookup_tool(), {"path": "/plan.md", ACTION_FIELD: title})

    async def handler(clean: ToolCallRequest) -> ToolMessage:
        assert observed[0].title == title
        # A later approval edit must not reuse the model's original intent.
        assert not observed[0].matches(
            "call-1", "lookup", {"path": "/approved-other.md"}
        )
        return ToolMessage(content="ok", tool_call_id=clean.tool_call["id"] or "")

    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    )
    original = await qualify(middleware, original)
    await middleware.awrap_tool_call(original, handler)


async def test_reserved_business_field_is_never_stripped_or_reinterpreted() -> None:
    schema = {
        "type": "object",
        "properties": {ACTION_FIELD: {"type": "integer"}},
        "required": [ACTION_FIELD],
    }

    def business(**kwargs: Any) -> str:
        return "ok"

    tool = StructuredTool(
        name="lookup",
        description="Original business schema",
        args_schema=schema,
        func=business,
    )
    original = request_for(tool, {ACTION_FIELD: 7})
    observed: list[ActionObservation] = []

    async def handler(request: ToolCallRequest) -> ToolMessage:
        assert request is original
        return ToolMessage(content="ok", tool_call_id="call-1")

    await ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    ).awrap_tool_call(original, handler)
    assert observed == []


async def test_strict_model_schema_does_not_qualify_mounted_tool_for_stripping() -> (
    None
):
    observed: list[ActionObservation] = []
    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    )
    convert: Any = getattr(function_calling, "convert_to_openai_tool")
    definition = convert(lookup_tool())
    definition["function"]["strict"] = True
    call = request_for(lookup_tool(), {"path": "/plan.md", ACTION_FIELD: "核对检查单"})

    async def model_handler(request: ModelRequest[Any]) -> ModelResponse:
        return ModelResponse(
            result=[AIMessage(content="", tool_calls=[call.tool_call])]
        )

    response = await middleware.awrap_model_call(
        ModelRequest(model=LocalFakeChatModel(), messages=[], tools=[definition]),
        model_handler,
    )
    call = call.override(state={"messages": response.result})

    async def tool_handler(request: ToolCallRequest) -> ToolMessage:
        assert ACTION_FIELD in request.tool_call["args"]
        return ToolMessage(
            content="invalid arguments", tool_call_id="call-1", status="error"
        )

    await middleware.awrap_tool_call(call, tool_handler)
    assert observed == []


@pytest.mark.parametrize("edited", [False, True])
async def test_actual_hitl_resume_preserves_only_original_action(edited: bool) -> None:
    from langchain.agents.middleware import HumanInTheLoopMiddleware
    from langgraph.types import Command

    observed: list[ActionObservation] = []
    saver = InMemorySaver()
    config: RunnableConfig = {"configurable": {"thread_id": f"action-hitl-{edited}"}}
    args = {"path": "/plan.md", ACTION_FIELD: "核对原检查单"}
    create: Any = getattr(agents, "create_agent")

    def build(model: LocalFakeChatModel) -> Any:
        return create(
            model=model,
            tools=[lookup_tool()],
            checkpointer=saver,
            middleware=[
                ActionDisplayMiddleware(
                    frozenset({"lookup"}), observed.append, run_id="run-1"
                ),
                HumanInTheLoopMiddleware(interrupt_on={"lookup": True}),
            ],
        )

    graph = build(
        LocalFakeChatModel.with_script(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "lookup",
                            "id": "call-1",
                            "args": args,
                            "type": "tool_call",
                        }
                    ],
                ),
            ]
        )
    )
    interrupted = await graph.ainvoke(
        {"messages": [HumanMessage(content="Read the plan")]}, config
    )
    assert interrupted["__interrupt__"] and observed == []
    # A new middleware/graph instance must rely on the durable checkpoint,
    # not a process-local map left by the model invocation.
    resumed = build(LocalFakeChatModel.with_script([AIMessage(content="done")]))
    decision = (
        {
            "type": "edit",
            "edited_action": {"name": "lookup", "args": {**args, "path": "/other.md"}},
        }
        if edited
        else {"type": "approve"}
    )
    result = await resumed.ainvoke(Command(resume={"decisions": [decision]}), config)
    tool = next(
        message for message in result["messages"] if isinstance(message, ToolMessage)
    )
    assert tool.status == "success"
    assert tool.text == ("read:/other.md" if edited else "read:/plan.md")
    if edited:
        assert observed == []
    else:
        assert len(observed) == 1 and observed[0].matches(
            "call-1", "lookup", {"path": "/plan.md"}
        )


@pytest.mark.parametrize(
    "case", ["other-run", "missing-source", "duplicate-id", "newer-message"]
)
async def test_no_qualification_is_borrowed_from_other_call_state(case: str) -> None:
    observed: list[ActionObservation] = []
    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), observed.append, run_id="run-1"
    )
    request = request_for(
        lookup_tool(), {"path": "/plan.md", ACTION_FIELD: "核对检查单"}
    )
    request = await qualify(middleware, request)
    if case == "other-run":
        middleware = ActionDisplayMiddleware(
            frozenset({"lookup"}), observed.append, run_id="run-2"
        )
    elif case == "missing-source":
        request = request.override(
            state={"messages": [AIMessage(content="", tool_calls=[request.tool_call])]}
        )
    elif case == "duplicate-id":
        request.state["messages"][0].tool_calls.append(deepcopy(request.tool_call))
    else:
        request.state["messages"].append(AIMessage(content="new model response"))

    async def handler(untouched: ToolCallRequest) -> ToolMessage:
        assert ACTION_FIELD in untouched.tool_call["args"]
        return ToolMessage(content="invalid", tool_call_id="call-1", status="error")

    await middleware.awrap_tool_call(request, handler)
    assert observed == []


async def test_checkpoint_metadata_is_not_sent_on_actual_openai_message_wire() -> None:
    from langchain_openai.chat_models import base
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

    middleware = ActionDisplayMiddleware(
        frozenset({"lookup"}), lambda _: None, run_id="run-1"
    )
    request = await qualify(
        middleware,
        request_for(lookup_tool(), {"path": "/plan.md", ACTION_FIELD: "核对检查单"}),
    )
    message = request.state["messages"][0]
    serde = JsonPlusSerializer()
    restored = serde.loads_typed(serde.dumps_typed(message))
    assert restored == message
    # This is the serializer used by the installed OpenAI transport, not
    # model_dump(): checkpoints and model wire deliberately differ.
    convert: Any = getattr(base, "_convert_message_to_dict")
    wire = convert(restored)
    assert "response_metadata" not in wire
    assert "kokoro_action_source_v1" not in str(wire)
    assert ACTION_FIELD in wire["tool_calls"][0]["function"]["arguments"]
