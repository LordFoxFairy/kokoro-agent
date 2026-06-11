from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from langchain.agents import create_agent  # pyright: ignore[reportUnknownVariableType]  # langchain create_agent symbol is partially typed
from kokoro_agent.infrastructure.message_extractors import (
    as_mapping,
    as_ai_message,
    is_object_list,
    is_tool_call_only_chunk,
    message_content,
    reasoning_of,
    result_text,
    text_of,
)
from kokoro_agent.infrastructure.subagent_registry import (
    RuntimeSubagentRegistry,
    subagent_source_for,
)

# write_todos / task are mapped to dedicated event families; every other tool is
# a generic tool.invoked/returned pair.
TODO_TOOL = "write_todos"
SUBAGENT_TOOL = "task"
RUNTIME_SUBAGENT_TOOL = "agent"

# Intent kinds that run_agent expands rather than emitting verbatim.
TEXT_INTENT = "text"
# An incremental token slice from on_chat_model_stream; run_agent turns each into
# a real text.delta and remembers it streamed so the matching TEXT_INTENT (from
# on_chat_model_end) closes the segment with one text.completed, no extra delta.
TEXT_STREAM_INTENT = "text.stream"


class _AgentRunner(Protocol):
    async def ainvoke(self, inp: dict[str, object]) -> object: ...


class RuntimeSubagentToolInput(BaseModel):
    name: str = Field(min_length=1, description="Runtime custom subagent name")
    description: str = Field(min_length=1, description="Short role or responsibility summary")
    system_prompt: str = Field(min_length=1, description="System prompt for the runtime custom subagent")
    task: str = Field(min_length=1, description="The concrete task the runtime custom subagent should perform")


def translate_stream_event(
    ev: Mapping[str, object],
) -> list[tuple[str, dict[str, object]]]:
    """Pure map of one ``astream_events(version="v2")`` event to (kind, payload)
    intents. run_agent assigns run_id/seq/message_ref and expands the ``text``
    intent into text.delta + text.completed.

    Emits only on tool starts/ends and final model messages; internal graph
    nodes (LangGraph/model/tools/*Middleware) and intermediate tool-call turns
    (empty content) produce nothing.
    """
    event = ev.get("event")
    name_obj = ev.get("name")
    name = name_obj if isinstance(name_obj, str) else ""
    data = as_mapping(ev.get("data"))
    run_id_obj = ev.get("run_id")
    tool_id = run_id_obj if isinstance(run_id_obj, str) else ""
    out: list[tuple[str, dict[str, object]]] = []

    if event == "on_tool_start":
        args = as_mapping(data.get("input"))
        if name == TODO_TOOL:
            todos = args.get("todos")
            out.append(("todo.updated", {"todos": todos if isinstance(todos, list) else []}))
        elif name == SUBAGENT_TOOL:
            subagent_type = str(args.get("subagent_type") or "subagent")
            out.append(
                (
                    "subagent.started",
                    {
                        "subagent_id": tool_id,
                        "name": subagent_type,
                        "description": str(args.get("description") or ""),
                        "subagent_type": subagent_type,
                        "source": subagent_source_for(subagent_type),
                    },
                )
            )
        elif name == RUNTIME_SUBAGENT_TOOL:
            runtime_name = str(args.get("name") or "runtime-subagent")
            out.append(
                (
                    "subagent.started",
                    {
                        "subagent_id": tool_id,
                        "name": runtime_name,
                        "description": str(args.get("description") or ""),
                        "subagent_type": runtime_name,
                        "source": "runtime-custom",
                    },
                )
            )
        else:
            out.append(("tool.invoked", {"tool_id": tool_id, "name": name, "args": dict(args)}))
    elif event == "on_tool_end":
        if name == TODO_TOOL:
            return out  # the list was already emitted on tool start
        if name == SUBAGENT_TOOL:
            args = as_mapping(data.get("input"))
            subagent_type = str(args.get("subagent_type") or "subagent")
            out.append(
                (
                    "subagent.finished",
                    {
                        "subagent_id": tool_id,
                        "name": subagent_type,
                        "subagent_type": subagent_type,
                        "source": subagent_source_for(subagent_type),
                    },
                )
            )
        elif name == RUNTIME_SUBAGENT_TOOL:
            args = as_mapping(data.get("input"))
            runtime_name = str(args.get("name") or "runtime-subagent")
            out.append(
                (
                    "subagent.finished",
                    {
                        "subagent_id": tool_id,
                        "name": runtime_name,
                        "subagent_type": runtime_name,
                        "source": "runtime-custom",
                    },
                )
            )
        else:
            out.append(
                (
                    "tool.returned",
                    {"tool_id": tool_id, "name": name, "result": result_text(data.get("output"))},
                )
            )
    elif event == "on_chat_model_stream":
        chunk = data.get("chunk")
        if isinstance(chunk, AIMessageChunk) and not is_tool_call_only_chunk(chunk):
            reasoning = reasoning_of(chunk)
            if reasoning:
                out.append(("thinking.delta", {"text": reasoning}))
            text = text_of(message_content(chunk))
            if text:
                out.append((TEXT_STREAM_INTENT, {"text": text}))
    elif event == "on_chat_model_end":
        message = as_ai_message(data.get("output"))
        if message is not None:
            reasoning = reasoning_of(message)
            if reasoning:
                out.append(("thinking.delta", {"text": reasoning}))
            text = text_of(message_content(message))
            # Intermediate turns carry tool_calls (and usually empty text); only
            # a final answer (no tool_calls) becomes a user-visible message.
            if text and not message.tool_calls:
                out.append((TEXT_INTENT, {"text": text}))
    return out


def _make_runner(model: BaseChatModel, system_prompt: str, name: str) -> _AgentRunner:
    # langchain create_agent returns a CompiledStateGraph whose generic params are
    # irreducibly Unknown under strict; pin the ainvoke slice we use at this one
    # boundary.
    runner: _AgentRunner = create_agent(  # pyright: ignore[reportUnknownVariableType, reportAssignmentType]
        model, system_prompt=system_prompt, tools=[], name=name
    )
    return runner


def build_runtime_custom_subagent_tool(
    model: BaseChatModel,
    runtime_registry: RuntimeSubagentRegistry,
) -> StructuredTool:
    async def agent_runtime(
        name: str,
        description: str,
        system_prompt: str,
        task: str,
    ) -> str:
        spec = runtime_registry.get(name)
        if spec is None:
            spec = runtime_registry.register(name, description, system_prompt)

        runner = _make_runner(model, spec.system_prompt, spec.name)
        result_obj = await runner.ainvoke(
            {"messages": [{"role": "user", "content": task}]}
        )
        messages_obj = as_mapping(result_obj).get("messages")
        if is_object_list(messages_obj):
            for message_obj in reversed(messages_obj):
                if isinstance(message_obj, AIMessage):
                    text_value = str(message_obj.text)
                    if text_value:
                        text = text_value.rstrip()
                    else:
                        text = text_of(message_content(message_obj))
                    if text:
                        return text
        return ""

    def agent_runtime_sync(
        name: str,
        description: str,
        system_prompt: str,
        task: str,
    ) -> str:
        msg = "runtime custom subagent tool requires async execution"
        raise RuntimeError(msg)

    return StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]  # langchain from_function classmethod is partially typed
        name=RUNTIME_SUBAGENT_TOOL,
        func=agent_runtime_sync,
        coroutine=agent_runtime,
        infer_schema=False,
        args_schema=RuntimeSubagentToolInput,
        description=(
            "Create and run a runtime custom subagent. Use this when you need an ad-hoc"
            " specialized helper that is not part of the built-in or config-defined"
            " subagent set."
        ),
    )
