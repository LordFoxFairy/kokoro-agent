from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from kokoro_agent.infrastructure.message_extractors import (
    as_ai_message,
    is_tool_call_only_chunk,
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
    data_obj = ev.get("data")
    data: Mapping[str, object] = (
        cast("Mapping[str, object]", data_obj) if isinstance(data_obj, Mapping) else {}
    )
    run_id_obj = ev.get("run_id")
    tool_id = run_id_obj if isinstance(run_id_obj, str) else ""
    out: list[tuple[str, dict[str, object]]] = []

    if event == "on_tool_start":
        input_obj = data.get("input")
        args: Mapping[str, object] = (
            cast("Mapping[str, object]", input_obj)
            if isinstance(input_obj, Mapping)
            else {}
        )
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
            input_obj = data.get("input")
            args: Mapping[str, object] = (
                cast("Mapping[str, object]", input_obj)
                if isinstance(input_obj, Mapping)
                else {}
            )
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
            input_obj = data.get("input")
            args: Mapping[str, object] = (
                cast("Mapping[str, object]", input_obj)
                if isinstance(input_obj, Mapping)
                else {}
            )
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
            text = text_of(cast("object", chunk.content))  # pyright: ignore  # langchain Any field
            if text:
                out.append((TEXT_STREAM_INTENT, {"text": text}))
    elif event == "on_chat_model_end":
        message = as_ai_message(data.get("output"))
        if message is not None:
            reasoning = reasoning_of(message)
            if reasoning:
                out.append(("thinking.delta", {"text": reasoning}))
            text = text_of(cast("object", message.content))  # pyright: ignore  # langchain Any field
            # Intermediate turns carry tool_calls (and usually empty text); only
            # a final answer (no tool_calls) becomes a user-visible message.
            if text and not message.tool_calls:
                out.append((TEXT_INTENT, {"text": text}))
    return out


def build_runtime_custom_subagent_tool(
    model: BaseChatModel,
    runtime_registry: RuntimeSubagentRegistry,
) -> StructuredTool:
    from langchain.agents import create_agent  # pyright: ignore[reportUnknownVariableType]

    create_agent_fn = cast("Any", create_agent)
    tool_cls = cast("Any", StructuredTool)

    async def agent_runtime(
        name: str,
        description: str,
        system_prompt: str,
        task: str,
    ) -> str:
        spec = runtime_registry.get(name)
        if spec is None:
            spec = runtime_registry.register(name, description, system_prompt)

        runner = create_agent_fn(
            model,
            system_prompt=spec.system_prompt,
            tools=[],
            name=spec.name,
        )
        result_obj: object = await runner.ainvoke(
            {"messages": [{"role": "user", "content": task}]}
        )
        if isinstance(result_obj, Mapping):
            result_map = cast("Mapping[str, object]", result_obj)
            messages_obj = result_map.get("messages")
        else:
            messages_obj = None
        if isinstance(messages_obj, list):
            message_items = cast("list[object]", messages_obj)
            for message_obj in reversed(message_items):
                if isinstance(message_obj, AIMessage):
                    text_attr = cast("str | None", message_obj.text)
                    if text_attr:
                        text = text_attr.rstrip()
                    else:
                        raw_content = getattr(message_obj, "content", "")
                        text = text_of(raw_content)
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

    return tool_cls.from_function(
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
