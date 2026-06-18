from __future__ import annotations

from collections.abc import Mapping
from typing import TypeAlias, TypeGuard

from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.runnables.schema import StreamEvent
from pydantic import TypeAdapter, ValidationError
from typing_extensions import TypedDict

from kokoro_agent.infrastructure.control import rejection_result
from kokoro_agent.infrastructure.stream_events.events import (
    RUNTIME_SUBAGENT_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
    TODO_TOOL_NAME,
    TOOL_RESULT_MAX_CHARS,
    EventHeader,
    MessageParts,
    StreamIntent,
    SubagentFinished,
    SubagentStarted,
    TextFinal,
    TextStream,
    ThinkingDelta,
    TodoItem,
    TodoUpdated,
    ToolInput,
    ToolInvoked,
    ToolReturned,
    ToolScalar,
)
from kokoro_agent.infrastructure.subagent_registry import subagent_source_for


class RawToolInput(TypedDict, total=False):
    todos: list[dict[str, str]]
    subagent_type: str
    description: str
    name: str


_ObjectMapping: TypeAlias = Mapping[object, object]
_ObjectList: TypeAlias = list[object]
_RAW_TOOL_INPUT = TypeAdapter(RawToolInput)


def _is_object_mapping(value: object) -> TypeGuard[_ObjectMapping]:
    return isinstance(value, Mapping)


def _is_object_list(value: object) -> TypeGuard[_ObjectList]:
    return isinstance(value, list)


def _is_tool_scalar(value: object) -> TypeGuard[ToolScalar]:
    return value is None or isinstance(value, (str, int, float, bool))


def _scalar_args_from(value: object) -> dict[str, ToolScalar]:
    scalar_args: dict[str, ToolScalar] = {}
    if not _is_object_mapping(value):
        return scalar_args
    for key, item in value.items():
        if isinstance(key, str) and _is_tool_scalar(item):
            scalar_args[key] = item
    return scalar_args


def read_header(event: StreamEvent) -> EventHeader:
    match event:
        case {
            "event": str() as kind,
            "name": str() as name,
            "run_id": str() as run_id,
            "metadata": {"lc_agent_name": str() as lc_agent_name},
        }:
            return EventHeader(kind, name, run_id, lc_agent_name)
        case {
            "event": str() as kind,
            "name": str() as name,
            "metadata": {"lc_agent_name": str() as lc_agent_name},
        }:
            return EventHeader(kind, name, "", lc_agent_name)
        case {"event": str() as kind, "name": str() as name, "run_id": str() as run_id}:
            return EventHeader(kind, name, run_id, "")
        case {"event": str() as kind, "name": str() as name}:
            return EventHeader(kind, name, "", "")
        case _:
            return EventHeader("", "", "", "")


def read_tool_input(event: StreamEvent) -> ToolInput:
    match event:
        case {"data": {"input": raw_input}}:
            raw_input_obj: object = raw_input
            try:
                typed_input = _RAW_TOOL_INPUT.validate_python(raw_input_obj)
            except ValidationError:
                return ToolInput({}, (), "", "", "")
        case _:
            return ToolInput({}, (), "", "", "")

    scalar_args = _scalar_args_from(raw_input_obj)

    todos: list[TodoItem] = []
    match typed_input.get("todos"):
        case list() as raw_todos:
            for todo in raw_todos:
                match todo:
                    case {"content": str() as content, "status": "pending" | "in_progress" | "completed" as status}:
                        todos.append(TodoItem(content, status))
                    case _:
                        continue
        case _:
            pass

    match typed_input.get("subagent_type"):
        case str() as subagent_type:
            parsed_subagent_type = subagent_type
        case _:
            parsed_subagent_type = ""
    match typed_input.get("description"):
        case str() as description:
            parsed_description = description
        case _:
            parsed_description = ""
    match typed_input.get("name"):
        case str() as name:
            parsed_name = name
        case _:
            parsed_name = ""

    return ToolInput(
        args=scalar_args,
        todos=tuple(todos),
        subagent_type=parsed_subagent_type,
        description=parsed_description,
        name=parsed_name,
    )


def read_output(event: StreamEvent) -> BaseMessage | ToolScalar:
    match event:
        case {"data": {"output": BaseMessage() as output}}:
            return output
        case {"data": {"output": str() as output}}:
            return output
        case {"data": {"output": int() as output}}:
            return output
        case {"data": {"output": float() as output}}:
            return output
        case {"data": {"output": bool() as output}}:
            return output
        case {"data": {"output": None}}:
            return None
        case _:
            return None


def read_error(event: StreamEvent) -> BaseException | ToolScalar:
    match event:
        case {"data": {"error": BaseException() as error}}:
            return error
        case {"data": {"error": str() as error}}:
            return error
        case {"data": {"error": int() as error}}:
            return error
        case {"data": {"error": float() as error}}:
            return error
        case {"data": {"error": bool() as error}}:
            return error
        case {"data": {"error": None}}:
            return None
        case _:
            return None


def read_chunk(event: StreamEvent) -> BaseMessage | None:
    match event:
        case {"data": {"chunk": BaseMessage() as chunk}}:
            return chunk
        case _:
            return None


def message_parts(message: BaseMessage) -> MessageParts:
    reasoning_override = message.additional_kwargs.get("reasoning_content")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    if isinstance(reasoning_override, str) and reasoning_override:
        return MessageParts(text=str(message.text), reasoning=reasoning_override)

    reasoning_parts: list[str] = []
    for block in message.content_blocks:
        match block:
            case {"type": "reasoning", "reasoning": str() as reasoning_text}:
                reasoning_parts.append(reasoning_text)
            case {
                "type": "non_standard",
                "value": {"type": "thinking", "thinking": str() as thinking_text},
            }:
                reasoning_parts.append(thinking_text)
            case {
                "type": "non_standard",
                "value": {"type": "thinking", "text": str() as thinking_text},
            }:
                reasoning_parts.append(thinking_text)
            case _:
                continue
    return MessageParts(text=str(message.text), reasoning="".join(reasoning_parts))


def result_text(output: BaseMessage | ToolScalar) -> str:
    match output:
        case BaseMessage() as message:
            return message_parts(message).text
        case str() as text:
            return text
        case None:
            return ""
        case _:
            return str(output)


def result_messages(result: Mapping[str, object]) -> list[BaseMessage]:
    raw_messages = result.get("messages")
    if not _is_object_list(raw_messages):
        return []
    messages: list[BaseMessage] = []
    for message in raw_messages:
        if isinstance(message, BaseMessage):
            messages.append(message)
    return messages


def _truncated(result: str) -> str:
    if len(result) <= TOOL_RESULT_MAX_CHARS:
        return result
    return f"{result[:TOOL_RESULT_MAX_CHARS]}…（结果过长，事件流中已在 {TOOL_RESULT_MAX_CHARS} 字符处截断）"


def _subagent_started(tool_id: str, name: str, tool_input: ToolInput) -> SubagentStarted | None:
    if name == SUBAGENT_TOOL_NAME:
        subagent_type = tool_input.subagent_type or "subagent"
        return SubagentStarted(
            subagent_id=tool_id,
            name=subagent_type,
            description=tool_input.description,
            subagent_type=subagent_type,
            source=subagent_source_for(subagent_type),
        )
    if name == RUNTIME_SUBAGENT_TOOL_NAME:
        runtime_name = tool_input.name or "runtime-subagent"
        return SubagentStarted(
            subagent_id=tool_id,
            name=runtime_name,
            description=tool_input.description,
            subagent_type=runtime_name,
            source="runtime-custom",
        )
    return None


def _subagent_finished(tool_id: str, name: str, tool_input: ToolInput) -> SubagentFinished | None:
    if name == SUBAGENT_TOOL_NAME:
        if not tool_input.subagent_type:
            return SubagentFinished(
                subagent_id=tool_id,
                name="",
                subagent_type="",
                source="built-in",
            )
        subagent_type = tool_input.subagent_type
        return SubagentFinished(
            subagent_id=tool_id,
            name=subagent_type,
            subagent_type=subagent_type,
            source=subagent_source_for(subagent_type),
        )
    if name == RUNTIME_SUBAGENT_TOOL_NAME:
        runtime_name = tool_input.name or "runtime-subagent"
        return SubagentFinished(
            subagent_id=tool_id,
            name=runtime_name,
            subagent_type=runtime_name,
            source="runtime-custom",
        )
    return None


def translate_stream_event(ev: StreamEvent) -> list[StreamIntent]:
    header = read_header(ev)
    tool_input = read_tool_input(ev)

    if header.event == "on_tool_start":
        if header.name == TODO_TOOL_NAME:
            return [TodoUpdated(tool_input.todos)]
        subagent = _subagent_started(header.run_id, header.name, tool_input)
        if subagent is not None:
            return [subagent]
        return [ToolInvoked(header.run_id, header.name, tool_input.args)]

    if header.event == "on_tool_end":
        if header.name == TODO_TOOL_NAME:
            return []
        subagent = _subagent_finished(header.run_id, header.name, tool_input)
        if subagent is not None:
            return [subagent]
        result = _truncated(result_text(read_output(ev)))
        return [
            ToolReturned(
                tool_id=header.run_id,
                name=header.name,
                result=result,
                is_error=False,
                rejected=result == rejection_result(header.name),
            )
        ]

    if header.event == "on_tool_error":
        if header.name == TODO_TOOL_NAME:
            return []
        subagent = _subagent_finished(header.run_id, header.name, tool_input)
        if subagent is not None:
            return [subagent]
        error = read_error(ev)
        error_text = str(error) or type(error).__name__
        return [
            ToolReturned(
                tool_id=header.run_id,
                name=header.name,
                result=_truncated(error_text),
                is_error=True,
            )
        ]

    if header.event == "on_chat_model_stream":
        chunk = read_chunk(ev)
        if chunk is None:
            return []
        parts = message_parts(chunk)
        intents: list[StreamIntent] = []
        if parts.reasoning:
            intents.append(ThinkingDelta(parts.reasoning))
        if parts.text:
            intents.append(TextStream(parts.text))
        return intents

    if header.event == "on_chat_model_end":
        output = read_output(ev)
        if isinstance(output, AIMessage):
            parts = message_parts(output)
            intents: list[StreamIntent] = []
            if parts.reasoning:
                intents.append(ThinkingDelta(parts.reasoning))
            if parts.text:
                intents.append(TextFinal(parts.text))
            return intents
        return []

    return []
