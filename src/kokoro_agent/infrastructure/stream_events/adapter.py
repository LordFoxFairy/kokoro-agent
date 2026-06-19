from __future__ import annotations

from collections.abc import Mapping
from typing import TypeAlias, TypeGuard

from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.runnables.schema import StreamEvent
from pydantic import TypeAdapter, ValidationError
from typing_extensions import TypedDict

from kokoro_agent.domain.stream_intent import TodoItem, ToolScalar
from kokoro_agent.infrastructure.stream_events.events import EventHeader, MessageParts, ToolInput


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
        case {"data": {"output": output}} if _is_tool_scalar(output):
            return output
        case _:
            return None


def read_error(event: StreamEvent) -> BaseException | ToolScalar:
    match event:
        case {"data": {"error": BaseException() as error}}:
            return error
        case {"data": {"error": error}} if _is_tool_scalar(error):
            return error
        case _:
            return None


def read_chunk(event: StreamEvent) -> BaseMessage | None:
    match event:
        case {"data": {"chunk": BaseMessage() as chunk}}:
            return chunk
        case _:
            return None


def read_ai_message(event: StreamEvent) -> AIMessage | None:
    output = read_output(event)
    return output if isinstance(output, AIMessage) else None


def _reasoning_override(message: BaseMessage) -> str | None:
    # langchain types additional_kwargs as a bare dict; the boundary's unknown
    # value type is contained to this single access.
    value = message.additional_kwargs.get("reasoning_content")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    return value if isinstance(value, str) and value else None


def message_parts(message: BaseMessage) -> MessageParts:
    override = _reasoning_override(message)
    if override is not None:
        return MessageParts(text=str(message.text), reasoning=override)

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
