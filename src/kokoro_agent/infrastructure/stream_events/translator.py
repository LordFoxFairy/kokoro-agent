from __future__ import annotations

from langchain_core.runnables.schema import StreamEvent

from kokoro_agent.infrastructure.control import rejection_result
from kokoro_agent.infrastructure.stream_events.adapter import (
    message_parts,
    read_ai_message,
    read_chunk,
    read_error,
    read_header,
    read_output,
    read_tool_input,
    result_text,
)
from kokoro_agent.domain.stream_intent import (
    StreamIntent,
    SubagentFinished,
    SubagentStarted,
    TextFinal,
    TextStream,
    ThinkingDelta,
    TodoUpdated,
    ToolInvoked,
    ToolReturned,
)
from kokoro_agent.infrastructure.stream_events.events import (
    RUNTIME_SUBAGENT_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
    TODO_TOOL_NAME,
    TOOL_RESULT_MAX_CHARS,
    MessageParts,
    ToolInput,
)
from kokoro_agent.infrastructure.subagent import subagent_source_for


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


def _message_intents(parts: MessageParts, *, final: bool) -> list[StreamIntent]:
    intents: list[StreamIntent] = []
    if parts.reasoning:
        intents.append(ThinkingDelta(parts.reasoning))
    if parts.text:
        intents.append(TextFinal(parts.text) if final else TextStream(parts.text))
    return intents


def translate_stream_event(event: StreamEvent) -> list[StreamIntent]:
    header = read_header(event)
    tool_input = read_tool_input(event)

    match header.event:
        case "on_tool_start":
            if header.name == TODO_TOOL_NAME:
                return [TodoUpdated(tool_input.todos)]
            started = _subagent_started(header.run_id, header.name, tool_input)
            if started is not None:
                return [started]
            return [ToolInvoked(header.run_id, header.name, tool_input.args)]

        case "on_tool_end":
            if header.name == TODO_TOOL_NAME:
                return []
            finished = _subagent_finished(header.run_id, header.name, tool_input)
            if finished is not None:
                return [finished]
            result = _truncated(result_text(read_output(event)))
            return [
                ToolReturned(
                    tool_id=header.run_id,
                    name=header.name,
                    result=result,
                    is_error=False,
                    rejected=result == rejection_result(header.name),
                )
            ]

        case "on_tool_error":
            if header.name == TODO_TOOL_NAME:
                return []
            finished = _subagent_finished(header.run_id, header.name, tool_input)
            if finished is not None:
                return [finished]
            error = read_error(event)
            error_text = str(error) or type(error).__name__
            return [
                ToolReturned(
                    tool_id=header.run_id,
                    name=header.name,
                    result=_truncated(error_text),
                    is_error=True,
                )
            ]

        case "on_chat_model_stream":
            chunk = read_chunk(event)
            if chunk is None:
                return []
            return _message_intents(message_parts(chunk), final=False)

        case "on_chat_model_end":
            output = read_ai_message(event)
            if output is not None:
                return _message_intents(message_parts(output), final=True)
            return []

        case _:
            return []
