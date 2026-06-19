"""读取消息载荷：output/error/chunk 读取器 + 文本/推理拆分 + result 聚合。"""

from __future__ import annotations

from collections.abc import Mapping

from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.runnables.schema import StreamEvent

from kokoro_agent.domain.stream_intent import ToolScalar
from kokoro_agent.infrastructure.stream_events._guards import is_object_list, is_object_mapping, is_tool_scalar
from kokoro_agent.infrastructure.stream_events.parsed_event import MessageParts


def read_output(event: StreamEvent) -> BaseMessage | ToolScalar:
    match event:
        case {"data": {"output": BaseMessage() as output}}:
            return output
        case {"data": {"output": output}} if is_tool_scalar(output):
            return output
        case _:
            return None


def read_error(event: StreamEvent) -> BaseException | ToolScalar:
    match event:
        case {"data": {"error": BaseException() as error}}:
            return error
        case {"data": {"error": error}} if is_tool_scalar(error):
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
    # langchain 将 additional_kwargs 声明为无类型 dict；getattr 把未知值类型收口到 object 边界。
    kwargs: object = getattr(message, "additional_kwargs", None)
    if not is_object_mapping(kwargs):
        return None
    value = kwargs.get("reasoning_content")
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
    if not is_object_list(raw_messages):
        return []
    messages: list[BaseMessage] = []
    for message in raw_messages:
        if isinstance(message, BaseMessage):
            messages.append(message)
    return messages
