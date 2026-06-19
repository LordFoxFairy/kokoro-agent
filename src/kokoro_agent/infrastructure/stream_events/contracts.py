from __future__ import annotations

from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict

from kokoro_agent.domain.registered_subagent import SubagentSource
from kokoro_agent.domain.stream_intent import (
    StreamIntent,
    SubagentFinished,
    SubagentStarted,
    TextFinal,
    TextStream,
    ThinkingDelta,
    TodoItem,
    TodoStatus,
    TodoUpdated,
    ToolInvoked,
    ToolReturned,
    ToolScalar,
)
from kokoro_agent.infrastructure.stream_events.events import EventHeader, MessageParts, ToolInput


class TodoItemContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    content: str
    status: TodoStatus


class MessagePartsContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    text: str
    reasoning: str


class TodoUpdatedPayloadContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    todos: list[TodoItemContract]


class ToolInvokedPayloadContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    tool_id: str
    name: str
    args: dict[str, ToolScalar]


class ToolReturnedPayloadContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    tool_id: str
    name: str
    result: str
    is_error: bool
    rejected: bool = False


class SubagentStartedPayloadContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    subagent_id: str
    name: str
    description: str
    subagent_type: str
    source: SubagentSource


class SubagentFinishedPayloadContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    subagent_id: str
    name: str
    subagent_type: str
    source: SubagentSource


class ThinkingDeltaPayloadContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    text: str


class TextPayloadContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    text: str


class TodoUpdatedContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["todo.updated"] = "todo.updated"
    payload: TodoUpdatedPayloadContract


class ToolInvokedContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["tool.invoked"] = "tool.invoked"
    payload: ToolInvokedPayloadContract


class ToolReturnedContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["tool.returned"] = "tool.returned"
    payload: ToolReturnedPayloadContract


class SubagentStartedContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["subagent.started"] = "subagent.started"
    payload: SubagentStartedPayloadContract


class SubagentFinishedContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["subagent.finished"] = "subagent.finished"
    payload: SubagentFinishedPayloadContract


class ThinkingDeltaContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["thinking.delta"] = "thinking.delta"
    payload: ThinkingDeltaPayloadContract


class TextStreamContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["text.stream"] = "text.stream"
    payload: TextPayloadContract


class TextFinalContract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["text"] = "text"
    payload: TextPayloadContract


StreamIntentContract: TypeAlias = (
    TodoUpdatedContract
    | ToolInvokedContract
    | ToolReturnedContract
    | SubagentStartedContract
    | SubagentFinishedContract
    | ThinkingDeltaContract
    | TextStreamContract
    | TextFinalContract
)


def todo_item_contract(item: TodoItem) -> TodoItemContract:
    return TodoItemContract(content=item.content, status=item.status)


def message_parts_contract(parts: MessageParts) -> MessagePartsContract:
    return MessagePartsContract(text=parts.text, reasoning=parts.reasoning)


def stream_intent_contract(intent: StreamIntent) -> StreamIntentContract:
    match intent:
        case TodoUpdated(todos=todos):
            return TodoUpdatedContract(
                payload=TodoUpdatedPayloadContract(
                    todos=[todo_item_contract(todo) for todo in todos]
                )
            )
        case ToolInvoked(tool_id=tool_id, name=name, args=args):
            return ToolInvokedContract(
                payload=ToolInvokedPayloadContract(
                    tool_id=tool_id,
                    name=name,
                    args=dict(args),
                )
            )
        case ToolReturned(tool_id=tool_id, name=name, result=result, is_error=is_error, rejected=rejected):
            return ToolReturnedContract(
                payload=ToolReturnedPayloadContract(
                    tool_id=tool_id,
                    name=name,
                    result=result,
                    is_error=is_error,
                    rejected=rejected,
                )
            )
        case SubagentStarted(
            subagent_id=subagent_id,
            name=name,
            description=description,
            subagent_type=subagent_type,
            source=source,
        ):
            return SubagentStartedContract(
                payload=SubagentStartedPayloadContract(
                    subagent_id=subagent_id,
                    name=name,
                    description=description,
                    subagent_type=subagent_type,
                    source=source,
                )
            )
        case SubagentFinished(
            subagent_id=subagent_id,
            name=name,
            subagent_type=subagent_type,
            source=source,
        ):
            return SubagentFinishedContract(
                payload=SubagentFinishedPayloadContract(
                    subagent_id=subagent_id,
                    name=name,
                    subagent_type=subagent_type,
                    source=source,
                )
            )
        case ThinkingDelta(text=text):
            return ThinkingDeltaContract(payload=ThinkingDeltaPayloadContract(text=text))
        case TextStream(text=text):
            return TextStreamContract(payload=TextPayloadContract(text=text))
        case TextFinal(text=text):
            return TextFinalContract(payload=TextPayloadContract(text=text))
        case _:
            msg = "unknown stream intent"
            raise ValueError(msg)


__all__ = [
    "EventHeader",
    "MessageParts",
    "MessagePartsContract",
    "StreamIntent",
    "StreamIntentContract",
    "SubagentFinished",
    "SubagentFinishedContract",
    "SubagentFinishedPayloadContract",
    "SubagentSource",
    "SubagentStarted",
    "SubagentStartedContract",
    "SubagentStartedPayloadContract",
    "TextFinal",
    "TextFinalContract",
    "TextPayloadContract",
    "TextStream",
    "TextStreamContract",
    "ThinkingDelta",
    "ThinkingDeltaContract",
    "ThinkingDeltaPayloadContract",
    "TodoItem",
    "TodoItemContract",
    "TodoStatus",
    "TodoUpdated",
    "TodoUpdatedContract",
    "TodoUpdatedPayloadContract",
    "ToolInput",
    "ToolInvoked",
    "ToolInvokedContract",
    "ToolInvokedPayloadContract",
    "ToolReturned",
    "ToolReturnedContract",
    "ToolReturnedPayloadContract",
    "ToolScalar",
    "message_parts_contract",
    "stream_intent_contract",
]
