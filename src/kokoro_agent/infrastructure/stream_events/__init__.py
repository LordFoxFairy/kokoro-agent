"""流事件子系统：把 LangChain 事件翻译为领域 StreamIntent 并提供契约镜像。"""

from .adapter import (
    message_parts,
    read_chunk,
    read_error,
    read_header,
    read_output,
    read_tool_input,
    result_messages,
    result_text,
)
from .translator import translate_stream_event
from .contracts import MessagePartsContract, StreamIntentContract, TodoItemContract, message_parts_contract, stream_intent_contract
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
from .events import (
    EventHeader,
    MessageParts,
    RUNTIME_SUBAGENT_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
    TODO_TOOL_NAME,
    ToolInput,
)

__all__ = [
    "EventHeader",
    "MessageParts",
    "MessagePartsContract",
    "RUNTIME_SUBAGENT_TOOL_NAME",
    "SUBAGENT_TOOL_NAME",
    "StreamIntent",
    "StreamIntentContract",
    "SubagentFinished",
    "SubagentSource",
    "SubagentStarted",
    "TODO_TOOL_NAME",
    "TextFinal",
    "TextStream",
    "ThinkingDelta",
    "TodoItem",
    "TodoItemContract",
    "TodoStatus",
    "TodoUpdated",
    "ToolInput",
    "ToolInvoked",
    "ToolReturned",
    "ToolScalar",
    "message_parts",
    "message_parts_contract",
    "read_chunk",
    "read_error",
    "read_header",
    "read_output",
    "read_tool_input",
    "result_messages",
    "result_text",
    "stream_intent_contract",
    "translate_stream_event",
]
