"""对外事件信封：消费端只见此统一外壳，绝不见 LangChain 原生流碎片。"""

from __future__ import annotations

import time
from typing import Literal, NotRequired, TypedDict

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from kokoro_agent.domain.registered_subagent import SubagentSource

ExternalEvent = Literal[
    "agent_status",
    "text_chunk",
    "reasoning_chunk",
    "tool_call_start",
    "tool_call_end",
    "agent_done",
    "agent_error",
]


# 每事件 data 的强类型形状（构造点静态查键/类型，运行时仍是同一 dict，wire 字节不变）。
# AgentEvent 是 strict/forbid 的 Pydantic 外边界；这些 TypedDict 是其内部 data 载荷契约。
class ChunkData(TypedDict):
    # text_chunk 与 reasoning_chunk 共用同形载荷；仅 event 字段区分通道（原生 .text/.reasoning）。
    segment_id: str
    text: str
    final: bool
    subagent_id: NotRequired[str]


class ToolStartData(TypedDict):
    segment_id: str
    tool_id: str
    name: str
    # 模型生成的入参原样透传；JSON 安全由 AgentEvent 信封单一边界 model_validate 校验。
    args: dict[str, object]


class ToolEndData(TypedDict):
    segment_id: str
    tool_id: str
    name: str
    result: str
    is_error: bool
    rejected: bool


class StartedStatus(TypedDict):
    status: Literal["started"]


class TodoUpdatedStatus(TypedDict):
    status: Literal["todo_updated"]
    segment_id: str
    todos: list[JsonValue]


class SubagentStartedStatus(TypedDict):
    status: Literal["subagent_started"]
    segment_id: str
    subagent_id: str
    name: str
    description: str
    subagent_type: str
    source: SubagentSource


class SubagentFinishedStatus(TypedDict):
    status: Literal["subagent_finished"]
    segment_id: str
    subagent_id: str
    name: str
    subagent_type: str
    source: SubagentSource


class CustomStatus(TypedDict):
    status: Literal["custom"]
    # 任意用户业务遥测原样透传；JSON 安全由 AgentEvent 信封单一边界校验。
    custom: object


class PendingApproval(TypedDict):
    tool_id: str
    name: str
    args: dict[str, object]


class AwaitingStatus(TypedDict):
    status: Literal["awaiting_approval"]
    segment_id: str
    pending: list[PendingApproval]


class DoneData(TypedDict):
    status: Literal["completed"]
    usage: dict[str, JsonValue]


class ErrorData(TypedDict):
    error_kind: str
    message: str


EventData = (
    ChunkData
    | ToolStartData
    | ToolEndData
    | StartedStatus
    | TodoUpdatedStatus
    | SubagentStartedStatus
    | SubagentFinishedStatus
    | CustomStatus
    | AwaitingStatus
    | DoneData
    | ErrorData
)


def _now_ms() -> int:
    return int(time.time() * 1000)


class AgentEvent(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    event: ExternalEvent
    request_id: str
    timestamp: int = Field(default_factory=_now_ms)
    data: dict[str, JsonValue]
