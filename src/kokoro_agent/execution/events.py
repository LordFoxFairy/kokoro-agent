"""wire 构造唯一地点：per-run 单调 index 单点递增，contract strict 模型构造即校验。"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping

from pydantic import BaseModel, JsonValue, TypeAdapter

from kokoro_agent.contract import (
    RUN_EVENTS_MAXLEN,
    MessageCompletedPayload,
    MessageDeltaPayload,
    RunCompletedPayload,
    RunFailedPayload,
    RunStartedPayload,
    SubagentFinishedPayload,
    SubagentSource,
    SubagentStartedPayload,
    SubagentTextCompletedPayload,
    SubagentTextDeltaPayload,
    ThinkingDeltaPayload,
    Todo,
    TodoUpdatedPayload,
    ToolAwaitingApprovalPayload,
    ToolInvokedPayload,
    ToolReturnedPayload,
    agent_event_adapter,
    run_events_stream,
)
from kokoro_agent.execution.protocols import SubagentInfo, ToolCallInfo
from kokoro_agent.streams.protocol import StreamProtocol

SourceResolver = Callable[[str], SubagentSource]

AgentEventPayload = (
    RunStartedPayload
    | ThinkingDeltaPayload
    | MessageDeltaPayload
    | MessageCompletedPayload
    | ToolInvokedPayload
    | ToolAwaitingApprovalPayload
    | ToolReturnedPayload
    | TodoUpdatedPayload
    | SubagentStartedPayload
    | SubagentFinishedPayload
    | SubagentTextDeltaPayload
    | SubagentTextCompletedPayload
    | RunCompletedPayload
    | RunFailedPayload
)

_KIND_BY_PAYLOAD: Mapping[type[BaseModel], str] = {
    RunStartedPayload: "run.started",
    ThinkingDeltaPayload: "thinking.delta",
    MessageDeltaPayload: "message.delta",
    MessageCompletedPayload: "message.completed",
    ToolInvokedPayload: "tool.invoked",
    ToolAwaitingApprovalPayload: "tool.awaiting_approval",
    ToolReturnedPayload: "tool.returned",
    TodoUpdatedPayload: "todo.updated",
    SubagentStartedPayload: "subagent.started",
    SubagentFinishedPayload: "subagent.finished",
    SubagentTextDeltaPayload: "subagent.text.delta",
    SubagentTextCompletedPayload: "subagent.text.completed",
    RunCompletedPayload: "run.completed",
    RunFailedPayload: "run.failed",
}


def _now_ms() -> int:
    return int(time.time() * 1000)


class RunEmitter:
    """一次 run 的唯一发射口：index 在此单点递增，event_id=f(run_id,index) 永不回卷。"""

    def __init__(
        self,
        bus: StreamProtocol,
        run_id: str,
        next_index: int = 0,
        tool_segments: dict[str, str] | None = None,
    ) -> None:
        self._bus = bus
        self._run_id = run_id
        self._next_index = next_index
        # tool_id → 归属 segment：awaiting 携带 AIMessage 段，resume 后的 invoked/returned
        # 只有 tool_call_id 兜底段——继承归属段，一次工具调用在渲染侧恒为一段。
        self._tool_segments = tool_segments if tool_segments is not None else {}

    @property
    def run_id(self) -> str:
        return self._run_id

    @classmethod
    async def attach(cls, bus: StreamProtocol, run_id: str) -> RunEmitter:
        # 续段（resume/重启/租约重拾）从既有最大 index 之后继续：event_id 幂等链不碰撞。
        # 同时从历史 awaiting 事件重建 tool_id→segment 归属（漂移正发生在 resume 重建之后）。
        next_index = 0
        tool_segments: dict[str, str] = {}
        for item in await bus.read_all(run_events_stream(run_id)):
            event = agent_event_adapter.validate_python(item.event)
            next_index = max(next_index, event.index + 1)
            if isinstance(event.payload, ToolAwaitingApprovalPayload):
                tool_segments[event.payload.tool_id] = event.payload.segment_id
        return cls(bus, run_id, next_index, tool_segments)

    def _with_owner_segment(self, payload: AgentEventPayload) -> AgentEventPayload:
        if isinstance(payload, ToolAwaitingApprovalPayload):
            self._tool_segments[payload.tool_id] = payload.segment_id
            return payload
        if isinstance(payload, ToolInvokedPayload | ToolReturnedPayload):
            owner = self._tool_segments.get(payload.tool_id)
            if owner is not None and owner != payload.segment_id:
                return payload.model_copy(update={"segment_id": owner})
        return payload

    async def emit(self, payload: AgentEventPayload) -> None:
        payload = self._with_owner_segment(payload)
        event = agent_event_adapter.validate_python(
            {
                "kind": _KIND_BY_PAYLOAD[type(payload)],
                "run_id": self._run_id,
                "index": self._next_index,
                "timestamp": _now_ms(),
                "payload": payload,
            }
        )
        self._next_index += 1
        # exclude_none：契约 optional 字段的 None 即"缺席"；null 上 wire 会被 session 的 zod .optional() 拒收。
        await self._bus.publish(
            run_events_stream(self._run_id),
            event.model_dump(exclude_none=True),
            maxlen=RUN_EVENTS_MAXLEN,
        )


# --- 投影 → payload 映射（v3 typed projection 元素转 contract 载荷的唯一地点） ---

_TODOS_ADAPTER: TypeAdapter[list[Todo]] = TypeAdapter(list[Todo])


def message_delta_payload(text: str, *, segment_id: str) -> MessageDeltaPayload | None:
    return MessageDeltaPayload(segment_id=segment_id, delta=text) if text else None


def message_completed_payload(text: str, *, segment_id: str) -> MessageCompletedPayload | None:
    # 空文本不发（tool-only 段 output_message.text==""）。
    return MessageCompletedPayload(segment_id=segment_id, content=text) if text else None


def thinking_delta_payload(text: str, *, segment_id: str) -> ThinkingDeltaPayload | None:
    return ThinkingDeltaPayload(segment_id=segment_id, delta=text) if text else None


def subagent_text_delta_payload(
    text: str, *, segment_id: str, subagent_id: str
) -> SubagentTextDeltaPayload | None:
    if not text:
        return None
    return SubagentTextDeltaPayload(segment_id=segment_id, subagent_id=subagent_id, text=text)


def subagent_text_completed_payload(
    text: str, *, segment_id: str, subagent_id: str
) -> SubagentTextCompletedPayload | None:
    if not text:
        return None
    return SubagentTextCompletedPayload(segment_id=segment_id, subagent_id=subagent_id, text=text)


def tool_invoked_payload(tc: ToolCallInfo) -> ToolInvokedPayload:
    return ToolInvokedPayload(
        segment_id=tc.tool_call_id,
        tool_id=tc.tool_call_id,
        name=tc.tool_name,
        # 模型生成的入参原样透传；JSON 安全由 strict payload 构造一次性校验。
        args=_json_args(tc.input),
    )


def tool_returned_payload(tc: ToolCallInfo) -> ToolReturnedPayload:
    # 经 v3 projection 浮现的工具=真实执行过（approve/edit/无门控）：rejected 缺省。
    return ToolReturnedPayload(
        segment_id=tc.tool_call_id,
        tool_id=tc.tool_call_id,
        name=tc.tool_name,
        result=_result_text(tc),
        is_error=tc.error is not None,
    )


def todo_payload(tc: ToolCallInfo) -> TodoUpdatedPayload:
    # todos 来自 LLM 工具入参（不可信载荷）：strict 洗净后进 wire。
    todos = (tc.input or {}).get("todos", [])
    return TodoUpdatedPayload(todos=_TODOS_ADAPTER.validate_python(todos))


def subagent_started_payload(sub: SubagentInfo, *, source: SubagentSource) -> SubagentStartedPayload:
    name = sub.name or "subagent"
    return SubagentStartedPayload(
        segment_id=sub.trigger_call_id or "subagent",
        subagent_id=sub.trigger_call_id or "subagent",
        name=name,
        description=sub.task_input or "",
        subagent_type=name,
        source=source,
    )


def subagent_finished_payload(
    sub: SubagentInfo, *, source: SubagentSource
) -> SubagentFinishedPayload:
    name = sub.name or "subagent"
    # SubgraphStatus=="failed" → 子代理内部异常：失败有归属，不被吞成顶层 run.failed。
    failed = sub.status == "failed"
    return SubagentFinishedPayload(
        segment_id=sub.trigger_call_id or "subagent",
        subagent_id=sub.trigger_call_id or "subagent",
        name=name,
        subagent_type=name,
        source=source,
        failed=True if failed else None,
        error="subagent failed" if failed else None,
    )


def run_failed_payload(error: BaseException) -> RunFailedPayload:
    # message 契约 NonEmptyStr：空 str(error) 回退异常类名，绝不发空错误。
    return RunFailedPayload(
        error_kind=type(error).__name__, message=str(error) or type(error).__name__
    )


_ARGS_ADAPTER: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])


def _json_args(args: dict[str, object] | None) -> dict[str, JsonValue]:
    return _ARGS_ADAPTER.validate_python(args or {})


def _result_text(tc: ToolCallInfo) -> str:
    if tc.error is not None:
        return tc.error
    output = tc.output
    if output is None:
        return ""
    text = getattr(output, "text", None)
    return text if isinstance(text, str) else str(output)
