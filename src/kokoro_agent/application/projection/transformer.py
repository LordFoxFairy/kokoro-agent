"""ACL 投影映射：把 v3 typed projection 元素映射为对外 AgentEvent，归属取自结构而非状态。"""

from __future__ import annotations

from pydantic import JsonValue

from kokoro_agent.application.protocols.agent import SubagentInfo, ToolCallInfo
from kokoro_agent.domain.registered_subagent import SubagentSource
from kokoro_agent.infrastructure.constants import (
    RUNTIME_SUBAGENT_TOOL_NAME,
    SUBAGENT_TOOL_NAME,
)
from kokoro_agent.infrastructure.subagent.specs import subagent_source_for
from kokoro_agent.interfaces.envelope import (
    AgentEvent,
    ChunkData,
    CustomStatus,
    DoneData,
    ErrorData,
    EventData,
    ExternalEvent,
    StartedStatus,
    SubagentFinishedStatus,
    SubagentStartedStatus,
    TodoUpdatedStatus,
    ToolEndData,
    ToolStartData,
)

SUBAGENT_LAUNCH_NAMES = frozenset({SUBAGENT_TOOL_NAME, RUNTIME_SUBAGENT_TOOL_NAME})


def _make_event(event: ExternalEvent, request_id: str, data: EventData) -> AgentEvent:
    return AgentEvent.model_validate({"event": event, "request_id": request_id, "data": data})


def run_started_event(request_id: str) -> AgentEvent:
    data: StartedStatus = {"status": "started"}
    return _make_event("agent_status", request_id, data)


def run_done_event(usage: dict[str, JsonValue], *, request_id: str) -> AgentEvent:
    data: DoneData = {"status": "completed", "usage": usage}
    return _make_event("agent_done", request_id, data)


def run_error_event(error: BaseException, *, request_id: str) -> AgentEvent:
    data: ErrorData = {"error_kind": type(error).__name__, "message": str(error)}
    return _make_event("agent_error", request_id, data)


def text_chunk_event(
    text: str, *, segment_id: str, request_id: str, subagent_id: str | None, final: bool
) -> AgentEvent | None:
    return _chunk_event(
        "text_chunk", text, segment_id=segment_id, request_id=request_id, subagent_id=subagent_id, final=final
    )


def reasoning_chunk_event(
    text: str, *, segment_id: str, request_id: str, subagent_id: str | None, final: bool
) -> AgentEvent | None:
    return _chunk_event(
        "reasoning_chunk", text, segment_id=segment_id, request_id=request_id, subagent_id=subagent_id, final=final
    )


def _chunk_event(
    event: ExternalEvent,
    text: str,
    *,
    segment_id: str,
    request_id: str,
    subagent_id: str | None,
    final: bool,
) -> AgentEvent | None:
    # 空文本不发（tool-only 段 output_message.text=""；reasoning 无内容同理）。
    if not text:
        return None
    data: ChunkData = {"segment_id": segment_id, "text": text, "final": final}
    if subagent_id is not None:
        data["subagent_id"] = subagent_id
    return _make_event(event, request_id, data)


def todo_event(tc: ToolCallInfo, *, request_id: str) -> AgentEvent:
    # deepagents write_todos 已按 args_schema 校验 todos 结构，原样透传；JSON 安全由信封单一边界兜。
    data: TodoUpdatedStatus = {
        "status": "todo_updated",
        "segment_id": tc.tool_call_id,
        "todos": (tc.input or {}).get("todos", []),
    }
    return _make_event("agent_status", request_id, data)


def tool_start_event(tc: ToolCallInfo, *, request_id: str, subagent_id: str | None = None) -> AgentEvent:
    data: ToolStartData = {
        "segment_id": tc.tool_call_id,
        "tool_id": tc.tool_call_id,
        "name": tc.tool_name,
        # 模型生成的入参原样透传；JSON 安全由 AgentEvent 信封单一边界校验，不在此重复。
        "args": dict(tc.input or {}),
    }
    if subagent_id is not None:
        data["subagent_id"] = subagent_id
    return _make_event("tool_call_start", request_id, data)


def tool_end_event(tc: ToolCallInfo, *, request_id: str, subagent_id: str | None = None) -> AgentEvent:
    # 经 v3 projection 浮现的工具=真实执行过（approve/edit/无门控）：rejected 恒 False；
    # reject/respond 工具不经 projection（见 tool_resolution_event）。
    data: ToolEndData = {
        "segment_id": tc.tool_call_id,
        "tool_id": tc.tool_call_id,
        "name": tc.tool_name,
        # 工具结果原样透传，绝不截断（deepagents/工具自身管大小；wire 不毁内容）。
        "result": _result_text(tc),
        "is_error": tc.error is not None,
        "rejected": False,
    }
    if subagent_id is not None:
        data["subagent_id"] = subagent_id
    return _make_event("tool_call_end", request_id, data)


def tool_resolution_event(
    *,
    tool_id: str,
    segment_id: str,
    name: str,
    result: str,
    request_id: str,
    rejected: bool,
    reject_reason: str | None = None,
) -> AgentEvent:
    # HITL reject/respond 生成 synthetic ToolMessage 跳过 tool 节点 → 工具不经 v3 projection 浮现；
    # 故由 resume 据 snapshot+decision 直发终态（与 tool_call_awaiting 同为快照直发，replay 安全）。
    data: ToolEndData = {
        "segment_id": segment_id,
        "tool_id": tool_id,
        "name": name,
        "result": result,
        "is_error": False,
        "rejected": rejected,
    }
    if rejected and reject_reason:
        data["reject_reason"] = reject_reason
    return _make_event("tool_call_end", request_id, data)


def subagent_started_event(sub: SubagentInfo, *, request_id: str) -> AgentEvent:
    name = sub.name or "subagent"
    data: SubagentStartedStatus = {
        "status": "subagent_started",
        "segment_id": sub.trigger_call_id or "",
        "subagent_id": sub.trigger_call_id or "",
        "name": name,
        "description": sub.task_input or "",
        "subagent_type": name,
        "source": _source_for(name),
    }
    return _make_event("agent_status", request_id, data)


def subagent_finished_event(sub: SubagentInfo, *, request_id: str) -> AgentEvent:
    name = sub.name or "subagent"
    data: SubagentFinishedStatus = {
        "status": "subagent_finished",
        "segment_id": sub.trigger_call_id or "",
        "subagent_id": sub.trigger_call_id or "",
        "name": name,
        "subagent_type": name,
        "source": _source_for(name),
    }
    # langgraph SubgraphStatus="failed" → 子代理内部异常：失败有归属，不再被吞成顶层 agent_error。
    if sub.status == "failed":
        data["failed"] = True
    return _make_event("agent_status", request_id, data)


def custom_event(payload: object, *, request_id: str) -> AgentEvent:
    # 守则D：get_stream_writer() 业务遥测原样挂 agent_status.data.custom；JSON 安全由信封单一边界校验。
    data: CustomStatus = {"status": "custom", "custom": payload}
    return _make_event("agent_status", request_id, data)


def _source_for(name: str) -> SubagentSource:
    # 未在内建/env catalog 的名即运行时注册 → runtime-custom（catalog 对未知名抛 ValueError）。
    try:
        return subagent_source_for(name)
    except ValueError:
        return "runtime-custom"


def _result_text(tc: ToolCallInfo) -> str:
    if tc.error is not None:
        return tc.error
    output = tc.output
    if output is None:
        return ""
    text = getattr(output, "text", None)
    return text if isinstance(text, str) else str(output)
