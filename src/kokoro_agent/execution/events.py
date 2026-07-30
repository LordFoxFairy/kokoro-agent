"""wire 构造唯一地点：per-run 单调 index 单点递增，contract strict 模型构造即校验。"""

from __future__ import annotations

import json
import hashlib
import time
from collections.abc import Callable, Mapping

from pydantic import BaseModel, JsonValue, TypeAdapter, ValidationError

from kokoro_agent.contract import (
    RUN_EVENTS_MAXLEN,
    DeliveryCreatedPayload,
    MessageCompletedPayload,
    MessageDeltaPayload,
    PlanProposedPayload,
    RunCompletedPayload,
    RunControlReceiptPayload,
    RunOwnerCompletedPayload,
    RunErrorCode,
    RunFailedPayload,
    RunStartedPayload,
    SubagentFinishedPayload,
    SubagentSource,
    SubagentStartedPayload,
    SubagentTextCompletedPayload,
    SubagentThinkingDeltaPayload,
    SubagentTextDeltaPayload,
    SubagentToolInvokedPayload,
    SubagentToolReturnedPayload,
    ThinkingDeltaPayload,
    Todo,
    TodoUpdatedPayload,
    ToolAwaitingApprovalPayload,
    ToolInvokedPayload,
    ToolOutputDeltaPayload,
    ToolReturnedPayload,
    agent_event_adapter,
    run_events_stream,
)
from langgraph.errors import GraphRecursionError
from langgraph.prebuilt.tool_node import ToolRuntime

from kokoro_agent.tools.middleware import TokenBudgetExceeded

from kokoro_agent import metrics
from kokoro_agent.execution.protocols import SubagentInfo, ToolCallInfo
from kokoro_agent.evidence.models import durable_output_drafts_for_event
from kokoro_agent.storage.ledger import OutboxFrame, RunLedger
from kokoro_agent.storage.execution_context import (
    CompletionEventDraft,
    CompletedExecutionContext,
    DurableCompletionFrame,
)
from kokoro_agent.streams.protocol import StreamProtocol
from kokoro_agent.tools.deliver import DELIVER_TOOL_NAME, DeliverResult

SourceResolver = Callable[[str], SubagentSource]

# R4 critical 集（V1）：这些 kind 走 durable outbox（分配 durable_seq/event_id、可补发）；
# 其余 live 帧（delta/tool 过程帧）不占 seq、丢了由 checkpoint 重建。
CRITICAL_KINDS: frozenset[str] = frozenset(
    {
        "run.started",
        "tool.awaiting_approval",
        "plan.proposed",
        "run.control.receipt",
        "run.owner.completed",
        "run.completed",
        "run.failed",
    }
)
# 终态帧：分配时 CAS 设 local fence（first-terminal），其后更大 seq 一律 superseded。
TERMINAL_KINDS: frozenset[str] = frozenset({"run.completed", "run.failed"})

AgentEventPayload = (
    RunStartedPayload
    | ThinkingDeltaPayload
    | MessageDeltaPayload
    | MessageCompletedPayload
    | ToolInvokedPayload
    | ToolOutputDeltaPayload
    | ToolAwaitingApprovalPayload
    | ToolReturnedPayload
    | TodoUpdatedPayload
    | PlanProposedPayload
    | SubagentStartedPayload
    | SubagentFinishedPayload
    | SubagentThinkingDeltaPayload
    | SubagentTextDeltaPayload
    | SubagentTextCompletedPayload
    | SubagentToolInvokedPayload
    | SubagentToolReturnedPayload
    | DeliveryCreatedPayload
    | RunCompletedPayload
    | RunFailedPayload
    | RunControlReceiptPayload
    | RunOwnerCompletedPayload
)

_KIND_BY_PAYLOAD: Mapping[type[BaseModel], str] = {
    RunStartedPayload: "run.started",
    ThinkingDeltaPayload: "thinking.delta",
    MessageDeltaPayload: "message.delta",
    MessageCompletedPayload: "message.completed",
    ToolInvokedPayload: "tool.invoked",
    ToolOutputDeltaPayload: "tool.output.delta",
    ToolAwaitingApprovalPayload: "tool.awaiting_approval",
    ToolReturnedPayload: "tool.returned",
    TodoUpdatedPayload: "todo.updated",
    PlanProposedPayload: "plan.proposed",
    SubagentStartedPayload: "subagent.started",
    SubagentThinkingDeltaPayload: "subagent.thinking.delta",
    SubagentFinishedPayload: "subagent.finished",
    SubagentTextDeltaPayload: "subagent.text.delta",
    SubagentTextCompletedPayload: "subagent.text.completed",
    SubagentToolInvokedPayload: "subagent.tool.invoked",
    SubagentToolReturnedPayload: "subagent.tool.returned",
    DeliveryCreatedPayload: "delivery.created",
    RunCompletedPayload: "run.completed",
    RunFailedPayload: "run.failed",
    RunControlReceiptPayload: "run.control.receipt",
    RunOwnerCompletedPayload: "run.owner.completed",
}


def _now_ms() -> int:
    return int(time.time() * 1000)


class DurableOutputCommitError(RuntimeError):
    """A required durable output could not be committed exactly once."""


class RunEmitter:
    """一次 run 的唯一发射口：index 在此单点递增，event_id=f(run_id,index) 永不回卷。"""

    def __init__(
        self,
        bus: StreamProtocol,
        run_id: str,
        next_index: int = 0,
        tool_segments: dict[str, str] | None = None,
        review_tool_names: frozenset[str] = frozenset(),
        outbox: RunLedger | None = None,
    ) -> None:
        self._bus = bus
        self._run_id = run_id
        self._next_index = next_index
        # 审核工具的 returned 由 supervisor 裁决后直发：投影在审核前就见到工具级完成，
        # raw 结果不得先于裁决上 wire，此处按名抑制投影侧 returned。
        self._review_tool_names = review_tool_names
        # tool_id → 归属 segment：awaiting 携带 AIMessage 段，resume 后的 invoked/returned
        # 只有 tool_call_id 兜底段——继承归属段，一次工具调用在渲染侧恒为一段。
        self._tool_segments = tool_segments if tool_segments is not None else {}
        # R4 durable outbox（None=纯 live 发布，向后兼容）：critical 帧经此分配 durable_seq/event_id、
        # 落 queued 行、发布后置 published。live 序（index）不动，durable_seq 独立并行。
        self._outbox = outbox

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def at_start(self) -> bool:
        # index==0 才是 run 真起点：resume/重启/重拾续段不重复宣告 run.started。
        return self._next_index == 0

    @classmethod
    async def attach(
        cls,
        bus: StreamProtocol,
        run_id: str,
        review_tool_names: frozenset[str] = frozenset(),
        outbox: RunLedger | None = None,
    ) -> RunEmitter:
        # 续段（resume/重启/租约重拾）从既有最大 index 之后继续：event_id 幂等链不碰撞。
        # 同时从历史 awaiting 事件重建 tool_id→segment 归属（漂移正发生在 resume 重建之后）。
        next_index = 0
        tool_segments: dict[str, str] = {}
        for item in await bus.read_all(run_events_stream(run_id)):
            event = agent_event_adapter.validate_python(item.event)
            next_index = max(next_index, event.index + 1)
            if isinstance(event.payload, ToolAwaitingApprovalPayload):
                tool_segments[event.payload.tool_id] = event.payload.segment_id
        return cls(bus, run_id, next_index, tool_segments, review_tool_names, outbox)

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
        if (
            isinstance(payload, ToolReturnedPayload)
            and payload.name in self._review_tool_names
            and payload.rejected is None
            and payload.responded is None
        ):
            # 投影侧 raw returned 抑制；带 rejected/responded 标记的是 supervisor 裁决直发，放行。
            return
        payload = self._with_owner_segment(payload)
        kind = _KIND_BY_PAYLOAD[type(payload)]
        index = self._next_index
        timestamp = _now_ms()
        semantic_key = (
            f"action_owner:{payload.tool_id}"
            if isinstance(payload, ToolAwaitingApprovalPayload)
            else f"plan.proposed:{payload.owner_ref}"
            if isinstance(payload, PlanProposedPayload)
            else "run.owner.completed"
            if isinstance(payload, RunOwnerCompletedPayload)
            else None
        )
        if self._outbox is not None:
            drafts = durable_output_drafts_for_event(payload)
            if drafts:
                source_seed = "event:" + hashlib.sha256(
                    (
                        f"v1\0{self._run_id}\0{kind}\0"
                        f"{semantic_key or f'index:{index}'}"
                    ).encode()
                ).hexdigest()
                try:
                    records = await self._outbox.append_durable_outputs(
                        self._run_id,
                        source_seed,
                        drafts,
                        recorded_at_ms=timestamp,
                    )
                except Exception as error:
                    reason = str(error)
                    detail = "APPEND_ERROR"
                    if reason in {
                        "OUTPUT_SOURCE_BATCH_CONFLICT",
                        "OUTPUT_SOURCE_CONFLICT",
                        "OUTPUT_SOURCE_PARTIAL",
                    }:
                        detail = reason
                    if semantic_key is not None and reason == "OUTPUT_SOURCE_CONFLICT":
                        detail = f"semantic critical frame conflict for {semantic_key!r}"
                    raise DurableOutputCommitError(
                        f"DURABLE_OUTPUT_COMMIT_FAILED: {detail}"
                    ) from error
                if records is None:
                    raise DurableOutputCommitError(
                        "DURABLE_OUTPUT_COMMIT_FAILED: DURABLE_OUTPUT_APPEND_REJECTED"
                    )
        base: dict[str, object] = {
            "kind": kind,
            "run_id": self._run_id,
            "index": index,
            "timestamp": timestamp,
            "payload": payload,
        }
        if self._outbox is not None and kind in CRITICAL_KINDS:
            # critical 帧：先分配 durable_seq/event_id + 落 queued 行，再发布（live），后置 published。
            payload_json = json.dumps(payload.model_dump(mode="json", exclude_none=True))
            staged = await self._outbox.stage_critical_frame(
                self._run_id,
                kind,
                index,
                timestamp,
                payload_json,
                terminal=kind in TERMINAL_KINDS,
                semantic_key=semantic_key,
            )
            if staged is None:
                # post-fence superseded：永不发布；index 不前进（保 live 序连续、浏览器面透明）。
                return
            if not staged.created:
                # semantic owner 已有固定 durable identity（即使 outbox 已收据 GC）；不重发、
                # 不推进 live index。不同 payload 已由 ledger fail-loud。
                return
            metrics.record_outbox("queued")
            event = agent_event_adapter.validate_python(
                {**base, "durable_seq": staged.durable_seq, "event_id": staged.event_id}
            )
            self._next_index += 1
            await self._bus.publish(
                run_events_stream(self._run_id),
                event.model_dump(exclude_none=True),
                maxlen=RUN_EVENTS_MAXLEN,
            )
            await self._outbox.mark_critical_published(self._run_id, staged.durable_seq)
            metrics.record_outbox("published")
            return
        event = agent_event_adapter.validate_python(base)
        self._next_index += 1
        # exclude_none：契约 optional 字段的 None 即"缺席"；null 上 wire 会被 session 的 zod .optional() 拒收。
        await self._bus.publish(
            run_events_stream(self._run_id),
            event.model_dump(exclude_none=True),
            maxlen=RUN_EVENTS_MAXLEN,
        )

    async def emit_completed(
        self,
        completion: CompletedExecutionContext,
        payload: RunCompletedPayload,
    ) -> bool:
        """Atomically own completion and stage its private owner/public terminal pair.

        Storage claims terminal ownership, persists the opaque checkpoint anchor, and creates
        both durable outbox slots in one write. Publishing is deliberately best-effort per slot:
        a loss on either slot leaves its queued row available to the normal replay scanner.
        """
        if self._outbox is None:
            raise RuntimeError("completion requires durable outbox")
        if completion.run_id != self._run_id:
            raise ValueError("completion run_id mismatch")
        owner_payload = RunOwnerCompletedPayload(
            execution_context_anchor=completion.anchor,
            execution_context_digest=completion.digest,
            owner_revision=completion.owner_revision,
        )
        timestamp = _now_ms()
        owner = CompletionEventDraft(
            kind="run.owner.completed",
            index=self._next_index,
            timestamp=timestamp,
            payload_json=json.dumps(owner_payload.model_dump(mode="json", exclude_none=True)),
        )
        terminal = CompletionEventDraft(
            kind="run.completed",
            index=self._next_index + 1,
            timestamp=timestamp,
            payload_json=json.dumps(payload.model_dump(mode="json", exclude_none=True)),
        )
        claimed = await self._outbox.try_complete_execution_context(
            completion, owner, terminal
        )
        if claimed is None:
            return False

        self._next_index += 2
        metrics.record_outbox("queued")
        metrics.record_outbox("queued")
        first_error: Exception | None = None
        for frame in (claimed.owner, claimed.terminal):
            try:
                await self._publish_completion_frame(frame)
            except Exception as error:  # noqa: BLE001 — leave this and later slots for replay
                first_error = error
                # Strict causal edge: Session must never observe terminal before the private
                # owner milestone. Both rows remain queued when owner publish/mark is uncertain;
                # the ordered scanner will replay owner before terminal.
                break
        if first_error is not None:
            raise first_error
        return True

    async def _publish_completion_frame(self, frame: DurableCompletionFrame) -> None:
        outbox = self._outbox
        if outbox is None:
            raise RuntimeError("completion requires durable outbox")
        event = agent_event_adapter.validate_python(
            {
                "kind": frame.kind,
                "run_id": self._run_id,
                "index": frame.index,
                "timestamp": frame.timestamp,
                "durable_seq": frame.durable_seq,
                "event_id": frame.event_id,
                "payload": json.loads(frame.payload_json),
            }
        )
        await self._bus.publish(
            run_events_stream(self._run_id),
            event.model_dump(exclude_none=True),
            maxlen=RUN_EVENTS_MAXLEN,
        )
        await outbox.mark_critical_published(self._run_id, frame.durable_seq)
        metrics.record_outbox("published")


def outbox_wire_event(frame: OutboxFrame) -> dict[str, JsonValue]:
    """queued outbox 行 → 补发用 wire 帧（复用原 index/timestamp/durable_seq/event_id，幂等不漂移）。"""
    event = agent_event_adapter.validate_python(
        {
            "kind": frame.kind,
            "run_id": frame.run_id,
            "index": frame.index,
            "timestamp": frame.timestamp,
            "durable_seq": frame.durable_seq,
            "event_id": frame.event_id,
            "payload": json.loads(frame.payload_json),
        }
    )
    return event.model_dump(exclude_none=True)


# --- 投影 → payload 映射（v3 typed projection 元素转 contract 载荷的唯一地点） ---

_TODOS_ADAPTER: TypeAdapter[list[Todo]] = TypeAdapter(list[Todo])


def message_delta_payload(text: str, *, segment_id: str) -> MessageDeltaPayload | None:
    return MessageDeltaPayload(segment_id=segment_id, delta=text) if text else None


def message_completed_payload(text: str, *, segment_id: str) -> MessageCompletedPayload | None:
    # 空文本不发（tool-only 段 output_message.text==""）。
    return MessageCompletedPayload(segment_id=segment_id, content=text) if text else None


def thinking_delta_payload(text: str, *, segment_id: str) -> ThinkingDeltaPayload | None:
    return ThinkingDeltaPayload(segment_id=segment_id, delta=text) if text else None


def subagent_thinking_delta_payload(
    text: str, *, segment_id: str, subagent_id: str
) -> SubagentThinkingDeltaPayload | None:
    if not text:
        return None
    return SubagentThinkingDeltaPayload(segment_id=segment_id, subagent_id=subagent_id, delta=text)


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


def tool_output_delta_payload(tc: ToolCallInfo, delta: str) -> ToolOutputDeltaPayload | None:
    if not delta:
        return None
    return ToolOutputDeltaPayload(
        segment_id=tc.tool_call_id, tool_id=tc.tool_call_id, name=tc.tool_name, delta=delta
    )


def output_delta_text(chunk: object) -> str:
    """工具输出增量的文本收窄：str 直用，带 .text 的取之，其余弃置（非文本流）。"""
    if isinstance(chunk, str):
        return chunk
    text = getattr(chunk, "text", None)
    return text if isinstance(text, str) else ""


# langgraph v3 把执行中途 interrupt 的工具浮现为 error=Interrupt 元组/列表的 repr；按其前缀识别。
_INTERRUPT_REPR_PREFIXES = ("Interrupt(", "(Interrupt(", "[Interrupt(")


def _interrupted(tc: ToolCallInfo) -> bool:
    # 工具执行中途 interrupt（request_input/request_human 等）时，langgraph v3 把该工具浮现为
    # error=Interrupt 元组的 repr（并非真错误）：这是暂停而非返回，其真正的 returned 由 resume
    # replay 产出。此处按 langgraph Interrupt repr 标记识别并抑制伪 returned（真错误文本无此标记）。
    error = tc.error
    return error is not None and error.startswith(_INTERRUPT_REPR_PREFIXES)


def tool_returned_payload(tc: ToolCallInfo) -> ToolReturnedPayload | None:
    # 经 v3 projection 浮现的工具=真实执行过（approve/edit/无门控）：rejected 缺省。
    if _interrupted(tc):
        return None
    result, truncated = _result_text(tc)
    return ToolReturnedPayload(
        segment_id=tc.tool_call_id,
        tool_id=tc.tool_call_id,
        name=tc.tool_name,
        result=result,
        is_error=tc.error is not None,
        truncated=True if truncated else None,
    )


def subagent_tool_invoked_payload(
    tc: ToolCallInfo, *, subagent_id: str
) -> SubagentToolInvokedPayload:
    return SubagentToolInvokedPayload(
        segment_id=tc.tool_call_id,
        subagent_id=subagent_id,
        tool_id=tc.tool_call_id,
        name=tc.tool_name,
        args=_json_args(tc.input),
    )


def subagent_tool_returned_payload(
    tc: ToolCallInfo, *, subagent_id: str
) -> SubagentToolReturnedPayload:
    result, truncated = _result_text(tc)
    return SubagentToolReturnedPayload(
        segment_id=tc.tool_call_id,
        subagent_id=subagent_id,
        tool_id=tc.tool_call_id,
        name=tc.tool_name,
        result=result,
        is_error=tc.error is not None,
        truncated=True if truncated else None,
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


def failure_code(error: BaseException) -> RunErrorCode:
    """执行期失败归码（闭集枚举）；装配期失败由调用点显式传 assembly_failed，不猜类型。"""
    if isinstance(error, TokenBudgetExceeded):
        return "token_budget_exceeded"
    if isinstance(error, GraphRecursionError):
        return "recursion_limit_exceeded"
    return "internal_error"


def run_failed_payload(
    error: BaseException, *, code: RunErrorCode | None = None
) -> RunFailedPayload:
    # 三层错误语义（契约注记）：code=web 本地化键（缺省自动归码）；error_kind=诊断类名；
    # message 契约 NonEmptyStr：空 str(error) 回退异常类名，绝不发空错误。
    return RunFailedPayload(
        code=code or failure_code(error),
        error_kind=type(error).__name__,
        message=str(error) or type(error).__name__,
    )


_ARGS_ADAPTER: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])


def _json_args(args: dict[str, object] | None) -> dict[str, JsonValue]:
    # 框架注入参数（ToolRuntime）是机器参数，不属于模型生成入参：剔除后再 strict 洗净。
    clean = {k: v for k, v in (args or {}).items() if not isinstance(v, ToolRuntime)}
    return _ARGS_ADAPTER.validate_python(clean)


# wire 是轻事件通道：完整产物走 backend 文件（canvas 预览面读取），事件只带摘要级文本。
# 截断是对不守规矩生产者的防御，模型侧 ToolMessage 不受影响。
TOOL_RESULT_MAX_CHARS = 4000


def clip_result(text: str) -> tuple[str, bool]:
    """(wire 文本, 是否截断)：契约 tool.returned.truncated 缺席=完整。"""
    if len(text) <= TOOL_RESULT_MAX_CHARS:
        return text, False
    omitted = len(text) - TOOL_RESULT_MAX_CHARS
    return f"{text[:TOOL_RESULT_MAX_CHARS]}…[truncated {omitted} chars]", True


def _raw_result_text(tc: ToolCallInfo) -> str:
    """工具终值的完整文本（未截断）：error 优先，其次原生 .text，再次 content blocks，兜底 str。"""
    if tc.error is not None:
        return tc.error
    output = tc.output
    if output is None:
        return ""
    text = getattr(output, "text", None)
    if isinstance(text, str):
        return text
    blocks = _render_content_blocks(output)
    return blocks if blocks is not None else str(output)


def _result_text(tc: ToolCallInfo) -> tuple[str, bool]:
    return clip_result(_raw_result_text(tc))


_DELIVER_RESULT_ADAPTER: TypeAdapter[DeliverResult] = TypeAdapter(DeliverResult)


def delivery_created_payload(tc: ToolCallInfo) -> DeliveryCreatedPayload | None:
    """deliver 成功归档后追发：tool.returned 之后紧跟 delivery.created（同 emitter，序号统一）。

    非 deliver 工具 / 工具异常 / 非 delivered JSON（含降级 error 文本）→ None（不追发）。
    解析未截断原文（大 note 也不丢事件）；note 空串省略（payload 局部可选）。
    """
    if tc.tool_name != DELIVER_TOOL_NAME or tc.error is not None:
        return None
    try:
        result = _DELIVER_RESULT_ADAPTER.validate_json(_raw_result_text(tc))
    except ValidationError:
        return None
    return DeliveryCreatedPayload(
        path=result.path,
        title=result.title,
        mime=result.mime,
        size=result.size,
        content_hash=result.content_hash,
        note=result.note or None,
    )


_BLOCKS_ADAPTER: TypeAdapter[list[dict[str, object]]] = TypeAdapter(list[dict[str, object]])


def _render_content_blocks(output: object) -> str | None:
    """MCP 等标准 content blocks（list[{type,...}]）→ 文本拼接；非该形状返回 None。"""
    if not isinstance(output, list) or not output:
        return None
    try:
        blocks = _BLOCKS_ADAPTER.validate_python(output)
    except ValidationError:
        return None
    texts: list[str] = []
    omitted = 0
    for block in blocks:
        if "type" not in block:
            return None
        text = block.get("text")
        if block.get("type") == "text" and isinstance(text, str):
            texts.append(text)
        else:
            omitted += 1
    if omitted:
        texts.append(f"[{omitted} non-text block(s) omitted]")
    return "\n".join(texts)
