from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import Protocol

from deepagents import create_deep_agent  # pyright: ignore[reportUnknownVariableType]  # deepagents create_deep_agent symbol is partially typed
from langchain_core.language_models import BaseChatModel

from kokoro_agent.infrastructure.message_extractors import as_mapping
from kokoro_agent.infrastructure.stream_translator import (
    TEXT_INTENT,
    TEXT_STREAM_INTENT,
    build_runtime_custom_subagent_tool,
    translate_stream_event,
)
from kokoro_agent.domain.agent_event import AgentEvent, is_agent_kind
from kokoro_agent.domain.run_request import RunRequest
from kokoro_agent.infrastructure.subagent_registry import (
    RuntimeSubagentRegistry,
    materialize_runtime_subagents,
)


class _StreamingAgent(Protocol):
    def astream_events(
        self, inp: dict[str, object], *, version: str
    ) -> AsyncIterator[Mapping[str, object]]: ...

ASTREAM_TIMEOUT_S = 120

# DeepAgents ships write_todos (CC-style planning), task (subagents), file ops
# and execute. We add no custom domain tools yet; the agent plans + answers.
_SYSTEM_PROMPT = (
    "你是 Kokoro，一个温和、克制的助手。遇到多步任务时，先用 write_todos 列出计划"
    "并随进展更新；需要时调用可用工具，必要时用 task 委派子智能体。回答简洁、清晰。"
)


def _build_agent(model: BaseChatModel) -> _StreamingAgent:
    # deepagents is an untyped boundary; keep the built-in subagent registry
    # explicit so richer task-path activity stays on the same resolved
    # provider/model rather than falling back to the SDK's default general-
    # purpose subagent path.
    runtime_registry = RuntimeSubagentRegistry()
    # create_deep_agent returns a CompiledStateGraph with irreducible Unknown
    # generics; pin the astream_events slice we use at this one boundary.
    agent: _StreamingAgent = create_deep_agent(  # pyright: ignore[reportUnknownVariableType, reportAssignmentType]
        model=model,
        tools=[build_runtime_custom_subagent_tool(model, runtime_registry)],
        system_prompt=_SYSTEM_PROMPT,
        subagents=materialize_runtime_subagents(model, runtime_registry=runtime_registry),
    )
    return agent


def _str_field(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    return value if isinstance(value, str) else ""


async def drive_agent_events(
    run_id: str, raw_events: AsyncIterator[Mapping[str, object]]
) -> AsyncIterator[AgentEvent]:
    """Wrap a raw astream_events iterator in the AgentEvent contract: run.started
    first, mapped activity events with a monotonic ``seq``, run.completed on
    success or a single run.failed on any error (never re-raised)."""
    seq = 0

    def nxt() -> int:
        nonlocal seq
        seq += 1
        return seq

    ref = 0
    active_message_ref: str | None = None
    segment_completed = False
    active_subagent: tuple[str, str] | None = None
    # Accumulated streamed text for the open parent / subagent segment. None means
    # no stream chunk has arrived yet -> on_chat_model_end takes the fallback path.
    streamed_text: str | None = None
    sub_streamed_text: str | None = None

    def new_ref() -> str:
        nonlocal ref
        ref += 1
        return f"msg_{ref:04d}"

    def ref_for_segment_body() -> str:
        nonlocal active_message_ref, segment_completed
        if active_message_ref is None or segment_completed:
            active_message_ref = new_ref()
            segment_completed = False
        return active_message_ref

    def ref_for_segment_activity() -> str:
        # 活动（工具/子智能体）与正文一样：上一段已落定后再来的活动，属于即将到来的
        # 下一段，开新 ref——而不是挂回旧段（否则「工具→文本→工具→文本」会塌成一段）。
        nonlocal active_message_ref, segment_completed
        if active_message_ref is None or segment_completed:
            active_message_ref = new_ref()
            segment_completed = False
        return active_message_ref

    def routed_subagent(ev: Mapping[str, object]) -> str | None:
        """The active sub-agent id when this model event belongs to it, else None."""
        if active_subagent is None:
            return None
        metadata = as_mapping(ev.get("metadata"))
        lc_agent_name_obj = metadata.get("lc_agent_name")
        lc_agent_name = lc_agent_name_obj if isinstance(lc_agent_name_obj, str) else ""
        return active_subagent[0] if lc_agent_name == active_subagent[1] else None

    yield AgentEvent(kind="run.started", run_id=run_id, seq=nxt(), payload={})
    try:
        async with asyncio.timeout(ASTREAM_TIMEOUT_S):
            async for ev in raw_events:
                for kind, payload in translate_stream_event(ev):
                    if kind == TEXT_STREAM_INTENT:
                        text = _str_field(payload, "text")
                        sub_id = routed_subagent(ev)
                        if sub_id is not None:
                            sub_streamed_text = (sub_streamed_text or "") + text
                            yield AgentEvent(
                                kind="subagent.text.delta",
                                run_id=run_id,
                                seq=nxt(),
                                payload={
                                    "message_ref": ref_for_segment_activity(),
                                    "subagent_id": sub_id,
                                    "text": text,
                                },
                            )
                            continue
                        streamed_text = (streamed_text or "") + text
                        yield AgentEvent(
                            kind="text.delta",
                            run_id=run_id,
                            seq=nxt(),
                            payload={"message_ref": ref_for_segment_body(), "text": text},
                        )
                    elif kind == TEXT_INTENT:
                        sub_id = routed_subagent(ev)
                        if sub_id is not None:
                            message_ref = ref_for_segment_activity()
                            if sub_streamed_text is not None:
                                yield AgentEvent(
                                    kind="subagent.text.completed",
                                    run_id=run_id,
                                    seq=nxt(),
                                    payload={
                                        "message_ref": message_ref,
                                        "subagent_id": sub_id,
                                        "text": sub_streamed_text,
                                    },
                                )
                                sub_streamed_text = None
                                continue
                            subagent_body = {
                                "message_ref": message_ref,
                                "subagent_id": sub_id,
                                "text": payload["text"],
                            }
                            yield AgentEvent(
                                kind="subagent.text.delta",
                                run_id=run_id,
                                seq=nxt(),
                                payload=subagent_body,
                            )
                            yield AgentEvent(
                                kind="subagent.text.completed",
                                run_id=run_id,
                                seq=nxt(),
                                payload=subagent_body,
                            )
                            continue
                        message_ref = ref_for_segment_body()
                        if streamed_text is not None:
                            yield AgentEvent(
                                kind="text.completed",
                                run_id=run_id,
                                seq=nxt(),
                                payload={"message_ref": message_ref, "text": streamed_text},
                            )
                            streamed_text = None
                            segment_completed = True
                            continue
                        body = {"message_ref": message_ref, "text": payload["text"]}
                        yield AgentEvent(kind="text.delta", run_id=run_id, seq=nxt(), payload=body)
                        yield AgentEvent(kind="text.completed", run_id=run_id, seq=nxt(), payload=body)
                        segment_completed = True
                    elif kind == "thinking.delta":
                        yield AgentEvent(
                            kind="thinking.delta",
                            run_id=run_id,
                            seq=nxt(),
                            payload={"message_ref": ref_for_segment_body(), "text": payload["text"]},
                        )
                    elif kind in {
                        "tool.invoked",
                        "tool.returned",
                        "subagent.started",
                        "subagent.finished",
                    }:
                        event_payload = {"message_ref": ref_for_segment_activity(), **payload}
                        if kind == "subagent.started":
                            active_subagent = (
                                _str_field(payload, "subagent_id"),
                                _str_field(payload, "name"),
                            )
                        elif kind == "subagent.finished":
                            active_subagent = None
                        if is_agent_kind(kind):
                            yield AgentEvent(
                                kind=kind,
                                run_id=run_id,
                                seq=nxt(),
                                payload=event_payload,
                            )
                    elif is_agent_kind(kind):
                        yield AgentEvent(
                            kind=kind,
                            run_id=run_id,
                            seq=nxt(),
                            payload=payload,
                        )
        yield AgentEvent(
            kind="run.completed", run_id=run_id, seq=nxt(), payload={"status": "completed"}
        )
    except Exception as error:  # noqa: BLE001 — boundary: any failure -> run.failed
        yield AgentEvent(
            kind="run.failed",
            run_id=run_id,
            seq=nxt(),
            payload={"error_kind": type(error).__name__, "message": str(error)},
        )


async def run_agent(
    req: RunRequest, model: BaseChatModel
) -> AsyncIterator[AgentEvent]:
    """Run the real DeepAgents loop for one request and stream mapped activity
    events (thinking / text / tool.* / todo.updated / subagent.*), wrapped in
    run.started…run.completed (or run.failed)."""
    agent = _build_agent(model)
    raw = agent.astream_events(
        {"messages": [{"role": "user", "content": req.input}]},
        version="v2",
    )
    async for event in drive_agent_events(req.run_id, raw):
        yield event
