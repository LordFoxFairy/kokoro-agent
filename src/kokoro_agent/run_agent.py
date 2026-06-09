from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import Any, cast

from deepagents import create_deep_agent  # pyright: ignore  # untyped third-party
from langchain.agents import create_agent  # pyright: ignore[reportUnknownVariableType]
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import StructuredTool
from langchain_core.messages import AIMessage, AIMessageChunk
from pydantic import BaseModel, Field

from kokoro_agent.events import AgentEvent, AgentKind, RunRequest
from kokoro_agent.subagents import (
    RuntimeSubagentRegistry,
    materialize_runtime_subagents,
    subagent_source_for,
)

ASTREAM_TIMEOUT_S = 120

# DeepAgents ships write_todos (CC-style planning), task (subagents), file ops
# and execute. We add no custom domain tools yet; the agent plans + answers.
_SYSTEM_PROMPT = (
    "你是 Kokoro，一个温和、克制的助手。遇到多步任务时，先用 write_todos 列出计划"
    "并随进展更新；需要时调用可用工具，必要时用 task 委派子智能体。回答简洁、清晰。"
)

# write_todos / task are mapped to dedicated event families; every other tool is
# a generic tool.invoked/returned pair.
_TODO_TOOL = "write_todos"
_SUBAGENT_TOOL = "task"
_RUNTIME_SUBAGENT_TOOL = "agent"


def _text_of(content: object) -> str:
    """Extract plain text from a message ``content``.

    Strings pass through. For list content (multi-modal / content blocks) only
    ``{"type": "text"}`` blocks are surfaced; thinking/tool/other blocks are
    deliberately dropped so they never leak into ``text``.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        blocks = cast("list[object]", content)
        parts: list[str] = []
        for block in blocks:
            if not isinstance(block, Mapping):
                continue
            typed_block = cast("Mapping[object, object]", block)
            if typed_block.get("type") != "text":
                continue
            text = typed_block.get("text", "")
            if isinstance(text, str):
                parts.append(text)
        return "".join(parts)
    return ""


def _reasoning_of(message: AIMessage) -> str:
    """Reasoning/thinking text, when the model exposes it (reasoning models).

    Looks at ``additional_kwargs.reasoning_content`` and any ``thinking`` /
    ``reasoning`` content blocks. Returns "" for models that don't surface
    reasoning (e.g. plain chat models) — thinking then simply doesn't appear.
    """
    extra = cast("Mapping[str, object]", message.additional_kwargs or {})  # pyright: ignore  # langchain Any field
    reasoning = extra.get("reasoning_content")
    if isinstance(reasoning, str) and reasoning:
        return reasoning
    content = cast("object", message.content)  # pyright: ignore  # langchain Any field
    if isinstance(content, list):
        parts: list[str] = []
        for block in cast("list[object]", content):
            if not isinstance(block, Mapping):
                continue
            typed = cast("Mapping[object, object]", block)
            kind = typed.get("type")
            if kind not in ("thinking", "reasoning"):
                continue
            value = typed.get(kind) or typed.get("text")
            if isinstance(value, str):
                parts.append(value)
        return "".join(parts)
    return ""


def _result_text(output: object) -> str:
    """Best-effort textual result of a tool call (ToolMessage/Command/str)."""
    content = getattr(output, "content", None)
    if isinstance(content, str):
        return content
    if content is not None:
        return str(content)
    return "" if output is None else str(output)


def _as_ai_message(output: object) -> AIMessage | None:
    return output if isinstance(output, AIMessage) else None


def _is_tool_call_only_chunk(chunk: AIMessageChunk) -> bool:
    """A chunk that carries only tool-call argument fragments, no answer text."""
    return bool(chunk.tool_call_chunks) and not _text_of(cast("object", chunk.content))  # pyright: ignore  # langchain Any field


class RuntimeSubagentToolInput(BaseModel):
    name: str = Field(min_length=1, description="Runtime custom subagent name")
    description: str = Field(min_length=1, description="Short role or responsibility summary")
    system_prompt: str = Field(min_length=1, description="System prompt for the runtime custom subagent")
    task: str = Field(min_length=1, description="The concrete task the runtime custom subagent should perform")


# Intent kinds that run_agent expands rather than emitting verbatim.
_TEXT_INTENT = "text"
# An incremental token slice from on_chat_model_stream; run_agent turns each into
# a real text.delta and remembers it streamed so the matching _TEXT_INTENT (from
# on_chat_model_end) closes the segment with one text.completed, no extra delta.
_TEXT_STREAM_INTENT = "text.stream"


def translate_stream_event(
    ev: Mapping[str, object],
) -> list[tuple[str, dict[str, object]]]:
    """Pure map of one ``astream_events(version="v2")`` event to (kind, payload)
    intents. run_agent assigns run_id/seq/message_ref and expands the ``text``
    intent into text.delta + text.completed.

    Emits only on tool starts/ends and final model messages; internal graph
    nodes (LangGraph/model/tools/*Middleware) and intermediate tool-call turns
    (empty content) produce nothing.
    """
    event = ev.get("event")
    name_obj = ev.get("name")
    name = name_obj if isinstance(name_obj, str) else ""
    data_obj = ev.get("data")
    data: Mapping[str, object] = (
        cast("Mapping[str, object]", data_obj) if isinstance(data_obj, Mapping) else {}
    )
    run_id_obj = ev.get("run_id")
    tool_id = run_id_obj if isinstance(run_id_obj, str) else ""
    out: list[tuple[str, dict[str, object]]] = []

    if event == "on_tool_start":
        input_obj = data.get("input")
        args: Mapping[str, object] = (
            cast("Mapping[str, object]", input_obj)
            if isinstance(input_obj, Mapping)
            else {}
        )
        if name == _TODO_TOOL:
            todos = args.get("todos")
            out.append(("todo.updated", {"todos": todos if isinstance(todos, list) else []}))
        elif name == _SUBAGENT_TOOL:
            subagent_type = str(args.get("subagent_type") or "subagent")
            out.append(
                (
                    "subagent.started",
                    {
                        "subagent_id": tool_id,
                        "name": subagent_type,
                        "description": str(args.get("description") or ""),
                        "subagent_type": subagent_type,
                        "source": subagent_source_for(subagent_type),
                    },
                )
            )
        elif name == _RUNTIME_SUBAGENT_TOOL:
            runtime_name = str(args.get("name") or "runtime-subagent")
            out.append(
                (
                    "subagent.started",
                    {
                        "subagent_id": tool_id,
                        "name": runtime_name,
                        "description": str(args.get("description") or ""),
                        "subagent_type": runtime_name,
                        "source": "runtime-custom",
                    },
                )
            )
        else:
            out.append(("tool.invoked", {"tool_id": tool_id, "name": name, "args": dict(args)}))
    elif event == "on_tool_end":
        if name == _TODO_TOOL:
            return out  # the list was already emitted on tool start
        if name == _SUBAGENT_TOOL:
            input_obj = data.get("input")
            args: Mapping[str, object] = (
                cast("Mapping[str, object]", input_obj)
                if isinstance(input_obj, Mapping)
                else {}
            )
            subagent_type = str(args.get("subagent_type") or "subagent")
            out.append(
                (
                    "subagent.finished",
                    {
                        "subagent_id": tool_id,
                        "name": subagent_type,
                        "subagent_type": subagent_type,
                        "source": subagent_source_for(subagent_type),
                    },
                )
            )
        elif name == _RUNTIME_SUBAGENT_TOOL:
            input_obj = data.get("input")
            args: Mapping[str, object] = (
                cast("Mapping[str, object]", input_obj)
                if isinstance(input_obj, Mapping)
                else {}
            )
            runtime_name = str(args.get("name") or "runtime-subagent")
            out.append(
                (
                    "subagent.finished",
                    {
                        "subagent_id": tool_id,
                        "name": runtime_name,
                        "subagent_type": runtime_name,
                        "source": "runtime-custom",
                    },
                )
            )
        else:
            out.append(
                (
                    "tool.returned",
                    {"tool_id": tool_id, "name": name, "result": _result_text(data.get("output"))},
                )
            )
    elif event == "on_chat_model_stream":
        chunk = data.get("chunk")
        if isinstance(chunk, AIMessageChunk) and not _is_tool_call_only_chunk(chunk):
            reasoning = _reasoning_of(chunk)
            if reasoning:
                out.append(("thinking.delta", {"text": reasoning}))
            text = _text_of(cast("object", chunk.content))  # pyright: ignore  # langchain Any field
            if text:
                out.append((_TEXT_STREAM_INTENT, {"text": text}))
    elif event == "on_chat_model_end":
        message = _as_ai_message(data.get("output"))
        if message is not None:
            reasoning = _reasoning_of(message)
            if reasoning:
                out.append(("thinking.delta", {"text": reasoning}))
            text = _text_of(cast("object", message.content))  # pyright: ignore  # langchain Any field
            # Intermediate turns carry tool_calls (and usually empty text); only
            # a final answer (no tool_calls) becomes a user-visible message.
            if text and not message.tool_calls:
                out.append((_TEXT_INTENT, {"text": text}))
    return out


def _build_runtime_custom_subagent_tool(
    model: BaseChatModel,
    runtime_registry: RuntimeSubagentRegistry,
) -> StructuredTool:
    create_agent_fn = cast("Any", create_agent)
    tool_cls = cast("Any", StructuredTool)

    async def agent_runtime(
        name: str,
        description: str,
        system_prompt: str,
        task: str,
    ) -> str:
        spec = runtime_registry.get(name)
        if spec is None:
            spec = runtime_registry.register(name, description, system_prompt)

        runner = create_agent_fn(
            model,
            system_prompt=spec.system_prompt,
            tools=[],
            name=spec.name,
        )
        result_obj: object = await runner.ainvoke(
            {"messages": [{"role": "user", "content": task}]}
        )
        if isinstance(result_obj, Mapping):
            result_map = cast("Mapping[str, object]", result_obj)
            messages_obj = result_map.get("messages")
        else:
            messages_obj = None
        if isinstance(messages_obj, list):
            message_items = cast("list[object]", messages_obj)
            for message_obj in reversed(message_items):
                if isinstance(message_obj, AIMessage):
                    text_attr = cast("str | None", message_obj.text)
                    if text_attr:
                        text = text_attr.rstrip()
                    else:
                        raw_content = getattr(message_obj, "content", "")
                        text = _text_of(raw_content)
                    if text:
                        return text
        return ""

    def agent_runtime_sync(
        name: str,
        description: str,
        system_prompt: str,
        task: str,
    ) -> str:
        msg = "runtime custom subagent tool requires async execution"
        raise RuntimeError(msg)

    return tool_cls.from_function(
        name=_RUNTIME_SUBAGENT_TOOL,
        func=agent_runtime_sync,
        coroutine=agent_runtime,
        infer_schema=False,
        args_schema=RuntimeSubagentToolInput,
        description=(
            "Create and run a runtime custom subagent. Use this when you need an ad-hoc"
            " specialized helper that is not part of the built-in or config-defined"
            " subagent set."
        ),
    )


def _build_agent(model: BaseChatModel) -> object:
    # deepagents is an untyped boundary; keep the built-in subagent registry
    # explicit so richer task-path activity stays on the same resolved
    # provider/model rather than falling back to the SDK's default general-
    # purpose subagent path.
    runtime_registry = RuntimeSubagentRegistry()
    return create_deep_agent(  # pyright: ignore[reportUnknownMemberType]
        model=model,
        tools=[_build_runtime_custom_subagent_tool(model, runtime_registry)],
        system_prompt=_SYSTEM_PROMPT,
        subagents=materialize_runtime_subagents(model, runtime_registry=runtime_registry),
    )


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
        metadata_obj = ev.get("metadata")
        metadata: Mapping[str, object] = (
            cast("Mapping[str, object]", metadata_obj)
            if isinstance(metadata_obj, Mapping)
            else {}
        )
        lc_agent_name_obj = metadata.get("lc_agent_name")
        lc_agent_name = lc_agent_name_obj if isinstance(lc_agent_name_obj, str) else ""
        return active_subagent[0] if lc_agent_name == active_subagent[1] else None

    yield AgentEvent(kind="run.started", run_id=run_id, seq=nxt(), payload={})
    try:
        async with asyncio.timeout(ASTREAM_TIMEOUT_S):
            async for ev in raw_events:
                for kind, payload in translate_stream_event(ev):
                    if kind == _TEXT_STREAM_INTENT:
                        text = cast("str", payload["text"])
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
                    elif kind == _TEXT_INTENT:
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
                                cast("str", payload["subagent_id"]),
                                cast("str", payload["name"]),
                            )
                        elif kind == "subagent.finished":
                            active_subagent = None
                        yield AgentEvent(
                            kind=cast("AgentKind", kind),
                            run_id=run_id,
                            seq=nxt(),
                            payload=event_payload,
                        )
                    else:
                        yield AgentEvent(
                            kind=cast("AgentKind", kind),
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
    raw = cast(
        "AsyncIterator[Mapping[str, object]]",
        agent.astream_events(  # type: ignore[attr-defined]
            {"messages": [{"role": "user", "content": req.input}]},
            version="v2",
        ),
    )
    async for event in drive_agent_events(req.run_id, raw):
        yield event
