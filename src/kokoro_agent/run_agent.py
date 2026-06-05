from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import cast

from deepagents import create_deep_agent  # pyright: ignore  # untyped third-party
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage

from kokoro_agent.events import AgentEvent, AgentKind, RunRequest

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


# Intent kinds that run_agent expands rather than emitting verbatim.
_TEXT_INTENT = "text"


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
            out.append(
                (
                    "subagent.started",
                    {
                        "subagent_id": tool_id,
                        "name": str(args.get("subagent_type") or "subagent"),
                        "description": str(args.get("description") or ""),
                    },
                )
            )
        else:
            out.append(("tool.invoked", {"tool_id": tool_id, "name": name, "args": dict(args)}))
    elif event == "on_tool_end":
        if name == _TODO_TOOL:
            return out  # the list was already emitted on tool start
        if name == _SUBAGENT_TOOL:
            out.append(("subagent.finished", {"subagent_id": tool_id, "name": name}))
        else:
            out.append(
                (
                    "tool.returned",
                    {"tool_id": tool_id, "name": name, "result": _result_text(data.get("output"))},
                )
            )
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


def _build_agent(model: BaseChatModel) -> object:
    # deepagents is an untyped boundary; treat the builder as returning an opaque
    # graph and reach it only via the cast astream_events iterator in run_agent.
    return create_deep_agent(  # pyright: ignore[reportUnknownMemberType]
        model=model, tools=[], system_prompt=_SYSTEM_PROMPT
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

    def new_ref() -> str:
        nonlocal ref
        ref += 1
        return f"msg_{ref:04d}"

    yield AgentEvent(kind="run.started", run_id=run_id, seq=nxt(), payload={})
    try:
        async with asyncio.timeout(ASTREAM_TIMEOUT_S):
            async for ev in raw_events:
                for kind, payload in translate_stream_event(ev):
                    if kind == _TEXT_INTENT:
                        message_ref = new_ref()
                        body = {"message_ref": message_ref, "text": payload["text"]}
                        yield AgentEvent(kind="text.delta", run_id=run_id, seq=nxt(), payload=body)
                        yield AgentEvent(kind="text.completed", run_id=run_id, seq=nxt(), payload=body)
                    elif kind == "thinking.delta":
                        yield AgentEvent(
                            kind="thinking.delta",
                            run_id=run_id,
                            seq=nxt(),
                            payload={"message_ref": new_ref(), "text": payload["text"]},
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
