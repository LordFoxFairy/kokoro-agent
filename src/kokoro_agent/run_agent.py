from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import cast

from langchain_core.language_models import LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.runnables import Runnable

from kokoro_agent.events import AgentEvent, RunRequest
from kokoro_agent.tools import run_tool

ASTREAM_TIMEOUT_S = 120
TOOL_LOOP_LIMIT = 8

# A brain is anything invocable with chat input -> a message: a BaseChatModel,
# a tool-bound RunnableBinding, or a scripted fake. Only ``ainvoke`` is used.
BrainModel = Runnable[LanguageModelInput, BaseMessage]


def _walk_blocks(content: object, want: str) -> str:
    """Concatenate the text payload of ``{"type": want}`` blocks in ``content``.

    Strings pass through only when ``want == "text"``; list content is scanned
    block-by-block. Non-matching blocks (thinking when extracting text, and vice
    versa) are dropped so reasoning never leaks into ``text.delta`` and final
    text never leaks into ``thinking.delta``.
    """
    if isinstance(content, str):
        return content if want == "text" else ""
    if isinstance(content, list):
        blocks = cast("list[object]", content)
        parts: list[str] = []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            typed_block = cast("dict[object, object]", block)
            if typed_block.get("type") != want:
                continue
            value = typed_block.get(want, "")
            if isinstance(value, str):
                parts.append(value)
        return "".join(parts)
    return ""


def _text_of(content: object) -> str:
    """Extract plain text blocks from a message's ``content``."""
    return _walk_blocks(content, "text")


def _thinking_of(content: object) -> str:
    """Extract reasoning text from ``{"type": "thinking"}`` content blocks."""
    return _walk_blocks(content, "thinking")


def _tool_calls_of(ai: BaseMessage) -> list[dict[str, object]]:
    """Return the message's tool calls as plain dicts (name/args/id)."""
    if not isinstance(ai, AIMessage):
        return []
    return cast("list[dict[str, object]]", ai.tool_calls)


def _content_of(ai: BaseMessage) -> object:
    """Return a message's content erased to ``object`` for block walking.

    LangChain types content as a partially-unknown union; this boundary cast
    keeps the downstream block walkers strict-clean without leaking ``Unknown``.
    """
    return cast("object", ai.content)  # pyright: ignore[reportUnknownMemberType]


async def run_agent(  # noqa: C901 — single cohesive brain loop
    req: RunRequest, model: BrainModel
) -> AsyncIterator[AgentEvent]:
    """Stream a brain's tool-calling loop as raw agent events.

    Emits ``run.started`` then iterates: on each turn it calls ``model.ainvoke``,
    surfaces ``thinking.delta`` (only when ``execution_style == "thinking"``),
    and either runs requested tools (``tool.invoked`` -> ``tool.returned``, feeding
    a ``ToolMessage`` back) or finalizes with ``text.delta`` -> ``text.completed``.
    Closes with ``run.completed``. Any failure -> a single ``run.failed`` (never
    re-raised); exceeding ``TOOL_LOOP_LIMIT`` -> ``run.failed{ToolLoopLimit}``.
    The agent fills only execution semantics — cursors/ids/owner belong to
    kokoro-session.
    """
    seq = 1
    message_ref = "m1"
    thinking_mode = req.execution_style == "thinking"
    messages: list[BaseMessage] = [HumanMessage(content=req.input)]
    yield AgentEvent(kind="run.started", run_id=req.run_id, seq=seq, payload={})
    try:
        async with asyncio.timeout(ASTREAM_TIMEOUT_S):
            for _ in range(TOOL_LOOP_LIMIT):
                ai = await model.ainvoke(messages)
                content = _content_of(ai)

                if thinking_mode:
                    thinking = _thinking_of(content)
                    if thinking:
                        seq += 1
                        yield AgentEvent(
                            kind="thinking.delta",
                            run_id=req.run_id,
                            seq=seq,
                            payload={"text": thinking},
                        )

                tool_calls = _tool_calls_of(ai)
                if tool_calls:
                    messages.append(ai)
                    for tc in tool_calls:
                        name = str(tc["name"])
                        ref = str(tc["id"])
                        args = cast("dict[str, object]", tc.get("args", {}))
                        seq += 1
                        yield AgentEvent(
                            kind="tool.invoked",
                            run_id=req.run_id,
                            seq=seq,
                            payload={"tool_call_ref": ref, "tool_name": name},
                        )
                        status, output = run_tool(name, args)
                        seq += 1
                        yield AgentEvent(
                            kind="tool.returned",
                            run_id=req.run_id,
                            seq=seq,
                            payload={
                                "tool_call_ref": ref,
                                "tool_name": name,
                                "status": status,
                            },
                        )
                        messages.append(
                            ToolMessage(content=output, tool_call_id=ref)
                        )
                    continue

                full = _text_of(content)
                if full:
                    seq += 1
                    yield AgentEvent(
                        kind="text.delta",
                        run_id=req.run_id,
                        seq=seq,
                        payload={"message_ref": message_ref, "text": full},
                    )
                seq += 1
                yield AgentEvent(
                    kind="text.completed",
                    run_id=req.run_id,
                    seq=seq,
                    payload={"message_ref": message_ref, "text": full},
                )
                break
            else:
                seq += 1
                yield AgentEvent(
                    kind="run.failed",
                    run_id=req.run_id,
                    seq=seq,
                    payload={
                        "error_kind": "ToolLoopLimit",
                        "message": f"tool loop exceeded {TOOL_LOOP_LIMIT} turns",
                    },
                )
                return
        seq += 1
        yield AgentEvent(
            kind="run.completed",
            run_id=req.run_id,
            seq=seq,
            payload={"status": "completed"},
        )
    except Exception as error:  # noqa: BLE001 — boundary: any brain failure -> run.failed
        seq += 1
        yield AgentEvent(
            kind="run.failed",
            run_id=req.run_id,
            seq=seq,
            payload={"error_kind": type(error).__name__, "message": str(error)},
        )
