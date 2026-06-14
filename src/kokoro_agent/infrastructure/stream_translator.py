from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from langchain.agents import create_agent  # pyright: ignore[reportUnknownVariableType]  # langchain create_agent symbol is partially typed
from kokoro_agent.infrastructure.message_extractors import (
    as_mapping,
    as_ai_message,
    is_object_list,
    is_tool_call_only_chunk,
    message_content,
    reasoning_of,
    result_text,
    text_of,
)
from kokoro_agent.infrastructure.control import rejection_result
from kokoro_agent.infrastructure.subagent_registry import (
    RuntimeSubagentRegistry,
    subagent_source_for,
)

# These tools map to dedicated event families; every other tool is generic.
TODO_TOOL = "write_todos"
SUBAGENT_TOOL = "task"
RUNTIME_SUBAGENT_TOOL = "agent"

# 事件载荷里工具结果的上限：防单条 redis 事件膨胀；模型在 graph 内仍拿全量结果。
TOOL_RESULT_MAX_CHARS = 8_000

# Intent kinds that run_agent expands rather than emitting verbatim.
TEXT_INTENT = "text"
# A streamed token slice; run_agent emits it as text.delta and remembers it
# streamed, so the matching TEXT_INTENT closes with text.completed and no delta.
TEXT_STREAM_INTENT = "text.stream"


class _AgentRunner(Protocol):
    async def ainvoke(self, inp: dict[str, object]) -> object: ...


class RuntimeSubagentToolInput(BaseModel):
    name: str = Field(min_length=1, description="Runtime custom subagent name")
    description: str = Field(min_length=1, description="Short role or responsibility summary")
    system_prompt: str = Field(min_length=1, description="System prompt for the runtime custom subagent")
    task: str = Field(min_length=1, description="The concrete task the runtime custom subagent should perform")


def _truncated(result: str) -> str:
    if len(result) <= TOOL_RESULT_MAX_CHARS:
        return result
    return f"{result[:TOOL_RESULT_MAX_CHARS]}…（结果过长，事件流中已在 {TOOL_RESULT_MAX_CHARS} 字符处截断）"


def _subagent_finished(
    name: str, tool_id: str, data: Mapping[str, object]
) -> dict[str, object] | None:
    """子智能体分派工具（task/agent）的 subagent.finished payload；普通工具返回 None。
    on_tool_end 与 on_tool_error 共用，保证子智能体无论成功/失败都收尾、不被当通用工具。"""
    args = as_mapping(data.get("input"))
    if name == SUBAGENT_TOOL:
        subagent_type = str(args.get("subagent_type") or "subagent")
        return {
            "subagent_id": tool_id,
            "name": subagent_type,
            "subagent_type": subagent_type,
            "source": subagent_source_for(subagent_type),
        }
    if name == RUNTIME_SUBAGENT_TOOL:
        runtime_name = str(args.get("name") or "runtime-subagent")
        return {
            "subagent_id": tool_id,
            "name": runtime_name,
            "subagent_type": runtime_name,
            "source": "runtime-custom",
        }
    return None


def translate_stream_event(
    ev: Mapping[str, object],
) -> list[tuple[str, dict[str, object]]]:
    """Pure map of one ``astream_events(version="v2")`` event to (kind, payload)
    intents. run_agent assigns run_id/seq/segment_id and expands the ``text``
    intent into text.delta + text.completed.

    Emits on tool starts/ends and every model message that carries text —
    including narration on intermediate tool-call turns; internal graph nodes
    (LangGraph/model/tools/*Middleware) and empty tool-call turns produce nothing.
    """
    event = ev.get("event")
    name_obj = ev.get("name")
    name = name_obj if isinstance(name_obj, str) else ""
    data = as_mapping(ev.get("data"))
    run_id_obj = ev.get("run_id")
    tool_id = run_id_obj if isinstance(run_id_obj, str) else ""
    out: list[tuple[str, dict[str, object]]] = []

    if event == "on_tool_start":
        args = as_mapping(data.get("input"))
        if name == TODO_TOOL:
            todos = args.get("todos")
            out.append(("todo.updated", {"todos": todos if isinstance(todos, list) else []}))
        elif name == SUBAGENT_TOOL:
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
        elif name == RUNTIME_SUBAGENT_TOOL:
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
        if name == TODO_TOOL:
            return out  # the list was already emitted on tool start
        finished = _subagent_finished(name, tool_id, data)
        if finished is not None:
            out.append(("subagent.finished", finished))
        else:
            result = _truncated(result_text(data.get("output")))
            payload: dict[str, object] = {
                "tool_id": tool_id,
                "name": name,
                "result": result,
                "is_error": False,
            }
            # 门控工具被拒绝走 on_tool_end(返回拒绝文案,不抛异常)：标记 rejected 让 UI 显
            # 「已拒绝」而非绿勾 done。文案单一来源(control.rejection_result),非脆弱散字符串。
            if result == rejection_result(name):
                payload["rejected"] = True
            out.append(("tool.returned", payload))
    elif event == "on_tool_error":
        # 工具抛异常发 on_tool_error（非 on_tool_end）。按名分派与 on_tool_end 对称：子智能体工具失败仍发
        # subagent.finished（让卡 running 的子智能体步收尾，不冒伪红工具行），todo 静默，其余发
        # tool.returned(is_error=True) 让 UI 显失败（红色）。运行随后通常仍以 run.failed 收尾。
        if name == TODO_TOOL:
            return out
        finished = _subagent_finished(name, tool_id, data)
        if finished is not None:
            out.append(("subagent.finished", finished))
        else:
            error = data.get("error")
            # 无消息异常（CancelledError / 无参异常）str 化为空串：回落到类型名，绝不渲染空白红条。
            error_text = str(error) or type(error).__name__
            out.append(
                (
                    "tool.returned",
                    {
                        "tool_id": tool_id,
                        "name": name,
                        "result": _truncated(error_text),
                        "is_error": True,
                    },
                )
            )
    elif event == "on_chat_model_stream":
        chunk = data.get("chunk")
        if isinstance(chunk, AIMessageChunk) and not is_tool_call_only_chunk(chunk):
            reasoning = reasoning_of(chunk)
            if reasoning:
                out.append(("thinking.delta", {"text": reasoning}))
            text = text_of(message_content(chunk))
            if text:
                out.append((TEXT_STREAM_INTENT, {"text": text}))
    elif event == "on_chat_model_end":
        message = as_ai_message(data.get("output"))
        if message is not None:
            reasoning = reasoning_of(message)
            if reasoning:
                out.append(("thinking.delta", {"text": reasoning}))
            text = text_of(message_content(message))
            # 带 tool_calls 的中间轮叙述也要浮出——真实模型常把实质内容写在这里，丢弃即丢答案。
            if text:
                out.append((TEXT_INTENT, {"text": text}))
    return out


def _make_runner(model: BaseChatModel, system_prompt: str, name: str) -> _AgentRunner:
    # create_agent's CompiledStateGraph generics are irreducibly Unknown under
    # strict; pin to the ainvoke slice we use at this boundary.
    runner: _AgentRunner = create_agent(  # pyright: ignore[reportUnknownVariableType, reportAssignmentType]
        model, system_prompt=system_prompt, tools=[], name=name
    )
    return runner


def build_runtime_custom_subagent_tool(
    model: BaseChatModel,
    runtime_registry: RuntimeSubagentRegistry,
) -> StructuredTool:
    async def agent_runtime(
        name: str,
        description: str,
        system_prompt: str,
        task: str,
    ) -> str:
        spec = runtime_registry.get(name)
        if spec is None:
            spec = runtime_registry.register(name, description, system_prompt)

        runner = _make_runner(model, spec.system_prompt, spec.name)
        result_obj = await runner.ainvoke(
            {"messages": [{"role": "user", "content": task}]}
        )
        messages_obj = as_mapping(result_obj).get("messages")
        if is_object_list(messages_obj):
            for message_obj in reversed(messages_obj):
                if isinstance(message_obj, AIMessage):
                    text_value = str(message_obj.text)
                    if text_value:
                        text = text_value.rstrip()
                    else:
                        text = text_of(message_content(message_obj))
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

    return StructuredTool.from_function(  # pyright: ignore[reportUnknownMemberType]  # langchain from_function classmethod is partially typed
        name=RUNTIME_SUBAGENT_TOOL,
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
