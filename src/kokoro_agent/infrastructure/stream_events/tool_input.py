"""读取工具入参：把未类型化 input 解析为 ToolInput（scalar args / todos / 命名串）。"""

from __future__ import annotations

from langchain_core.runnables.schema import StreamEvent
from pydantic import TypeAdapter, ValidationError
from typing_extensions import TypedDict

from kokoro_agent.domain.run_event import TodoItem, ToolScalar
from kokoro_agent.infrastructure.stream_events._guards import is_object_mapping, is_tool_scalar
from kokoro_agent.infrastructure.stream_events.parsed_event import ToolInput


class RawToolInput(TypedDict, total=False):
    todos: list[dict[str, str]]
    subagent_type: str
    description: str
    name: str


_RAW_TOOL_INPUT = TypeAdapter(RawToolInput)


def _scalar_args_from(value: object) -> dict[str, ToolScalar]:
    scalar_args: dict[str, ToolScalar] = {}
    if not is_object_mapping(value):
        return scalar_args
    for key, item in value.items():
        if isinstance(key, str) and is_tool_scalar(item):
            scalar_args[key] = item
    return scalar_args


def _todos_from(typed_input: RawToolInput) -> tuple[TodoItem, ...]:
    todos: list[TodoItem] = []
    match typed_input.get("todos"):
        case list() as raw_todos:
            for todo in raw_todos:
                match todo:
                    case {"content": str() as content, "status": "pending" | "in_progress" | "completed" as status}:
                        todos.append(TodoItem(content, status))
                    case _:
                        continue
        case _:
            pass
    return tuple(todos)


def _empty_tool_input() -> ToolInput:
    return ToolInput(args={}, todos=(), subagent_type="", description="", name="")


def read_tool_input(event: StreamEvent) -> ToolInput:
    match event:
        case {"data": {"input": raw_input}}:
            raw_input_obj: object = raw_input
            try:
                typed_input = _RAW_TOOL_INPUT.validate_python(raw_input_obj)
            except ValidationError:
                return _empty_tool_input()
        case _:
            return _empty_tool_input()

    return ToolInput(
        args=_scalar_args_from(raw_input_obj),
        todos=_todos_from(typed_input),
        subagent_type=typed_input.get("subagent_type", ""),
        description=typed_input.get("description", ""),
        name=typed_input.get("name", ""),
    )
