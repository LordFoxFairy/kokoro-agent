from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.schema import StreamEvent

from kokoro_agent.infrastructure.stream_events import message_parts, read_tool_input, result_messages


def test_message_parts_plain_text_message() -> None:
    parts = message_parts(AIMessage(content="答案"))
    assert parts.text == "答案"
    assert parts.reasoning == ""


def test_message_parts_extracts_text_and_thinking_blocks() -> None:
    parts = message_parts(
        AIMessage(
            content=[
                {"type": "thinking", "thinking": "先想"},
                {"type": "text", "text": "结论"},
            ]
        )
    )
    assert parts.text == "结论"
    assert parts.reasoning == "先想"


def test_message_parts_prefers_reasoning_content_additional_kwargs() -> None:
    parts = message_parts(
        AIMessage(
            content=[{"type": "reasoning", "text": "旧推理"}, {"type": "text", "text": "结论"}],
            additional_kwargs={"reasoning_content": "新推理"},
        )
    )
    assert parts.text == "结论"
    assert parts.reasoning == "新推理"


def test_result_messages_keeps_only_langchain_messages() -> None:
    result: dict[str, object] = {
        "messages": [
            HumanMessage(content="用户"),
            AIMessage(content="A"),
            {"rogue": "yes"},
            AIMessage(content="B"),
        ]
    }
    assert [message_parts(message).text for message in result_messages(result)] == ["用户", "A", "B"]


def test_result_messages_ignores_non_list_messages_field() -> None:
    result: dict[str, object] = {"messages": "not-a-list"}
    assert result_messages(result) == []


def test_read_tool_input_ignores_non_string_argument_keys() -> None:
    event: StreamEvent = {
        "event": "on_tool_start",
        "name": "task",
        "run_id": "run-1",
        "parent_ids": [],
        "tags": [],
        "metadata": {},
        "data": {
            "input": {
                "description": "delegate",
                "name": "worker",
                "subagent_type": "general-purpose",
                "todos": [{"content": "do it", "status": "pending"}],
                7: "ignored",
                "count": 3,
            }
        },
    }

    tool_input = read_tool_input(event)

    assert tool_input.args == {
        "description": "delegate",
        "name": "worker",
        "subagent_type": "general-purpose",
        "count": 3,
    }
    assert tuple((todo.content, todo.status) for todo in tool_input.todos) == (("do it", "pending"),)
