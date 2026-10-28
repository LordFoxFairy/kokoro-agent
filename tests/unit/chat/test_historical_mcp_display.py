from __future__ import annotations

import json
from typing import Any

import pytest
from langchain_core.messages import AIMessage
from kokoro_agent.application.run_display import RunDisplay, recover_display
from kokoro_agent.domain.chat.projection import safe_progress_id
from kokoro_agent.domain.run.models import ToolJournalRecord

SCOPE = ("tenant", "namespace", "session", "run")


def activity(call: str, code: str) -> dict[str, Any]:
    return {
        "activity": "tool",
        "activity_id": "act_" + safe_progress_id("tool-activity", *SCOPE, call, call),
        "display_code": code,
        "status": "completed",
    }


def recover(
    *,
    records: dict[str, ToolJournalRecord],
    messages: list[AIMessage],
    events: list[dict[str, Any]],
    connections: tuple[str, ...] = ("connection",),
) -> RunDisplay:
    return recover_display(
        scope=SCOPE,
        selected=(),
        events=events,
        metadata=[],
        messages=messages,
        journal={},
        journal_records=records,
        mcp_connections=connections,
    )


def call(
    call_id: str,
    name: str = "mcp_call",
    server: str = "connection",
    tool: str = "read_wiki_structure",
) -> AIMessage:
    return AIMessage(
        id="native",
        content="",
        tool_calls=[
            {"id": call_id, "name": name, "args": {"server": server, "tool": tool}}
        ],
    )


def record(
    name: str = "mcp_call",
    status: str = "succeeded",
    is_error: bool = False,
    result: str = '{"content":[],"isError":false}',
) -> ToolJournalRecord:
    return ToolJournalRecord(name=name, status=status, is_error=is_error, result=result)


def test_successful_exact_call_witness_names_original_call_and_matching_description_without_rewriting_source() -> (
    None
):
    events = [
        activity("success", "tool.mcp.call"),
        activity("describe", "tool.mcp.describe"),
        activity("failed", "tool.mcp.call"),
    ]
    original = json.dumps(events, sort_keys=True)
    result = recover(
        records={
            "success": record(),
            "describe": record(
                "mcp_describe_tool", result="error: MCP_TOOL_NOT_ALLOWED"
            ),
            "failed": record(
                status="failed", is_error=True, result="private upstream error"
            ),
        },
        messages=[call("success"), call("describe", "mcp_describe_tool")],
        events=events,
    )
    assert result.model_dump()["tool_facts"] == [
        {
            "activity_id": events[0]["activity_id"],
            "display_detail": {"kind": "mcp_tool", "name": "read_wiki_structure"},
        },
        {
            "activity_id": events[1]["activity_id"],
            "display_detail": {"kind": "mcp_tool", "name": "read_wiki_structure"},
        },
        {"activity_id": events[2]["activity_id"], "status": "failed"},
    ]
    assert json.dumps(events, sort_keys=True) == original


@pytest.mark.parametrize(
    "change",
    [
        "foreign_connection",
        "wrong_call",
        "wrong_code",
        "wrong_name",
        "started",
        "failed",
        "is_error",
        "error_string",
        "remote_error",
        "unsafe_name",
    ],
)
def test_unproven_name_never_becomes_public_mcp_target(change: str) -> None:
    event = activity("call", "tool.mcp.call")
    msg = call("call")
    row = record()
    if change == "foreign_connection":
        msg = call("call", server="other")
    elif change == "wrong_call":
        msg = call("other")
    elif change == "wrong_code":
        event["display_code"] = "tool.web.search"
    elif change == "wrong_name":
        row = record("another_tool")
    elif change == "started":
        row = record(status="started")
    elif change == "failed":
        row = record(status="failed", is_error=True)
    elif change == "is_error":
        row = record(is_error=True)
    elif change == "error_string":
        row = record(result="error: MCP_TOOL_CALL_FAILED")
    elif change == "remote_error":
        row = record(result='{"content":[],"isError":true}')
    elif change == "unsafe_name":
        msg = call("call", tool="bad\nname")
    facts = recover(records={"call": row}, messages=[msg], events=[event]).model_dump()[
        "tool_facts"
    ]
    assert not any("display_detail" in fact for fact in facts)
    assert all(fact.get("status") == "failed" for fact in facts)


def test_facts_are_strict_unique_meaningful_and_never_discover_a_fake_server() -> None:
    from kokoro_agent.application.run_display import ToolFact

    base = {"activity_id": "act_" + "a" * 64}
    for value in [
        base,
        {**base, "status": "completed"},
        {**base, "status": None},
        {**base, "display_detail": {"kind": "file", "name": "a"}},
        {
            **base,
            "display_detail": {
                "kind": "mcp_tool",
                "name": "tool",
                "virtual_path": "/a",
            },
        },
        {**base, "activity_id": base["activity_id"] + "\n", "status": "failed"},
    ]:
        with pytest.raises(ValueError):
            ToolFact.model_validate(value)
    fact = {**base, "status": "failed"}
    with pytest.raises(ValueError):
        RunDisplay.model_validate(
            {
                "run_id": "run",
                "display_skills": [],
                "tool_paths": [],
                "tool_facts": [fact, fact],
            }
        )
    result = recover(
        records={"discover": record("mcp_list_tools")},
        messages=[
            AIMessage(
                id="n",
                content="",
                tool_calls=[{"id": "discover", "name": "mcp_list_tools", "args": {}}],
            )
        ],
        events=[activity("discover", "tool.mcp.discover")],
    )
    assert result.tool_facts == []


@pytest.mark.parametrize(
    "variation",
    [
        "exact",
        "other_version",
        "substring",
        "wrong_message",
        "wrong_name",
        "no_typed_message",
    ],
)
def test_only_reviewed_legacy_describe_implementation_and_exact_typed_result_restore_error(
    variation: str,
) -> None:
    from langchain_core.messages import ToolMessage
    from kokoro_agent.application.run_display import recover_display

    text = "error: MCP_TOOL_NOT_ALLOWED"
    source = "f4f1b0c9ff95221cbf619cdf6376e79d8858c07f32b7c33c72f989458417ddea"
    if variation == "other_version":
        source = "a" * 64
    if variation == "substring":
        text += " as documented"
    message = ToolMessage(
        content=text,
        tool_call_id="other" if variation == "wrong_message" else "describe",
        name="another" if variation == "wrong_name" else "mcp_describe_tool",
    )
    result = recover_display(
        scope=SCOPE,
        selected=(),
        events=[activity("describe", "tool.mcp.describe")],
        metadata=[],
        journal={},
        journal_records={"describe": record("mcp_describe_tool", result=text)},
        mcp_connections=("connection",),
        mcp_source_sha256=source,
        messages=[
            call("describe", "mcp_describe_tool"),
            *([message] if variation != "no_typed_message" else []),
        ],
    )
    assert [fact.model_dump() for fact in result.tool_facts] == (
        [
            {
                "activity_id": activity("describe", "tool.mcp.describe")["activity_id"],
                "status": "failed",
            }
        ]
        if variation == "exact"
        else []
    )
