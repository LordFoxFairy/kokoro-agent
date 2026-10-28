from __future__ import annotations

import json
from typing import Any

import pytest
from langchain_core.messages import AIMessage

from kokoro_agent.application.run_display import recover_display
from kokoro_agent.domain.chat.projection import safe_progress_id
from kokoro_agent.domain.run.models import ToolJournalRecord

SCOPE = ("tenant", "namespace", "session", "run")


def read_details(
    descriptions: list[tuple[str, str, str]],
    *,
    description_status: str = "succeeded",
    missing_event: bool = False,
    missing_journal: bool = False,
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    records: dict[str, ToolJournalRecord] = {}
    events: list[dict[str, Any]] = []
    for index, (connection, tool, raw) in enumerate(descriptions):
        call_id = f"describe-{index}"
        calls.append(
            {
                "id": call_id,
                "name": "mcp_describe_tool",
                "args": {"server": connection, "tool": tool},
            }
        )
        records[call_id] = ToolJournalRecord(
            name="mcp_describe_tool",
            status=description_status,
            is_error=description_status == "failed",
            result=raw,
        )
        if not missing_event:
            events.append(
                {
                    "activity": "tool",
                    "activity_id": "act_"
                    + safe_progress_id("tool-activity", *SCOPE, call_id, call_id),
                    "display_code": "tool.mcp.describe",
                    "status": "completed",
                }
            )
    calls.append(
        {
            "id": "execute",
            "name": "mcp_call",
            "args": {
                "server": "one",
                "tool": "query",
                "arguments": {"query": "actual request"},
            },
        }
    )
    records["execute"] = ToolJournalRecord(
        name="mcp_call",
        status="succeeded",
        is_error=False,
        result=json.dumps({"content": [{"type": "text", "text": "actual answer"}]}),
    )
    events.append(
        {
            "activity": "tool",
            "activity_id": "act_"
            + safe_progress_id("tool-activity", *SCOPE, "execute", "execute"),
            "display_code": "tool.mcp.call",
            "status": "completed",
        }
    )
    if missing_journal:
        records = {"execute": records["execute"]}
    result = recover_display(
        scope=SCOPE,
        selected=(),
        events=events,
        metadata=[],
        messages=[AIMessage(id="native", content="", tool_calls=calls)],
        journal={},
        journal_records=records,
        mcp_connections=("one", "two"),
    )
    return result.model_dump(mode="json")["activity_details"]


def definition(
    text: str, connection: str = "one", tool: str = "query"
) -> tuple[str, str, str]:
    return (
        connection,
        tool,
        f'{connection}/{tool}\n{text}\n参数 schema：{{"type":"object"}}',
    )


def test_same_run_observed_tool_description_is_separate_from_identity_and_result() -> (
    None
):
    details = read_details([definition("Search the repository documentation.")])
    assert len(details) == 2
    for detail in details:
        assert detail["tool_description"] == {
            "text": "Search the repository documentation.",
            "truncated": False,
            "redacted": False,
        }
        assert detail["target"] == {"kind": "mcp_tool", "name": "query"}
    assert details[-1]["tool_name"] == "mcp_call"
    assert details[-1]["output_preview"]["text"] == "actual answer"


def test_identically_named_tools_on_different_connections_never_share_description() -> (
    None
):
    details = read_details(
        [definition("Other connection capability", connection="two")]
    )
    assert "tool_description" not in details[-1]


def test_conflicting_observed_descriptions_are_not_arbitrarily_selected() -> None:
    details = read_details(
        [definition("First capability"), definition("Changed capability")]
    )
    assert all("tool_description" not in detail for detail in details)


@pytest.mark.parametrize("status", ["started", "failed", "unknown"])
def test_unfinished_or_failed_description_is_not_capability_evidence(
    status: str,
) -> None:
    assert all(
        "tool_description" not in detail
        for detail in read_details(
            [definition("Not proven")], description_status=status
        )
    )


@pytest.mark.parametrize(
    "options", [{"missing_event": True}, {"missing_journal": True}]
)
def test_native_history_without_this_runs_event_and_journal_cannot_supply_description(
    options: dict[str, bool],
) -> None:
    assert all(
        "tool_description" not in detail
        for detail in read_details(
            [definition("Prior run")],
            missing_event=options.get("missing_event", False),
            missing_journal=options.get("missing_journal", False),
        )
    )


@pytest.mark.parametrize(
    "raw",
    [
        "one/query\nDoc\n参数 schema：[]",
        'one/query\nDoc\n参数 schema：{"type":"object","type":"string"}',
        "other/query\nDoc\n参数 schema：{}",
        "one/query\nDoc without producer boundary",
    ],
)
def test_invalid_producer_envelope_does_not_supply_description(raw: str) -> None:
    assert "tool_description" not in read_details([("one", "query", raw)])[-1]


@pytest.mark.parametrize(
    "text", ["", "Bearer SECRET", "file /Users/private/key", "password=SECRET"]
)
def test_empty_or_sensitive_tool_description_is_omitted(text: str) -> None:
    assert "tool_description" not in read_details([definition(text)])[-1]


def test_description_utf8_budget_is_truthfully_marked() -> None:
    value = read_details([definition("字" * 1000)])[-1]["tool_description"]
    assert len(value["text"].encode()) <= 1024
    assert value["truncated"] is True
    assert value["redacted"] is False


def test_conflicts_beyond_clipped_prefix_still_omit_description() -> None:
    prefix = "a" * 1200
    assert (
        "tool_description"
        not in read_details([definition(prefix + "one"), definition(prefix + "two")])[
            -1
        ]
    )


@pytest.mark.parametrize(
    "extra",
    [
        {"tool_description": None},
        {
            "tool_description": {
                "text": "字" * 342,
                "truncated": False,
                "redacted": False,
            }
        },
        {
            "tool_description": {"text": "Doc", "truncated": False, "redacted": False},
            "target": {"kind": "file", "name": "test.md"},
        },
        {
            "tool_description": {"text": "Doc", "truncated": False, "redacted": False},
            "tool_name": "read_file",
        },
    ],
)
def test_description_dto_rejects_null_over_budget_and_wrong_identity(
    extra: dict[str, Any],
) -> None:
    from kokoro_agent.domain.chat.tool_display_preview import ActivityDetail

    base = {
        "activity_id": "act_" + "a" * 64,
        "tool_name": "mcp_call",
        "target": {"kind": "mcp_tool", "name": "query"},
    }
    with pytest.raises(ValueError):
        ActivityDetail.model_validate({**base, **extra})
