from __future__ import annotations

import json
from typing import Any

import pytest
from langchain_core.messages import AIMessage

from kokoro_agent.application.run_display import recover_display
from kokoro_agent.domain.chat.projection import safe_progress_id
from kokoro_agent.domain.run.models import ToolJournalRecord

SCOPE = ("tenant", "namespace", "session", "run")


def recover(
    arguments: dict[str, Any],
    result: str,
    *,
    status: str = "succeeded",
    name: str = "mcp_call",
    connection: str = "connection",
) -> dict[str, Any]:
    code = {
        "mcp_call": "tool.mcp.call",
        "mcp_describe_tool": "tool.mcp.describe",
        "mcp_list_tools": "tool.mcp.discover",
    }[name]
    return recover_display(
        scope=SCOPE,
        selected=(),
        events=[
            {
                "activity": "tool",
                "activity_id": "act_"
                + safe_progress_id("tool-activity", *SCOPE, "call", "call"),
                "display_code": code,
                "status": "completed",
            }
        ],
        metadata=[],
        messages=[
            AIMessage(
                id="native",
                content="",
                tool_calls=[{"id": "call", "name": name, "args": arguments}],
            )
        ],
        journal={},
        journal_records={
            "call": ToolJournalRecord(
                name=name, status=status, is_error=status == "failed", result=result
            )
        },
        mcp_connections=(connection,),
    ).model_dump(mode="json")


def test_same_call_private_inputs_and_terminal_mcp_text_are_recovered() -> None:
    value = recover(
        {
            "server": "connection",
            "tool": "query",
            "arguments": {"repository": "owner/repo"},
        },
        json.dumps(
            {"content": [{"type": "text", "text": "Actual answer"}], "isError": False}
        ),
    )
    details = value.get("activity_details")
    assert details, "private activity details are missing"
    assert details[0]["target"] == {"kind": "mcp_tool", "name": "query"}
    assert json.loads(details[0]["input_preview"]["text"]) == {
        "repository": "owner/repo"
    }
    assert details[0]["output_preview"]["text"] == "Actual answer"


def test_started_call_has_input_without_invented_output() -> None:
    details = recover(
        {"server": "connection", "tool": "query", "arguments": {"q": "hello"}},
        "",
        status="started",
    ).get("activity_details")
    assert details
    assert "input_preview" in details[0] and "output_preview" not in details[0]


def test_foreign_connection_never_supplies_details() -> None:
    value = recover(
        {"server": "foreign", "tool": "query", "arguments": {"q": "hello"}}, "{}"
    )
    assert value.get("activity_details") == []


def test_structural_secret_is_removed_and_suspicious_output_block_omitted() -> None:
    details = recover(
        {
            "server": "connection",
            "tool": "query",
            "arguments": {
                "q": "hello",
                "Authorization": "Bearer SECRET",
                "nested": {"password": "SECRET", "ok": 1},
            },
        },
        json.dumps(
            {"content": [{"type": "text", "text": "Bearer SECRET"}], "isError": False}
        ),
    ).get("activity_details")
    assert details
    assert "SECRET" not in json.dumps(details)
    assert details[0]["input_preview"]["redacted"] is True
    assert "output_preview" not in details[0]


@pytest.mark.parametrize("name", ["mcp_list_tools", "mcp_describe_tool"])
def test_preparation_tools_have_exact_private_return_text(name: str) -> None:
    args = {} if name == "mcp_list_tools" else {"server": "connection", "tool": "query"}
    result = (
        "connection/query — Actual tool documentation"
        if name == "mcp_list_tools"
        else 'connection/query\nActual tool documentation\n参数 schema：{"type":"object"}'
    )
    details = recover(args, result, name=name).get("activity_details")
    assert details and details[0]["tool_name"] == name
    assert "Actual tool documentation" in details[0]["output_preview"]["text"]


def test_preview_bounds_and_redaction_are_not_a_claim_of_complete_output() -> None:
    details = recover(
        {"server": "connection", "tool": "query", "arguments": {"q": "字" * 3000}},
        json.dumps(
            {"content": [{"type": "text", "text": "字" * 3000}], "isError": False}
        ),
    ).get("activity_details")
    assert details
    for field, limit in [("input_preview", 2048), ("output_preview", 4096)]:
        assert len(details[0][field]["text"].encode()) <= limit
        assert details[0][field]["truncated"] is True


@pytest.mark.parametrize(
    "text",
    [
        "Traceback (most recent call last):\nsecret",
        "/Users/local/config.json",
        "RuntimeError: private internals",
        "https://name:password@example.test/data",
        "DATABASE_URL=postgresql://localhost/db",
    ],
)
def test_suspect_free_text_block_is_omitted(text: str) -> None:
    details = recover(
        {"server": "connection", "tool": "query", "arguments": {}},
        json.dumps({"content": [{"type": "text", "text": text}], "isError": False}),
    ).get("activity_details")
    assert details and "output_preview" not in details[0]


def test_duplicate_and_deep_mcp_json_are_omitted_before_payload_expansion() -> None:
    for raw in [
        '{"content":[],"content":[],"isError":false}',
        '{"content":[],"structuredContent":' + '{"a":' * 30 + "0" + "}" * 30 + "}",
    ]:
        details = recover(
            {"server": "connection", "tool": "query", "arguments": {}}, raw
        ).get("activity_details")
        assert details and "output_preview" not in details[0]


def test_conflicting_same_call_arguments_never_choose_a_last_writer() -> None:
    event = {
        "activity": "tool",
        "activity_id": "act_"
        + safe_progress_id("tool-activity", *SCOPE, "call", "call"),
        "display_code": "tool.mcp.call",
        "status": "completed",
    }
    messages = [
        AIMessage(
            id=str(i),
            content="",
            tool_calls=[
                {
                    "id": "call",
                    "name": "mcp_call",
                    "args": {
                        "server": "connection",
                        "tool": "query",
                        "arguments": {"q": str(i)},
                    },
                }
            ],
        )
        for i in range(2)
    ]
    value = recover_display(
        scope=SCOPE,
        selected=(),
        events=[event],
        metadata=[],
        messages=messages,
        journal={},
        journal_records={
            "call": ToolJournalRecord(
                name="mcp_call", status="started", is_error=False, result=""
            )
        },
        mcp_connections=("connection",),
    ).model_dump(mode="json")
    assert value["activity_details"] == []


def test_unknown_journal_status_never_supplies_input_or_result() -> None:
    value = recover(
        {"server": "connection", "tool": "query", "arguments": {}},
        "{}",
        status="unknown",
    )
    assert value["activity_details"] == []


def test_private_dto_rejects_null_duplicate_and_input_byte_overflow() -> None:
    from kokoro_agent.application.run_display import RunDisplay
    from kokoro_agent.domain.chat.tool_display_preview import ActivityDetail

    base = {"activity_id": "act_" + "a" * 64, "tool_name": "mcp_call"}
    for extra in [
        {"target": None},
        {"input_preview": {"text": "字" * 1000, "truncated": False, "redacted": False}},
        {
            "output_preview": {
                "text": "ok",
                "truncated": False,
                "redacted": False,
                "raw": "no",
            }
        },
    ]:
        with pytest.raises(ValueError):
            ActivityDetail.model_validate({**base, **extra})
    with pytest.raises(ValueError):
        RunDisplay(
            run_id="run",
            display_skills=[],
            tool_paths=[],
            activity_details=[ActivityDetail.model_validate(base)] * 2,
        )


def test_file_category_alone_does_not_invent_a_tool_name() -> None:
    result = recover_display(
        scope=SCOPE,
        selected=(),
        events=[
            {
                "activity": "tool",
                "activity_id": "act_" + "a" * 64,
                "display_code": "tool.file.write",
                "display_detail": {
                    "kind": "file",
                    "name": "a.txt",
                    "virtual_path": "/a.txt",
                },
            }
        ],
        metadata=[],
        messages=[],
        journal={},
    )
    assert result.activity_details == []


def test_serialized_escape_expansion_is_bounded_and_marked() -> None:
    events: list[dict[str, Any]] = []
    messages: list[AIMessage] = []
    records: dict[str, ToolJournalRecord] = {}
    for index in range(128):
        call_id = str(index)
        events.append(
            {
                "activity": "tool",
                "activity_id": "act_"
                + safe_progress_id("tool-activity", *SCOPE, call_id, call_id),
                "display_code": "tool.mcp.call",
                "status": "completed",
            }
        )
        messages.append(
            AIMessage(
                id=call_id,
                content="",
                tool_calls=[
                    {
                        "id": call_id,
                        "name": "mcp_call",
                        "args": {
                            "server": "connection",
                            "tool": "query",
                            "arguments": {"q": '"' * 6000},
                        },
                    }
                ],
            )
        )
        records[call_id] = ToolJournalRecord(
            name="mcp_call",
            status="succeeded",
            is_error=False,
            result=json.dumps(
                {"content": [{"type": "text", "text": '"' * 6000}], "isError": False}
            ),
        )
    result = recover_display(
        scope=SCOPE,
        selected=(),
        events=events,
        metadata=[],
        messages=messages,
        journal={},
        journal_records=records,
        mcp_connections=("connection",),
    )
    encoded = json.dumps(
        {"data": result.model_dump(mode="json"), "meta": {"request_id": "request"}},
        ensure_ascii=False,
    ).encode()
    assert len(result.activity_details) == 128
    assert len(encoded) <= 1024 * 1024
    assert any(
        detail.output_preview is not None
        and len(detail.output_preview.text.encode()) == 128
        and detail.output_preview.truncated
        for detail in result.activity_details
    )


def test_mcp_resource_only_omitted_and_partial_text_redaction_flagged() -> None:
    args: dict[str, Any] = {"server": "connection", "tool": "query", "arguments": {}}
    details = recover(
        args,
        json.dumps(
            {
                "content": [
                    {
                        "type": "resource_link",
                        "name": "a",
                        "uri": "https://example.test",
                    }
                ],
                "isError": False,
            }
        ),
    )["activity_details"]
    assert "output_preview" not in details[0]
    details = recover(
        args,
        json.dumps(
            {
                "content": [
                    {"type": "text", "text": "Actual visible text"},
                    {"type": "text", "text": "Bearer SECRET"},
                ],
                "isError": False,
            }
        ),
    )["activity_details"]
    assert details[0]["output_preview"]["redacted"] is True
    assert "Actual visible text" in details[0]["output_preview"]["text"]


def test_preparation_preview_strips_binding_identity_and_parses_only_producer_shape() -> (
    None
):
    args = {"server": "connection", "tool": "query"}
    details = recover(
        args,
        'connection/query\nActual documentation\n参数 schema：{"type":"object","properties":{"q":{"type":"string"},"token":{"type":"string"}}}',
        name="mcp_describe_tool",
    )["activity_details"]
    assert "connection" not in details[0]["output_preview"]["text"]
    assert "Actual documentation" in details[0]["output_preview"]["text"]
    assert "token" not in details[0]["output_preview"]["text"]
    details = recover({}, "connection/query — Useful tool", name="mcp_list_tools")[
        "activity_details"
    ]
    assert "connection" not in details[0]["output_preview"]["text"]
    assert "Useful tool" in details[0]["output_preview"]["text"]
    details = recover(args, "unstructured unexpected return", name="mcp_describe_tool")[
        "activity_details"
    ]
    assert "output_preview" not in details[0]


def test_omitted_mcp_media_and_signed_url_text_are_not_unmarked_complete_previews() -> (
    None
):
    args: dict[str, Any] = {"server": "connection", "tool": "query", "arguments": {}}
    result = json.dumps(
        {
            "content": [
                {"type": "text", "text": "Actual text"},
                {"type": "image", "mimeType": "image/png", "data": "AA=="},
            ],
            "isError": False,
        }
    )
    details = recover(args, result)["activity_details"]
    assert details[0]["output_preview"]["redacted"] is True
    result = json.dumps(
        {
            "content": [
                {"type": "text", "text": "https://example.test/file?token=opaque123"}
            ],
            "isError": False,
        }
    )
    assert "output_preview" not in recover(args, result)["activity_details"][0]


def test_exact_mirrored_mcp_text_is_not_truncated_by_a_duplicate_representation() -> (
    None
):
    text = "原始结果\n" + "检查项：会话恢复\n" * 90
    assert len(text.encode()) < 4096
    result = {
        "content": [{"type": "text", "text": text}],
        "structuredContent": {"result": text},
        "isError": False,
    }
    details = recover(
        {"server": "connection", "tool": "query", "arguments": {}},
        json.dumps(result, ensure_ascii=False),
    )["activity_details"]
    assert details[0]["output_preview"] == {
        "text": text,
        "truncated": False,
        "redacted": False,
    }


@pytest.mark.parametrize(
    "structured", [{"result": "same", "count": 1}, {"result": "different"}]
)
def test_distinct_structured_mcp_result_is_not_discarded(
    structured: dict[str, object],
) -> None:
    raw = json.dumps(
        {"content": [{"type": "text", "text": "same"}], "structuredContent": structured}
    )
    details = recover({"server": "connection", "tool": "query", "arguments": {}}, raw)[
        "activity_details"
    ]
    assert json.loads(details[0]["output_preview"]["text"]) == {
        "text": ["same"],
        "structured_content": structured,
    }


def test_mirrored_mcp_text_keeps_real_bound_and_does_not_expose_private_text() -> None:
    for text in ["字" * 3000, "Bearer SECRET"]:
        raw = json.dumps(
            {
                "content": [{"type": "text", "text": text}],
                "structuredContent": {"result": text},
            }
        )
        item = recover({"server": "connection", "tool": "query", "arguments": {}}, raw)[
            "activity_details"
        ][0]
        if text == "Bearer SECRET":
            assert "output_preview" not in item
        else:
            output = item["output_preview"]
            assert output["text"] == text.encode()[:4096].decode(errors="ignore")
            assert output["truncated"] is True


def test_mirrored_mcp_text_still_marks_omitted_nontext_content() -> None:
    raw = json.dumps(
        {
            "content": [
                {"type": "text", "text": "same"},
                {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"},
            ],
            "structuredContent": {"result": "same"},
        }
    )
    item = recover({"server": "connection", "tool": "query", "arguments": {}}, raw)[
        "activity_details"
    ][0]
    assert item["output_preview"] == {
        "text": "same",
        "truncated": False,
        "redacted": True,
    }


@pytest.mark.parametrize(
    "text",
    ['{"text":["inside"]}', "42", "null", '["inside"]', '"quoted"', '{"unfinished', ""],
)
def test_json_like_mcp_text_keeps_its_outer_representation(text: str) -> None:
    raw = json.dumps(
        {
            "content": [{"type": "text", "text": text}],
            "structuredContent": {"result": text},
        }
    )
    item = recover({"server": "connection", "tool": "query", "arguments": {}}, raw)[
        "activity_details"
    ][0]
    assert json.loads(item["output_preview"]["text"]) == {
        "text": [text],
        "structured_content": {"result": text},
    }


def test_multiple_mcp_text_blocks_are_not_joined_or_deduplicated() -> None:
    raw = json.dumps(
        {
            "content": [
                {"type": "text", "text": "same"},
                {"type": "text", "text": "same"},
            ],
            "structuredContent": {"result": "same"},
        }
    )
    item = recover({"server": "connection", "tool": "query", "arguments": {}}, raw)[
        "activity_details"
    ][0]
    assert json.loads(item["output_preview"]["text"]) == {
        "text": ["same", "same"],
        "structured_content": {"result": "same"},
    }
