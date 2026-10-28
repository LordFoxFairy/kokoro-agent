"""Native file activity details preserve exact calls without a tool-effect journal."""

from __future__ import annotations

import json
from typing import Any

import pytest
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage

from kokoro_agent.application.run_display import recover_display
from kokoro_agent.domain.chat.projection import safe_progress_id

SCOPE = ("tenant", "namespace", "session", "run")


def details(
    name: str = "ls",
    args: dict[str, Any] | None = None,
    output: str = "['/README.md']",
    *,
    extra: list[BaseMessage] | None = None,
    allowed: tuple[str, ...] = (
        "ls",
        "read_file",
        "write_file",
        "edit_file",
        "glob",
        "grep",
    ),
    status: str = "completed",
    tool_status: str = "success",
) -> list[Any]:
    code = (
        "tool.file.read"
        if name == "read_file"
        else "tool.file.write"
        if name == "write_file"
        else "tool.execution"
    )
    messages: list[BaseMessage] = [
        AIMessage(
            id="model",
            content="",
            tool_calls=[
                {
                    "id": "call",
                    "name": name,
                    "args": args
                    if args is not None
                    else {"path": "/", "_kokoro_action": "查看工作区"},
                }
            ],
        ),
        ToolMessage(content=output, name=name, tool_call_id="call", status=tool_status),
    ]
    return recover_display(
        scope=SCOPE,
        selected=(),
        events=[
            {
                "activity": "tool",
                "activity_id": "act_"
                + safe_progress_id("tool-activity", *SCOPE, "call", "call"),
                "display_code": code,
                "status": status,
            }
        ],
        metadata=[],
        messages=messages + (extra or []),
        journal={},
        native_tools=allowed,
    ).activity_details


def test_directory_call_has_real_input_and_output_without_fabricating_file_target() -> (
    None
):
    rows = details()
    assert len(rows) == 1
    assert rows[0].tool_name == "ls" and rows[0].target is None
    assert json.loads(rows[0].input_preview.text) == {"path": "/"}
    assert rows[0].output_preview.text == "['/README.md']"


@pytest.mark.parametrize(
    ("name", "args"),
    [
        ("read_file", {"file_path": "/README.md", "offset": 1, "limit": 20}),
        ("write_file", {"file_path": "/README.md", "content": "New text"}),
        (
            "edit_file",
            {"file_path": "/README.md", "old_string": "old", "new_string": "new"},
        ),
        ("glob", {"pattern": "**/*.md", "path": "/"}),
        ("grep", {"pattern": "example", "path": "/", "output_mode": "content"}),
    ],
)
def test_each_native_file_tool_keeps_its_own_recorded_arguments(
    name: str, args: dict[str, Any]
) -> None:
    rows = details(name, args, "Actual result")
    assert len(rows) == 1 and rows[0].tool_name == name
    assert json.loads(rows[0].input_preview.text) == args
    assert rows[0].output_preview.text == "Actual result"


def test_missing_frozen_authority_does_not_enable_native_details() -> None:
    assert details(allowed=()) == []


@pytest.mark.parametrize(
    "extra",
    [
        AIMessage(
            id="model",
            content="",
            tool_calls=[{"id": "call", "name": "ls", "args": {"path": "/changed"}}],
        ),
        AIMessage(
            id="different-model",
            content="",
            tool_calls=[
                {
                    "id": "call",
                    "name": "ls",
                    "args": {"path": "/", "_kokoro_action": "查看工作区"},
                }
            ],
        ),
        ToolMessage(name="ls", tool_call_id="call", content="Different result"),
        ToolMessage(name="grep", tool_call_id="call", content="['/README.md']"),
    ],
)
def test_conflicting_native_identity_or_result_is_not_chosen_arbitrarily(
    extra: BaseMessage,
) -> None:
    assert details(extra=[extra]) == []


def test_private_input_and_output_are_not_exposed_as_raw_dumps() -> None:
    rows = details("read_file", {"file_path": "/.env"}, "password=DO_NOT_DISPLAY")
    assert len(rows) == 1
    assert rows[0].input_preview is None and rows[0].output_preview is None


def test_output_is_bounded_and_failure_status_is_not_promoted() -> None:
    row = details(output="文" * 5000)[0]
    assert (
        row.output_preview.truncated and len(row.output_preview.text.encode()) <= 4096
    )
    assert details(status="running") == []
    assert details(tool_status="error") == []
    assert (
        details(status="failed", tool_status="error", output="No matching file")[
            0
        ].output_preview.text
        == "No matching file"
    )


def test_native_authority_requires_every_peer_and_excludes_human_rewrites() -> None:
    from kokoro_agent.application.native_activity_details import frozen_native_tools

    peer: dict[str, Any] = {
        "backend": {"kind": "state"},
        "permissions": {
            "approval_tools": ["write_file"],
            "review_tools": ["read_file"],
        },
        "pause_tools": ["edit_file"],
    }
    assert set(frozen_native_tools([peer])) == {"ls", "glob", "grep"}
    assert frozen_native_tools([]) == ()
    assert frozen_native_tools([peer, {"backend": {"kind": "custom"}}]) == ()
    assert frozen_native_tools([{**peer, "permissions": {}}]) == ()
    assert frozen_native_tools([{**peer, "pause_tools": None}]) == ()
    assert frozen_native_tools([{**peer, "pause_tools": [123]}]) == ()


def test_duplicate_call_id_in_one_message_is_not_a_replayed_copy() -> None:
    call: Any = {
        "id": "call",
        "name": "ls",
        "args": {"path": "/", "_kokoro_action": "查看工作区"},
    }
    assert (
        details(extra=[AIMessage(id="model", content="", tool_calls=[call, call])])
        == []
    )
