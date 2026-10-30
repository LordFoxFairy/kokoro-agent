from __future__ import annotations

import pytest
from kokoro_agent.domain.chat.projection import ToolDisplaySource


def test_real_skill_mount_keeps_basename_and_its_safe_virtual_path() -> None:
    prefix = "/.skills/YWxwaGE/"
    source = ToolDisplaySource(
        name="read_file",
        code="tool.file.read",
        origin="native",
        skill_prefixes=(prefix,),
    )
    detail = source.detail({"file_path": prefix + "references/guide.md"})
    assert detail is not None
    assert detail.model_dump(exclude_none=True) == {
        "kind": "file",
        "name": "guide.md",
        "virtual_path": prefix + "references/guide.md",
    }
    for path in [
        "/.skills/other/SKILL.md",
        "/Users/owner/a.md",
        prefix + "../a",
        prefix + ".env",
    ]:
        assert source.detail({"file_path": path}) is None


def test_native_skill_display_binds_exact_source_path_and_optional_revision() -> None:
    from kokoro_agent.domain.chat.projection import SkillDisplay

    value = {
        "source_ref": "skill:alpha",
        "name": "actual-package",
        "virtual_path": "/.skills/YWxwaGE/SKILL.md",
    }
    assert SkillDisplay.model_validate(value).model_dump(exclude_none=True) == value
    assert SkillDisplay.model_validate({**value, "revision": "3"}).revision == "3"
    for change in [
        {"virtual_path": "/.skills/YmV0YQ/SKILL.md"},
        {"revision": None},
        {"revision": "03"},
        {"name": "token-secret"},
        {"description": "private"},
    ]:
        with pytest.raises(ValueError):
            SkillDisplay.model_validate({**value, **change})


def test_old_display_requires_ready_exact_selected_path_and_matching_tool_provenance() -> (
    None
):
    from kokoro_agent.application.run_display import recover_display
    from langchain_core.messages import AIMessage
    from kokoro_agent.domain.chat.projection import safe_progress_id

    scope = ("tenant", "namespace", "session", "run")
    activity_id = "act_" + safe_progress_id("tool-activity", *scope, "call", "call")
    events = [
        {"activity": "skill", "phase": "ready", "source_refs": ["skill:alpha"]},
        {
            "activity": "tool",
            "activity_id": activity_id,
            "display_code": "tool.file.read",
            "display_detail": {"kind": "file", "name": "SKILL.md"},
        },
    ]
    messages = [
        AIMessage(
            id="segment",
            content="",
            tool_calls=[
                {
                    "id": "call",
                    "name": "read_file",
                    "args": {"file_path": "/.skills/YWxwaGE/SKILL.md"},
                }
            ],
        )
    ]
    metadata = [{"name": "actual", "path": "/.skills/YWxwaGE/SKILL.md"}]
    result = recover_display(
        scope=scope,
        selected=("skill:alpha",),
        events=events,
        metadata=metadata,
        messages=messages,
        journal={"call": "read_file"},
    )
    assert result.model_dump(mode="json") == {
        "run_id": "run",
        "display_skills": [
            {
                "source_ref": "skill:alpha",
                "name": "actual",
                "virtual_path": "/.skills/YWxwaGE/SKILL.md",
            }
        ],
        "tool_paths": [
            {"activity_id": activity_id, "virtual_path": "/.skills/YWxwaGE/SKILL.md"}
        ],
        "tool_facts": [],
        "activity_details": [],
    }
    assert "revision" not in result.display_skills[0].model_dump()
    assert (
        recover_display(
            scope=scope,
            selected=(),
            events=events,
            metadata=metadata,
            messages=messages,
            journal={"call": "read_file"},
        ).display_skills
        == []
    )
    assert (
        recover_display(
            scope=scope,
            selected=("skill:alpha",),
            events=events,
            metadata=metadata,
            messages=messages,
            journal={"foreign": "read_file"},
        ).tool_paths
        == []
    )
    assert (
        recover_display(
            scope=(*scope[:3], "foreign"),
            selected=("skill:alpha",),
            events=events,
            metadata=metadata,
            messages=messages,
            journal={"call": "read_file"},
        ).tool_paths
        == []
    )


def test_native_display_budget_counts_unique_locators_and_all_raw_bytes_before_decode() -> (
    None
):
    from kokoro_agent.infrastructure.checkpoint_display import DisplayReadBudget

    budget = DisplayReadBudget()
    for index in range(32):
        budget.accept(str(index), 1)
    budget.accept("0", 1)
    with pytest.raises(ValueError):
        budget.accept("33", 1)
    budget = DisplayReadBudget()
    budget.accept("one", 8 * 1024 * 1024)
    with pytest.raises(ValueError):
        budget.accept("two", 1)


def test_peer_mcp_display_intersects_actual_declarations_not_whole_name_sets() -> None:
    from dataclasses import replace

    source = ToolDisplaySource(
        name="mcp_call",
        code="tool.mcp.call",
        origin="mcp",
        mcp_names=(("common", "echo", "Echo"), ("peer-one", "search", "Search")),
    )
    peer = replace(
        source, mcp_names=(("common", "echo", "Echo"), ("peer-two", "read", "Read"))
    )
    shared = source.shared_with(peer)
    assert shared is not None
    common = shared.detail({"server": "common", "tool": "echo"})
    assert common is not None and common.name == "Echo"
    assert shared.detail({"server": "peer-one", "tool": "search"}) is None
    assert shared.detail({"server": "peer-two", "tool": "read"}) is None
    assert (
        source.shared_with(replace(peer, mcp_names=(("common", "echo", "Different"),)))
        is None
    )
    assert source.shared_with(replace(peer, origin="web")) is None


@pytest.mark.parametrize(
    "name",
    [
        "ask_question",
        "task_read",
        "desk_tool",
        "1sk_name",
        "ASK_QUESTION",
        "namespace/task_read",
    ],
)
def test_embedded_sk_in_a_real_tool_word_is_not_a_secret_prefix(name: str) -> None:
    from kokoro_agent.domain.chat.projection import (
        ToolDisplayDetail,
        safe_virtual_path,
        sensitive_display_text,
    )

    assert not sensitive_display_text(name)
    assert ToolDisplayDetail(kind="mcp_tool", name=name).name == name
    assert safe_virtual_path(f"/workspace/{name}.md")


@pytest.mark.parametrize(
    "name",
    [
        "sk-token",
        "sk_token",
        "SK-token",
        " sk_token",
        "/sk-token",
        ".sk_token",
        "_sk_token",
        ":sk-token",
        "(sk_token)",
        "name sk-token",
        "credentials",
        "secret.txt",
        "/.ssh/id_rsa",
        "/.env",
        "Bearer value",
        "ghp_value",
        "github_pat_value",
        "id_ed25519",
        "key.pem",
        "eyJabc.def.ghi",
    ],
)
def test_real_secret_prefix_boundaries_and_all_other_sensitive_families_stay_rejected(
    name: str,
) -> None:
    from kokoro_agent.domain.chat.projection import (
        ToolDisplayDetail,
        safe_virtual_path,
        sensitive_display_text,
    )

    assert sensitive_display_text(name)
    with pytest.raises(ValueError):
        ToolDisplayDetail(kind="mcp_tool", name=name)
    assert not safe_virtual_path(f"/workspace/{name}")
