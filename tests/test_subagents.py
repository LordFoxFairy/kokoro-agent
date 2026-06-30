from __future__ import annotations

import pytest
from pydantic import ValidationError

from kokoro_agent.infrastructure.subagent import (
    BUILT_IN_SUBAGENTS,
    CUSTOM_SUBAGENTS_ENV,
    load_custom_subagents_from_env,
    materialize_subagents,
    subagent_specs,
    subagent_source_for,
)


def test_subagent_specs_include_built_in_researcher() -> None:
    specs = subagent_specs({})
    researcher = next(spec for spec in specs if spec.name == "researcher")
    assert researcher.source == "built-in"
    assert researcher.description
    assert len(BUILT_IN_SUBAGENTS) >= 1


def test_custom_subagents_parse_from_json_env() -> None:
    custom = load_custom_subagents_from_env(
        {
            CUSTOM_SUBAGENTS_ENV: (
                '[{"name":"reviewer","description":"审稿","system_prompt":"检查内容质量"}]'
            )
        }
    )

    assert len(custom) == 1
    assert custom[0].name == "reviewer"
    assert custom[0].source == "config-custom"


def test_custom_subagent_name_collision_fails_loud() -> None:
    with pytest.raises(ValueError):
        load_custom_subagents_from_env(
            {
                CUSTOM_SUBAGENTS_ENV: (
                    '[{"name":"researcher","description":"冲突","system_prompt":"bad"}]'
                )
            }
        )


def test_custom_subagent_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        load_custom_subagents_from_env(
            {CUSTOM_SUBAGENTS_ENV: '[{"name":"r","description":"d","system_prompt":"s","rogue":1}]'}
        )


def test_custom_subagent_rejects_non_string_field() -> None:
    with pytest.raises(ValidationError):
        load_custom_subagents_from_env(
            {CUSTOM_SUBAGENTS_ENV: '[{"name":123,"description":"d","system_prompt":"s"}]'}
        )


def test_custom_subagent_rejects_non_array() -> None:
    with pytest.raises(ValidationError):
        load_custom_subagents_from_env({CUSTOM_SUBAGENTS_ENV: '{"name":"r"}'})


def test_custom_subagent_rejects_missing_field() -> None:
    with pytest.raises(ValidationError):
        load_custom_subagents_from_env(
            {CUSTOM_SUBAGENTS_ENV: '[{"name":"r","description":"d"}]'}
        )


def test_custom_subagent_rejects_blank_field() -> None:
    with pytest.raises(ValidationError):
        load_custom_subagents_from_env(
            {CUSTOM_SUBAGENTS_ENV: '[{"name":"  ","description":"d","system_prompt":"s"}]'}
        )


def test_custom_subagent_strips_whitespace() -> None:
    custom = load_custom_subagents_from_env(
        {CUSTOM_SUBAGENTS_ENV: '[{"name":" reviewer ","description":" 审稿 ","system_prompt":" 检查 "}]'}
    )
    assert custom[0].name == "reviewer"
    assert custom[0].description == "审稿"


def test_subagent_source_for_marks_config_custom_subagents() -> None:
    source = subagent_source_for(
        "reviewer",
        {
            CUSTOM_SUBAGENTS_ENV: (
                '[{"name":"reviewer","description":"审稿","system_prompt":"检查内容质量"}]'
            )
        },
    )

    assert source == "config-custom"


def test_subagent_source_for_unknown_name_fails_loud() -> None:
    with pytest.raises(ValueError, match="unknown subagent name"):
        subagent_source_for("runtime-reviewer", {})


def test_materialize_subagents_includes_custom_specs() -> None:
    from kokoro_agent.infrastructure.model import make_local_fake_chat_model

    runtime = materialize_subagents(
        make_local_fake_chat_model(),
        {
            CUSTOM_SUBAGENTS_ENV: (
                '[{"name":"reviewer","description":"审稿","system_prompt":"检查内容质量"}]'
            )
        },
    )

    names = [spec["name"] for spec in runtime]
    assert "researcher" in names
    assert "reviewer" in names


def test_materialize_subagents_defaults_to_built_in_only() -> None:
    from kokoro_agent.infrastructure.model import make_local_fake_chat_model

    runtime = materialize_subagents(make_local_fake_chat_model())
    assert [spec["name"] for spec in runtime] == ["researcher"]
