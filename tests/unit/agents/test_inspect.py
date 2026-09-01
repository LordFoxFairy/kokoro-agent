"""Agent/Feature authored surface inspection."""

from __future__ import annotations

import json

import pytest

from kokoro_agent.features import FEATURE_CATALOG
from kokoro_agent.inspect import describe_catalog, describe_feature, render_catalog


def test_catalog_description_is_stable_and_does_not_expose_prompt_text() -> None:
    description = describe_catalog(FEATURE_CATALOG)

    assert tuple(item.key for item in description.features) == ("chat", "music", "music_chat")
    music = next(item for item in description.agents if item.key == "music")
    assert music.features == ("music", "music_chat")
    assert music.skills == ("music",)
    assert not hasattr(music, "prompt")


def test_feature_description_exposes_composition_not_runtime_state() -> None:
    description = describe_feature(FEATURE_CATALOG, "music_chat")

    assert description.key == "music_chat"
    assert description.agents == ("general", "music")
    assert description.entry_agent == "general"
    assert description.handoffs == (("general", "music"), ("music", "general"))
    assert not hasattr(description, "namespace")
    assert not hasattr(description, "thread_id")


def test_json_render_is_machine_readable_and_sorted() -> None:
    payload = json.loads(render_catalog(FEATURE_CATALOG, output="json"))

    assert [feature["key"] for feature in payload["features"]] == [
        "chat",
        "music",
        "music_chat",
    ]
    assert [agent["key"] for agent in payload["agents"]] == ["general", "music"]
    assert "prompt" not in payload["agents"][0]


def test_unknown_feature_fails_loud() -> None:
    with pytest.raises(KeyError, match="unknown feature"):
        describe_feature(FEATURE_CATALOG, "ghost")


def test_unknown_output_format_fails_loud() -> None:
    with pytest.raises(ValueError, match="unsupported inspect output"):
        render_catalog(FEATURE_CATALOG, output="yaml")
