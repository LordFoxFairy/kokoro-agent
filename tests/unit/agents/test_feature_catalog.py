"""Feature/Agent 组装声明的最小契约。"""

from __future__ import annotations

import pytest
from dataclasses import replace

from kokoro_agent.agents import GENERAL_AGENT, MUSIC_AGENT
from kokoro_agent.features import (
    CHAT_FEATURE,
    FEATURE_CATALOG,
    FEATURES,
    FeatureCatalog,
    MUSIC_CHAT_FEATURE,
    MUSIC_FEATURE,
    get_feature,
)
from kokoro_agent.features.definition import Feature


def test_single_agent_features_are_direct_capabilities() -> None:
    assert CHAT_FEATURE.agents == (GENERAL_AGENT,)
    assert MUSIC_FEATURE.agents == (MUSIC_AGENT,)
    assert MUSIC_FEATURE.entry_agent == "music"
    assert MUSIC_FEATURE.handoffs == ()


def test_combined_feature_declares_only_peer_handoffs() -> None:
    assert MUSIC_CHAT_FEATURE.agents == (GENERAL_AGENT, MUSIC_AGENT)
    assert MUSIC_CHAT_FEATURE.entry_agent == "general"
    assert MUSIC_CHAT_FEATURE.handoffs == (("general", "music"), ("music", "general"))


def test_catalog_is_the_only_feature_lookup() -> None:
    assert FEATURE_CATALOG.get("music") is MUSIC_FEATURE
    assert get_feature("music") is MUSIC_FEATURE
    assert get_feature("music_chat") is MUSIC_CHAT_FEATURE
    assert set(FEATURES) == {"chat", "music", "music_chat"}
    with pytest.raises(KeyError):
        get_feature("unknown")


def test_catalog_rejects_duplicate_keys() -> None:
    with pytest.raises(ValueError, match="duplicate feature key"):
        FeatureCatalog((CHAT_FEATURE, CHAT_FEATURE))


def test_feature_rejects_mixed_backends() -> None:
    with pytest.raises(ValueError, match="one shared backend"):
        Feature(
            key="mixed",
            agents=(GENERAL_AGENT, replace(MUSIC_AGENT, backend="local_shell")),
            entry_agent="general",
            handoffs=(("general", "music"),),
        )


def test_feature_can_narrow_agent_capabilities_without_a_new_role_type() -> None:
    configured = MUSIC_AGENT.configured(skills=("music", "lyrics"), mcp=("music_provider",))
    assert configured.key == MUSIC_AGENT.key
    assert configured.skills == ("music", "lyrics")
    assert configured.mcp == ("music_provider",)
    assert MUSIC_AGENT.skills == ("music",)
    assert MUSIC_AGENT.mcp == ()


def test_agent_mcp_surface_contains_names_only() -> None:
    configured = replace(MUSIC_AGENT, mcp=("music_provider",))
    assert configured.mcp == ("music_provider",)
    assert not hasattr(configured, "mcp_grants")


def test_multi_agent_feature_requires_handoffs_at_declaration() -> None:
    with pytest.raises(ValueError, match="must declare official handoffs"):
        Feature(key="invalid", agents=(GENERAL_AGENT, MUSIC_AGENT), entry_agent="general")


def test_multi_agent_feature_rejects_members_unreachable_from_entry() -> None:
    with pytest.raises(ValueError, match="unreachable agents.*music"):
        Feature(
            key="invalid",
            agents=(GENERAL_AGENT, MUSIC_AGENT),
            entry_agent="general",
            handoffs=(("music", "general"),),
        )
