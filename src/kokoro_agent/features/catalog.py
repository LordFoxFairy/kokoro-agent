"""GA 内置 Feature 目录。

The catalog is deliberately a registry, not an orchestration runtime.  It only
maps a trusted product key to a static :class:`Feature` declaration; construction
is still owned by ``AgentFactory``.
"""

from collections.abc import Iterable, Mapping
from types import MappingProxyType

from kokoro_agent.features.chat import CHAT_FEATURE
from kokoro_agent.features.definition import Feature
from kokoro_agent.features.music import MUSIC_FEATURE
from kokoro_agent.features.music_chat import MUSIC_CHAT_FEATURE


class FeatureCatalog:
    """Worker-local lookup of trusted product Feature declarations.

    A catalog has no session state and does not compile or execute a graph.  A
    future builder may produce another catalog instance, but the worker still
    hands the selected declaration to the same ``AgentFactory``.
    """

    __slots__ = ("_features",)

    def __init__(self, features: Iterable[Feature]) -> None:
        entries: dict[str, Feature] = {}
        for feature in features:
            if feature.key in entries:
                raise ValueError(f"duplicate feature key: {feature.key!r}")
            entries[feature.key] = feature
        self._features: Mapping[str, Feature] = MappingProxyType(entries)

    @property
    def entries(self) -> Mapping[str, Feature]:
        """Read-only view used by diagnostics and tests."""

        return self._features

    def get(self, key: str) -> Feature:
        feature = self._features.get(key)
        if feature is None:
            raise KeyError(f"unknown feature: {key!r}")
        return feature

    def keys(self) -> tuple[str, ...]:
        return tuple(self._features)


FEATURE_CATALOG = FeatureCatalog((CHAT_FEATURE, MUSIC_FEATURE, MUSIC_CHAT_FEATURE))
# Keep the mapping as a convenience for introspection; all runtime lookup goes
# through FeatureCatalog.get so there is one semantic lookup surface.
FEATURES: Mapping[str, Feature] = FEATURE_CATALOG.entries


def get_feature(key: str) -> Feature:
    return FEATURE_CATALOG.get(key)


__all__ = ["FEATURE_CATALOG", "FEATURES", "FeatureCatalog", "get_feature"]
