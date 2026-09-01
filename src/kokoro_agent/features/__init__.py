"""对外产品 Feature 的静态装配入口。"""

from kokoro_agent.features.catalog import FEATURE_CATALOG, FEATURES, FeatureCatalog, get_feature
from kokoro_agent.features.chat import CHAT_FEATURE
from kokoro_agent.features.definition import Feature
from kokoro_agent.features.music import MUSIC_FEATURE
from kokoro_agent.features.music_chat import MUSIC_CHAT_FEATURE

__all__ = [
    "CHAT_FEATURE",
    "FEATURE_CATALOG",
    "FEATURES",
    "FeatureCatalog",
    "Feature",
    "MUSIC_CHAT_FEATURE",
    "MUSIC_FEATURE",
    "get_feature",
]
