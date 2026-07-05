"""资产域：skills/personas 统一从资产源（local/s3）启动装载为不可变快照库。

配置与 wire 只传名称；"文件从哪来"归源（source），"怎么用"归库（skills/personas）。
"""

from __future__ import annotations

from kokoro_agent.assets.personas import PersonaLibrary
from kokoro_agent.assets.skills import SKILL_MAX_CHARS, SkillAssetError, SkillLibrary
from kokoro_agent.assets.source import (
    AssetSettings,
    AssetSource,
    AssetSourceError,
    LocalAssets,
    LocalAssetSource,
    S3Assets,
    S3AssetSource,
    load_asset_libraries,
    load_assets_config,
    make_asset_source,
)

__all__ = [
    "SKILL_MAX_CHARS",
    "AssetSettings",
    "AssetSource",
    "AssetSourceError",
    "LocalAssetSource",
    "LocalAssets",
    "PersonaLibrary",
    "S3AssetSource",
    "S3Assets",
    "SkillAssetError",
    "SkillLibrary",
    "load_asset_libraries",
    "load_assets_config",
    "make_asset_source",
]
