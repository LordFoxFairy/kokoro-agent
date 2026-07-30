"""Deployment persona source. Runtime Skills have no local/S3 asset-source fallback."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from kokoro_agent.content_source import (
    AssetSettings,
    AssetSourceError,
    LocalAssets,
    LocalAssetSource,
    S3Assets,
    load_assets_config,
    make_asset_source,
)
from kokoro_agent.prompts import PromptLibrary


def test_local_personas_are_snapshotted(tmp_path: Path) -> None:
    (tmp_path / "poet.md").write_text("original\n")
    source = LocalAssetSource(LocalAssets(type="local", personas_dir=str(tmp_path)))
    snapshot = dict(source.load_personas())
    (tmp_path / "poet.md").write_text("changed\n")
    assert snapshot == {"poet": "original"}


def test_configured_missing_persona_dir_fails_loud(tmp_path: Path) -> None:
    source = LocalAssetSource(
        LocalAssets(type="local", personas_dir=str(tmp_path / "missing"))
    )
    with pytest.raises(AssetSourceError, match="not a directory"):
        source.load_personas()


def test_unconfigured_source_is_empty_library() -> None:
    source = make_asset_source(
        AssetSettings(source=LocalAssets(type="local"), s3_access_key=None, s3_secret_key=None)
    )
    assert PromptLibrary(source.load_personas()).get("ghost") is None


def test_assets_config_local_yaml(tmp_path: Path) -> None:
    path = tmp_path / "assets.yaml"
    path.write_text("assets:\n  type: local\n  personas_dir: /data/personas\n")
    config = load_assets_config(str(path))
    assert isinstance(config, LocalAssets)
    assert config.personas_dir == "/data/personas"


def test_assets_config_s3_yaml_with_defaults(tmp_path: Path) -> None:
    path = tmp_path / "assets.yaml"
    path.write_text(
        "assets:\n  type: s3\n  endpoint: https://s3.internal\n  bucket: kokoro-assets\n"
    )
    config = load_assets_config(str(path))
    assert isinstance(config, S3Assets)
    assert config.region == "us-east-1"
    assert config.prefix == ""


def test_assets_config_unset_means_none() -> None:
    assert load_assets_config(None) is None
    assert load_assets_config("") is None


@pytest.mark.parametrize(
    "body",
    [
        "assets:\n  type: s3\n  endpoint: e\n  bucket: b\n  access_key: leaked\n",
        "assets:\n  type: local\n  skills_dir: /legacy/skills\n",
        "assets:\n  type: ftp\n  endpoint: e\n",
    ],
)
def test_assets_config_rejects_bad_or_legacy_yaml(tmp_path: Path, body: str) -> None:
    path = tmp_path / "assets.yaml"
    path.write_text(body)
    with pytest.raises(ValidationError):
        load_assets_config(str(path))


def test_s3_source_requires_credentials() -> None:
    with pytest.raises(ValidationError, match="KOKORO_ASSETS_S3_ACCESS_KEY"):
        AssetSettings(
            source=S3Assets(type="s3", endpoint="https://s3.internal", bucket="b"),
            s3_access_key=None,
            s3_secret_key=None,
        )


def test_local_asset_schema_rejects_skill_directory() -> None:
    with pytest.raises(ValidationError):
        LocalAssets.model_validate(
            {"type": "local", "skills_dir": "/legacy/skills"}
        )
