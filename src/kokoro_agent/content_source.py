"""Deployment persona source; runtime Skills are resolved exclusively by Platform Hub.

local scans a deployment directory; s3 reads a deployment prefix. Credentials are env-only.
Personas are snapshotted once at worker startup and failures are fail-loud.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Literal, Protocol

import boto3
import yaml
from botocore.config import Config as BotoConfig
from mypy_boto3_s3 import S3Client
from pydantic import BaseModel, ConfigDict, SecretStr, TypeAdapter, model_validator



class AssetSourceError(Exception):
    pass


class LocalAssets(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    type: Literal["local"]
    personas_dir: str | None = None


class S3Assets(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    type: Literal["s3"]
    endpoint: str
    bucket: str
    region: str = "us-east-1"
    force_path_style: bool = True
    # Object layout: {prefix}personas/<name>.md.
    prefix: str = ""


class _AssetsFile(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    assets: LocalAssets | S3Assets


_ASSETS_ADAPTER: TypeAdapter[_AssetsFile] = TypeAdapter(_AssetsFile)


def load_assets_config(path: str | None) -> LocalAssets | S3Assets | None:
    """Load the optional deployment persona-source YAML."""
    if path is None or path == "":
        return None
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return _ASSETS_ADAPTER.validate_python(raw).assets


class AssetSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    source: LocalAssets | S3Assets
    s3_access_key: SecretStr | None
    s3_secret_key: SecretStr | None

    @model_validator(mode="after")
    def _require_s3_credentials(self) -> AssetSettings:
        if isinstance(self.source, S3Assets) and (
            self.s3_access_key is None or self.s3_secret_key is None
        ):
            raise ValueError("assets type s3 requires KOKORO_ASSETS_S3_ACCESS_KEY/SECRET_KEY")
        return self


class AssetSource(Protocol):
    def load_personas(self) -> Mapping[str, str]: ...


class LocalAssetSource:
    def __init__(self, config: LocalAssets) -> None:
        self._config = config

    def load_personas(self) -> Mapping[str, str]:
        root = _existing_dir(self._config.personas_dir, "prompts")
        if root is None:
            return {}
        return {
            child.stem: child.read_text(encoding="utf-8").strip()
            for child in sorted(root.iterdir())
            if child.is_file() and child.suffix == ".md"
        }


def _existing_dir(raw: str | None, kind: str) -> Path | None:
    if raw is None or raw == "":
        return None
    path = Path(raw)
    if not path.is_dir():
        raise AssetSourceError(f"{kind} dir {raw!r} is not a directory")
    return path


class S3AssetSource:
    """Load deployment personas from `{prefix}personas/<name>.md` at startup."""

    def __init__(self, config: S3Assets, *, access_key: SecretStr, secret_key: SecretStr) -> None:
        self._bucket = config.bucket
        base = config.prefix.strip("/")
        self._base = f"{base}/" if base else ""
        self._client: S3Client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            region_name=config.region,
            aws_access_key_id=access_key.get_secret_value(),
            aws_secret_access_key=secret_key.get_secret_value(),
            config=BotoConfig(
                s3={"addressing_style": "path" if config.force_path_style else "auto"},
                # 资产是 prompt 载荷、启动期一次装载：正常超时+重试，装不到 fail-loud。
                connect_timeout=5,
                read_timeout=30,
                retries={"max_attempts": 3},
            ),
        )

    def _list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket, Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                # boto3 stubs 把 Key 标为可缺（协议面宽松）；真实 list 响应恒携带。
                if key is not None:
                    keys.append(key)
        return keys

    def _read(self, key: str) -> str:
        return self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read().decode("utf-8")

    def load_personas(self) -> Mapping[str, str]:
        prefix = f"{self._base}personas/"
        contents: dict[str, str] = {}
        for key in self._list(prefix):
            rel = key[len(prefix) :]
            if "/" in rel or not rel.endswith(".md"):
                continue
            contents[rel.removesuffix(".md")] = self._read(key).strip()
        return contents


def make_asset_source(settings: AssetSettings) -> AssetSource:
    if isinstance(settings.source, S3Assets):
        # validator 已保证凭据在位；显式复核以完成类型收窄（不设 cast/assert 捷径）。
        if settings.s3_access_key is None or settings.s3_secret_key is None:
            raise AssetSourceError("assets type s3 requires KOKORO_ASSETS_S3_ACCESS_KEY/SECRET_KEY")
        return S3AssetSource(
            settings.source,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
        )
    return LocalAssetSource(settings.source)

