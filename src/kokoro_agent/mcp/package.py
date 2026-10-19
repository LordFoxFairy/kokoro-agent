"""Managed MCP content validation; it conveys no launch or invocation authority."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    JsonValue,
    TypeAdapter,
    field_validator,
)
from kokoro_agent.execution.platform_request_binding_values import (
    project_bounded_text,
    PlatformRequestBindingError,
)
from kokoro_agent.infrastructure.package_archive import (
    PackageArchiveError,
    read_package_archive,
)


class McpPackageError(ValueError):
    """Fixed safe code, without manifest, path or archive bytes."""


def package_path(value: str) -> str:
    if (
        not value
        or len(value.encode("utf-8")) > 256
        or unicodedata.normalize("NFC", value) != value
        or any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in value)
        or re.search(r'[\\:*?"<>|]', value)
        or any(
            part in {"", ".", ".."}
            or part.endswith((".", " "))
            or re.match(r"^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)", part, re.I)
            for part in value.split("/")
        )
    ):
        raise ValueError("invalid package path")
    return value


Digest = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


class _File(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    path: str
    sha256: Digest
    _path = field_validator("path")(package_path)


class _Declaration(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    kind: Literal["tool", "resource", "prompt"]
    selector: str
    display_name: str
    input_schema_digest: Digest
    output_schema_digest: Digest | Literal[""]
    required_scopes: list[str] = Field(max_length=128)

    @field_validator("selector", "display_name")
    @classmethod
    def text(cls, value: str) -> str:
        project_bounded_text(value, label="manifest text", maximum=512)
        if any(127 <= ord(c) <= 159 for c in value):
            raise ValueError("invalid scalar")
        return value

    @field_validator("required_scopes")
    @classmethod
    def scopes(cls, values: list[str]) -> list[str]:
        for value in values:
            cls.text(value)
            project_bounded_text(value, label="scope", maximum=256)
        return values


class _Manifest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    schema_version: Literal[1]
    entrypoint: str
    files: list[_File] = Field(min_length=1, max_length=127)
    declarations: list[_Declaration] = Field(min_length=1, max_length=2000)

    @field_validator("schema_version", mode="before")
    @classmethod
    def version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("unsupported schema")
        return value

    _path = field_validator("entrypoint")(package_path)


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class McpPackage:
    entrypoint: str
    executables: frozenset[str]
    files: Mapping[str, bytes] = field(repr=False)


def _unique(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate manifest key")
        result[key] = value
    return result


_JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


def _depth(value: JsonValue, level: int = 0) -> None:
    if isinstance(value, (dict, list)):
        if level >= 16:
            raise ValueError("manifest depth")
        children = value.values() if isinstance(value, dict) else value
        for child in children:
            _depth(child, level + 1)


def validate_mcp_package(
    payload: bytes, *, artifact_sha256: str, entrypoint: str
) -> McpPackage:
    failure = "MCP_PACKAGE_INVALID"
    try:
        if (
            re.fullmatch(r"[a-f0-9]{64}", artifact_sha256) is None
            or hashlib.sha256(payload).hexdigest() != artifact_sha256
        ):
            raise ValueError("artifact mismatch")
        package_path(entrypoint)
        members = read_package_archive(payload)
        paths: dict[str, str] = {}
        for path in members:
            package_path(path)
            prefix = ""
            for segment in path.split("/"):
                prefix = f"{prefix}/{segment}" if prefix else segment
                previous = paths.setdefault(prefix.lower(), prefix)
                if previous != prefix:
                    raise ValueError("non-portable package")
        raw = members["mcp.json"].content
        if not 0 < len(raw) <= 65536 or members["mcp.json"].directory:
            raise ValueError("manifest budget")
        decoded: JsonValue = _JSON.validate_python(
            json.loads(raw.decode("utf-8"), object_pairs_hook=_unique), strict=True
        )
        _depth(decoded)
        manifest = _Manifest.model_validate(decoded, strict=True)
        files = {
            name: member.content
            for name, member in members.items()
            if not member.directory
        }
        expected = {value.path: value.sha256 for value in manifest.files}
        if (
            len(expected) != len(manifest.files)
            or set(expected) != set(files) - {"mcp.json"}
            or any(
                hashlib.sha256(files[path]).hexdigest() != digest
                for path, digest in expected.items()
            )
        ):
            raise ValueError("file mismatch")
        selectors = [value.selector for value in manifest.declarations]
        if len(set(selectors)) != len(selectors):
            raise ValueError("duplicate declaration")
        executable = members[entrypoint]
        if (
            manifest.entrypoint != entrypoint
            or executable.directory
            or not executable.content
            or not executable.executable
        ):
            raise ValueError("entrypoint mismatch")
        return McpPackage(
            entrypoint=entrypoint,
            files=MappingProxyType(files),
            executables=frozenset(
                name
                for name, member in members.items()
                if not member.directory and member.executable
            ),
        )
    except (
        ValueError,
        KeyError,
        UnicodeError,
        RecursionError,
        ValidationError,
        PackageArchiveError,
        PlatformRequestBindingError,
    ):
        pass
    raise McpPackageError(failure) from None
