"""Bind read-only Agent assets to the distribution running this module.

Editable checkouts are an explicit metadata-backed layout, never a recovery
path for an incomplete installation. No discovery or file I/O runs on import.
"""

from __future__ import annotations

import base64
from collections.abc import Collection, Mapping
from dataclasses import dataclass
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path, PurePosixPath
import sys
import tomllib
from urllib.parse import unquote, urlsplit

_NAME = "kokoro-agent"
_PACKAGE = "kokoro_agent"
_DDL = "share/kokoro-agent/schema.sql"
_AUDIT = "share/kokoro-agent/audit"


class _MetadataObject(dict[str, object]):
    """A JSON object whose member pairs passed the duplicate-key check."""


@dataclass(frozen=True, slots=True)
class _Binding:
    distribution: metadata.Distribution
    base: Path
    package: Path
    source: Path | None
    records: Mapping[str, metadata.PackagePath]


def _plain_file(path: Path, boundary: Path) -> Path:
    """Normalize parent segments without resolving away an asset symlink."""
    path = Path(os.path.abspath(path))
    if not path.is_relative_to(boundary):
        raise ValueError("Agent asset escapes its distribution boundary")
    cursor = boundary
    for part in path.relative_to(boundary).parts:
        cursor /= part
        if cursor.is_symlink():
            raise ValueError("Agent asset contains a symlink")
    if not path.is_file():
        raise FileNotFoundError(f"Agent distribution asset is missing: {path}")
    return path


def _metadata_object(pairs: list[tuple[str, object]]) -> _MetadataObject:
    result = _MetadataObject()
    for key, value in pairs:
        if key in result:
            raise ValueError("Agent metadata contains duplicate JSON members")
        result[key] = value
    return result


def _editable_source(distribution: metadata.Distribution) -> Path | None:
    raw = distribution.read_text("direct_url.json")
    if raw is None:
        return None
    value: object = json.loads(raw, object_pairs_hook=_metadata_object)
    if not isinstance(value, _MetadataObject):
        raise ValueError("Agent direct_url metadata must be an object")
    # JSON object keys are strings; values are checked before use below.
    record = value
    directory = record.get("dir_info")
    if not isinstance(directory, _MetadataObject):
        return None
    if directory.get("editable") is not True:
        return None
    url = record.get("url")
    if not isinstance(url, str):
        raise ValueError("Agent editable metadata has no local URL")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "file"
        or parsed.netloc not in {"", "localhost"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Agent editable metadata must identify a local project")
    path = Path(unquote(parsed.path))
    if not path.is_absolute():
        raise ValueError("Agent editable project path must be absolute")
    return path.resolve(strict=True)


def _record_files(
    distribution: metadata.Distribution,
) -> dict[str, metadata.PackagePath]:
    if not distribution.read_text("RECORD"):
        raise ValueError("Agent installation is missing RECORD")
    files = distribution.files
    if files is None:
        raise ValueError("Agent installation is missing its RECORD file list")
    records: dict[str, metadata.PackagePath] = {}
    for file in files:
        name = str(file)
        if file.is_absolute() or "\\" in name or name in records:
            raise ValueError("Agent RECORD contains an ambiguous file path")
        records[name] = file
    return records


def _source_identity(source: Path, version: str) -> None:
    manifest = _plain_file(source / "pyproject.toml", source)
    with manifest.open("rb") as stream:
        project = tomllib.load(stream).get("project", {})
    if project.get("name") != _NAME or project.get("version") != version:
        raise ValueError("Agent editable project identity does not match metadata")


def _binding() -> _Binding:
    own_file = Path(__file__).resolve(strict=True)
    matches: dict[Path, _Binding] = {}
    for distribution in metadata.distributions(name=_NAME):
        # A source-tree .egg-info build artifact is not an installed distribution.
        if distribution.read_text("METADATA") is None:
            continue
        base = Path(str(distribution.locate_file(""))).resolve(strict=True)
        source = _editable_source(distribution)
        package = (source / "src" if source is not None else base) / _PACKAGE
        if own_file != package / "distribution_assets.py":
            continue
        name = distribution.metadata["Name"]
        if name.lower().replace("_", "-").replace(".", "-") != _NAME:
            raise ValueError("Agent distribution has the wrong package identity")
        version = distribution.version
        if version != getattr(sys.modules.get(_PACKAGE), "__version__", None):
            raise ValueError("Agent package version does not match its distribution")
        identity = _plain_file(base / f"{_PACKAGE}-{version}.dist-info/METADATA", base)
        if source is not None:
            _source_identity(source, version)
            records: dict[str, metadata.PackagePath] = {}
        else:
            records = _record_files(distribution)
        matches[identity] = _Binding(distribution, base, package, source, records)
    if len(matches) != 1:
        raise ValueError("Agent requires exactly one origin-matched distribution")
    binding = next(iter(matches.values()))
    _check_module_origins(binding)
    return binding


def _located(binding: _Binding, record: metadata.PackagePath) -> Path:
    raw_base = Path(str(binding.distribution.locate_file("")))
    located = Path(str(binding.distribution.locate_file(record)))
    relative = located.relative_to(raw_base)
    if relative != Path(str(record)):
        raise ValueError("Agent distribution locator disagrees with RECORD")
    return Path(os.path.abspath(binding.base / relative))


def _read_record(record: metadata.PackagePath, path: Path) -> bytes:
    data = path.read_bytes()
    digest = record.hash
    if digest is None or digest.mode != "sha256" or record.size != len(data):
        raise ValueError("Agent asset has incomplete or stale RECORD metadata")
    actual = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    if actual.decode("ascii") != digest.value:
        raise ValueError("Agent asset digest does not match RECORD")
    return data


def _module_record(
    binding: _Binding, relative: str
) -> tuple[Path, metadata.PackagePath]:
    record = binding.records.get(relative)
    if record is None:
        raise ValueError("Agent running module is absent from RECORD")
    path = _plain_file(_located(binding, record), binding.package)
    if path != binding.base / relative:
        raise ValueError("Agent module is located outside its recorded package")
    return path, record


def _check_module_origins(binding: _Binding) -> None:
    for name, module in tuple(sys.modules.items()):
        if name != _PACKAGE and not name.startswith(_PACKAGE + "."):
            continue
        origin = getattr(module, "__file__", None)
        spec = getattr(module, "__spec__", None)
        relative = name.replace(".", "/")
        relative += "/__init__.py" if hasattr(module, "__path__") else ".py"
        expected = binding.package.parent / relative
        if not isinstance(origin, str) or Path(origin).resolve() != expected:
            raise ValueError("Agent imports are mixed across package origins")
        if (
            spec is None
            or spec.origin is None
            or Path(spec.origin).resolve() != expected
        ):
            raise ValueError("Agent module loader origin does not match its package")
        _plain_file(expected, binding.package)
        if binding.source is None:
            path, record = _module_record(binding, relative)
            _read_record(record, path)


def _data_record(binding: _Binding, logical: str) -> tuple[Path, metadata.PackagePath]:
    suffix = PurePosixPath(logical).parts
    matches = [
        record
        for record in binding.records.values()
        if record.parts[-len(suffix) :] == suffix
        and all(part == ".." for part in record.parts[: -len(suffix)])
    ]
    if len(matches) != 1:
        raise ValueError(f"Agent asset must have one RECORD entry: {logical}")
    record = matches[0]
    roots = {binding.base}  # --target data_files live beside the installed package.
    if binding.base.name == "site-packages":
        if binding.base.parent.name.startswith(
            "python"
        ) and binding.base.parent.parent.name in {"lib", "lib64"}:
            roots.add(binding.base.parents[2])
        elif binding.base.parent.name == "Lib":
            roots.add(binding.base.parents[1])
    located = _located(binding, record)
    allowed = [root for root in roots if located == root / logical]
    if len(allowed) != 1:
        raise ValueError("Agent data asset has a foreign installation prefix")
    return _plain_file(located, allowed[0]), record


def canonical_ddl_path() -> Path:
    """Locate the sole DDL asset; never try a second layout after failure."""
    binding = _binding()
    if binding.source is not None:
        return _plain_file(binding.source / "database/schema.sql", binding.source)
    path, _ = _data_record(binding, _DDL)
    return path


def read_canonical_ddl(path: Path) -> str:
    """Read once and validate the exact SQL bytes that the installer will use.

    The schema module owns its existing path seam. Editable source has no
    immutable RECORD digest; installed SQL must match the selected record.
    """
    binding = _binding()
    if binding.source is not None:
        return path.read_bytes().decode("utf-8")
    recorded_path, record = _data_record(binding, _DDL)
    if path != recorded_path:
        raise ValueError("Agent DDL path does not belong to this installation")
    return _read_record(record, recorded_path).decode("utf-8")


def contract_audit_root(expected_files: Collection[str]) -> Path:
    """Bind the full audit tree and its Python copies to the running package."""
    binding = _binding()
    if binding.source is not None:
        return binding.source
    provenance, _ = _data_record(binding, f"{_AUDIT}/contract/provenance.json")
    root = provenance.parents[1]
    actual: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError("Agent audit tree contains a symlink")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    if actual != set(expected_files):
        raise ValueError("Agent audit resource closure is incomplete or unexpected")
    for relative in expected_files:
        path, record = _data_record(binding, f"{_AUDIT}/{relative}")
        if path != root / relative:
            raise ValueError("Agent audit assets are mixed across installations")
        data = _read_record(record, path)
        if relative.startswith("src/kokoro_agent/"):
            runtime_path, runtime_record = _module_record(
                binding, relative.removeprefix("src/")
            )
            if _read_record(runtime_record, runtime_path) != data:
                raise ValueError("Agent audit copy differs from its installed module")
    return root
