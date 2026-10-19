"""Managed package bytes and portable executable identity, never launch authority."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile

import pytest


def archive(
    *,
    manifest: bytes | None = None,
    executable: bool = True,
    extra: str | None = None,
    helper: bool = False,
) -> bytes:
    content = b"#!/bin/sh\nexit 0\n"
    if manifest is None:
        manifest = json.dumps(
            {
                "schema_version": 1,
                "entrypoint": "bin/run",
                "files": [
                    {"path": "bin/run", "sha256": hashlib.sha256(content).hexdigest()}
                ],
                "declarations": [
                    {
                        "kind": "tool",
                        "selector": "search",
                        "display_name": "Search",
                        "input_schema_digest": "a" * 64,
                        "output_schema_digest": "",
                        "required_scopes": [],
                    }
                ],
            }
        ).encode()
    helper_bytes = b"#!/bin/sh\necho helper\n"
    if helper:
        document = json.loads(manifest)
        document["files"].append(
            {"path": "bin/helper", "sha256": hashlib.sha256(helper_bytes).hexdigest()}
        )
        manifest = json.dumps(document).encode()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("mcp.json", manifest)
        entry = zipfile.ZipInfo("bin/run")
        entry.external_attr = (0o100755 if executable else 0o100644) << 16
        z.writestr(entry, content)
        if helper:
            member = zipfile.ZipInfo("bin/helper")
            member.external_attr = 0o100755 << 16
            z.writestr(member, helper_bytes)
        if extra:
            z.writestr(extra, b"unexpected")
    return buffer.getvalue()


def test_exact_package_and_entrypoint_are_bound_before_extraction() -> None:
    from kokoro_agent.mcp.package import validate_mcp_package

    payload = archive(helper=True)
    result = validate_mcp_package(
        payload,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        entrypoint="bin/run",
    )
    assert result.entrypoint == "bin/run"
    assert result.files["bin/run"] == b"#!/bin/sh\nexit 0\n"
    assert set(result.files) == {"mcp.json", "bin/run", "bin/helper"}
    from kokoro_agent.mcp.managed_session import staged_package

    with staged_package(result) as root:
        assert (root / "bin/helper").stat().st_mode & 0o7777 == 0o700
        assert (root / "mcp.json").stat().st_mode & 0o7777 == 0o600


def test_package_mismatch_unsafe_paths_and_manifest_never_escape() -> None:
    from kokoro_agent.mcp.package import McpPackageError, validate_mcp_package

    payload = archive()
    # Pydantic literal equality alone accepts bool/float; owner schema requires integer 1.
    import copy

    with zipfile.ZipFile(io.BytesIO(payload)) as zipped:
        canonical = json.loads(zipped.read("mcp.json"))
    for version in [True, 1.0]:
        changed = copy.deepcopy(canonical)
        changed["schema_version"] = version
        malformed = archive(manifest=json.dumps(changed).encode())
        with pytest.raises(McpPackageError):
            validate_mcp_package(
                malformed,
                artifact_sha256=hashlib.sha256(malformed).hexdigest(),
                entrypoint="bin/run",
            )
    for value, digest, entry in [
        (payload, "0" * 64, "bin/run"),
        (payload, hashlib.sha256(payload).hexdigest(), "other"),
        (archive(executable=False), None, "bin/run"),
        (archive(extra="Bin/other"), None, "bin/run"),
        (archive(extra="../outside"), None, "bin/run"),
        (archive(extra="hidden"), None, "bin/run"),
        (
            archive(
                manifest=b'{"schema_version":1,"schema_version":1,"secret":"private-value"}'
            ),
            None,
            "bin/run",
        ),
    ]:
        with pytest.raises(McpPackageError) as caught:
            validate_mcp_package(
                value,
                artifact_sha256=digest or hashlib.sha256(value).hexdigest(),
                entrypoint=entry,
            )
        assert "private-value" not in str(caught.value)
        assert caught.value.__context__ is None
