from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from pydantic import TypeAdapter

OWNER_VECTORS = json.loads(
    (
        Path(__file__).parents[3]
        / "contract/platform/v1/skill-package/skill-package-profile-v1.vectors.json"
    ).read_bytes()
)["vectors"]
VECTORS = [
    row for row in OWNER_VECTORS if row["validation_stage"] == "owner-and-consumer"
]
_OUTCOME = TypeAdapter(dict[str, object])
_FILES = TypeAdapter(dict[str, str])


@pytest.mark.parametrize("vector", VECTORS, ids=[v["name"] for v in VECTORS])
def test_pinned_consumer_archive_vectors(vector: dict[str, object]) -> None:
    from kokoro_agent.skills.package import SkillPackageError, validate_package

    payload = base64.b64decode(str(vector["archive_base64"]))
    expected = _OUTCOME.validate_python(vector["expected"], strict=True)
    if "reason" not in expected:
        result = validate_package(
            payload,
            manifest_identity=str(expected["manifestIdentity"]),
        )
        encoded_files = _FILES.validate_python(vector["files_base64"], strict=True)
        assert dict(result) == {
            name: base64.b64decode(encoded) for name, encoded in encoded_files.items()
        }
    else:
        with pytest.raises(SkillPackageError) as error:
            validate_package(payload, manifest_identity=None)
        assert str(error.value) == expected["reason"]


def test_deeply_nested_optional_manifest_is_not_parsed() -> None:
    import io
    import zipfile
    from kokoro_agent.skills.package import validate_package

    markdown = base64.b64decode(VECTORS[0]["files_base64"]["SKILL.md"])
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("manifest.json", b"[" * 6000 + b"0" + b"]" * 6000)
        archive.writestr("SKILL.md", markdown)
    assert (
        validate_package(buffer.getvalue(), manifest_identity=None)["SKILL.md"]
        == markdown
    )


@pytest.mark.parametrize(
    "identity", ["skill-md-v1:sha256:", "zip-v1:sha256:", "zip-v2:sha256:"]
)
def test_current_package_refuses_wrong_or_retired_identity(identity: str) -> None:
    from kokoro_agent.skills.package import SkillPackageError, validate_package

    payload = base64.b64decode(VECTORS[0]["archive_base64"])
    with pytest.raises(SkillPackageError, match="package_identity_mismatch"):
        validate_package(payload, manifest_identity=identity + "0" * 64)
