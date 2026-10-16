from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

VECTORS = json.loads(
    (
        Path(__file__).parents[3]
        / "contract/platform/v1/execution-operations/v4/vectors/zip-v1.json"
    ).read_bytes()
)["vectors"]


@pytest.mark.parametrize("vector", VECTORS, ids=[v["name"] for v in VECTORS])
def test_owner_zip_profile_vectors(vector: dict[str, object]) -> None:
    from kokoro_agent.skills.package import SkillPackageError, validate_package

    payload = base64.b64decode(str(vector["zipBase64"]))
    skill_id = str(vector["skillId"])
    revision = int(str(vector["revision"]))
    identity = vector.get("manifestIdentity")
    assert identity is None or isinstance(identity, str)
    if vector["expectedReason"] == "none":
        result = validate_package(
            payload, skill_id=skill_id, revision=revision, manifest_identity=identity
        )
        assert isinstance(result["SKILL.md"], bytes)
    else:
        with pytest.raises(SkillPackageError) as error:
            validate_package(
                payload,
                skill_id=skill_id,
                revision=revision,
                manifest_identity=identity,
            )
        assert str(error.value) == vector["expectedReason"]


@pytest.mark.parametrize(
    "manifest_bytes",
    [
        b"[" * 6000 + b"0" + b"]" * 6000,
        b'["PRIVATE_MANIFEST_SENTINEL"]',
        b'{"PRIVATE_MANIFEST_SENTINEL":',
    ],
    ids=["deeply-nested", "wrong-root-type", "malformed-json"],
)
def test_invalid_manifest_has_stable_error_without_retaining_private_decoder_context(
    manifest_bytes: bytes,
) -> None:
    import io
    import zipfile
    from kokoro_agent.skills.package import SkillPackageError, validate_package

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("manifest.json", manifest_bytes)
        archive.writestr("SKILL.md", b"# Skill")
    with pytest.raises(SkillPackageError, match="package_manifest_invalid") as error:
        validate_package(
            buffer.getvalue(), skill_id="skill-1", revision=1, manifest_identity=None
        )
    assert error.value.__context__ is None
    assert error.value.__cause__ is None
    assert "PRIVATE_MANIFEST_SENTINEL" not in str(error.value)
