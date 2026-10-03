"""Behavioral contract for Agent-owned attempt evidence, not provider execution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kokoro_agent.model.usage_evidence import parse_evidence, validate_successor
from kokoro_agent.model_usage_contract import generate_model_usage_models

ROOT = Path(__file__).resolve().parents[2]
VECTORS = json.loads((ROOT / "contract/usage/v1/vectors.json").read_text())


@pytest.mark.parametrize("vector", VECTORS["positive"], ids=lambda row: row["id"])
def test_positive_evidence_uses_real_generated_boundary(vector: dict[str, str]) -> None:
    evidence = parse_evidence(vector["json_utf8"].encode())
    assert evidence.model_dump(mode="json") == json.loads(vector["json_utf8"])
    assert evidence.digest == vector["digest"]


@pytest.mark.parametrize("vector", VECTORS["negative"], ids=lambda row: row["id"])
def test_invalid_evidence_is_rejected(vector: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        parse_evidence(vector["json_utf8"].encode())


@pytest.mark.parametrize("vector", VECTORS["pairs"], ids=lambda row: row["id"])
def test_revision_binding_is_not_latest_wins(vector: dict[str, str | bool]) -> None:
    previous = parse_evidence(str(vector["previous_json_utf8"]).encode())
    incoming = parse_evidence(str(vector["incoming_json_utf8"]).encode())
    if vector["accepted"]:
        validate_successor(previous, incoming)
    else:
        with pytest.raises(ValueError):
            validate_successor(previous, incoming)


def test_generated_boundary_is_reproducible_and_check_is_read_only(
    tmp_path: Path,
) -> None:
    schema = tmp_path / "contract/usage/v1/schema.json"
    schema.parent.mkdir(parents=True)
    schema.write_bytes((ROOT / "contract/usage/v1/schema.json").read_bytes())
    generate_model_usage_models(tmp_path, check=False)
    output = tmp_path / "src/kokoro_agent/protocol/model_usage_generated.py"
    original = output.read_bytes()
    generate_model_usage_models(tmp_path, check=False)
    assert output.read_bytes() == original
    generate_model_usage_models(tmp_path, check=True)
    assert output.read_bytes() == original
    output.write_bytes(original + b"# drift\n")
    with pytest.raises(ValueError, match="stale"):
        generate_model_usage_models(tmp_path, check=True)
    assert output.read_bytes() == original + b"# drift\n"


@pytest.mark.parametrize(
    "mutation", ["decimal_bound", "source", "path_limit", "keyword"]
)
def test_finite_compiler_rejects_semantic_schema_drift(mutation: str) -> None:
    from kokoro_agent.model_usage_contract import model_usage_model_bytes

    document = json.loads((ROOT / "contract/usage/v1/schema.json").read_text())
    if mutation == "decimal_bound":
        document["$defs"]["Count"].pop("x-max-decimal")
    elif mutation == "source":
        document["$defs"]["TokenUsageV1"]["properties"]["source_protocol"][
            "enum"
        ].append("guessed")
    elif mutation == "path_limit":
        document["$defs"]["EvidenceIdentity"]["properties"]["peer_path"]["maxItems"] = (
            100
        )
    else:
        document["$defs"]["Count"]["format"] = "coercible-number"
    with pytest.raises(ValueError):
        model_usage_model_bytes(json.dumps(document).encode())


def test_manifest_covers_exact_source_and_check_never_repairs(tmp_path: Path) -> None:
    import shutil

    from kokoro_agent.model_usage_contract import (
        MANIFEST_RELATIVE,
        USAGE_SOURCE_FILES,
        generate_model_usage_artifact,
        validate_model_usage_contract,
    )

    for relative in USAGE_SOURCE_FILES:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    generate_model_usage_artifact(tmp_path, check=False)
    manifest = tmp_path / MANIFEST_RELATIVE
    original = manifest.read_bytes()
    generate_model_usage_artifact(tmp_path, check=False)
    assert manifest.read_bytes() == original
    validate_model_usage_contract(tmp_path)
    vector_path = tmp_path / "contract/usage/v1/vectors.json"
    vector_path.write_bytes(vector_path.read_bytes() + b"\n")
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    with pytest.raises(ValueError, match="stale"):
        validate_model_usage_contract(tmp_path)
    assert {
        path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    } == before


@pytest.mark.parametrize("raw", [b"\xff", b'{"x":NaN}', b'{"x":"\\ud800"}'])
def test_original_bytes_reject_invalid_unicode_and_nonfinite_json(raw: bytes) -> None:
    with pytest.raises(ValueError):
        parse_evidence(raw)


def test_evidence_size_and_depth_are_bounded_before_model_validation() -> None:
    legal = VECTORS["positive"][0]["json_utf8"].encode()
    with pytest.raises(ValueError, match="byte budget"):
        parse_evidence(b" " * 65536 + legal)
    with pytest.raises(ValueError, match="nesting"):
        parse_evidence(b"[" * 17 + b"null" + b"]" * 17)
