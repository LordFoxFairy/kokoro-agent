"""Behavioral contract for Agent-owned attempt evidence, not provider execution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kokoro_agent.model.call_artifacts import (
    encode_granted_artifact,
    encode_requested_artifact,
    strict_granted_artifact,
)
from kokoro_agent.model.call_models import ModelCallInputInvalid
from kokoro_agent.model.usage_evidence import parse_evidence, validate_successor
from kokoro_agent.protocol.model_usage_generated import (
    AuthorizationGranted,
    AuthorizationRequested,
)

ROOT = Path(__file__).resolve().parents[2]
VECTORS = json.loads((ROOT / "contract/usage/v1/vectors.json").read_text())


@pytest.mark.parametrize(
    ("observation", "expected_bytes", "expected_digest"),
    [
        (
            AuthorizationRequested(
                state="requested_unknown", request_ref="billing-operation-a1"
            ),
            b'{"request_ref":"billing-operation-a1","state":"requested_unknown"}',
            "15fbddf3e15802123e1473877688cc3bb73ee18cc151a6b2c899a9419f42da4e",
        ),
        (
            AuthorizationGranted(
                state="granted",
                request_ref="billing-operation-a1",
                admission_ref="billing-admission-a1",
            ),
            b'{"admission_ref":"billing-admission-a1",'
            b'"request_ref":"billing-operation-a1","state":"granted"}',
            "ce6a2f57272a469ff0c875e78c46ef27195ba0f43b28abd1ed55743760bbb59a",
        ),
    ],
)
def test_authorization_observation_has_stable_canonical_bytes_and_digest(
    observation: AuthorizationRequested | AuthorizationGranted,
    expected_bytes: bytes,
    expected_digest: str,
) -> None:
    actual = (
        encode_requested_artifact(observation.request_ref)
        if isinstance(observation, AuthorizationRequested)
        else encode_granted_artifact(observation.request_ref, observation.admission_ref)
    )
    assert actual == (expected_bytes, expected_digest)


def test_semantically_valid_noncanonical_authorization_is_not_canonical() -> None:
    raw = b'{ "state": "granted", "request_ref": "billing-operation-a1", "admission_ref": "billing-admission-a1" }'
    AuthorizationGranted.model_validate_json(raw, strict=True)
    with pytest.raises(ModelCallInputInvalid, match="not canonical"):
        strict_granted_artifact(raw)


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


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_generated_boundary_is_reproducible_and_check_is_read_only(
    tmp_path: Path,
    version: str,
) -> None:
    from kokoro_agent import model_usage_contract, model_usage_v2_contract

    compiler = model_usage_contract if version == "v1" else model_usage_v2_contract
    schema = tmp_path / compiler.SCHEMA_RELATIVE
    schema.parent.mkdir(parents=True)
    schema.write_bytes((ROOT / compiler.SCHEMA_RELATIVE).read_bytes())
    compiler.generate_model_usage_models(tmp_path, check=False)
    output = tmp_path / compiler.GENERATED_RELATIVE
    original = output.read_bytes()
    compiler.generate_model_usage_models(tmp_path, check=False)
    assert output.read_bytes() == original
    compiler.generate_model_usage_models(tmp_path, check=True)
    assert output.read_bytes() == original
    output.write_bytes(original + b"# drift\n")
    with pytest.raises(ValueError, match="stale"):
        compiler.generate_model_usage_models(tmp_path, check=True)
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


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_manifest_covers_exact_source_and_check_never_repairs(
    tmp_path: Path, version: str
) -> None:
    import shutil

    from kokoro_agent import model_usage_contract, model_usage_v2_contract

    compiler = model_usage_contract if version == "v1" else model_usage_v2_contract

    for relative in compiler.USAGE_SOURCE_FILES:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    compiler.generate_model_usage_artifact(tmp_path, check=False)
    manifest = tmp_path / compiler.MANIFEST_RELATIVE
    original = manifest.read_bytes()
    compiler.generate_model_usage_artifact(tmp_path, check=False)
    assert manifest.read_bytes() == original
    compiler.validate_model_usage_contract(tmp_path)
    vector_path = tmp_path / compiler.VECTORS_RELATIVE
    vector_path.write_bytes(vector_path.read_bytes() + b"\n")
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    with pytest.raises(ValueError, match="stale"):
        compiler.validate_model_usage_contract(tmp_path)
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


@pytest.mark.parametrize("mutation", ["schema", "vectors"])
def test_public_checker_rejects_coordinated_rewrite_of_historical_usage_v1(
    tmp_path: Path, mutation: str
) -> None:
    """A self-consistent rebuilt manifest must not redefine a published decoder."""
    import shutil

    from kokoro_agent import contract_check
    from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES
    from kokoro_agent.model_usage_contract import (
        USAGE_AUDIT_FILES,
        generate_model_usage_artifact,
        validate_model_usage_contract,
    )

    from kokoro_agent.model_usage_v2_contract import (
        USAGE_AUDIT_FILES as USAGE_V2_AUDIT_FILES,
    )

    shutil.copytree(ROOT / "contract", tmp_path / "contract")
    for relative in {*OWNER_SOURCE_FILES, *USAGE_AUDIT_FILES, *USAGE_V2_AUDIT_FILES}:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    contract_check.validate(tmp_path)
    path = tmp_path / f"contract/usage/v1/{mutation}.json"
    document = json.loads(path.read_bytes())
    if mutation == "schema":
        document["title"] += " rewritten"
    else:
        document["positive"][0]["id"] += "-rewritten"
    path.write_text(json.dumps(document, indent=2) + "\n")
    generate_model_usage_artifact(tmp_path, check=False)
    # The old integrity check accepts the coordinated regeneration. Its new
    # caller must additionally bind the exact immutable historical release.
    validate_model_usage_contract(tmp_path)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="historical usage v1 release"):
        contract_check.validate(tmp_path)
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("mutation", ["binding_version", "request_model"])
def test_public_checker_validates_current_usage2_not_only_historical_usage1(
    tmp_path: Path, mutation: str
) -> None:
    import shutil
    from kokoro_agent import contract_check
    from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES
    from kokoro_agent.model_usage_contract import USAGE_AUDIT_FILES

    from kokoro_agent.model_usage_v2_contract import (
        USAGE_AUDIT_FILES as USAGE_V2_AUDIT_FILES,
    )

    shutil.copytree(ROOT / "contract", tmp_path / "contract")
    for relative in {*OWNER_SOURCE_FILES, *USAGE_AUDIT_FILES, *USAGE_V2_AUDIT_FILES}:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    contract_check.validate(tmp_path)
    schema = tmp_path / "contract/usage/v2/schema.json"
    document = json.loads(schema.read_bytes())
    properties = document["$defs"]["PlannedBinding"]["properties"]
    if mutation == "binding_version":
        properties["binding_version"]["const"] = "1.0.0"
    else:
        properties["request_model"] = {"type": "string"}
    schema.write_text(json.dumps(document, indent=2) + "\n")
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with pytest.raises(ValueError):
        contract_check.validate(tmp_path)
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_binding",
        "wrong_binding",
        "legacy_model",
        "unbounded_model",
        "missing_model",
        "wrong_evidence_version",
        "weakened_model_reference",
        "wrong_planned_reference",
        "weak_planned_alias",
    ],
)
def test_usage2_compiler_rejects_rebuilt_unapproved_binding_shapes(
    mutation: str,
) -> None:
    from kokoro_agent.model_usage_v2_contract import model_usage_model_bytes

    document = json.loads((ROOT / "contract/usage/v2/schema.json").read_bytes())
    planned = document["$defs"]["PlannedBinding"]
    properties = planned["properties"]
    if mutation == "missing_binding":
        properties.pop("binding_version")
    elif mutation == "wrong_binding":
        properties["binding_version"]["const"] = "1.0.0"
    elif mutation == "legacy_model":
        properties["gateway_model_name"] = properties.pop("request_model")
    elif mutation == "unbounded_model":
        properties["request_model"] = {"type": "string"}
    elif mutation == "missing_model":
        properties.pop("request_model")
    elif mutation == "weakened_model_reference":
        document["$defs"]["Opaque"]["minLength"] = 0
    elif mutation == "wrong_planned_reference":
        document["$defs"]["ModelUsageEvidence"]["properties"]["planned"] = {
            "$ref": "#/$defs/Opaque"
        }
    elif mutation == "weak_planned_alias":
        document["$defs"]["ModelUsageEvidence"]["properties"]["planned"] = {
            "type": "string"
        }
    else:
        document["$defs"]["ModelUsageEvidence"]["properties"]["artifact_version"][
            "const"
        ] = "1.0.0"
    # Keep generic required/properties consistency: a finite compiler must
    # still reject this self-consistent but unapproved execution binding.
    planned["required"] = list(properties)
    with pytest.raises(ValueError, match="v2 binding"):
        model_usage_model_bytes(json.dumps(document).encode())


def test_usage_versions_have_distinct_strict_decoders_and_digest_domains() -> None:
    from kokoro_agent.model.usage_evidence_v2 import parse_evidence as parse_v2
    from kokoro_agent.model_usage_v2_contract import validate_model_usage_contract

    raw_v1 = VECTORS["positive"][0]["json_utf8"].encode()
    current = json.loads((ROOT / "contract/usage/v2/vectors.json").read_bytes())
    raw_v2 = current["positive"][0]["json_utf8"].encode()
    validate_model_usage_contract(ROOT)
    old = parse_evidence(raw_v1)
    new = parse_v2(raw_v2)
    assert old.artifact_version == "1.0.0"
    assert new.artifact_version == new.planned.binding_version == "2.0.0"
    assert new.planned.request_model == old.planned.gateway_model_name
    assert new.planned.provider_model_name == old.planned.provider_model_name
    assert new.digest != old.digest
    with pytest.raises(ValueError):
        parse_evidence(raw_v2)
    with pytest.raises(ValueError):
        parse_v2(raw_v1)
