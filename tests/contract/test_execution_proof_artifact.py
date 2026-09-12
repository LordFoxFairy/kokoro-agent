"""Agent-owned execution-proof artifact and conformance-vector contract."""

from __future__ import annotations

import json
import hashlib
import base64
import shutil
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, validate as validate_json_schema
from pydantic import TypeAdapter
import pytest

from kokoro_agent import contract_check
from kokoro_agent.execution_proof_negative_specs import (
    NEGATIVE_SPEC_INDEX,
    NEGATIVE_SPECS,
    build_negative_spec_index,
)
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest


ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "contract" / "execution-proof" / "v1" / "schema.json"
VECTORS = ROOT / "contract" / "execution-proof" / "v1" / "vectors.json"
PROVENANCE = ROOT / "contract" / "provenance.json"
OWNER_INVENTORY = [
    "contract/openapi/v1/openapi.json",
    "contract/execution-proof/v1/schema.json",
    "contract/execution-proof/v1/vectors.json",
    "src/kokoro_agent/protocol/control.py",
    "src/kokoro_agent/protocol/events.py",
    "src/kokoro_agent/protocol/streams.py",
]
HEADER_FIELDS = {"typ", "alg", "kid"}
CLAIMS_OBJECT_PATH = ("properties", "claims")
CLAIMS_SCHEMA_PATH = (*CLAIMS_OBJECT_PATH, "properties")
CLAIM_FIELDS = set(
    "contract_version iss aud tenant_ref actor subject run_id execution_session_id "
    "lease_generation operation request_binding_sha256 iat exp jti".split()
)
NEGATIVE_STAGES = {spec.name: spec.stage for spec in NEGATIVE_SPECS}
FIX_R1_REQUIRED_NEGATIVES = set(NEGATIVE_STAGES) - {"unknown_claim"}
_TEST_OBJECT = TypeAdapter(dict[str, Any])
_TEST_ARRAY = TypeAdapter(list[Any])


def _object(value: object) -> dict[str, Any]:
    return _TEST_OBJECT.validate_python(value)


def _array(value: object) -> list[Any]:
    return _TEST_ARRAY.validate_python(value)


def _load(path: Path) -> dict[str, Any]:
    return _object(json.loads(path.read_text(encoding="utf-8")))


def _combined_digest(root: Path, sources: list[str]) -> str:
    digest = hashlib.sha256()
    for relative in sources:
        digest.update((root / relative).read_bytes())
    return digest.hexdigest()


def _copy_contract_repository(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    shutil.copytree(ROOT / "contract", repository / "contract")
    protocol = repository / "src" / "kokoro_agent" / "protocol"
    protocol.mkdir(parents=True)
    for filename in ("control.py", "events.py", "streams.py"):
        shutil.copy2(ROOT / "src" / "kokoro_agent" / "protocol" / filename, protocol)
    return repository


def _write_provenance(repository: Path, provenance: dict[str, Any]) -> None:
    sources = [str(source) for source in _array(provenance["source_files"])]
    provenance["combined_sha256"] = _combined_digest(repository, sources)
    (repository / "contract" / "provenance.json").write_text(
        json.dumps(provenance, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _refresh_contract_digests(repository: Path) -> None:
    provenance = _load(repository / "contract" / "provenance.json")
    proof = _object(provenance["execution_proof"])
    for name in ("schema", "vectors"):
        record = _object(proof[name])
        artifact = repository / str(record["path"])
        record["sha256"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
        proof[name] = record
    provenance["execution_proof"] = proof
    _write_provenance(repository, provenance)


def _artifact_fixture(
    tmp_path: Path, artifact: Path
) -> tuple[Path, Path, dict[str, Any]]:
    repository = _copy_contract_repository(tmp_path)
    path = repository / artifact.relative_to(ROOT)
    return repository, path, _load(path)


def _save_artifact(repository: Path, path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    _refresh_contract_digests(repository)


def _negative(
    vectors: dict[str, Any], name: str
) -> tuple[list[Any], int, dict[str, Any]]:
    negatives = _array(vectors["negative"])
    index = next(i for i, item in enumerate(negatives) if _object(item)["name"] == name)
    return negatives, index, _object(negatives[index])


def test_execution_proof_artifacts_are_in_the_complete_owner_inventory() -> None:
    assert SCHEMA.is_file()
    assert VECTORS.is_file()
    provenance = json.loads(PROVENANCE.read_text(encoding="utf-8"))
    assert provenance["source_files"] == OWNER_INVENTORY


def test_schema_is_draft_2020_12_strict_and_describes_only_the_decoded_profile() -> (
    None
):
    schema = _load(SCHEMA)
    Draft202012Validator.check_schema(schema)

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert (
        schema["$id"]
        == "https://kokoro.dev/contracts/agent/execution-proof/v1/schema.json"
    )
    assert schema["x-kokoro-contract-version"] == "1.0.0"
    assert schema["x-kokoro-canonicalization"] == "RFC 8785"
    assert schema["x-kokoro-base64url-padding"] == "forbidden"
    assert schema["x-kokoro-compact-jws-max-bytes"] == 16 * 1024
    assert schema["additionalProperties"] is False
    assert set(_array(schema["required"])) == {"protected_header", "claims"}

    properties = _object(schema["properties"])
    header = _object(properties["protected_header"])
    claims = _object(properties["claims"])
    assert header["additionalProperties"] is False
    assert claims["additionalProperties"] is False
    assert set(_array(header["required"])) == HEADER_FIELDS
    assert set(_object(header["properties"])) == HEADER_FIELDS
    assert set(_array(claims["required"])) == CLAIM_FIELDS
    assert set(_object(claims["properties"])) == CLAIM_FIELDS

    for identity_name in ("actor", "subject"):
        identity = _object(_object(claims["properties"])[identity_name])
        assert identity["additionalProperties"] is False
        assert set(_array(identity["required"])) == {"kind", "opaque_ref"}

    for number_name in ("lease_generation", "iat", "exp"):
        number = _object(_object(claims["properties"])[number_name])
        assert number["type"] == "integer"
        assert number["maximum"] == 9_007_199_254_740_991
        assert number["x-kokoro-require-integer-token"] is True


def test_schema_accepts_current_canonical_run_and_identity_values() -> None:
    long_opaque_ref = "subject " + "界" * 600
    request = RunRequest(
        kind="run.request",
        run_id="_run",
        session_id="session/会话 with space",
        feature_key="fixture",
        execution_identity=ExecutionIdentity(
            tenant_ref="租户 with space",
            actor=IdentityRef(kind="user", opaque_ref="actor/含 空格"),
            subject=IdentityRef(kind="project", opaque_ref=long_opaque_ref),
            identity_assertion_ref="assertion",
        ),
        input=RunInput(message_id="message", content="fixture"),
    )
    vectors = _load(VECTORS)
    positive = _object(vectors["positive"])
    header = dict(_object(_object(positive["protected_header"])["decoded"]))
    claims = dict(_object(_object(positive["claims"])["decoded"]))
    header["kid"] = "agent.key.2026"
    claims.update(
        {
            "iss": "urn:kokoro:agent:fixture",
            "tenant_ref": request.execution_identity.tenant_ref,
            "actor": request.execution_identity.actor.model_dump(mode="json"),
            "subject": request.execution_identity.subject.model_dump(mode="json"),
            "run_id": request.run_id,
            "execution_session_id": request.session_id,
        }
    )

    validate_json_schema(
        instance={"protected_header": header, "claims": claims},
        schema=_load(SCHEMA),
        cls=Draft202012Validator,
    )


def test_vectors_fix_exact_canonical_bytes_segments_signature_jwk_and_stages() -> None:
    vectors = _load(VECTORS)
    assert vectors["contract_version"] == "1.0.0"
    positive = _object(vectors["positive"])
    header = _object(positive["protected_header"])
    claims = _object(positive["claims"])

    assert set(_object(header["decoded"])) == HEADER_FIELDS
    assert set(_object(claims["decoded"])) == CLAIM_FIELDS
    assert header["raw_json_utf8"] == header["canonical_json_utf8"]
    assert claims["raw_json_utf8"] == claims["canonical_json_utf8"]
    assert positive["signing_input"] == f"{header['base64url']}.{claims['base64url']}"
    assert positive["compact_jws"] == (
        f"{positive['signing_input']}.{positive['signature_base64url']}"
    )
    assert "=" not in str(positive["compact_jws"])
    assert len(str(positive["signature_base64url"])) == 86
    assert _object(positive["public_jwk"])["kty"] == "OKP"
    assert _object(positive["public_jwk"])["crv"] == "Ed25519"
    assert len(str(positive["jwk_thumbprint_sha256"])) == 43

    stages = {
        str(_object(vector)["name"]): str(_object(vector)["stage"])
        for vector in _array(vectors["negative"])
    }
    assert stages == NEGATIVE_STAGES
    for vector in _array(vectors["negative"]):
        item = _object(vector)
        assert "raw_json_base64url" in item or "encoded_segment" in item


def test_jti_is_canonical_unpadded_base64url_for_exactly_16_bytes() -> None:
    schema = _load(SCHEMA)
    claims = _object(_object(schema["properties"])["claims"])
    jti_schema = _object(_object(claims["properties"])["jti"])
    assert jti_schema["pattern"] == "^[A-Za-z0-9_-]{21}[AQgw]$"
    assert jti_schema["x-kokoro-random-bits"] == 128
    assert jti_schema["x-kokoro-base64url-padding"] == "forbidden"

    vectors = _load(VECTORS)
    positive = _object(vectors["positive"])
    jti = str(_object(_object(positive["claims"])["decoded"])["jti"])
    decoded = base64.urlsafe_b64decode(jti + "==")
    assert len(decoded) == 16
    assert base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii") == jti

    alias = next(
        _object(item)
        for item in _array(vectors["negative"])
        if _object(item)["name"] == "jti_trailing_pad_bits_alias"
    )
    alias_value = str(alias["encoded_segment"])
    assert alias_value != jti
    assert alias_value[:-1] == jti[:-1]
    assert base64.urlsafe_b64decode(alias_value + "==") == decoded


def test_vectors_fix_a_one_bit_signature_tamper_without_claim_drift() -> None:
    vectors = _load(VECTORS)
    positive = _object(vectors["positive"])
    tampered = _object(vectors["tampered_signature"])
    header_segment, claims_segment, signature_segment = str(
        positive["compact_jws"]
    ).split(".")
    tampered_header, tampered_claims, tampered_signature = str(
        tampered["compact_jws"]
    ).split(".")

    assert tampered_header == header_segment
    assert tampered_claims == claims_segment
    assert tampered["signing_input"] == positive["signing_input"]
    assert tampered_signature == tampered["signature_base64url"]
    expected = base64.urlsafe_b64decode(signature_segment + "==")
    changed = base64.urlsafe_b64decode(tampered_signature + "==")
    assert len(changed) == len(expected) == 64
    assert (
        sum((left ^ right).bit_count() for left, right in zip(expected, changed)) == 1
    )


def test_negative_vectors_bind_each_rejection_to_semantic_evidence() -> None:
    vectors = _load(VECTORS)
    items = {
        str(_object(item)["name"]): _object(item)
        for item in _array(vectors["negative"])
    }
    assert FIX_R1_REQUIRED_NEGATIVES <= set(items)

    for name in (
        "duplicate_root_member",
        "duplicate_header_member",
        "duplicate_actor_member",
        "duplicate_claim_member",
    ):
        assert items[name]["error_kind"] == "duplicate_member"
        assert isinstance(items[name]["expected_member"], str)

    for item in items.values():
        if item["stage"] == "schema":
            assert item["error_kind"] == "schema_validation"
            assert str(item["expected_pointer"]).startswith("/")
            assert isinstance(item["expected_keyword"], str)
            assert isinstance(item["difference"], dict)


def test_checker_rejects_malformed_json_substituted_for_duplicate_fixture(
    tmp_path: Path,
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    negatives, duplicate_index, duplicate = _negative(vectors, "duplicate_claim_member")
    duplicate["raw_json_base64url"] = (
        base64.urlsafe_b64encode(b"{").rstrip(b"=").decode("ascii")
    )
    negatives[duplicate_index] = duplicate
    vectors["negative"] = negatives
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="duplicate member fixture"):
        contract_check.validate(repository)


def test_checker_rejects_unknown_claim_substituted_for_exp_bool_fixture(
    tmp_path: Path,
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    negatives = _array(vectors["negative"])
    by_name = {
        str(_object(item)["name"]): index for index, item in enumerate(negatives)
    }
    exp_bool = _object(negatives[by_name["exp_bool"]])
    unknown_claim = _object(negatives[by_name["unknown_claim"]])
    exp_bool["raw_json_base64url"] = unknown_claim["raw_json_base64url"]
    negatives[by_name["exp_bool"]] = exp_bool
    vectors["negative"] = negatives
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="expected schema rejection"):
        contract_check.validate(repository)


def test_checker_rejects_extra_serialization_difference_in_schema_fixture(
    tmp_path: Path,
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    negatives, index, item = _negative(vectors, "exp_bool")
    raw = base64.urlsafe_b64decode(str(item["raw_json_base64url"]) + "==")
    item["raw_json_base64url"] = (
        base64.urlsafe_b64encode(b" " + raw).rstrip(b"=").decode("ascii")
    )
    negatives[index] = item
    vectors["negative"] = negatives
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="canonical single-difference"):
        contract_check.validate(repository)


@pytest.mark.parametrize(
    ("path", "action", "keyword", "value"),
    [
        ((*CLAIMS_SCHEMA_PATH, "operation"), "delete", "type", None),
        ((*CLAIMS_SCHEMA_PATH, "request_binding_sha256"), "delete", "type", None),
        ((), "add", "minProperties", 2),
        (CLAIMS_OBJECT_PATH, "add", "maxProperties", 14),
        ((*CLAIMS_SCHEMA_PATH, "operation"), "add", "const", "skills.resolve"),
        (
            (*CLAIMS_SCHEMA_PATH, "actor", "properties", "kind"),
            "add",
            "const",
            "user",
        ),
        (
            (*CLAIMS_SCHEMA_PATH, "lease_generation"),
            "add",
            "not",
            {"const": 2},
        ),
        ((*CLAIMS_SCHEMA_PATH, "iat"), "add", "enum", [1789236000]),
    ],
    ids=[
        "operation-type-deleted",
        "binding-type-deleted",
        "root-hidden-keyword",
        "object-hidden-keyword",
        "operation-hidden-const",
        "identity-kind-hidden-const",
        "numeric-hidden-not",
        "numeric-hidden-enum",
    ],
)
def test_checker_rejects_exact_schema_semantic_drift_after_digest_recompute(
    tmp_path: Path,
    path: tuple[str, ...],
    action: str,
    keyword: str,
    value: object,
) -> None:
    repository, schema_path, schema = _artifact_fixture(tmp_path, SCHEMA)
    node: Any = schema
    for part in path:
        node = node[part]
    if action == "delete":
        del node[keyword]
    else:
        node[keyword] = value
    _save_artifact(repository, schema_path, schema)

    with pytest.raises(ValueError, match="schema"):
        contract_check.validate(repository)


def test_checker_binds_declared_tamper_to_actual_signature_mutation(
    tmp_path: Path,
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    positive = _object(vectors["positive"])
    tampered = _object(vectors["tampered_signature"])
    changed = bytearray(
        base64.urlsafe_b64decode(str(positive["signature_base64url"]) + "==")
    )
    changed[1] ^= 2
    signature = base64.urlsafe_b64encode(changed).rstrip(b"=").decode("ascii")
    tampered["signature_base64url"] = signature
    tampered["compact_jws"] = f"{tampered['signing_input']}.{signature}"
    vectors["tampered_signature"] = tampered
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="declared mutation"):
        contract_check.validate(repository)


def test_checker_rejects_tamper_metadata_only_drift(tmp_path: Path) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    tampered = _object(vectors["tampered_signature"])
    tampered["difference"] = {"signature_byte_index": 1, "bit_mask": 2}
    vectors["tampered_signature"] = tampered
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="tampered signature"):
        contract_check.validate(repository)


@pytest.mark.parametrize(
    ("target", "source"),
    [
        ("exp_bool", "exp_negative"),
        ("forbidden_header_jku", "forbidden_header_x5u"),
        ("ttl_zero", "ttl_over_60"),
        ("lease_generation_2pow53", "lease_generation_2pow53_plus_1"),
    ],
)
def test_checker_rejects_named_negative_same_stage_substitution(
    tmp_path: Path, target: str, source: str
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    negatives = _array(vectors["negative"])
    by_name = {str(_object(item)["name"]): i for i, item in enumerate(negatives)}
    substitute = dict(_object(negatives[by_name[source]]))
    substitute["name"] = target
    negatives[by_name[target]] = substitute
    vectors["negative"] = negatives
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="named negative metadata"):
        contract_check.validate(repository)


@pytest.mark.parametrize("placement", ["leading", "internal", "trailing"])
def test_checker_rejects_unsafe_maximum_serialization_drift(
    tmp_path: Path, placement: str
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    negatives, index, item = _negative(vectors, "lease_generation_2pow53")
    raw = base64.urlsafe_b64decode(str(item["raw_json_base64url"]) + "==")
    if placement == "leading":
        raw = b" " + raw
    elif placement == "trailing":
        raw += b" "
    else:
        raw = raw.replace(
            b'"lease_generation":9007199254740992',
            b'"lease_generation": 9007199254740992',
        )
    item["raw_json_base64url"] = base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    negatives[index] = item
    vectors["negative"] = negatives
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="single token replacement"):
        contract_check.validate(repository)


def test_checker_rejects_noncanonical_pair_raw_bytes(tmp_path: Path) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    negatives, index, item = _negative(vectors, "ttl_zero")
    raw = base64.urlsafe_b64decode(str(item["raw_json_base64url"]) + "==")
    item["raw_json_base64url"] = (
        base64.urlsafe_b64encode(b" " + raw).rstrip(b"=").decode()
    )
    negatives[index] = item
    vectors["negative"] = negatives
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="pair raw bytes"):
        contract_check.validate(repository)


def test_ttl_over_60_declares_only_the_actual_exp_change() -> None:
    vectors = _load(VECTORS)
    item = next(
        _object(candidate)
        for candidate in _array(vectors["negative"])
        if _object(candidate)["name"] == "ttl_over_60"
    )
    assert item["difference"] == {
        "changes": [{"pointer": "/claims/exp", "value": 1789236061}]
    }


@pytest.mark.parametrize(
    "mutation", ["padding-difference", "jti-difference", "extra-field"]
)
def test_checker_rejects_named_negative_metadata_drift(
    tmp_path: Path, mutation: str
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    name = (
        "jti_trailing_pad_bits_alias"
        if mutation == "jti-difference"
        else "base64url_padding"
    )
    negatives, index, item = _negative(vectors, name)
    if mutation == "padding-difference":
        item["difference"] = {"base": "positive_claims_segment", "suffix": "=="}
    elif mutation == "jti-difference":
        item["difference"] = {"character_index": 20, "from": "D", "to": "E"}
    else:
        item["note"] = "hidden drift"
    negatives[index] = item
    vectors["negative"] = negatives
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="named negative metadata"):
        contract_check.validate(repository)


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("x-kokoro-claim-pair-rules", "exp_must_be_greater_than_iat"), 1),
        ((*CLAIMS_SCHEMA_PATH, "jti", "x-kokoro-random-bits"), 128.0),
        ((*CLAIMS_SCHEMA_PATH, "iat", "x-kokoro-require-integer-token"), 1),
    ],
    ids=["pair-rule-bool-to-int", "jti-int-to-float", "integer-token-bool-to-int"],
)
def test_checker_rejects_json_type_exact_schema_drift(
    tmp_path: Path, path: tuple[str, ...], replacement: object
) -> None:
    repository, schema_path, schema = _artifact_fixture(tmp_path, SCHEMA)
    node: Any = schema
    for part in path[:-1]:
        node = node[part]
    node[path[-1]] = replacement
    _save_artifact(repository, schema_path, schema)

    with pytest.raises(ValueError, match="exact"):
        contract_check.validate(repository)


@pytest.mark.parametrize(
    ("name", "replacement"),
    [("exp_bool", 1), ("ttl_over_60", 1_789_236_061.0)],
    ids=["negative-bool-to-int", "pair-int-to-float"],
)
def test_checker_rejects_json_type_exact_negative_metadata_drift(
    tmp_path: Path, name: str, replacement: object
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    negatives, index, item = _negative(vectors, name)
    difference = _object(item["difference"])
    if name == "ttl_over_60":
        changes = _array(difference["changes"])
        change = _object(changes[0])
        change["value"] = replacement
        changes[0] = change
        difference["changes"] = changes
    else:
        difference["value"] = replacement
    item["difference"] = difference
    negatives[index] = item
    vectors["negative"] = negatives
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="exact"):
        contract_check.validate(repository)


@pytest.mark.parametrize("mutation", ["delete-name", "rename", "extra-field"])
def test_checker_rejects_tamper_complete_identity_and_shape(
    tmp_path: Path, mutation: str
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    tampered = _object(vectors["tampered_signature"])
    if mutation == "delete-name":
        del tampered["name"]
    elif mutation == "rename":
        tampered["name"] = "renamed_tamper"
    else:
        tampered["note"] = "hidden drift"
    vectors["tampered_signature"] = tampered
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="tampered signature"):
        contract_check.validate(repository)


def test_negative_spec_module_is_typed_ordered_unique_and_immutable() -> None:
    source_path = ROOT / "src" / "kokoro_agent" / "execution_proof_negative_specs.py"
    assert source_path.is_file()
    assert isinstance(NEGATIVE_SPECS, tuple)
    names = tuple(spec.name for spec in NEGATIVE_SPECS)
    vectors = _load(VECTORS)
    vector_names = tuple(
        str(_object(item)["name"]) for item in _array(vectors["negative"])
    )
    assert len(NEGATIVE_SPECS) == 31
    assert names == vector_names
    assert len(set(names)) == len(names)
    with pytest.raises(FrozenInstanceError):
        setattr(NEGATIVE_SPECS[0], "name", "mutated")
    difference = next(
        spec.difference for spec in NEGATIVE_SPECS if spec.difference is not None
    )
    with pytest.raises(FrozenInstanceError):
        setattr(difference, "pointer", "/mutated")
    mutable_index: Any = NEGATIVE_SPEC_INDEX
    with pytest.raises(TypeError):
        mutable_index["mutated"] = NEGATIVE_SPECS[0]
    with pytest.raises(ValueError, match="duplicate negative specification"):
        build_negative_spec_index((NEGATIVE_SPECS[0], NEGATIVE_SPECS[0]))
    source = source_path.read_text(encoding="utf-8")
    assert ";;" not in source
    assert "_NEGATIVE_SPEC_TEXT" not in source
    contract_path = ROOT / "src" / "kokoro_agent" / "execution_proof_contract.py"
    for reviewable_path in (source_path, contract_path):
        reviewable_source = reviewable_path.read_text(encoding="utf-8")
        assert max(map(len, reviewable_source.splitlines())) <= 100


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("wire_format", "wire format"),
        ("jti_padding", "jti padding"),
    ],
)
def test_checker_locks_normative_schema_metadata_after_digest_recompute(
    tmp_path: Path, mutation: str, message: str
) -> None:
    repository, schema_path, schema = _artifact_fixture(tmp_path, SCHEMA)
    if mutation == "wire_format":
        schema["x-kokoro-wire-format"] = "detached-jws"
    else:
        properties = _object(schema["properties"])
        claims = _object(properties["claims"])
        claim_properties = _object(claims["properties"])
        jti = _object(claim_properties["jti"])
        jti["x-kokoro-base64url-padding"] = "allowed"
        claim_properties["jti"] = jti
        claims["properties"] = claim_properties
        properties["claims"] = claims
        schema["properties"] = properties
    _save_artifact(repository, schema_path, schema)

    with pytest.raises(ValueError, match=message):
        contract_check.validate(repository)


def test_execution_proof_checker_detail_has_a_focused_module_boundary() -> None:
    detail = ROOT / "src" / "kokoro_agent" / "execution_proof_contract.py"
    assert detail.is_file()
    orchestration = (ROOT / "src" / "kokoro_agent" / "contract_check.py").read_text(
        encoding="utf-8"
    )
    assert "from kokoro_agent.execution_proof_contract import" in orchestration
    assert len(orchestration.splitlines()) < 200


def test_contract_checker_validates_schema_vectors_and_provenance() -> None:
    contract_check.validate(ROOT)


@pytest.mark.parametrize(
    "inventory",
    [
        OWNER_INVENTORY[::-1],
        [OWNER_INVENTORY[0], *OWNER_INVENTORY],
        [*OWNER_INVENTORY, "contract/extra.json"],
    ],
    ids=["reordered", "duplicate", "extra"],
)
def test_checker_rejects_noncanonical_owner_inventory(
    tmp_path: Path, inventory: list[str]
) -> None:
    repository = _copy_contract_repository(tmp_path)
    if "contract/extra.json" in inventory:
        (repository / "contract" / "extra.json").write_text("{}\n", encoding="utf-8")
    provenance = _load(repository / "contract" / "provenance.json")
    provenance["source_files"] = inventory
    _write_provenance(repository, provenance)

    with pytest.raises(ValueError, match="ordered owner inventory"):
        contract_check.validate(repository)


def test_checker_rejects_deleted_artifact_even_if_inventory_and_aggregate_are_rewritten(
    tmp_path: Path,
) -> None:
    repository = _copy_contract_repository(tmp_path)
    (repository / "contract" / "execution-proof" / "v1" / "vectors.json").unlink()
    provenance = _load(repository / "contract" / "provenance.json")
    provenance["source_files"] = [
        source
        for source in _array(provenance["source_files"])
        if source != "contract/execution-proof/v1/vectors.json"
    ]
    _write_provenance(repository, provenance)

    with pytest.raises(
        ValueError, match="contract source is missing|ordered owner inventory"
    ):
        contract_check.validate(repository)


def test_checker_rejects_changed_artifact_when_only_aggregate_is_recomputed(
    tmp_path: Path,
) -> None:
    repository = _copy_contract_repository(tmp_path)
    schema_path = repository / "contract" / "execution-proof" / "v1" / "schema.json"
    schema = _load(schema_path)
    schema["description"] = "mutated after review"
    schema_path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    provenance = _load(repository / "contract" / "provenance.json")
    _write_provenance(repository, provenance)

    with pytest.raises(ValueError, match="schema artifact digest"):
        contract_check.validate(repository)


def test_checker_rejects_version_drift_even_with_all_digests_recomputed(
    tmp_path: Path,
) -> None:
    repository = _copy_contract_repository(tmp_path)
    schema_path = repository / "contract" / "execution-proof" / "v1" / "schema.json"
    schema = _load(schema_path)
    schema["x-kokoro-contract-version"] = "1.0.1"
    schema_path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    provenance = _load(repository / "contract" / "provenance.json")
    proof = _object(provenance["execution_proof"])
    proof["version"] = "1.0.1"
    schema_record = _object(proof["schema"])
    schema_record["sha256"] = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    _write_provenance(repository, provenance)

    with pytest.raises(ValueError, match="version must be 1.0.0"):
        contract_check.validate(repository)


@pytest.mark.parametrize(
    "negative_name",
    [
        "duplicate_claim_member",
        "lease_generation_float",
        "exp_bool",
        "lease_generation_2pow53",
        "noncanonical_claims",
        "base64url_padding",
    ],
)
def test_checker_requires_each_security_rejection_vector(
    tmp_path: Path, negative_name: str
) -> None:
    repository, vector_path, vectors = _artifact_fixture(tmp_path, VECTORS)
    vectors["negative"] = [
        item
        for item in _array(vectors["negative"])
        if _object(item)["name"] != negative_name
    ]
    _save_artifact(repository, vector_path, vectors)

    with pytest.raises(ValueError, match="negative vector inventory"):
        contract_check.validate(repository)
