from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from pathlib import Path
from typing import TypedDict, cast

import pytest

from kokoro_agent.interaction.domain.identities import (
    CanonicalInput,
    IdentityContractError,
    InteractionIdentityFactory,
)
from kokoro_agent.interaction.generated.contract_metadata import (
    AGENT_IDENTITY_CORPUS_SHA256,
    ROOT_CONTRACT_COMMIT,
    ROOT_IDENTITY_CORPUS_SHA256,
)


class _Vector(TypedDict):
    kind: str
    material: dict[str, CanonicalInput]
    canonical_json: str
    expected_ref: str


class _Corpus(TypedDict):
    vectors: list[_Vector]


def _corpus() -> _Corpus:
    payload = (
        files("kokoro_agent.interaction.generated")
        .joinpath("agent_interaction_identity_v2.json")
        .read_bytes()
    )
    assert hashlib.sha256(payload).hexdigest() == AGENT_IDENTITY_CORPUS_SHA256
    return cast(_Corpus, json.loads(payload))


def test_identity_contract_is_pinned_to_adr_014_root_commit() -> None:
    assert ROOT_CONTRACT_COMMIT == "1d60b01"


def test_factory_matches_only_agent_owned_root_identity_vectors() -> None:
    corpus = _corpus()
    vectors = corpus["vectors"]
    assert [vector["kind"] for vector in vectors] == [
        "application_request",
        "interaction_owner",
        "projection_event",
        "group_projection",
    ]

    factory = InteractionIdentityFactory()
    for vector in vectors:
        material = vector["material"]
        derived = factory.derive_contract_ref(
            kind=str(vector["kind"]),
            material=material,
        )
        assert derived.canonical_json == vector["canonical_json"]
        assert derived.value == vector["expected_ref"]


@pytest.mark.parametrize(
    ("kind", "material"),
    [
        ("projection_event", {"run_id": "r", "owner_revision": 1}),
        ("projection_event", {"run_id": "r", "owner_revision": "01"}),
        ("projection_event", {"run_id": "r", "owner_revision": "1", "x": "y"}),
        ("unknown", {"run_id": "r"}),
    ],
)
def test_factory_rejects_noncanonical_or_drifting_material(
    kind: str, material: dict[str, object]
) -> None:
    with pytest.raises(IdentityContractError):
        InteractionIdentityFactory().derive_contract_ref(
            kind=kind,
            material=cast(dict[str, CanonicalInput], material),
        )


@pytest.mark.parametrize(
    "session_owned_plane",
    [
        "human_decision",
        "decision_receipt",
        "run_resume",
        "resume_receipt",
        "resume_receipt_event",
    ],
)
def test_factory_does_not_derive_session_or_control_owned_identity_planes(
    session_owned_plane: str,
) -> None:
    with pytest.raises(IdentityContractError, match="unknown identity plane"):
        InteractionIdentityFactory().derive_contract_ref(
            kind=session_owned_plane,
            material={"run_id": "run-1"},
        )


@pytest.mark.parametrize("noncanonical", [1, True, b"iown-bytes"])
def test_factory_fail_closes_exact_material_with_noncanonical_scalar(
    noncanonical: object,
) -> None:
    material = cast(
        dict[str, CanonicalInput],
        {
            "run_id": "run-1",
            "interaction_owner_ref": "iown-1",
            "owner_revision": noncanonical,
        },
    )
    with pytest.raises(IdentityContractError):
        InteractionIdentityFactory().derive_contract_ref(
            kind="projection_event", material=material
        )


def test_factory_fail_closes_nested_non_string_mapping_key() -> None:
    material = cast(
        dict[str, CanonicalInput],
        {
            "run_id": "run-1",
            "interaction_owner_ref": "iown-1",
            "owner_revision": {7: "not-a-string-key"},
        },
    )
    with pytest.raises(IdentityContractError):
        InteractionIdentityFactory().derive_contract_ref(
            kind="projection_event", material=material
        )


def test_agent_typed_origin_helpers_do_not_accept_site_or_subject_axes() -> None:
    factory = InteractionIdentityFactory()
    request = factory.application_request(
        run_id="run-1",
        stable_task_path="root/research",
        origin_tool_call_ref="tool-1",
        interaction_kind="approval",
        elicitation_ordinal=1,
    )
    owner = factory.interaction_owner(
        run_id="run-1",
        stable_task_path="root/research",
        origin_tool_call_ref="tool-1",
        interaction_kind="approval",
        application_request_ref=request.value,
    )
    assert request.value.startswith("areq_")
    assert owner.value.startswith("iown_")
    assert "site" not in request.canonical_json
    assert "subject" not in owner.canonical_json


def test_agent_corpus_records_root_digest_without_distributing_session_material() -> (
    None
):
    root_corpus = (
        Path(__file__).resolve().parents[2]
        / "contract/corpus/interaction-identity-v2.json"
    )
    if not root_corpus.is_file():
        pytest.skip("standalone Agent clone has no Root contract checkout")
    root_bytes = root_corpus.read_bytes()
    assert hashlib.sha256(root_bytes).hexdigest() == ROOT_IDENTITY_CORPUS_SHA256
    committed = (
        files("kokoro_agent.interaction.generated")
        .joinpath("agent_interaction_identity_v2.json")
        .read_bytes()
    )
    assert committed != root_bytes
    for forbidden in (b"site_id", b"session_id", b"actor_subject"):
        assert forbidden not in committed


def test_interaction_runtime_sources_have_no_session_identity_material() -> None:
    package = Path(__file__).resolve().parents[1] / "src/kokoro_agent/interaction"
    offenders = {
        source.relative_to(package): forbidden
        for source in package.rglob("*")
        if source.is_file() and "__pycache__" not in source.parts
        for forbidden in (b"site_id", b"session_id", b"actor_subject")
        if forbidden in source.read_bytes()
    }
    assert offenders == {}
