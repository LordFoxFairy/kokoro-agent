"""Exact CustomAPI profile choice is immutable intent, never a trace or an authorization grant."""

import json

import pytest
from pydantic import ValidationError

from kokoro_agent.interfaces.http.ingress import (
    AgentIngress,
    IngressError,
)
from kokoro_agent.application.chat.service import ChatService
from kokoro_agent.protocol import RunRequest, inbound_adapter
from kokoro_agent.protocol.control import CustomApiProfileSelection
from support.chat import FakeChatRepository
from support.fakes import FakeBus, FakeRunRepository
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef


def identity() -> ExecutionIdentity:
    return ExecutionIdentity(
        tenant_ref="tenant",
        actor=IdentityRef(kind="user", opaque_ref="actor"),
        subject=IdentityRef(kind="user", opaque_ref="subject"),
        identity_assertion_ref="assertion",
    )


def _body() -> dict[str, object]:
    return {
        "request_id": "request-legacy",
        "run_id": "legacy",
        "session_id": "session",
        "feature_key": "chat",
        "project_ref": None,
        "selected_skill_source_refs": [],
        "message_id": "message",
        "content": "hello",
    }


async def _admit(body: dict[str, object]) -> tuple[RunRequest, str]:
    repository = FakeRunRepository()
    ingress = AgentIngress(
        bus=FakeBus(),
        run_repository=repository,
        chat_service=ChatService(FakeChatRepository()),
    )
    await ingress.launch(body, execution_identity=identity())
    return repository.dispatch_requests["legacy"], repository.dispatch_fences["legacy"]


LEGACY_JSON = '{"kind":"run.request","request_id":"request-legacy","run_id":"legacy","session_id":"session","feature_key":"chat","project_ref":null,"selected_skill_source_refs":[],"execution_identity":{"tenant_ref":"tenant","actor":{"kind":"user","opaque_ref":"actor"},"subject":{"kind":"user","opaque_ref":"subject"},"identity_assertion_ref":"assertion"},"input":{"message_id":"message","content":"hello"},"requested_model_label":null,"trace":null}'
LEGACY_FENCE = "sha256:e2f309d47b13c7c0f9b1d92797d1f15a79cdf282bcc9c0d6ec7876fcccc15fd2"


@pytest.mark.parametrize("explicit", [False, True])
async def test_empty_choice_preserves_published_request_bytes_and_fence(
    explicit: bool,
) -> None:
    body = _body()
    if explicit:
        body["selected_custom_api_profiles"] = []
    request, fence = await _admit(body)
    assert request.selected_custom_api_profiles == ()
    assert request.model_dump_json() == LEGACY_JSON
    assert fence == LEGACY_FENCE
    assert RunRequest.model_validate_json(LEGACY_JSON).model_dump_json() == LEGACY_JSON
    assert inbound_adapter.validate_json(LEGACY_JSON) == request


async def test_nonempty_choice_roundtrips_in_original_order_and_changes_fence() -> None:
    body = {
        **_body(),
        "selected_custom_api_profiles": [
            {"profile_id": "profile-b", "expected_state_version": 2},
            {"profile_id": "profile-a", "expected_state_version": 1},
        ],
    }
    request, fence = await _admit(body)
    assert request.selected_custom_api_profiles == (
        CustomApiProfileSelection(profile_id="profile-b", expected_state_version=2),
        CustomApiProfileSelection(profile_id="profile-a", expected_state_version=1),
    )
    assert (
        json.loads(request.model_dump_json())["selected_custom_api_profiles"]
        == body["selected_custom_api_profiles"]
    )
    assert inbound_adapter.validate_json(request.model_dump_json()) == request
    assert fence != LEGACY_FENCE
    with pytest.raises(ValidationError):
        request.selected_custom_api_profiles = ()


@pytest.mark.parametrize(
    "value",
    [
        None,
        "profile-a",
        1,
        {},
        [1],
        [True],
        [None],
        [""],
        [{"profile_id": "profile"}],
        [{"profile_id": "a\n", "expected_state_version": 1}],
        [{"profile_id": "a" * 192, "expected_state_version": 1}],
        [{"profile_id": "a", "expected_state_version": version} for version in (1, 2)],
        [{"profile_id": f"p{i}", "expected_state_version": 1} for i in range(33)],
        *[
            [{"profile_id": "p", "expected_state_version": version}]
            for version in (True, "1", 1.5, 0, -1, 2147483648)
        ],
        *[
            [{"profile_id": "p", "expected_state_version": 1, field: "secret"}]
            for field in ("name", "instructions", "environment", "value")
        ],
    ],
)
async def test_malformed_choices_fail_at_admission(value: object) -> None:
    with pytest.raises(IngressError) as error:
        await _admit({**_body(), "selected_custom_api_profiles": value})
    assert (error.value.status, error.value.code) == (400, "invalid_launch_request")


def test_direct_worker_choice_uses_same_strict_boundary() -> None:
    raw = json.loads(LEGACY_JSON)
    raw["selected_custom_api_profiles"] = [
        {"profile_id": "profile-1", "expected_state_version": 1}
    ]
    request = inbound_adapter.validate_python(raw)
    assert isinstance(request, RunRequest)
    assert request.selected_custom_api_profiles == (
        CustomApiProfileSelection(profile_id="profile-1", expected_state_version=1),
    )
    for invalid in (["x", "x"], [1], {"connection-1"}, ["a\n"]):
        with pytest.raises(ValidationError):
            inbound_adapter.validate_python(
                {**raw, "selected_custom_api_profiles": invalid}
            )


async def test_exact_choice_is_durable_intent_and_same_run_drift_conflicts() -> None:
    repository, bus = FakeRunRepository(), FakeBus()
    ingress = AgentIngress(
        bus=bus,
        run_repository=repository,
        chat_service=ChatService(FakeChatRepository()),
    )
    body = {
        **_body(),
        "selected_custom_api_profiles": [
            {"profile_id": "profile-b", "expected_state_version": 2},
            {"profile_id": "profile-a", "expected_state_version": 1},
        ],
    }
    assert not (await ingress.launch(body, execution_identity=identity())).replayed
    assert (
        await repository.claim_dispatch(
            repository.dispatch_requests["legacy"], "worker"
        )
        is not None
    )
    assert (await ingress.launch(body, execution_identity=identity())).replayed
    assert repository.dispatch_requests["legacy"].selected_custom_api_profiles == (
        CustomApiProfileSelection(profile_id="profile-b", expected_state_version=2),
        CustomApiProfileSelection(profile_id="profile-a", expected_state_version=1),
    )
    assert len(bus.published) == 1
    assert (
        bus.published[0][1]["selected_custom_api_profiles"]
        == body["selected_custom_api_profiles"]
    )
    for changed in (
        [],
        [
            {"profile_id": "profile-a", "expected_state_version": 1},
            {"profile_id": "profile-b", "expected_state_version": 2},
        ],
        [
            {"profile_id": "profile-b", "expected_state_version": 3},
            {"profile_id": "profile-a", "expected_state_version": 1},
        ],
    ):
        with pytest.raises(IngressError) as error:
            await ingress.launch(
                {**body, "selected_custom_api_profiles": changed},
                execution_identity=identity(),
            )
        assert error.value.status == 409
    assert len(bus.published) == 1


def test_profile_selections_are_deeply_frozen_and_boundary_versions_are_valid() -> None:
    for version in (1, 2147483647):
        selection = CustomApiProfileSelection(
            profile_id="p", expected_state_version=version
        )
        with pytest.raises(ValidationError):
            selection.expected_state_version = 2
    raw = json.loads(LEGACY_JSON)
    raw["selected_custom_api_profiles"] = [
        {"profile_id": f"p{i}", "expected_state_version": 1} for i in range(32)
    ]
    request = RunRequest.model_validate(raw)
    assert len(request.selected_custom_api_profiles) == 32
    assert RunRequest.model_validate_json(request.model_dump_json()) == request


async def test_pending_dispatch_retry_republishes_original_safe_refs() -> None:
    repository, bus = FakeRunRepository(), FakeBus()
    ingress = AgentIngress(
        bus=bus,
        run_repository=repository,
        chat_service=ChatService(FakeChatRepository()),
    )
    body = {
        **_body(),
        "selected_custom_api_profiles": [
            {"profile_id": "p", "expected_state_version": 4}
        ],
    }
    await ingress.launch(body, execution_identity=identity())
    original = bus.published[0][1]
    persisted = repository.dispatch_requests["legacy"].model_dump_json()
    # Same parsing path used by the PostgreSQL pending-dispatch reader on recovery.
    restored = RunRequest.model_validate_json(persisted)
    repository.dispatch_requests["legacy"] = restored
    assert (await ingress.launch(body, execution_identity=identity())).replayed
    assert len(bus.published) == 2
    assert bus.published[1][1] == original
    assert restored.selected_custom_api_profiles == (
        CustomApiProfileSelection(profile_id="p", expected_state_version=4),
    )


def _asset_ref(**changes: object) -> dict[str, object]:
    return {
        "kind": "asset",
        "resource_id": "asset-1",
        "asset_id": "asset-1",
        "content_sha256": "a" * 64,
        "size_bytes": "0",
        "mime_type": "text/plain",
        "filename": "资料.txt",
        "source_scope": {"kind": "personal", "id": "subject"},
        **changes,
    }


def _artifact_ref(**changes: object) -> dict[str, object]:
    return {
        **_asset_ref(),
        "kind": "artifact",
        "resource_id": "artifact-1",
        "source_scope": {"kind": "conversation", "id": "previous-conversation"},
        "source_run_id": "previous-run",
        **changes,
    }


@pytest.mark.parametrize("explicit", [False, True])
async def test_empty_resource_selection_preserves_request_bytes_and_fence(
    explicit: bool,
) -> None:
    body = _body()
    if explicit:
        body["selected_resource_refs"] = []
    request, fence = await _admit(body)
    assert request.model_dump_json() == LEGACY_JSON
    assert fence == LEGACY_FENCE


async def test_canonical_resources_preserve_source_and_consumer_run_on_recovery() -> (
    None
):
    body = {
        **_body(),
        "selected_resource_refs": [
            _asset_ref(size_bytes="18446744073709551615"),
            _artifact_ref(),
        ],
    }
    request, fence = await _admit(body)
    assert (
        json.loads(request.model_dump_json())["selected_resource_refs"]
        == body["selected_resource_refs"]
    )
    restored = inbound_adapter.validate_json(request.model_dump_json())
    assert restored == request
    assert request.has_private_prompt_material
    assert request.run_id == "legacy"
    assert request.selected_resource_refs[1].kind == "artifact"
    assert request.selected_resource_refs[1].source_run_id == "previous-run"
    assert fence != LEGACY_FENCE
    _, changed = await _admit(
        {
            **body,
            "selected_resource_refs": [
                _asset_ref(content_sha256="b" * 64),
                _artifact_ref(),
            ],
        }
    )
    assert changed != fence
    with pytest.raises(ValidationError):
        request.selected_resource_refs[0].source_scope.id = "other"


@pytest.mark.parametrize(
    "value",
    [
        None,
        "asset-1",
        [{"kind": "asset", "resource_id": "asset-1"}],
        [_asset_ref(kind="url")],
        [_asset_ref(url="https://x")],
        [_asset_ref(content="secret")],
        [_asset_ref(version_id="v1")],
        [_asset_ref(content_sha256="A" * 64)],
        [_asset_ref(resource_id="a\n")],
        [_asset_ref(asset_id="other")],
        [_asset_ref(), _asset_ref(filename="other.txt")],
        [_asset_ref(size_bytes=value) for value in (0, True)],
        *[
            [_asset_ref(size_bytes=value)]
            for value in ("01", "-1", "1.0", "1\n", "18446744073709551616")
        ],
        [_artifact_ref(source_run_id=None)],
        [_artifact_ref(source_scope={"kind": "personal", "id": "subject"})],
        [_asset_ref(source_scope={"kind": "conversation", "id": "session"})],
        [_asset_ref(source_scope={"kind": "personal", "id": "actor"})],
        [_asset_ref(source_scope={"kind": "project", "id": "other-project"})],
        *[
            [_asset_ref(filename=name)]
            for name in ("bad\n", "../file", "a\\b", ".", "..")
        ],
        [_asset_ref(mime_type="text/plain; charset=utf-8")],
        [_asset_ref(filename="x\x00")],
        [_asset_ref(filename="x\ud800")],
        [_asset_ref(filename="x" * 256)],
        [_asset_ref(mime_type="x" * 192)],
        [
            _asset_ref(resource_id=f"asset-{i}", asset_id=f"asset-{i}")
            for i in range(17)
        ],
    ],
)
async def test_malformed_resource_refs_fail_at_admission(value: object) -> None:
    with pytest.raises(IngressError) as error:
        await _admit({**_body(), "selected_resource_refs": value})
    assert (error.value.status, error.value.code) == (400, "invalid_launch_request")


async def test_project_resource_must_match_launch_project_and_direct_worker_boundary() -> (
    None
):
    ref = _asset_ref(source_scope={"kind": "project", "id": "project-1"})
    request, _ = await _admit(
        {**_body(), "project_ref": "project-1", "selected_resource_refs": [ref]}
    )
    assert inbound_adapter.validate_json(request.model_dump_json()) == request
    raw = json.loads(request.model_dump_json())
    raw["project_ref"] = "other-project"
    with pytest.raises(ValidationError):
        RunRequest.model_validate(raw)


async def test_resource_budget_is_bounded_for_complete_canonical_manifest() -> None:
    refs = [
        _asset_ref(resource_id=f"asset-{i}", asset_id=f"asset-{i}", filename="x" * 255)
        for i in range(16)
    ]
    assert (
        len(json.dumps(refs, ensure_ascii=True, separators=(",", ":")).encode()) > 4096
    )
    request, _ = await _admit({**_body(), "selected_resource_refs": refs})
    assert len(request.selected_resource_refs) == 16
    oversized = [
        {
            **ref,
            "resource_id": f"a{i:02}-" + "x" * 187,
            "asset_id": f"a{i:02}-" + "x" * 187,
            "filename": "界" * 255,
            "mime_type": "x" * 95 + "/" + "x" * 95,
        }
        for i, ref in enumerate(refs)
    ]
    with pytest.raises(IngressError):
        await _admit({**_body(), "selected_resource_refs": oversized})
