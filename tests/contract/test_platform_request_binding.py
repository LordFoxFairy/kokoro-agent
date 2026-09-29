from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter

from kokoro_agent.generated.kokoro.common.v1 import common_pb
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as platform_pb


ROOT = Path(__file__).parents[2]
ARTIFACT_ROOT = ROOT / "contract/platform/v1/execution-operations/v3"
PIN_PATH = ROOT / "contract/platform/v1/provenance.json"
EXPECTED_AGGREGATE = "324e749da1bc66c1ff03de74e7299716f798f5f5bb5fa19556033b79fa09ff8d"


def test_runtime_binding_uses_current_platform_v3_owner_release() -> None:
    from kokoro_agent.execution.platform_request_binding_values import BINDING_VERSION

    pin = _json(PIN_PATH)
    execution = _object(pin["execution_operations"])
    assert pin["owner_commit"] == "5b6eb2c1532b23b9747bc4bf6ac99f69ad453de0"
    assert execution["aggregate_sha256"] == (
        "324e749da1bc66c1ff03de74e7299716f798f5f5bb5fa19556033b79fa09ff8d"
    )
    assert BINDING_VERSION == "3.0.0"


_OBJECT = TypeAdapter(dict[str, object])
_OBJECT_LIST = TypeAdapter(list[object])


def _json(path: Path) -> dict[str, object]:
    return _OBJECT.validate_json(path.read_bytes())


def _object(value: object) -> dict[str, object]:
    return _OBJECT.validate_python(value, strict=True)


def _list(value: object) -> list[object]:
    return _OBJECT_LIST.validate_python(value, strict=True)


def _string(value: object) -> str:
    assert isinstance(value, str)
    return value


def _decode(value: object) -> bytes:
    assert isinstance(value, str)
    return base64.b64decode(value, validate=True)


def _artifact_fixture(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    destination = repository / "contract/platform/v1"
    shutil.copytree(ROOT / "contract/platform/v1", destination)
    return repository


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _positive_vectors() -> dict[str, dict[str, object]]:
    document = _json(ARTIFACT_ROOT / "vectors/positive.json")
    vectors = [_object(vector) for vector in _list(document["vectors"])]
    return {_string(vector["name"]): vector for vector in vectors}


def _field_kind(member: str) -> tuple[str, str]:
    name, kind = member.split(":", 1)
    return name, kind


def _message_value(kind: str, value: object) -> object:
    presence = _object(value)
    assert presence["present"] is True
    inner = presence["value"]
    if kind == "command":
        command = _object(inner)
        return common_pb.CommandIdentity(
            command_id=_string(command["command_id"]),
            request_digest=_string(command["request_digest"]),
        )
    if kind == "page":
        page = _object(inner)
        cursor = _object(page["cursor"])
        limit = page["limit"]
        assert type(limit) is int
        cursor_value: str | None = None
        if cursor["present"]:
            cursor_value = _string(cursor["value"])
        return common_pb.PageRequest(limit=limit, cursor=cursor_value)
    if kind.startswith("id<"):
        wrapper = kind.removeprefix("id<").removesuffix(">")
        wrapper_type = getattr(platform_pb, wrapper)
        return wrapper_type(value=_string(inner))
    if kind == "msg<OwnerScope>":
        owner = _object(inner)
        return platform_pb.OwnerScope(
            kind=_string(owner["kind"]), id=_string(owner["id"])
        )
    if kind.startswith("opt<"):
        return inner
    raise AssertionError(f"unhandled test projection kind: {kind}")


def _request_from_owner_vector(
    binding: dict[str, object], vector: dict[str, object]
) -> object:
    raw = _OBJECT.validate_json(_decode(vector["rawBase64"]))
    request_projection = _object(raw["request"])
    fq_method = _string(binding["fqMethod"])
    method_name = fq_method.rsplit("/", 1)[1]
    kwargs: dict[str, object] = {"request_id": _string(raw["request_id"])}
    request_members = _list(binding["requestMembers"])
    for member_value in request_members:
        member = _string(member_value)
        name, kind = _field_kind(member)
        value = request_projection[name]
        if kind in {"command", "page"} or kind.startswith(("id<", "msg<", "opt<")):
            presence = _object(value)
            if presence["present"]:
                kwargs[name] = _message_value(kind, value)
        else:
            kwargs[name] = value
    request_type = getattr(platform_pb, f"{method_name}Request")
    return request_type(**kwargs)


def test_vendored_execution_operation_artifact_matches_owner_provenance() -> None:
    pin = _json(PIN_PATH)
    execution = _object(pin["execution_operations"])
    assert execution["owner_commit"] == pin["owner_commit"]
    assert execution["aggregate_sha256"] == EXPECTED_AGGREGATE
    sources = [_object(source) for source in _list(execution["sources"])]
    assert len(sources) == 17
    for source in sources:
        path = _string(source["path"])
        assert (
            hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == source["sha256"]
        )
    owner_provenance = _json(ARTIFACT_ROOT / "provenance.json")
    assert owner_provenance["aggregateSha256"] == EXPECTED_AGGREGATE
    assert len(_list(owner_provenance["files"])) == 16


def test_owner_vector_inventory_is_enforced_by_the_contract_checker() -> None:
    from kokoro_agent.platform_binding_contract import (
        validate_platform_binding_artifact,
    )

    assert validate_platform_binding_artifact(ROOT) == (53, 142, 139)


def test_v3_vendor_rejects_extra_file_even_with_unchanged_provenance(
    tmp_path: Path,
) -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
    )
    from kokoro_agent.platform_binding_contract import (
        validate_platform_binding_artifact,
    )

    repository = _artifact_fixture(tmp_path)
    extra = repository / "contract/platform/v1/execution-operations/v3/extra.json"
    extra.write_text("{}\n", encoding="utf-8")
    with pytest.raises(PlatformRequestBindingError):
        validate_platform_binding_artifact(repository)


@pytest.mark.parametrize(
    "tamper",
    [
        "owner-repository",
        "owner-commit",
        "owner-path",
        "duplicate",
        "missing",
        "extra",
        "path-substitution",
    ],
)
def test_execution_pin_rejects_metadata_and_exact_inventory_drift(
    tmp_path: Path, tamper: str
) -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
    )
    from kokoro_agent.platform_binding_contract import (
        validate_platform_binding_artifact,
    )

    repository = _artifact_fixture(tmp_path)
    pin_path = repository / "contract/platform/v1/provenance.json"
    pin = _json(pin_path)
    execution = _object(pin["execution_operations"])
    sources = [_object(source) for source in _list(execution["sources"])]
    if tamper == "owner-repository":
        pin["owner_repository"] = "apps/substituted-owner"
    elif tamper == "owner-commit":
        pin["owner_commit"] = "0" * 40
        execution["owner_commit"] = "0" * 40
    elif tamper == "owner-path":
        sources[0]["owner_path"] = "contract/substituted.json"
    elif tamper == "duplicate":
        sources.append(dict(sources[0]))
    elif tamper == "missing":
        sources.pop()
    elif tamper == "extra":
        sources.append(
            {
                "path": sources[0]["path"],
                "owner_path": "contract/execution-operations/v3/extra.json",
                "sha256": sources[0]["sha256"],
            }
        )
    elif tamper == "path-substitution":
        sources[0]["path"] = sources[1]["path"]
        sources[0]["sha256"] = sources[1]["sha256"]
    else:
        raise AssertionError(tamper)
    execution["sources"] = sources
    pin["execution_operations"] = execution
    _write_json(pin_path, pin)

    with pytest.raises(PlatformRequestBindingError):
        validate_platform_binding_artifact(repository)


def test_execution_pin_rejects_changed_owner_provenance_with_synced_pin_digest(
    tmp_path: Path,
) -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
    )
    from kokoro_agent.platform_binding_contract import (
        validate_platform_binding_artifact,
    )

    repository = _artifact_fixture(tmp_path)
    provenance_path = (
        repository / "contract/platform/v1/execution-operations/v3/provenance.json"
    )
    provenance_path.write_bytes(provenance_path.read_bytes() + b"\n")
    pin_path = repository / "contract/platform/v1/provenance.json"
    pin = _json(pin_path)
    execution = _object(pin["execution_operations"])
    sources = [_object(source) for source in _list(execution["sources"])]
    for source in sources:
        if source["path"] == (
            "contract/platform/v1/execution-operations/v3/provenance.json"
        ):
            source["sha256"] = hashlib.sha256(provenance_path.read_bytes()).hexdigest()
    execution["sources"] = sources
    pin["execution_operations"] = execution
    _write_json(pin_path, pin)

    with pytest.raises(PlatformRequestBindingError):
        validate_platform_binding_artifact(repository)


def test_generated_descriptor_maps_exactly_the_24_tenant_operations() -> None:
    from kokoro_agent.generated.platform_request_projector import (
        FQ_METHOD_BY_OPERATION,
        OPERATION_BY_FQ_METHOD,
    )

    catalog = _json(ARTIFACT_ROOT / "operation-catalog.json")
    operations = [_object(row) for row in _list(catalog["operations"])]
    tenant = [row for row in operations if row["class"] == "tenant-execution"]
    expected = {_string(row["operation"]): _string(row["fqMethod"]) for row in tenant}
    assert FQ_METHOD_BY_OPERATION == expected
    assert OPERATION_BY_FQ_METHOD == {
        method: operation for operation, method in expected.items()
    }
    assert len(expected) == 24


def test_typed_projector_matches_23_owner_preimages_and_canonical_sha256() -> None:
    from kokoro_agent.execution.platform_request_binding import project_request_binding

    artifact = _json(ARTIFACT_ROOT / "request-bindings.json")
    bindings = [_object(row) for row in _list(artifact["bindings"])]
    vectors = _positive_vectors()
    checked = 0
    for binding in bindings:
        operation = _string(binding["operation"])
        if operation == "mcp.authorize_tool":
            continue
        vector = vectors[f"binding.{operation}.valid"]
        request = _request_from_owner_vector(binding, vector)
        projected = project_request_binding(tenant_ref="tenant-1", request=request)
        assert projected.operation == operation
        assert projected.fq_method == binding["fqMethod"]
        assert projected.canonical_bytes == _decode(vector["canonicalBase64"])
        assert projected.sha256 == vector["sha256"]
        checked += 1
    assert checked == 23


_AGENT_OPERATIONS = {
    "skill.resolve_visible_skill",
    "skill.get_approved_package_reference",
    "mcp.get_connector",
    "mcp.get_connection",
    "mcp.list_connector_capabilities",
    "mcp.authorize_tool",
}


def test_six_agent_rpc_v3_positive_and_negative_owner_binding_vectors() -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
        project_request_binding,
    )
    from kokoro_agent.platform_binding_contract import (
        strict_parse_raw_json,
        validate_projected_binding,
    )

    bindings = [
        _object(row)
        for row in _list(_json(ARTIFACT_ROOT / "request-bindings.json")["bindings"])
    ]
    selected = {
        _string(row["operation"]): row
        for row in bindings
        if row["operation"] in _AGENT_OPERATIONS
    }
    assert set(selected) == _AGENT_OPERATIONS
    positives = _positive_vectors()
    for operation, binding in selected.items():
        vector = positives[f"binding.{operation}.valid"]
        raw = strict_parse_raw_json(_decode(vector["rawBase64"]))
        validate_projected_binding(raw, binding=binding)
        if operation != "mcp.authorize_tool":
            request = _request_from_owner_vector(binding, vector)
            actual = project_request_binding(tenant_ref="tenant-1", request=request)
            assert actual.canonical_bytes == _decode(vector["canonicalBase64"])
            assert actual.sha256 == vector["sha256"]

    negatives = [
        _object(row)
        for row in _list(_json(ARTIFACT_ROOT / "vectors/negative.json")["vectors"])
    ]
    rejected = 0
    for vector in negatives:
        method = vector.get("fqMethod")
        matching = next(
            (row for row in selected.values() if row["fqMethod"] == method), None
        )
        if vector.get("target") != "binding" or matching is None:
            continue
        with pytest.raises(PlatformRequestBindingError):
            validate_projected_binding(
                strict_parse_raw_json(_decode(vector["rawBase64"])), binding=matching
            )
        rejected += 1
    assert rejected == 24


def test_authorize_tool_hashes_the_actual_raw_typed_argument_bytes() -> None:
    from kokoro_agent.execution.platform_request_binding import project_request_binding

    vectors = _positive_vectors()
    owner_binding = vectors["binding.mcp.authorize_tool.valid"]
    owner_bytes = vectors["bytes.typed-arguments.raw-sha256"]
    raw = _decode(owner_bytes["rawBase64"])
    assert hashlib.sha256(raw).hexdigest() == owner_bytes["sha256"]
    expected = _OBJECT.validate_json(_decode(owner_binding["rawBase64"]))
    owner_canonical = _decode(owner_binding["canonicalBase64"])
    assert hashlib.sha256(owner_canonical).hexdigest() == owner_binding["sha256"]
    expected_request = _object(expected["request"])
    connector = _object(expected_request["connector_id"])
    assert connector["present"] is True
    assert expected_request["typed_arguments_sha256"] == "a" * 64
    digest_member = b'"typed_arguments_sha256":"' + b"a" * 64 + b'"'
    assert owner_canonical.count(b"a" * 64) == 1
    assert owner_canonical.count(digest_member) == 1
    digest = _string(owner_bytes["sha256"]).encode("ascii")
    assert len(digest) == 64
    expected_canonical = owner_canonical.replace(
        digest_member, b'"typed_arguments_sha256":"' + digest + b'"', 1
    )

    request = platform_pb.AuthorizeMcpToolRequest(
        request_id=_string(expected["request_id"]),
        connector_id=platform_pb.McpConnectorId(value=_string(connector["value"])),
        tool_selector=_string(expected_request["tool_selector"]),
        typed_arguments_json=raw,
        idempotency_key=_string(expected_request["idempotency_key"]),
    )
    result = project_request_binding(
        tenant_ref=_string(expected["tenant_ref"]), request=request
    )
    assert result.canonical_bytes == expected_canonical
    assert result.sha256 == hashlib.sha256(expected_canonical).hexdigest()


def test_binding_uses_utf16_key_order_and_rejects_lone_surrogates() -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
        project_request_binding,
    )
    from kokoro_agent.execution.platform_request_binding_values import (
        canonical_binding_bytes,
    )

    assert canonical_binding_bytes({"\ue000": 2, "\U00010000": 1}) == (
        b'{"\xf0\x90\x80\x80":1,"\xee\x80\x80":2}'
    )
    with pytest.raises(PlatformRequestBindingError):
        project_request_binding(
            tenant_ref="tenant-1",
            request=platform_pb.DiscoverVisibleSkillsRequest(
                request_id="request-1", query="\ud800"
            ),
        )


@pytest.mark.parametrize(
    "candidate",
    [
        platform_pb.CreateSkillDraftRequest(request_id="request-1"),
        platform_pb.RegisterMcpServerRequest(request_id="request-1"),
        object(),
    ],
)
def test_projector_rejects_workload_global_and_unknown_requests(
    candidate: object,
) -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
        project_request_binding,
    )

    with pytest.raises(PlatformRequestBindingError):
        project_request_binding(tenant_ref="tenant-1", request=candidate)


def test_proto_presence_default_scalars_array_and_set_semantics() -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
        project_request_binding,
    )

    default_scalar = platform_pb.ListMcpConnectorsRequest(request_id="request-1")
    projected = project_request_binding(tenant_ref="tenant-1", request=default_scalar)
    decoded = json.loads(projected.canonical_bytes)
    assert decoded["request"]["status_filter"] == 0
    assert decoded["request"]["owner_scope"] == {"present": False}
    assert decoded["request"]["page"] == {"present": False}

    array = platform_pb.DiscoverVisibleSkillsRequest(
        request_id="request-1", tags=["z", "a"]
    )
    decoded = json.loads(
        project_request_binding(tenant_ref="tenant-1", request=array).canonical_bytes
    )
    assert decoded["request"]["tags"] == ["z", "a"]

    set_request = platform_pb.CreateMcpConnectionRequest(
        request_id="request-1", allowed_selectors=["é", "z", "a"]
    )
    decoded = json.loads(
        project_request_binding(
            tenant_ref="tenant-1", request=set_request
        ).canonical_bytes
    )
    assert decoded["request"]["allowed_selectors"] == ["a", "z", "é"]
    for invalid in (["a", "a"], [""], ["   "], ["\ufeff"]):
        with pytest.raises(PlatformRequestBindingError):
            project_request_binding(
                tenant_ref="tenant-1",
                request=platform_pb.CreateMcpConnectionRequest(
                    request_id="request-1", allowed_selectors=invalid
                ),
            )
    accepted = platform_pb.CreateMcpConnectionRequest(
        request_id="request-1", allowed_selectors=["\u001c"]
    )
    decoded = json.loads(
        project_request_binding(tenant_ref="tenant-1", request=accepted).canonical_bytes
    )
    assert decoded["request"]["allowed_selectors"] == ["\u001c"]


_ACTUAL_IDENTITY_WRAPPERS = (
    "SkillSourceRef",
    "SkillInstallationId",
    "McpConnectorId",
    "McpAuthorizationId",
    "McpServerId",
    "McpConnectionId",
)
_IDENTITY_REQUEST = {
    "SkillSourceRef": ("ResolveVisibleSkillRequest", "source_ref"),
    "SkillInstallationId": (
        "SetSkillInstallationEnabledRequest",
        "installation_id",
    ),
    "McpConnectorId": ("BeginMcpConnectorAuthorizationRequest", "connector_id"),
    "McpAuthorizationId": (
        "CompleteMcpConnectorAuthorizationRequest",
        "authorization_id",
    ),
    "McpServerId": ("GetMcpServerRequest", "server_id"),
    "McpConnectionId": ("GetMcpConnectionRequest", "connection_id"),
}
_CROSS_WRAPPER_CASES = [
    (mode, expected, actual)
    for mode in ("constructor", "setattr")
    for expected in _ACTUAL_IDENTITY_WRAPPERS
    for actual in _ACTUAL_IDENTITY_WRAPPERS
    if actual != expected
]


@pytest.mark.parametrize("mode,expected_wrapper,actual_wrapper", _CROSS_WRAPPER_CASES)
def test_projection_rejects_cross_injected_identity_wrapper_classes(
    mode: str, expected_wrapper: str, actual_wrapper: str
) -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
        project_request_binding,
    )

    request_name, field = _IDENTITY_REQUEST[expected_wrapper]
    request_type = getattr(platform_pb, request_name)
    wrapper_type = getattr(platform_pb, actual_wrapper)
    wrong_wrapper = wrapper_type(value="skill:id-1")
    if mode == "constructor":
        request = request_type(**{"request_id": "request-1", field: wrong_wrapper})
    else:
        request = request_type(request_id="request-1")
        setattr(request, field, wrong_wrapper)

    with pytest.raises(PlatformRequestBindingError):
        project_request_binding(tenant_ref="tenant-1", request=request)


class _PageLike:
    limit = 10

    def has_field(self, _name: str) -> bool:
        return False


@pytest.mark.parametrize("mode", ["constructor", "setattr"])
@pytest.mark.parametrize(
    "request_name,field,wrong_value",
    [
        (
            "InstallSkillRequest",
            "command",
            SimpleNamespace(command_id="command-1", request_digest="0" * 64),
        ),
        ("ListMcpConnectorsRequest", "page", _PageLike()),
        (
            "ListMcpConnectorsRequest",
            "owner_scope",
            SimpleNamespace(kind="tenant", id="tenant-1"),
        ),
    ],
)
def test_projection_rejects_same_shape_non_proto_messages_from_both_assignment_paths(
    mode: str, request_name: str, field: str, wrong_value: object
) -> None:
    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
        project_request_binding,
    )

    request_type = getattr(platform_pb, request_name)
    if mode == "constructor":
        request = request_type(**{"request_id": "request-1", field: wrong_value})
    else:
        request = request_type(request_id="request-1")
        setattr(request, field, wrong_value)

    with pytest.raises(PlatformRequestBindingError):
        project_request_binding(tenant_ref="tenant-1", request=request)


def _malformed_request(kind: str) -> object:
    if kind == "array-item":
        value = platform_pb.DiscoverVisibleSkillsRequest(request_id="request-1")
        setattr(value, "tags", [1])
        return value
    if kind == "boolean":
        value = platform_pb.SetSkillInstallationEnabledRequest(request_id="request-1")
        setattr(value, "enabled", "false")
        return value
    if kind == "enum":
        value = platform_pb.DiscoverVisibleSkillsRequest(request_id="request-1")
        setattr(value, "scope_kind", True)
        return value
    if kind == "float":
        page = common_pb.PageRequest()
        setattr(page, "limit", 1.0)
        return platform_pb.ListMcpConnectorProvidersRequest(
            request_id="request-1", page=page
        )
    if kind == "bytes":
        value = platform_pb.AuthorizeMcpToolRequest(
            request_id="request-1",
            connector_id=platform_pb.McpConnectorId(value="id-1"),
        )
        setattr(value, "typed_arguments_json", "{}")
        return value
    raise AssertionError(kind)


@pytest.mark.parametrize(
    "candidate",
    [
        _malformed_request("array-item"),
        _malformed_request("boolean"),
        _malformed_request("enum"),
        _malformed_request("float"),
        _malformed_request("bytes"),
    ],
)
def test_projection_validation_rejects_values_that_proto_runtime_can_construct(
    candidate: object,
) -> None:
    """protobuf-py construction is permissive; the binding projector is the value gate."""

    from kokoro_agent.execution.platform_request_binding import (
        PlatformRequestBindingError,
        project_request_binding,
    )

    with pytest.raises(PlatformRequestBindingError):
        project_request_binding(tenant_ref="tenant-1", request=candidate)
