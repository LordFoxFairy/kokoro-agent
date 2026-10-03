"""Agent HTTP machine contract is present, owned, and covers every route."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from jsonschema import Draft202012Validator, validate
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import BaseModel, TypeAdapter, ValidationError
import pytest


ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "contract" / "openapi" / "v1" / "openapi.json"
CONTRACT_README = ROOT / "contract" / "README.md"
_OBJECT = TypeAdapter(dict[str, object])


def _object(value: object) -> dict[str, object]:
    return _OBJECT.validate_python(value)


def _document() -> dict[str, object]:
    return _object(json.loads(CONTRACT.read_text(encoding="utf-8")))


def test_agent_http_contract_is_versioned_and_owned() -> None:
    document = _document()
    assert isinstance(document["openapi"], str)
    assert document["openapi"].startswith("3.")
    assert _object(document["info"])["version"] == "5.0.0"
    assert document["x-kokoro-owner"] == "kokoro-agent"
    assert document["x-kokoro-visibility"] == "internal-owner"
    assert CONTRACT_README.is_file()


def test_launch_requires_exact_typed_skill_selection_schema() -> None:
    document = _document()
    schema = _object(_object(document["components"])["schemas"])["LaunchRequest"]
    launch = _object(schema)
    required = launch["required"]
    assert isinstance(required, list)
    assert "selected_skill_source_refs" in required
    refs = _object(_object(launch["properties"])["selected_skill_source_refs"])
    assert refs["maxItems"] == 16
    assert refs["uniqueItems"] is True
    assert refs["x-kokoro-json-byte-limit"] == 4096
    assert _object(refs["items"])["pattern"] == (
        r"^skill:(?!skill:)[A-Za-z0-9][A-Za-z0-9._:-]{0,190}(?![\s\S])"
    )


@pytest.mark.parametrize("suffix", ["\n", "\r", "\r\n", "\u2028", "\u2029"])
def test_skill_selection_schema_rejects_trailing_line_terminators(suffix: str) -> None:
    document = _document()
    schema = _object(_object(document["components"])["schemas"])["LaunchRequest"]
    refs = _object(_object(_object(schema)["properties"])["selected_skill_source_refs"])
    validate(["skill:valid"], refs, cls=Draft202012Validator)
    with pytest.raises(SchemaValidationError):
        validate([f"skill:valid{suffix}"], refs, cls=Draft202012Validator)


def test_every_operation_declares_governance_metadata() -> None:
    document = _document()
    paths = _object(document["paths"])
    operations: list[dict[str, object]] = [
        _object(operation)
        for path_item in paths.values()
        for method, operation in _object(path_item).items()
        if method in {"get", "head", "post", "put", "patch", "delete"}
    ]
    assert operations
    for operation in operations:
        assert operation["x-kokoro-owner"] == "kokoro-agent"
        assert operation["x-kokoro-visibility"] == "internal-owner"
        assert operation["x-kokoro-stability"] == "stable"
        assert "x-kokoro-permission" in operation


def test_http_route_set_is_explicit() -> None:
    document = _document()
    paths = _object(document["paths"])
    assert set(paths) == {
        "/healthz",
        "/readyz",
        "/v1/execution-proof/jwks",
        "/v1/runs",
        "/v1/runs/{run_id}/control",
        "/v1/runs/{run_id}/events",
        "/v1/sessions",
        "/v1/sessions/{session_id}/messages",
        "/v1/sessions/{session_id}/events",
    }


_FAILURE_CODES = {
    "token_budget_exceeded",
    "recursion_limit_exceeded",
    "assembly_failed",
    "enqueue_failed",
    "dispatch_exhausted",
    "contract_incompatible",
    "internal_error",
    "model_unavailable",
    "dependency_unavailable",
    "model_access_denied",
}


def _failure_document() -> dict[str, object]:
    document = _document()
    schemas = _object(_object(document["components"])["schemas"])
    assert {"Failure", "RunFailure", "ChatFailure"} <= set(schemas), (
        "Missing owner safe failure profiles"
    )
    return document


def _profile_validator(document: dict[str, object], profile: str) -> dict[str, object]:
    return {
        "$ref": f"#/components/schemas/{profile}",
        "components": document["components"],
    }


def test_failure_contract_has_one_canonical_base_and_profile_references() -> None:
    document = _failure_document()
    schemas = _object(_object(document["components"])["schemas"])
    failure = _object(schemas["Failure"])
    assert (
        set(
            TypeAdapter(list[str]).validate_python(
                _object(_object(failure["properties"])["code"])["enum"]
            )
        )
        == _FAILURE_CODES
    )
    # Base remains composable; each final profile seals its evaluated fields.
    assert failure.get("additionalProperties") is not False
    assert failure.get("unevaluatedProperties") is not False
    run = _object(schemas["RunFailure"])
    run_parts = run.get("allOf", [])
    assert isinstance(run_parts, list)
    assert (
        run.get("$ref") == "#/components/schemas/Failure"
        or {"$ref": "#/components/schemas/Failure"} in run_parts
    )
    assert run.get("unevaluatedProperties") is False
    assert '"enum"' not in json.dumps(run)
    chat = _object(schemas["ChatFailure"])
    assert {"$ref": "#/components/schemas/Failure"} in TypeAdapter(
        list[dict[str, object]]
    ).validate_python(chat["allOf"])
    assert chat.get("unevaluatedProperties") is False
    assert '"enum"' not in json.dumps(chat), (
        "ChatFailure must reference, not duplicate, the code enum"
    )


def test_failure_contract_profiles_validate_every_code_retryable_tuple() -> None:
    document = _failure_document()
    for profile in ("RunFailure", "ChatFailure"):
        validator = _profile_validator(document, profile)
        for code in sorted(_FAILURE_CODES):
            for retryable in (False, True):
                payload = {"code": code, "retryable": retryable}
                if profile == "ChatFailure":
                    payload["status"] = "failed"
                if retryable and code not in {
                    "model_unavailable",
                    "dependency_unavailable",
                }:
                    with pytest.raises(SchemaValidationError):
                        validate(payload, validator, cls=Draft202012Validator)
                else:
                    validate(payload, validator, cls=Draft202012Validator)


def test_failure_contract_profiles_are_closed_and_require_strict_fields() -> None:
    document = _failure_document()
    for profile in ("RunFailure", "ChatFailure"):
        validator = _profile_validator(document, profile)
        valid = {"code": "internal_error", "retryable": False}
        if profile == "ChatFailure":
            valid["status"] = "failed"
        invalid = [
            {key: value for key, value in valid.items() if key != "code"},
            {key: value for key, value in valid.items() if key != "retryable"},
            {**valid, "retryable": "false"},
            {**valid, "retryable": 0},
            {**valid, "retryable": None},
            {**valid, "code": "UNKNOWN"},
            {**valid, "error_kind": "SENTINEL"},
            {**valid, "message": "SENTINEL"},
            {**valid, "extra": "SENTINEL"},
        ]
        if profile == "ChatFailure":
            invalid.extend(
                [
                    {key: value for key, value in valid.items() if key != "status"},
                    {**valid, "status": "completed"},
                ]
            )
        else:
            invalid.append({**valid, "status": "failed"})
        for payload in invalid:
            with pytest.raises(SchemaValidationError):
                validate(payload, validator, cls=Draft202012Validator)


def test_failure_contract_http_payload_string_has_explicit_failed_discriminator() -> (
    None
):
    schemas = _object(_object(_document()["components"])["schemas"])
    chat_event = _object(schemas["ChatEvent"])
    assert (
        _object(_object(chat_event["properties"])["payload_json"])["type"] == "string"
    )
    assert chat_event.get("x-kokoro-decoded-payloads") == {
        "discriminator": "event_type",
        "property": "payload_json",
        "mapping": {
            "run.failed": "#/components/schemas/ChatFailure",
            "interaction.state": "#/components/schemas/ChatInteractionState",
            "activity": "#/components/schemas/ChatActivity",
            "todo.updated": "#/components/schemas/ChatTodo",
        },
    }


def test_generated_failure_models_enforce_strict_profiles_and_tuples() -> None:
    import kokoro_agent.protocol as protocol

    run = protocol.RunFailedPayload
    chat = getattr(protocol, "ChatFailure", None)
    assert isinstance(chat, type) and issubclass(chat, BaseModel), (
        "ChatFailure must be published"
    )
    for model in (run, chat):
        for code in sorted(_FAILURE_CODES):
            for retryable in (False, True):
                payload: dict[str, object] = {"code": code, "retryable": retryable}
                if model is chat:
                    payload["status"] = "failed"
                if retryable and code not in {
                    "model_unavailable",
                    "dependency_unavailable",
                }:
                    with pytest.raises(ValidationError):
                        model.model_validate(payload)
                else:
                    assert model.model_validate(payload).model_dump() == payload
        valid: dict[str, object] = {"code": "internal_error", "retryable": False}
        if model is chat:
            valid["status"] = "failed"
        invalid = [
            {key: value for key, value in valid.items() if key != "code"},
            {key: value for key, value in valid.items() if key != "retryable"},
            *[{**valid, "retryable": value} for value in ("false", 0, None)],
            {**valid, "code": "UNKNOWN"},
            {**valid, "error_kind": "SENTINEL"},
            {**valid, "message": "SENTINEL"},
        ]
        if model is chat:
            invalid.extend(
                [
                    {key: value for key, value in valid.items() if key != "status"},
                    {**valid, "status": "completed"},
                ]
            )
        else:
            invalid.append({**valid, "status": "failed"})
        for payload in invalid:
            with pytest.raises(ValidationError):
                model.model_validate(payload)


def test_failure_generator_checks_complete_bytes_and_invalid_source(
    tmp_path: Path,
) -> None:
    generator = ROOT / "scripts/generate_failure_models.py"
    generated = Path("src/kokoro_agent/protocol/run_failure_generated.py")
    assert generator.is_file(), "Owner generator must exist"
    assert (ROOT / generated).is_file(), "Owner generated wire must exist"
    shutil.copytree(ROOT / "contract", tmp_path / "contract")
    (tmp_path / generated).parent.mkdir(parents=True)
    shutil.copy2(ROOT / generated, tmp_path / generated)

    def check() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(generator), "--root", str(tmp_path), "--check"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

    assert check().returncode == 0
    provenance = tmp_path / "contract/provenance.json"
    original_provenance = provenance.read_bytes()
    original = (tmp_path / generated).read_bytes()
    (tmp_path / generated).write_bytes(original + b"# stale\n")
    assert check().returncode != 0
    assert (tmp_path / generated).read_bytes() == original + b"# stale\n"
    assert provenance.read_bytes() == original_provenance
    (tmp_path / generated).unlink()
    assert check().returncode != 0
    assert not (tmp_path / generated).exists()
    (tmp_path / generated).write_bytes(original)
    source = tmp_path / "contract/openapi/v1/openapi.json"
    original_source = source.read_bytes()
    document = json.loads(original_source)
    document["info"]["title"] += " changed"
    source.write_text(json.dumps(document))
    assert check().returncode != 0  # valid schema, stale full-source header/digest
    assert (tmp_path / generated).read_bytes() == original
    source.write_bytes(original_source)
    record = json.loads(original_provenance)
    record["generated_artifacts"][0]["sha256"] = "0" * 64
    provenance.write_text(json.dumps(record))
    assert check().returncode != 0
    provenance.write_bytes(original_provenance)
    document = json.loads(original_source)
    document["components"]["schemas"]["Failure"]["required"] = ["code"]
    source.write_text(json.dumps(document))
    assert check().returncode != 0
    assert (tmp_path / generated).read_bytes() == original
    assert provenance.read_bytes() == original_provenance


def test_run_evidence_initial_cursor_is_separate_from_chat_after_seq() -> None:
    document = _document()
    components = _object(document["components"])
    parameters = _object(components["parameters"])
    paths = _object(document["paths"])
    run = _object(_object(paths["/v1/runs/{run_id}/events"])["get"])
    run_parameters = run["parameters"]
    assert isinstance(run_parameters, list)
    assert {"$ref": "#/components/parameters/EvidenceAfterSeq"} in run_parameters
    assert {"$ref": "#/components/parameters/AfterSeq"} not in run_parameters
    evidence_cursor = _object(parameters["EvidenceAfterSeq"])
    assert evidence_cursor == {
        "name": "after_seq",
        "in": "query",
        "required": False,
        "schema": {
            "type": "integer",
            "format": "int64",
            "minimum": -1,
            "maximum": 2**63 - 1,
            "default": -1,
        },
    }
    chat = _object(_object(paths["/v1/sessions/{session_id}/events"])["get"])
    chat_parameters = chat["parameters"]
    assert isinstance(chat_parameters, list)
    assert {"$ref": "#/components/parameters/AfterSeq"} in chat_parameters
    assert {"$ref": "#/components/parameters/EvidenceAfterSeq"} not in chat_parameters
    assert _object(_object(parameters["AfterSeq"])["schema"]) == {
        "type": "integer",
        "format": "int64",
        "minimum": 0,
        "default": 0,
    }


def test_evidence_page_next_seq_allows_initial_empty_cursor_only_down_to_minus_one() -> (
    None
):
    document = _document()
    schemas = _object(_object(document["components"])["schemas"])
    next_seq = _object(
        _object(_object(schemas["EvidencePage"])["properties"])["next_seq"]
    )
    assert next_seq == {
        "type": "integer",
        "format": "int64",
        "minimum": -1,
        "maximum": 2**63 - 1,
    }
    for value in [-1, 0, 1, 2**63 - 1]:
        validate(value, next_seq, cls=Draft202012Validator)
    for invalid in [-2, 2**63, "-1", None, True, 0.5]:
        with pytest.raises(SchemaValidationError):
            validate(invalid, next_seq, cls=Draft202012Validator)


# R31 owner-first RED: full HITL4, never accept the old tool/request addressing.
def _hitl4_control(decision: dict[str, object]) -> dict[str, object]:
    return {
        "kind": "run.resume",
        "session_id": "session-hitl",
        "expected_pause_revision": 1,
        "pause_ref": "pause-opaque-1",
        "decisions": [decision],
    }


def test_hitl4_resume_requires_revision_ref_and_full_item_decisions() -> None:
    document = _document()
    schemas = _object(_object(document["components"])["schemas"])
    resume = _object(schemas["ResumeControl"])
    required = TypeAdapter(list[str]).validate_python(resume["required"])
    assert {"expected_pause_revision", "pause_ref", "decisions"} <= set(required), (
        "HITL4 requires an explicit frozen pause identity; no default/fallback"
    )
    properties = _object(resume["properties"])
    assert _object(properties["expected_pause_revision"])["minimum"] == 1
    assert _object(properties["pause_ref"])["minLength"] == 1
    assert resume["additionalProperties"] is False


@pytest.mark.parametrize(
    "decision",
    [
        {"type": "approve", "item_id": "item-1"},
        {"type": "edit", "item_id": "item-1", "args": {"query": "approved"}},
        {"type": "reject", "item_id": "item-1", "reason": "declined"},
        {"type": "respond", "item_id": "item-1", "response": "reviewed"},
        {"type": "submit", "item_id": "item-1", "value": {"approved": True}},
    ],
)
@pytest.mark.asyncio
async def test_hitl4_machine_and_runtime_accept_the_same_item_addressed_decisions(
    decision: dict[str, object],
) -> None:
    from kokoro_agent.protocol.control import inbound_adapter

    control = _hitl4_control(decision)
    validate(control, _profile_validator(_document(), "ResumeControl"))
    parsed = inbound_adapter.validate_python(
        {**control, "run_id": "run-hitl", "command_id": "command-1"}
    )
    assert parsed.model_dump(mode="json")["expected_pause_revision"] == 1
    from kokoro_agent.application.chat.service import ChatService
    from kokoro_agent.interfaces.http.ingress import AgentIngress
    from support.fakes import FakeBus, FakeRunRepository, request

    repository = FakeRunRepository()
    run = request("run-hitl", session_id="session-hitl")
    await repository.try_claim(run, "worker-1")
    bus = FakeBus()
    ingress = AgentIngress(
        bus=bus,
        run_repository=repository,
        chat_service=ChatService(repository.chat_repository),
    )
    receipt = await ingress.control(
        run.run_id,
        control,
        command_id="command-1",
        execution_identity=run.execution_identity,
    )
    assert receipt["status"] == "pending"
    assert len(bus.published) == 1
    event = bus.published[0][1]
    assert event["expected_pause_revision"] == 1
    assert event["pause_ref"] == "pause-opaque-1"
    assert event["decisions"] == [decision]


@pytest.mark.parametrize(
    "decision",
    [
        {"type": "approve", "tool_id": "call-1"},
        {"type": "edit", "tool_id": "call-1", "args": {}},
        {"type": "reject", "tool_id": "call-1", "reason": "declined"},
        {"type": "respond", "tool_id": "call-1", "response": "reviewed"},
        {"type": "submit", "request_id": "call-1", "value": {}},
    ],
)
def test_hitl4_machine_rejects_old_decision_addressing_without_alias(
    decision: dict[str, object],
) -> None:
    with pytest.raises(SchemaValidationError):
        validate(decision, _profile_validator(_document(), "ResumeDecision"))
    from kokoro_agent.protocol.control import ResumeDecision

    with pytest.raises(ValidationError):
        TypeAdapter(ResumeDecision).validate_python(decision)


@pytest.mark.parametrize("field", ["expected_pause_revision", "pause_ref"])
def test_hitl4_runtime_does_not_default_missing_pause_identity(field: str) -> None:
    from kokoro_agent.protocol.control import inbound_adapter

    valid = {
        **_hitl4_control({"type": "approve", "item_id": "item-1"}),
        "run_id": "run-hitl",
        "command_id": "command-1",
    }
    # Positive precondition makes rejection meaningful, rather than all inputs invalid.
    inbound_adapter.validate_python(valid)
    del valid[field]
    with pytest.raises(ValidationError):
        inbound_adapter.validate_python(valid)


@pytest.mark.parametrize("bad_revision", [0, -1, True, "1"])
def test_hitl4_machine_rejects_invalid_pause_revision(bad_revision: object) -> None:
    valid = _hitl4_control({"type": "approve", "item_id": "item-1"})
    schema = _profile_validator(_document(), "ResumeControl")
    validate(valid, schema)
    with pytest.raises(SchemaValidationError):
        validate({**valid, "expected_pause_revision": bad_revision}, schema)


@pytest.mark.parametrize(
    "component,field",
    [
        ("ResumeControl", "expected_pause_revision"),
        ("ResumeControl", "pause_ref"),
        ("ChatInteractionState", "action_result"),
        ("InteractionItem", "item_id"),
        ("InteractionDisplay", "input_schema"),
        ("InteractionActionResult", "command_id"),
    ],
)
def test_owner_checker_rejects_relaxed_hitl_required_fields(
    component: str, field: str
) -> None:
    from kokoro_agent.contract_check import validate_openapi_document

    document = _document()
    schemas = _object(_object(document["components"])["schemas"])
    shape = _object(schemas[component])
    required = TypeAdapter(list[str]).validate_python(shape["required"])
    shape["required"] = [name for name in required if name != field]
    schemas[component] = shape
    _object(document["components"])["schemas"] = schemas
    # _object validates/copies dictionaries: mutate the actual root explicitly.
    document["components"] = {**_object(document["components"]), "schemas": schemas}
    with pytest.raises(ValueError, match="HITL"):
        validate_openapi_document(document)


# R95 HTTP5 target assertions are additive: HTTP4 baseline assertions above remain.
def test_r95_http5_version_is_explicit_without_changing_proof_version() -> None:
    document = _document()
    proof = _object(
        json.loads((ROOT / "contract/execution-proof/v1/schema.json").read_text())
    )
    assert proof["x-kokoro-contract-version"] == "1.0.0"
    assert _object(document["info"])["version"] == "5.0.0"


def test_r95_safe_progress_has_exact_decoded_discriminators() -> None:
    schemas = _object(_object(_document()["components"])["schemas"])
    event = _object(schemas["ChatEvent"])
    values = TypeAdapter(list[str]).validate_python(
        _object(_object(event["properties"])["event_type"])["enum"]
    )
    assert len(values) == len(set(values))
    assert set(values) == {
        "run.started",
        "assistant.delta",
        "assistant.completed",
        "activity",
        "todo.updated",
        "interaction.state",
        "delivery",
        "run.completed",
        "run.failed",
    }
    decoded = _object(event["x-kokoro-decoded-payloads"])
    assert decoded["discriminator"] == "event_type"
    assert decoded["property"] == "payload_json"
    mapping = _object(decoded["mapping"])
    assert set(mapping) == {
        "run.failed",
        "interaction.state",
        "activity",
        "todo.updated",
    }
    assert mapping["run.failed"] == "#/components/schemas/ChatFailure"
    assert mapping["interaction.state"] == "#/components/schemas/ChatInteractionState"
    for kind in ("activity", "todo.updated"):
        ref = mapping[kind]
        assert isinstance(ref, str) and ref.startswith("#/components/schemas/")
        assert ref.rsplit("/", 1)[1] in schemas
    assert mapping["activity"] != mapping["todo.updated"]


def _r95_todo_component(document: dict[str, object]) -> tuple[str, dict[str, object]]:
    schemas = _object(_object(document["components"])["schemas"])
    event = _object(schemas["ChatEvent"])
    mapping = _object(_object(event["x-kokoro-decoded-payloads"])["mapping"])
    ref = mapping.get("todo.updated")
    assert isinstance(ref, str) and ref.startswith("#/components/schemas/"), (
        "HTTP5 must register strict decoded Todo, not leave payload_json untyped"
    )
    name = ref.rsplit("/", 1)[1]
    return name, _object(schemas[name])


@pytest.mark.parametrize("invalid_limit", [None, 65537, "65536", True])
def test_r95_checker_rejects_missing_or_drifted_complete_todo_byte_limit(
    invalid_limit: object,
) -> None:
    from kokoro_agent.contract_check import validate_openapi_document

    document = _document()
    # Existing document-only entry: no provenance/hash check in this test.
    validate_openapi_document(document)
    name, shape = _r95_todo_component(document)
    assert shape.get("x-kokoro-json-byte-limit") == 65536
    if invalid_limit is None:
        del shape["x-kokoro-json-byte-limit"]
    else:
        shape["x-kokoro-json-byte-limit"] = invalid_limit
    schemas = _object(_object(document["components"])["schemas"])
    schemas[name] = shape
    document["components"] = {**_object(document["components"]), "schemas": schemas}
    with pytest.raises(ValueError):
        validate_openapi_document(document)


@pytest.mark.parametrize("extra_byte", [False, True])
def test_r95_machine_byte_annotation_agrees_with_real_runtime_canonical_budget(
    extra_byte: bool,
) -> None:
    from kokoro_agent.protocol import TodoUpdatedPayload

    value = {
        "todos": [{"content": "😀" * 1024, "status": "pending"} for _ in range(16)]
    }

    def canonical() -> bytes:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")

    excess = len(canonical()) - 65536
    count, remainder = divmod(excess, 3)
    tail = "" if remainder == 0 else "汉" if remainder == 1 else "é"
    value["todos"][0]["content"] = (
        "a" * count + tail + "😀" * (1024 - count - bool(remainder))
    )
    if extra_byte:
        value["todos"][0]["content"] = "é" + value["todos"][0]["content"][1:]
    assert len(canonical()) == 65536 + int(extra_byte)
    assert all(len(item["content"]) == 1024 for item in value["todos"])
    if extra_byte:
        with pytest.raises(ValidationError, match="UTF-8 budget"):
            TodoUpdatedPayload.model_validate(value)
    else:
        assert TodoUpdatedPayload.model_validate(value).canonical_bytes() == canonical()
    _, shape = _r95_todo_component(_document())
    assert shape.get("x-kokoro-json-byte-limit") == 65536


def test_distribution_declares_complete_checker_asset_closure() -> None:
    """Declaration RED is distinct from Root's real installed-wheel E49 RED."""
    import tomllib

    from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES
    from kokoro_agent.platform_binding_contract import EXPECTED_EXECUTION_SOURCES

    with (ROOT / "pyproject.toml").open("rb") as source:
        package = tomllib.load(source)
    declared: dict[str, Path] = {}
    for destination, patterns in package["tool"]["setuptools"]["data-files"].items():
        for pattern in patterns:
            for path in ROOT.glob(pattern):
                key = f"{destination}/{path.name}"
                assert key not in declared, f"duplicate data-file destination: {key}"
                declared[key] = path
    required = {
        *OWNER_SOURCE_FILES,
        "contract/provenance.json",
        "contract/platform/v1/provenance.json",
        *(record[0] for record in EXPECTED_EXECUTION_SOURCES),
    }
    missing = sorted(
        relative
        for relative in required
        if declared.get(f"share/kokoro-agent/audit/{relative}") != ROOT / relative
    )
    assert not missing, (
        f"checker assets missing from distribution declaration: {missing}"
    )
    platform_prefix = (
        "share/kokoro-agent/audit/contract/platform/v1/execution-operations/v4/"
    )
    assert {key for key in declared if key.startswith(platform_prefix)} == {
        f"share/kokoro-agent/audit/{record[0]}" for record in EXPECTED_EXECUTION_SOURCES
    }
    assert declared["share/kokoro-agent/schema.sql"] == ROOT / "database/schema.sql"


@pytest.mark.parametrize(
    "relative",
    [
        "contract/openapi/v1/openapi.json",
        "contract/provenance.json",
        "contract/execution-proof/v1/vectors.json",
        "contract/platform/v1/execution-operations/v4/command-identities.json",
        "src/kokoro_agent/protocol/run_failure_generated.py",
        "scripts/generate_failure_models.py",
    ],
)
@pytest.mark.parametrize("mutation", ["missing", "drift"])
def test_explicit_checker_rejects_incomplete_assets_without_repair_or_fallback(
    tmp_path: Path, relative: str, mutation: str
) -> None:
    """Explicit source fixture, not an installation or a patched wheel target."""
    from kokoro_agent import contract_check
    from kokoro_agent.execution_proof_contract import OWNER_SOURCE_FILES

    root = tmp_path / "explicit-source-fixture"
    shutil.copytree(ROOT / "contract", root / "contract")
    for source in OWNER_SOURCE_FILES:
        path = root / source
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((ROOT / source).read_bytes())
    contract_check.validate(root)  # Complete real validation is the legal control.
    asset = root / relative
    if mutation == "missing":
        asset.unlink()
    elif relative == "contract/provenance.json":
        provenance = json.loads(asset.read_text(encoding="utf-8"))
        provenance["combined_sha256"] = "0" * 64
        asset.write_text(json.dumps(provenance), encoding="utf-8")
    else:
        asset.write_bytes(asset.read_bytes() + b"\n ")
    before = {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }
    with pytest.raises((ValueError, FileNotFoundError)):
        contract_check.validate(root)
    assert {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    } == before


def test_console_checker_accepts_matching_editable_source_control() -> None:
    from kokoro_agent import contract_check

    # Current environment is the declared editable source checkout. Real wheel
    # venv/target controls belong to Root's later installed gate, not this test.
    assert contract_check.main() == 0


@pytest.mark.parametrize(
    "metadata_fault", ["missing_record_noneditable", "foreign_editable", "noneditable"]
)
def test_console_checker_rejects_metadata_mismatch_without_source_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, metadata_fault: str
) -> None:
    from importlib import metadata

    from kokoro_agent import contract_check

    read_text = metadata.PathDistribution.read_text

    def altered_metadata(
        distribution: metadata.PathDistribution, name: str
    ) -> str | None:
        identity = read_text(distribution, "METADATA") or ""
        if "Name: kokoro-agent\n" not in identity:
            return read_text(distribution, name)
        if name == "direct_url.json":
            if metadata_fault == "foreign_editable":
                return json.dumps(
                    {"url": tmp_path.as_uri(), "dir_info": {"editable": True}}
                )
            return None  # Not editable: source __file__ is not installation proof.
        if name == "RECORD" and metadata_fault == "missing_record_noneditable":
            return None
        return read_text(distribution, name)

    monkeypatch.setattr(metadata.PathDistribution, "read_text", altered_metadata)
    with pytest.raises((ValueError, FileNotFoundError, RuntimeError)):
        contract_check.main()  # Existing real entrypoint; no future-helper import.
