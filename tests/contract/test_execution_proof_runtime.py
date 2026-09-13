"""Contract tests binding the runtime signer to the Agent-owned A1 artifacts."""

from __future__ import annotations

import ast
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import TypeAdapter

from kokoro_agent.execution.execution_proof_profile import (
    ALGORITHM,
    AUDIENCE,
    CLAIM_FIELDS,
    CONTRACT_VERSION,
    HEADER_FIELDS,
    TYPE,
    ExecutionProofInput,
)
from kokoro_agent.execution.execution_proof_signer import (
    ExecutionProofSigner,
    ExecutionProofSignerConfig,
)
from kokoro_agent.protocol import IdentityRef


ROOT = Path(__file__).resolve().parents[2]
VECTORS = ROOT / "contract" / "execution-proof" / "v1" / "vectors.json"
RFC8032_SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
)
_OBJECT: TypeAdapter[dict[str, object]] = TypeAdapter(dict[str, object])
SIGNER_PATH = ROOT / "src" / "kokoro_agent" / "execution" / "execution_proof_signer.py"
KEYS_PATH = ROOT / "src" / "kokoro_agent" / "execution" / "execution_proof_keys.py"
PROFILE_PATH = (
    ROOT / "src" / "kokoro_agent" / "execution" / "execution_proof_profile.py"
)
PRODUCTION_ROOT = ROOT / "src"
SIGNER_MODULE = "kokoro_agent.execution.execution_proof_signer"


def _vectors() -> dict[str, object]:
    return _OBJECT.validate_json(VECTORS.read_bytes())


def _object(value: object) -> dict[str, object]:
    return _OBJECT.validate_python(value)


def _integer(value: object) -> int:
    assert type(value) is int
    return value


def test_runtime_signer_reproduces_a1_positive_compact_proof_exactly() -> None:
    positive = _object(_vectors()["positive"])
    claims_record = _object(positive["claims"])
    claims = _object(claims_record["decoded"])
    actor = _object(claims["actor"])
    subject = _object(claims["subject"])
    signer = ExecutionProofSigner(
        ExecutionProofSignerConfig(
            issuer=str(claims["iss"]),
            kid=str(_object(_object(positive["protected_header"])["decoded"])["kid"]),
            private_key=Ed25519PrivateKey.from_private_bytes(RFC8032_SEED),
        )
    )
    issue = ExecutionProofInput(
        tenant_ref=str(claims["tenant_ref"]),
        actor=IdentityRef.model_validate(actor),
        subject=IdentityRef.model_validate(subject),
        run_id=str(claims["run_id"]),
        execution_session_id=str(claims["execution_session_id"]),
        lease_generation=_integer(claims["lease_generation"]),
        operation=str(claims["operation"]),
        request_binding_sha256=str(claims["request_binding_sha256"]),
        iat=_integer(claims["iat"]),
        exp=_integer(claims["exp"]),
        jti=str(claims["jti"]),
    )

    assert signer.issue_execution_proof(issue) == positive["compact_jws"]


def test_runtime_profile_owns_exact_v1_constants_and_fields() -> None:
    assert CONTRACT_VERSION == "1.0.0"
    assert TYPE == "kokoro-agent-execution+jwt"
    assert ALGORITHM == "EdDSA"
    assert AUDIENCE == "https://kokoro.dev/resources/iam-execution-authorization"
    assert HEADER_FIELDS == frozenset({"typ", "alg", "kid"})
    assert CLAIM_FIELDS == frozenset(
        {
            "contract_version",
            "iss",
            "aud",
            "tenant_ref",
            "actor",
            "subject",
            "run_id",
            "execution_session_id",
            "lease_generation",
            "operation",
            "request_binding_sha256",
            "iat",
            "exp",
            "jti",
        }
    )
    assert CLAIM_FIELDS.isdisjoint(
        {
            "series_id",
            "skill_id",
            "installation_id",
            "connector_id",
            "server_id",
            "connection_id",
            "authorization_id",
            "invocation_grant",
        }
    )


def _resolved_from_imports(path: Path, node: ast.ImportFrom) -> set[str]:
    module_parts = node.module.split(".") if node.module else []
    if node.level == 0:
        prefix = module_parts
    else:
        try:
            relative = path.relative_to(PRODUCTION_ROOT).with_suffix("")
        except ValueError:
            return set()
        current = list(relative.parts)
        package = current[:-1]
        parents = node.level - 1
        if parents > len(package):
            return set()
        prefix = package[: len(package) - parents] + module_parts
    resolved: set[str] = {".".join(prefix)} if prefix else set()
    resolved.update(".".join([*prefix, alias.name]) for alias in node.names)
    return resolved


def _production_source_violations(path: Path, source: str) -> tuple[list[str], int]:
    violations: list[str] = []
    jwt_encode_calls = 0
    plain_jwt_imports = 0
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "jwt" and alias.asname is None:
                    plain_jwt_imports += 1
                    if path != SIGNER_PATH:
                        violations.append("jwt import outside signer")
                elif alias.name == "jwt" or alias.name.startswith("jwt."):
                    violations.append("unapproved jwt import")
                if alias.name == SIGNER_MODULE and path != SIGNER_PATH:
                    violations.append("signer import outside signer")
        if isinstance(node, ast.ImportFrom):
            if node.module == "jwt" or (
                node.module is not None and node.module.startswith("jwt.")
            ):
                violations.append("unapproved jwt from-import")
            resolved = _resolved_from_imports(path, node)
            if SIGNER_MODULE in resolved and path not in {SIGNER_PATH, KEYS_PATH}:
                violations.append("signer import outside approved modules")
            if path == KEYS_PATH and SIGNER_MODULE in resolved:
                if {alias.name for alias in node.names} != {
                    "ExecutionProofSigner",
                    "ExecutionProofSignerConfig",
                }:
                    violations.append("keys signer import shape is invalid")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id == "jwt"
                and node.func.attr == "encode"
            ):
                jwt_encode_calls += 1
                if path != SIGNER_PATH:
                    violations.append("jwt.encode outside signer")
    if path == SIGNER_PATH and plain_jwt_imports != 1:
        violations.append("signer must have one plain jwt import")
    if path.is_relative_to(PRODUCTION_ROOT):
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "issue_execution_proof"
            ):
                violations.append("production proof issue call is forbidden before A2c")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "ExecutionProofSigner"
                and path != KEYS_PATH
            ):
                violations.append("signer construction outside key loader")
    return violations, jwt_encode_calls


def test_runtime_has_only_approved_jwt_import_and_no_production_call_site() -> None:
    sources = sorted((ROOT / "src" / "kokoro_agent").rglob("*.py"))
    jwt_encode_calls = 0

    for path in sources:
        source = path.read_text(encoding="utf-8")
        violations, count = _production_source_violations(path, source)
        assert violations == []
        jwt_encode_calls += count

    assert jwt_encode_calls == 1
    assert "execution_proof_contract" not in PROFILE_PATH.read_text(encoding="utf-8")


def test_runtime_ast_gate_rejects_jwt_alias_import_mutations() -> None:
    mutations = (
        "import jwt as jose\njose.encode({})",
        "from jwt import encode as sign\nsign({})",
        "from jwt import api_jws\napi_jws.encode({})",
    )

    for source in mutations:
        violations, _ = _production_source_violations(Path("other.py"), source)
        assert violations


def test_runtime_ast_gate_rejects_signer_import_outside_signer() -> None:
    mutations = (
        (
            ROOT / "src" / "kokoro_agent" / "execution" / "consumer.py",
            "from kokoro_agent.execution.execution_proof_signer import ExecutionProofSigner",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "execution" / "consumer.py",
            "from .execution_proof_signer import ExecutionProofSigner",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "execution" / "consumer.py",
            "from . import execution_proof_signer",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "execution" / "consumer.py",
            "from . import execution_proof_signer as proof_signer",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "worker" / "consumer.py",
            "from ..execution import execution_proof_signer",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "worker" / "consumer.py",
            "from ..execution.execution_proof_signer import ExecutionProofSigner",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "worker" / "nested" / "consumer.py",
            "from ...execution.execution_proof_signer import ExecutionProofSigner",
        ),
    )

    for path, source in mutations:
        violations, _ = _production_source_violations(path, source)
        assert violations


def test_runtime_ast_gate_resolves_package_init_imports_from_parent_directory() -> None:
    mutations = (
        (
            ROOT / "src" / "kokoro_agent" / "execution" / "__init__.py",
            "from . import execution_proof_signer",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "execution" / "__init__.py",
            "from . import execution_proof_signer as proof_signer",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "execution" / "__init__.py",
            "from .execution_proof_signer import ExecutionProofSigner",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "worker" / "__init__.py",
            "from ..execution import execution_proof_signer",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "worker" / "__init__.py",
            "from ..execution.execution_proof_signer import ExecutionProofSigner",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "worker" / "nested" / "__init__.py",
            "from ...execution import execution_proof_signer",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "worker" / "nested" / "__init__.py",
            "from ...execution.execution_proof_signer import ExecutionProofSigner",
        ),
    )

    for path, source in mutations:
        violations, _ = _production_source_violations(path, source)
        assert violations


def test_runtime_ast_gate_detects_package_reexport_or_its_consumer() -> None:
    package_path = ROOT / "src" / "kokoro_agent" / "execution" / "__init__.py"
    consumer_path = ROOT / "src" / "kokoro_agent" / "worker" / "consumer.py"
    package_violations, _ = _production_source_violations(
        package_path,
        "from .execution_proof_signer import ExecutionProofSigner as ProofSigner",
    )
    consumer_violations, _ = _production_source_violations(
        consumer_path,
        "from kokoro_agent.execution import ProofSigner",
    )

    assert package_violations or consumer_violations


def test_runtime_ast_gate_allows_only_key_loader_construction_and_no_issue_calls() -> (
    None
):
    key_source = KEYS_PATH.read_text(encoding="utf-8")
    violations, _ = _production_source_violations(KEYS_PATH, key_source)
    assert violations == []

    consumer = ROOT / "src" / "kokoro_agent" / "worker" / "consumer.py"
    bad_constructor, _ = _production_source_violations(
        consumer,
        "from kokoro_agent.execution.execution_proof_signer import ExecutionProofSigner\nExecutionProofSigner(config)",
    )
    bad_issue, _ = _production_source_violations(
        KEYS_PATH,
        "from kokoro_agent.execution.execution_proof_signer import ExecutionProofSigner, ExecutionProofSignerConfig\nsigner.issue_execution_proof(value)",
    )
    assert bad_constructor
    assert bad_issue


def test_runtime_ast_gate_does_not_reject_unrelated_same_named_method() -> None:
    violations, jwt_encode_calls = _production_source_violations(
        Path("other.py"), "client.issue_execution_proof(value)"
    )

    assert violations == []
    assert jwt_encode_calls == 0
