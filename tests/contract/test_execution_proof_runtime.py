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
SUPPLIER_PATH = (
    ROOT / "src" / "kokoro_agent" / "execution" / "execution_proof_supplier.py"
)
LEASE_READER_PATH = (
    ROOT
    / "src"
    / "kokoro_agent"
    / "infrastructure"
    / "postgres_execution_proof_lease.py"
)
PROFILE_PATH = (
    ROOT / "src" / "kokoro_agent" / "execution" / "execution_proof_profile.py"
)
PRODUCTION_ROOT = ROOT / "src"
SIGNER_MODULE = "kokoro_agent.execution.execution_proof_signer"
SUPPLIER_MODULE = "kokoro_agent.execution.execution_proof_supplier"


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
    if path in {SUPPLIER_PATH, LEASE_READER_PATH}:
        violations.extend(_proof_boundary_violations(source))
    if path == LEASE_READER_PATH:
        violations.extend(_lease_sql_policy_violations(source))
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
                if alias.name == SUPPLIER_MODULE and path not in {
                    SUPPLIER_PATH,
                    LEASE_READER_PATH,
                }:
                    violations.append("supplier import outside approved modules")
        if isinstance(node, ast.ImportFrom):
            if node.module == "jwt" or (
                node.module is not None and node.module.startswith("jwt.")
            ):
                violations.append("unapproved jwt from-import")
            resolved = _resolved_from_imports(path, node)
            if SIGNER_MODULE in resolved and path not in {
                SIGNER_PATH,
                KEYS_PATH,
                SUPPLIER_PATH,
            }:
                violations.append("signer import outside approved modules")
            if path == KEYS_PATH and SIGNER_MODULE in resolved:
                if {alias.name for alias in node.names} != {
                    "ExecutionProofSigner",
                    "ExecutionProofSignerConfig",
                }:
                    violations.append("keys signer import shape is invalid")
            if SUPPLIER_MODULE in resolved and path not in {
                SUPPLIER_PATH,
                LEASE_READER_PATH,
            }:
                violations.append("supplier import outside approved modules")
            if path == LEASE_READER_PATH and SUPPLIER_MODULE in resolved:
                if {alias.name for alias in node.names} != {
                    "CurrentLeaseObservation",
                    "ExecutionProofUnavailableError",
                }:
                    violations.append("lease reader supplier import shape is invalid")
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
        issue_calls = 0
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "issue_execution_proof"
            ):
                issue_calls += 1
                if path != SUPPLIER_PATH:
                    violations.append("proof issue call outside supplier")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Call)
                and isinstance(node.func.func, ast.Name)
                and node.func.func.id == "getattr"
                and len(node.func.args) >= 2
                and isinstance(node.func.args[1], ast.Constant)
                and node.func.args[1].value == "issue_execution_proof"
            ):
                violations.append("dynamic proof issue call")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "ExecutionProofSigner"
                and path != KEYS_PATH
            ):
                violations.append("signer construction outside key loader")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "ExecutionProofSupplier"
                and path != SUPPLIER_PATH
            ):
                violations.append("supplier construction outside factory module")
            if (
                isinstance(node, (ast.Assign, ast.AnnAssign))
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "issue_execution_proof"
            ):
                violations.append("proof issue method alias")
        if path == SUPPLIER_PATH and issue_calls != 1:
            violations.append("supplier must call signer exactly once")
    return violations, jwt_encode_calls


def test_runtime_has_only_approved_jwt_import_and_one_supplier_call_site() -> None:
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


def test_runtime_ast_gate_rejects_dynamic_and_aliased_signer_calls() -> None:
    consumer = ROOT / "src" / "kokoro_agent" / "worker" / "consumer.py"
    mutations = (
        "getattr(signer, 'issue_execution_proof')(value)",
        "issue = signer.issue_execution_proof\nissue(value)",
        "from kokoro_agent.execution.execution_proof_signer import ExecutionProofSigner as S\ns = S(config)\ns.issue_execution_proof(value)",
        "from ..execution.execution_proof_signer import ExecutionProofSigner as S\ns = S(config)\ns.issue_execution_proof(value)",
    )

    for source in mutations:
        violations, _ = _production_source_violations(consumer, source)
        assert violations


def test_runtime_ast_gate_rejects_supplier_reexport_construction_and_dead_composition() -> (
    None
):
    mutations = (
        (
            ROOT / "src" / "kokoro_agent" / "execution" / "__init__.py",
            "from .execution_proof_supplier import ExecutionProofSupplier",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "worker" / "main.py",
            "from ..execution.execution_proof_supplier import ExecutionProofSupplier\nExecutionProofSupplier(snapshot=x, lease_reader=y, signer=z, clock=c, nonce_provider=n)",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "agent_factory.py",
            "from kokoro_agent.execution.execution_proof_supplier import create_execution_proof_supplier",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "clients" / "capability.py",
            "from ..execution import execution_proof_supplier as proof_supplier",
        ),
        (
            ROOT / "src" / "kokoro_agent" / "clients" / "capability.py",
            "import kokoro_agent.execution.execution_proof_supplier as proof_supplier",
        ),
    )

    for path, source in mutations:
        violations, _ = _production_source_violations(path, source)
        assert violations


def test_runtime_ast_gate_allows_supplier_and_narrow_reader_control() -> None:
    supplier_violations, _ = _production_source_violations(
        SUPPLIER_PATH, SUPPLIER_PATH.read_text(encoding="utf-8")
    )
    reader_violations, _ = _production_source_violations(
        LEASE_READER_PATH, LEASE_READER_PATH.read_text(encoding="utf-8")
    )

    assert supplier_violations == []
    assert reader_violations == []


def test_a2c_documents_standalone_current_fact_without_new_wire_or_worker_gate() -> (
    None
):
    documents = {
        name: (ROOT / "docs" / name).read_text(encoding="utf-8")
        for name in (
            "API_CONTRACT.md",
            "DATA_MODEL.md",
            "RUNBOOK.md",
            "RELIABILITY.md",
        )
    }

    assert "A2b 已提交" in documents["API_CONTRACT.md"]
    assert "A2b 当前是待复审候选，增加" not in documents["API_CONTRACT.md"]
    assert "A2c standalone owner-internal component" in documents["API_CONTRACT.md"]
    assert "A2c 不新增 wire" in documents["API_CONTRACT.md"]
    assert "statement-time reader 已落地" in documents["DATA_MODEL.md"]
    assert "不写入 schema 或业务事务" in documents["DATA_MODEL.md"]
    assert "worker private loader 仍未装配" in documents["RUNBOOK.md"]
    assert "A2c wires private loader" not in documents["RUNBOOK.md"]
    reliability = documents["RELIABILITY.md"]
    assert "逻辑数据库工作 deadline" in reliability
    assert "0.25 秒 cleanup budget" in reliability
    assert "public `AsyncConnection.close()` 恰好一次" in reliability
    assert "`cancel_safe()`" not in reliability
    assert "libpq" not in reliability
    assert "不是 Python/OS hard real-time wall guarantee" in reliability
    assert "production transport 仍未完成" in reliability
    current = (ROOT / "docs" / "CURRENT.md").read_text(encoding="utf-8")
    assert (
        "production signer call site 仅 standalone supplier 一个；"
        "runtime/client transport consumer/composition 为零。" in current
    )
    assert (
        current.count(
            "production signer call site 仅 standalone supplier 一个；"
            "runtime/client transport consumer/composition 为零。"
        )
        == 2
    )
    assert "production caller/composition 仍为零" not in current

    reader_source = LEASE_READER_PATH.read_text(encoding="utf-8")
    for forbidden in ("cancel_safe", "pgconn", "_abort_and_discard"):
        assert forbidden not in reader_source


def _supplier_time_policy_violations(source: str) -> list[str]:
    violations: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id == "float":
            violations.append("float conversion")
        if isinstance(node.func, ast.Attribute) and node.func.attr == "timestamp":
            violations.append("datetime.timestamp")
    return violations


def test_supplier_time_gate_rejects_float_epoch_mutants_and_allows_control() -> None:
    assert (
        _supplier_time_policy_violations(SUPPLIER_PATH.read_text(encoding="utf-8"))
        == []
    )
    for mutant in (
        "iat = int(value.timestamp())",
        "iat = float(delta.total_seconds())",
    ):
        assert _supplier_time_policy_violations(mutant)


def _factory_business_input_violations(source: str) -> list[str]:
    tree = ast.parse(source)
    functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "create_execution_proof_supplier"
    ]
    if len(functions) != 1:
        return ["factory count"]
    arguments = functions[0].args
    names = {
        argument.arg
        for argument in (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
        )
    }
    forbidden = names.intersection(
        {
            "tenant_ref",
            "actor",
            "subject",
            "run_id",
            "session_id",
            "execution_session_id",
            "lease_owner",
            "lease_generation",
            "identity_assertion_ref",
        }
    )
    violations = [f"split business input: {name}" for name in sorted(forbidden)]
    if "leased_run" not in names:
        violations.append("missing leased_run")
    return violations


def test_supplier_factory_gate_requires_one_leased_run_and_defeats_source_mutant() -> (
    None
):
    source = SUPPLIER_PATH.read_text(encoding="utf-8")
    assert _factory_business_input_violations(source) == []
    mutant = source.replace(
        "leased_run: LeasedRun,",
        "leased_run: LeasedRun,\n    tenant_ref: str,\n    session_id: str,",
        1,
    )
    assert _factory_business_input_violations(mutant)


def _proof_boundary_violations(source: str) -> list[str]:
    violations: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "is_lease_current":
            violations.append("retired lease reference")
        if isinstance(node, ast.Attribute) and node.attr == "is_lease_current":
            violations.append("retired lease attribute")
        if isinstance(node, ast.Constant) and node.value == "is_lease_current":
            violations.append("dynamic retired lease reference")
        if _static_string(node) == "is_lease_current":
            violations.append("computed retired lease reference")
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_transport_or_client_module(alias.name):
                    violations.append("transport/client import")
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if any(alias.name == "is_lease_current" for alias in node.names):
                violations.append("retired lease import")
            if _is_transport_or_client_module(module) or any(
                alias.name in {"client", "clients"} for alias in node.names
            ):
                violations.append("transport/client from-import")
    return violations


def _static_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and type(node.value) is str:
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_string(node.left)
        right = _static_string(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _is_transport_or_client_module(module: str) -> bool:
    parts = module.split(".")
    return bool(parts) and (
        parts[0] in {"httpx", "requests"} or "client" in parts or "clients" in parts
    )


def _lease_sql_policy_violations(source: str) -> list[str]:
    tree = ast.parse(source)
    statements = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_statement"
    ]
    violations: list[str] = []
    if len(statements) != 1:
        violations.append("statement factory count")
        return violations
    returns = [node for node in ast.walk(statements[0]) if isinstance(node, ast.Return)]
    if len(returns) != 1 or returns[0].value is None:
        violations.append("statement return count")
        return violations
    returned = returns[0].value
    if not isinstance(returned, ast.JoinedStr):
        violations.append("statement return must be a direct template")
        return violations
    formatted = [
        node for node in returned.values if isinstance(node, ast.FormattedValue)
    ]
    if (
        len(formatted) != 1
        or not isinstance(formatted[0].value, ast.Name)
        or formatted[0].value.id != "table"
        or formatted[0].conversion != -1
        or formatted[0].format_spec is not None
    ):
        violations.append("statement table interpolation")
        return violations
    sql_literals = "".join(
        node.value
        for node in returned.values
        if isinstance(node, ast.Constant) and type(node.value) is str
    )
    if sql_literals.count("run.lease_generation BETWEEN 1 AND %s") != 1:
        violations.append("safe generation BETWEEN")
    return violations


def test_proof_boundary_gate_rejects_retired_lease_calls_and_transport_imports() -> (
    None
):
    forbidden_sources = (
        "lease.is_lease_current(run_id)",
        "check = lease.is_lease_current\ncheck(run_id)",
        "getattr(lease, 'is_lease_current')(run_id)",
        "getattr(lease, 'is_' + 'lease_current')(run_id)",
        "from legacy import is_lease_current as current\ncurrent(run_id)",
        "import httpx as transport",
        "from requests import Session as Transport",
        "from kokoro_agent.clients import capability as transport",
        "from ..clients.capability import CapabilityClient as Transport",
    )
    for source in forbidden_sources:
        assert _proof_boundary_violations(source)

    assert (
        _proof_boundary_violations(
            "from typing import Protocol\nclass Lease(Protocol): ..."
        )
        == []
    )


def test_production_proof_modules_pass_boundary_gate() -> None:
    assert _proof_boundary_violations(SUPPLIER_PATH.read_text(encoding="utf-8")) == []
    assert (
        _proof_boundary_violations(LEASE_READER_PATH.read_text(encoding="utf-8")) == []
    )


def test_lease_sql_gate_requires_safe_generation_between_and_defeats_mutant() -> None:
    source = LEASE_READER_PATH.read_text(encoding="utf-8")
    assert _lease_sql_policy_violations(source) == []

    mutant = source.replace(
        "run.lease_generation BETWEEN 1 AND %s",
        "run.lease_generation <= %s",
        1,
    )
    assert _lease_sql_policy_violations(mutant)

    dead_constant_mutant = """
def _statement(schema_name: str) -> str:
    unused = 'run.lease_generation BETWEEN 1 AND %s'
    return 'SELECT 1'
"""
    assert _lease_sql_policy_violations(dead_constant_mutant)

    dead_return_mutants = (
        """
def _statement(schema_name: str) -> str:
    return 'SELECT 1' if True else 'run.lease_generation BETWEEN 1 AND %s'
""",
        """
def _statement(schema_name: str) -> str:
    return 'SELECT 1' or 'run.lease_generation BETWEEN 1 AND %s'
""",
    )
    for dead_return_mutant in dead_return_mutants:
        assert _lease_sql_policy_violations(dead_return_mutant)

    legal_control = """
def _statement(schema_name: str) -> str:
    table = schema_name
    return f'''SELECT * FROM {table} AS run
        WHERE run.lease_generation BETWEEN 1 AND %s'''
"""
    assert _lease_sql_policy_violations(legal_control) == []
