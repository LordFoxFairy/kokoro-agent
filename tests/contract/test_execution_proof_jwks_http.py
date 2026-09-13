"""Agent HTTP 1.1.0 JWKS machine-contract pins."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from kokoro_agent import contract_check

ROOT = Path(__file__).resolve().parents[2]
OPENAPI = ROOT / "contract/openapi/v1/openapi.json"
PROVENANCE = ROOT / "contract/provenance.json"
PATH = "/v1/execution-proof/jwks"
JWK_SET_REF = "#/components/schemas/ExecutionProofJwkSet"
JWK_SET_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["keys"],
    "properties": {
        "keys": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["kty", "crv", "use", "alg", "kid", "x"],
                "properties": {
                    "kty": {"type": "string", "const": "OKP"},
                    "crv": {"type": "string", "const": "Ed25519"},
                    "use": {"type": "string", "const": "sig"},
                    "alg": {"type": "string", "const": "EdDSA"},
                    "kid": {"type": "string", "minLength": 1},
                    "x": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
                    },
                },
            },
        }
    },
}


def _documents() -> tuple[dict[str, Any], dict[str, Any]]:
    return json.loads(OPENAPI.read_text()), json.loads(PROVENANCE.read_text())


def test_jwks_contract_is_exact_anonymous_owner_wire() -> None:
    document, provenance = _documents()
    assert document["info"]["version"] == "1.1.0"
    path = document["paths"][PATH]
    assert set(path) == {"get", "head"}
    for method in ("get", "head"):
        operation = path[method]
        assert operation["security"] == []
        assert operation["x-kokoro-owner"] == "kokoro-agent"
        assert operation["x-kokoro-visibility"] == "internal-owner"
        assert operation["x-kokoro-stability"] == "stable"
        assert operation["x-kokoro-idempotency"] == "read-only"
        assert operation["x-kokoro-permission"] == "none"
        assert set(operation["responses"]) == {"200", "400", "405", "503"}
        assert "application/jwk-set+json" in operation["responses"]["200"]["content"]
        assert operation["responses"]["200"]["content"]["application/jwk-set+json"][
            "schema"
        ] == {"$ref": JWK_SET_REF}
        text = json.dumps(operation, sort_keys=True)
        for expected in (
            "execution_proof_jwks_invalid_request",
            "execution_proof_jwks_method_not_allowed",
            "execution_proof_jwks_unavailable",
            "GET, HEAD",
            "no-store",
            "Content-Length",
        ):
            assert expected in text
    direct = provenance["http_contract"]
    assert direct == {
        "version": "1.1.0",
        "path": "contract/openapi/v1/openapi.json",
        "sha256": hashlib.sha256(OPENAPI.read_bytes()).hexdigest(),
    }
    assert document["components"]["schemas"]["ExecutionProofJwkSet"] == JWK_SET_SCHEMA


@pytest.mark.parametrize(
    "mutation",
    [
        "whole",
        "missing",
        "extra",
        "private",
        "url",
        "certificate",
        "literal",
        "pattern",
        "inline_get",
        "inline_head",
    ],
)
def test_checker_rejects_jwk_set_shape_mutations(mutation: str) -> None:
    document, _ = _documents()
    changed = copy.deepcopy(document)
    schema = changed["components"]["schemas"]["ExecutionProofJwkSet"]
    item = schema["properties"]["keys"]["items"]
    if mutation == "whole":
        schema["type"] = "array"
    elif mutation == "missing":
        item["required"].remove("use")
    elif mutation == "extra":
        item["additionalProperties"] = True
    elif mutation == "private":
        item["properties"]["d"] = {"type": "string"}
    elif mutation == "url":
        item["properties"]["jku"] = {"type": "string"}
    elif mutation == "certificate":
        item["properties"]["x5c"] = {"type": "array"}
    elif mutation == "literal":
        item["properties"]["alg"]["const"] = "ES256"
    elif mutation == "pattern":
        item["properties"]["x"]["pattern"] = ".+"
    else:
        method = mutation.removeprefix("inline_")
        changed["paths"][PATH][method]["responses"]["200"]["content"][
            "application/jwk-set+json"
        ]["schema"] = {"type": "object"}
    with pytest.raises(ValueError):
        contract_check.validate_openapi_document(changed)


@pytest.mark.parametrize(
    "mutation",
    [
        "delete_get",
        "delete_head",
        "bearer",
        "permission",
        "media",
        "error",
        "allow",
        "cache",
        "version",
    ],
)
def test_checker_rejects_jwks_contract_semantic_mutations(mutation: str) -> None:
    document, _ = _documents()
    changed = copy.deepcopy(document)
    item = changed["paths"][PATH]
    if mutation == "delete_get":
        del item["get"]
    elif mutation == "delete_head":
        del item["head"]
    elif mutation == "bearer":
        item["get"]["security"] = [{"AgentServiceBearer": []}]
    elif mutation == "permission":
        item["get"]["x-kokoro-permission"] = "agent-service"
    elif mutation == "media":
        item["get"]["responses"]["200"]["content"] = {"application/json": {}}
    elif mutation == "error":
        item["get"]["responses"]["400"]["x-kokoro-error-code"] = "bad"
    elif mutation == "allow":
        item["get"]["responses"]["405"]["headers"]["Allow"]["schema"]["const"] = "GET"
    elif mutation == "cache":
        del item["get"]["responses"]["200"]["headers"]["Cache-Control"]
    else:
        changed["info"]["version"] = "1.0.0"
    with pytest.raises(ValueError):
        contract_check.validate_openapi_document(changed)


@pytest.mark.parametrize("status", ["400", "405", "503"])
def test_checker_rejects_each_error_envelope_schema_mutation(status: str) -> None:
    document, _ = _documents()
    changed = copy.deepcopy(document)
    changed["paths"][PATH]["get"]["responses"][status]["content"]["application/json"][
        "schema"
    ] = {"type": "object"}
    with pytest.raises(ValueError):
        contract_check.validate_openapi_document(changed)


@pytest.mark.parametrize(
    ("status", "header", "mutation"),
    [
        ("200", "Cache-Control", "required"),
        ("400", "Cache-Control", "type"),
        ("503", "Cache-Control", "const"),
        ("200", "Content-Length", "required"),
        ("400", "Content-Length", "type"),
        ("503", "Content-Length", "pattern"),
        ("405", "Allow", "required"),
        ("405", "Allow", "type"),
        ("405", "Allow", "const"),
    ],
)
def test_checker_rejects_exact_representation_header_mutations(
    status: str, header: str, mutation: str
) -> None:
    document, _ = _documents()
    changed = copy.deepcopy(document)
    item = changed["paths"][PATH]["head"]["responses"][status]["headers"][header]
    if mutation == "required":
        item["required"] = False
    elif mutation == "type":
        item["schema"]["type"] = "integer"
    elif mutation == "const":
        item["schema"]["const"] = "wrong"
    else:
        item["schema"]["pattern"] = ".+"
    with pytest.raises(ValueError):
        contract_check.validate_openapi_document(changed)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("version", "1.0.0"),
        ("path", "contract/openapi/v1/other.json"),
        ("sha256", "0" * 64),
    ],
)
def test_checker_rejects_each_direct_http_provenance_mutation(
    field: str, value: str
) -> None:
    _, provenance = _documents()
    changed = copy.deepcopy(provenance)
    changed["http_contract"][field] = value
    with pytest.raises(ValueError):
        contract_check.validate_http_provenance(ROOT, changed)


def test_aggregate_digest_does_not_substitute_for_stale_direct_digest() -> None:
    _, provenance = _documents()
    changed = copy.deepcopy(provenance)
    changed["combined_sha256"] = hashlib.sha256(OPENAPI.read_bytes()).hexdigest()
    changed["http_contract"]["sha256"] = "0" * 64
    with pytest.raises(ValueError):
        contract_check.validate_http_provenance(ROOT, changed)


def test_updated_direct_digest_does_not_hide_wrong_openapi_version(
    tmp_path: Path,
) -> None:
    document, provenance = _documents()
    changed = copy.deepcopy(document)
    changed["info"]["version"] = "1.0.0"
    encoded = json.dumps(changed, ensure_ascii=False).encode("utf-8")
    contract_dir = tmp_path / "contract/openapi/v1"
    contract_dir.mkdir(parents=True)
    (contract_dir / "openapi.json").write_bytes(encoded)
    direct = copy.deepcopy(provenance)
    direct["http_contract"]["sha256"] = hashlib.sha256(encoded).hexdigest()

    contract_check.validate_http_provenance(tmp_path, direct)
    with pytest.raises(ValueError):
        contract_check.validate_openapi_document(changed)


def test_execution_proof_docs_state_exact_current_facts() -> None:
    api = (ROOT / "docs/API_CONTRACT.md").read_text(encoding="utf-8")
    current = (ROOT / "docs/CURRENT.md").read_text(encoding="utf-8")
    technical = (ROOT / "docs/TECHNICAL_DESIGN.md").read_text(encoding="utf-8")
    adr = (ROOT / "docs/ADR/ADR-004-agent-execution-proof-and-jwks.md").read_text(
        encoding="utf-8"
    )
    security = (ROOT / "docs/SECURITY.md").read_text(encoding="utf-8")
    runbook = (ROOT / "docs/RUNBOOK.md").read_text(encoding="utf-8")
    agent_api = (ROOT / "docs/agent/api-contract.md").read_text(encoding="utf-8")
    bff = (ROOT / "docs/agent/bff-integration.md").read_text(encoding="utf-8")
    ga_plan = (ROOT / "docs/agent/technical-plan.md").read_text(encoding="utf-8")
    assert "400 execution_proof_jwks_invalid_request" in api
    assert "400 `invalid_request`" not in api
    assert "A2 才进行 Ed25519 数学验签" not in api
    assert "OpenAPI 仍未新增 JWKS route" not in technical
    assert "未来 OpenAPI 只描述 JWKS HTTP projection" not in adr
    assert "still no worker private-key loader" not in current
    assert "仍没有 worker private-key loader" not in current
    assert "A2b current candidate" in current
    for pending in ("A2c", "IAM", "Platform", "transport"):
        assert pending in current
    assert "worker private-key consumption gate 要到 A2c" not in security
    assert "A2b 已阻止 worker consumption" not in security
    assert "A2c/consumer composition" in security
    assert "多 key 或同一 fd 校验失败均阻止 worker 开始消费" not in security
    assert "standalone loader 本身 fail closed" in security
    assert "HTTP YAML scalar 遵循 SafeLoader tag 语义" in security
    assert "draining admission gate" in security
    assert "不强制取消超时 Python handler thread" in security
    for document in (api, technical, adr, security):
        assert "完整响应不超过64 KiB" not in document
        assert "完整 canonical response 必须小于等于 64 KiB" not in document
        assert "完整canonical response不超过64 KiB" not in document
        assert "JWKS representation body" in document
        assert "不含 HTTP headers" in document
    assert "public ring" in runbook.split("`GET /readyz`")[1].split("\n", 1)[0]
    assert "SIGINT/SIGTERM" in runbook
    assert "active handler 在固定 2 秒 deadline 内 drain" in runbook
    assert "Python 线程不可强制取消" in runbook
    assert "agent_draining" in runbook
    assert "pre-existing TCP connection 不预留 admission" in runbook
    assert "service bearer preflight 优先于 drain gate" in runbook
    assert "active handler 在固定 2 秒 deadline 内 drain" in (
        ROOT / "docs/ACCEPTANCE.md"
    ).read_text(encoding="utf-8")
    assert "除 `/healthz` 外，入口要求" not in agent_api
    api_inventory = api.split("## HTTP 边界", 1)[1].split("成功响应", 1)[0]
    assert "exact `/v1/execution-proof/jwks` GET/HEAD" in api_inventory
    agent_inventory = agent_api.split("## 2. HTTP ingress", 1)[1].split(
        "Session detail", 1
    )[0]
    assert "`GET/HEAD`" in agent_inventory
    assert "`/v1/execution-proof/jwks`" in agent_inventory
    assert "exact `GET /healthz` and exact `GET|HEAD /v1/execution-proof/jwks`" in api
    assert "A1 artifact 与 A2a signer 已提交" in api
    assert "A2b 当前是待复审候选" in api
    assert "A2c/consumer、IAM verifier、Platform proof wire 均待实现" in api
    assert "exact `GET /healthz` and exact `GET|HEAD` JWKS" in agent_api
    assert (
        "`execution_identity`"
        not in agent_api.split("### Run admission", 1)[1].split("### Control", 1)[0]
    )
    assert "除 `/healthz` 外的请求始终" not in bff
    assert "exact `GET /healthz`" in bff
    assert "exact JWKS `GET/HEAD`" in bff
    bff_business_inventory = bff.split("## 2. 当前 Agent HTTP business ingress", 1)[
        1
    ].split("另外提供", 1)[0]
    assert "/v1/execution-proof/jwks" not in bff_business_inventory
    launch = bff.split("### 请求、认证与响应约束", 1)[1].split(
        "- `POST /v1/runs/{run_id}/control`", 1
    )[0]
    assert "`execution_identity`" not in launch
    assert "trusted headers" in launch
    assert "HTTP ingress 的非 `/healthz` 请求始终要求" not in ga_plan
    assert (
        "匿名例外仅为 exact `GET /healthz` 与 exact "
        "`GET|HEAD /v1/execution-proof/jwks`" in ga_plan
    )
    assert "其余 ingress 始终要求已配置的内部密钥" in ga_plan
