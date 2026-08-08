from __future__ import annotations

import copy
import json
from pathlib import Path
import tomllib
from typing import Any, Protocol, cast

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_ENTRYPOINTS = {
    "kokoro-agent-worker": "kokoro_agent.worker.main:main",
    "kokoro-agent-evidence": "kokoro_agent.evidence.main:main",
    "kokoro-agent-presentation": "kokoro_agent.presentation.main:main",
}
EXPECTED_DEPLOYABLES = tuple(EXPECTED_ENTRYPOINTS)


class _SchemaValidator(Protocol):
    def validate(self, instance: object) -> None: ...


def _inventory() -> dict[str, Any]:
    loaded = yaml.safe_load((ROOT / "deployables.yaml").read_text())
    assert isinstance(loaded, dict)
    return cast(dict[str, Any], loaded)


def _schema() -> dict[str, Any]:
    loaded = json.loads((ROOT / "deployables.schema.json").read_text())
    assert isinstance(loaded, dict)
    return cast(dict[str, Any], loaded)


def test_owner_inventory_is_a_closed_valid_v1_document() -> None:
    schema = _schema()
    inventory = _inventory()

    Draft202012Validator.check_schema(schema)
    validator = cast(_SchemaValidator, Draft202012Validator(schema))
    validator.validate(inventory)

    assert set(inventory) == {
        "schemaVersion",
        "image",
        "releasePolicyContract",
        "deployables",
    }
    assert inventory["schemaVersion"] == 1
    assert tuple(item["id"] for item in inventory["deployables"]) == EXPECTED_DEPLOYABLES

    unknown_top_level = copy.deepcopy(inventory)
    unknown_top_level["compatibilityDeployables"] = []
    with pytest.raises(ValidationError):
        validator.validate(unknown_top_level)

    unknown_process_field = copy.deepcopy(inventory)
    unknown_process_field["deployables"][0]["legacyEntrypoint"] = "worker.py"
    with pytest.raises(ValidationError):
        validator.validate(unknown_process_field)


def test_inventory_binds_only_real_console_entrypoints_and_no_compatibility_shims() -> None:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)["project"]
    scripts = cast(dict[str, str], project["scripts"])
    inventory = _inventory()

    assert {
        name: target
        for name, target in scripts.items()
        if name.startswith("kokoro-agent-")
    } == EXPECTED_ENTRYPOINTS
    assert {
        item["id"]: item["command"] for item in inventory["deployables"]
    } == {name: name for name in EXPECTED_ENTRYPOINTS}
    assert not any(
        token in item["command"]
        for item in inventory["deployables"]
        for token in ("compat", "legacy", "python -m", "uv run")
    )


def test_every_process_exposes_real_readiness_while_release_policy_stays_blocked() -> None:
    deployables = {
        item["id"]: item for item in _inventory()["deployables"]
    }

    for deployable in deployables.values():
        assert deployable["activationAuthorized"] is False
        assert deployable["runtimeTraffic"] is False
        assert deployable["launchReadiness"] == "blocked"
        assert deployable["replicaPolicy"] == {
            "minimum": 0,
            "maximum": 0,
            "autoscaling": "forbidden-while-blocked",
        }
        assert deployable["probes"]["liveness"] == {
            "kind": "container-process",
            "status": "available",
        }
        assert deployable["probes"]["readiness"]["kind"] == "dependency-aware"
        assert deployable["probes"]["readiness"]["status"] == "available"
        assert deployable["probes"]["readiness"]["command"] == [
            deployable["command"],
            "--readiness",
        ]
        assert deployable["probes"]["readiness"]["dependencies"]
        assert "dependency-aware-readiness-not-implemented" not in deployable["launchBlockers"]
        assert "root-k8s-secret-mount-contract-mismatch" in deployable["launchBlockers"]

    execution_blockers = {
        "execution-owner-lease-epoch-not-bound",
        "terminal-outbox-evidence-not-atomic",
    }
    assert execution_blockers <= set(deployables["kokoro-agent-worker"]["launchBlockers"])
    assert execution_blockers <= set(deployables["kokoro-agent-evidence"]["launchBlockers"])
    assert "root-boundary-contract-only" in deployables[
        "kokoro-agent-evidence"
    ]["launchBlockers"]
    assert "root-boundary-contract-only" in deployables[
        "kokoro-agent-presentation"
    ]["launchBlockers"]


def test_listener_and_contract_inventory_matches_the_three_process_boundaries() -> None:
    deployables = {
        item["id"]: item for item in _inventory()["deployables"]
    }

    assert deployables["kokoro-agent-worker"]["listeners"] == []
    assert deployables["kokoro-agent-worker"]["inboundContracts"] == []
    assert deployables["kokoro-agent-worker"]["declaredInboundContracts"] == [
        "session-agent-execution@v1",
    ]
    assert deployables["kokoro-agent-worker"]["outboundContracts"] == [
        "hub-runtime@v1",
        "model-gateway@v1",
        "platform-media-runtime@v1",
    ]

    evidence_listener = deployables["kokoro-agent-evidence"]["listeners"]
    presentation_listener = deployables["kokoro-agent-presentation"]["listeners"]
    assert evidence_listener == [
        {
            "name": "connect",
            "containerPort": 8443,
            "protocol": "connect-http2",
            "exposure": "service",
            "mtls": True,
        }
    ]
    assert presentation_listener == [
        {
            "name": "connect",
            "containerPort": 8444,
            "protocol": "connect-http2",
            "exposure": "service",
            "mtls": True,
        }
    ]
    assert deployables["kokoro-agent-evidence"]["declaredInboundContracts"] == [
        "agent-execution-evidence@v1"
    ]
    assert "root-boundary-version-mismatch" in deployables[
        "kokoro-agent-evidence"
    ]["launchBlockers"]
    assert deployables["kokoro-agent-presentation"]["declaredInboundContracts"] == [
        "agent-presentation@v1"
    ]

    assert {
        "KOKORO_AGENT_READINESS_TIMEOUT_MS",
        "KOKORO_AGENT_EVIDENCE_READINESS_URL",
        "KOKORO_AGENT_EVIDENCE_READINESS_CA_FILE",
        "KOKORO_AGENT_EVIDENCE_READINESS_CERT_FILE",
        "KOKORO_AGENT_EVIDENCE_READINESS_KEY_FILE",
    } <= set(deployables["kokoro-agent-evidence"]["requiredEnvironment"])
    assert {
        "KOKORO_AGENT_READINESS_TIMEOUT_MS",
        "KOKORO_AGENT_PRESENTATION_READINESS_URL",
        "KOKORO_AGENT_PRESENTATION_READINESS_CA_FILE",
        "KOKORO_AGENT_PRESENTATION_READINESS_CERT_FILE",
        "KOKORO_AGENT_PRESENTATION_READINESS_KEY_FILE",
    } <= set(deployables["kokoro-agent-presentation"]["requiredEnvironment"])

    evidence_entrypoint = (ROOT / "src/kokoro_agent/evidence/main.py").read_text()
    presentation_entrypoint = (ROOT / "src/kokoro_agent/presentation/main.py").read_text()
    assert "kokoro.agent.execution.v1" in evidence_entrypoint
    assert "kokoro.agent.execution.v2" not in evidence_entrypoint
    assert "kokoro.agent.presentation.v1" in presentation_entrypoint


def test_production_image_is_pinned_non_root_and_read_only_compatible() -> None:
    inventory = _inventory()
    image = inventory["image"]
    dockerfile = (ROOT / image["dockerfile"]).read_text()
    runtime = dockerfile.split(" AS runtime\n", maxsplit=1)[1]

    assert image == {
        "artifact": "kokoro-agent",
        "buildRoot": ".",
        "dockerfile": "Dockerfile",
        "runtimeTarget": "runtime",
        "defaultCommand": "kokoro-agent-worker",
        "runtimeUser": "10001:10001",
        "readOnlyRootFilesystem": True,
        "writableTmpfs": ["/tmp"],
        "serviceAccountToken": False,
        "allowPrivilegeEscalation": False,
        "capabilities": {"add": [], "drop": ["ALL"]},
        "seccompProfile": "RuntimeDefault",
    }
    assert dockerfile.count(" AS build\n") == 1
    assert dockerfile.count(" AS runtime\n") == 1
    assert "python:3.11-slim-bookworm@sha256:" in dockerfile
    assert "uv==0.9.4" in dockerfile
    assert "uv sync --frozen --no-dev --no-editable" in dockerfile
    assert "USER 10001:10001" in runtime
    assert "PYTHONDONTWRITEBYTECODE=1" in runtime
    assert 'CMD ["kokoro-agent-worker"]' in runtime
    assert "HEALTHCHECK NONE" in runtime
    assert "uv run" not in runtime
    assert "UV_CACHE_DIR" not in runtime
    assert "pip install" not in runtime
    assert "/usr/local/lib/python3.11/site-packages/pip" in runtime
    assert "/usr/local/bin/pip" in runtime
    for entrypoint in EXPECTED_ENTRYPOINTS:
        assert f"/app/.venv/bin/{entrypoint}" in runtime


def test_non_editable_wheel_declares_every_runtime_package_resource() -> None:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)

    assert project["tool"]["setuptools"]["package-data"]["kokoro_agent"] == [
        "prompts/*.md",
        "interaction/generated/*.json",
    ]
    assert "jsonschema>=4.26.0" in project["dependency-groups"]["dev"]


def test_docker_context_excludes_local_and_prebuilt_artifacts() -> None:
    ignored = {
        line.strip()
        for line in (ROOT / ".dockerignore").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert {
        ".git",
        ".venv",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "*.pyc",
        ".env",
        ".env.*",
        "*.egg-info",
        "build",
        "dist",
        "tests",
        "tmp",
    } <= ignored
    repository_ignored = {
        line.strip()
        for line in (ROOT / ".gitignore").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert "build/" in repository_ignored


def test_architecture_docs_expose_the_inventory_without_stale_presentation_claims() -> None:
    readme = (ROOT / "README.md").read_text()
    index = (ROOT / "INDEX.md").read_text()

    for source in (readme, index):
        assert "deployables.yaml" in source
        assert "deployables.schema.json" in source
        assert "kokoro-agent-worker" in source
        assert "kokoro-agent-evidence" in source
        assert "kokoro-agent-presentation" in source
    assert "【休眠候选边界】" not in readme
    assert "It has no transport and is not wired into `RunEmitter`" not in index
    assert "The Hub compatibility consumer" not in index
    assert "readiness as missing" not in index
    assert "缺依赖感知" not in readme
