"""Normal generation/check and installed distribution registration of capacity."""

from __future__ import annotations
import json
from pathlib import Path
import shutil

import pytest
from jsonschema import ValidationError

from kokoro_agent.model_call_capacity_guarantee_contract import (
    AUDIT_FILES,
    BASE,
    generate,
)
from kokoro_agent.model.capacity_guarantee import load_capacity_guarantee

ROOT = Path(__file__).resolve().parents[2]


def test_formal_generator_has_no_drift_and_all_resources_are_packaged() -> None:
    generate(ROOT, check=True)
    source = (ROOT / "pyproject.toml").read_text()
    assert all(f'"{name}"' in source for name in AUDIT_FILES)
    assert load_capacity_guarantee().owner == "kokoro-agent"


@pytest.mark.parametrize(
    "name",
    ["schema.json", "guarantee.json", "evidence.json", "vectors.json", "manifest.json"],
)
def test_current_artifact_mutation_is_detected(tmp_path: Path, name: str) -> None:
    for relative in AUDIT_FILES:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    path = tmp_path / BASE / name
    value = json.loads(path.read_bytes())
    value["extra"] = "drift"
    path.write_text(json.dumps(value))
    with pytest.raises((ValueError, AssertionError, ValidationError)):
        generate(tmp_path, check=True)


@pytest.mark.parametrize(
    "mutation", ["guarantee-contradiction", "evidence-contradiction", "unknown-keyword"]
)
def test_write_generator_rejects_schema_semantic_drift_before_regeneration(
    tmp_path: Path,
    mutation: str,
) -> None:
    for relative in AUDIT_FILES:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    path = tmp_path / BASE / "schema.json"
    value = json.loads(path.read_bytes())
    if mutation == "guarantee-contradiction":
        value["properties"]["owner"]["maxLength"] = 1
    elif mutation == "evidence-contradiction":
        value["$defs"]["ReviewedEvidence"]["properties"]["owner"]["maxLength"] = 1
    else:
        value["properties"]["owner"]["unrecognizedConstraint"] = True
    path.write_text(json.dumps(value))
    before = {
        str(p.relative_to(tmp_path)): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    with pytest.raises((ValueError, ValidationError)):
        generate(tmp_path, check=False)
    assert {
        str(p.relative_to(tmp_path)): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    } == before
