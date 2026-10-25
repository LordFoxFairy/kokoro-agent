"""Finite schema/compiler identity and installed resource registration."""

from __future__ import annotations
import json
from pathlib import Path
import shutil
import pytest
from jsonschema import ValidationError
from kokoro_agent.model_call_bound_contract import AUDIT_FILES, BASE, generate
from kokoro_agent.distribution_assets import read_model_call_bound_assets

ROOT = Path(__file__).resolve().parents[2]


def test_bound_normal_generation_and_resource_inventory() -> None:
    generate(ROOT, check=True)
    assert set(read_model_call_bound_assets()) == {
        "schema.json",
        "vectors.json",
        "manifest.json",
    }
    package = (ROOT / "pyproject.toml").read_text()
    assert all(f'"{name}"' in package for name in AUDIT_FILES)


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown_keyword",
        "contradiction",
        "extra_field",
        "scalar",
        "system",
        "guarantee",
        "vector",
    ],
)
def test_generation_rejects_schema_or_identity_drift_in_write_mode(
    tmp_path: Path, mutation: str
) -> None:
    for relative in (
        *AUDIT_FILES,
        "contract/usage/v2/schema.json",
        "contract/provenance.json",
    ):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    path = tmp_path / BASE / "schema.json"
    schema = json.loads(path.read_bytes())
    if mutation == "unknown_keyword":
        schema["properties"]["producer"]["unknownConstraint"] = True
    elif mutation == "contradiction":
        schema["properties"]["producer"]["maxLength"] = 1
    elif mutation == "extra_field":
        schema["properties"]["extra"] = {"type": "string", "const": "x"}
        schema["required"].append("extra")
    elif mutation == "scalar":
        schema["$defs"]["Count"]["maxLength"] = 1
    elif mutation == "system":
        schema["properties"]["system_provenance"]["const"]["version"] = "3.0.0"
    elif mutation == "guarantee":
        schema["properties"]["capacity_guarantee_digest"]["const"] = "0" * 64
    elif mutation == "vector":
        vp = tmp_path / BASE / "vectors.json"
        v = json.loads(vp.read_bytes())
        v["valid"][0]["sealed"] = False
        vp.write_text(json.dumps(v))
    path.write_text(json.dumps(schema))
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
