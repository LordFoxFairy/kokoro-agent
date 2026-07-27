from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def test_ci_provisions_runtime_dependencies_and_runs_locked_gates() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")

    assert "redis:" in source
    assert "image: redis:" in source
    assert "mongo:" in source
    assert "image: mongo:" in source
    assert "uv sync --locked" in source
    assert "uv run ruff check src tests" in source
    assert "uv run pyright" in source
    assert "uv run pytest -q" in source
