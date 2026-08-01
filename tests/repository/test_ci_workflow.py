import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def test_ci_runs_locked_static_gates_before_starting_mongo() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")
    services = source[source.index("    services:") : source.index("    env:")]
    assert "      redis:" in services
    assert "      mongo:" not in services

    for command in (
        "uv sync --locked",
        "uv run ruff check src tests",
        "uv run pyright",
        "uv run pytest tests/repository/test_ci_workflow.py -q",
        "uv run pytest -q",
    ):
        assert command in source

    pyright = source.index("      - name: pyright")
    start_mongo = source.index("      - name: Start Mongo replica set")
    pytest = source.index("      - name: pytest")
    stop_mongo = source.index("      - name: Stop Mongo replica set")
    assert pyright < start_mongo < pytest < stop_mongo
    assert source.rstrip().endswith(
        'run: docker rm --force "${KOKORO_CI_MONGO_CONTAINER}" >/dev/null 2>&1 || true'
    )


def test_ci_bootstraps_bounded_pinned_mongo_replica_set_and_always_cleans_up() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")
    assert (
        "KOKORO_MONGO_URL: "
        "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true"
    ) in source
    assert re.search(
        r"^      KOKORO_CI_MONGO_IMAGE: mongo:7@sha256:[0-9a-f]{64}$",
        source,
        flags=re.MULTILINE,
    )

    start_offset = source.index("      - name: Start Mongo replica set")
    pytest_offset = source.index("      - name: pytest")
    start_script = source[start_offset:pytest_offset]
    assert "        timeout-minutes: 5" in start_script
    for required in (
        "docker run --detach",
        '--name "${KOKORO_CI_MONGO_CONTAINER}"',
        "--publish 127.0.0.1:27017:27017",
        '"${KOKORO_CI_MONGO_IMAGE}"',
        "--replSet kokoro-rs",
        "--bind_ip_all",
        "mongosh --quiet --host 127.0.0.1 --eval",
        "db.runCommand({ ping: 1 })",
        "for attempt in $(seq 1 30)",
        "rs.status()",
        "NotYetInitialized",
        "rs.initiate",
        'host: "localhost:27017"',
        "AlreadyInitialized",
        "isWritablePrimary",
        "docker logs",
    ):
        assert required in start_script
    assert start_script.count("for attempt in $(seq 1 30)") == 2
    assert (
        start_script.index("db.runCommand({ ping: 1 })")
        < start_script.index("rs.initiate")
        < start_script.index("isWritablePrimary")
    )

    cleanup_script = source[source.index("      - name: Stop Mongo replica set") :]
    assert "        if: always()" in cleanup_script
    assert 'docker rm --force "${KOKORO_CI_MONGO_CONTAINER}"' in cleanup_script
