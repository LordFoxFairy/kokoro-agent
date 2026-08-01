import re
import subprocess
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
CANONICAL_MONGO_IMAGE = (
    "mongo:7@sha256:340c1c56fb10e95cf79ff547f8664b96bc6ead9909bc355238cbf865a9695a6f"
)
_BOOL_TAG = "tag:yaml.org,2002:bool"


class _Yaml12SafeLoader(yaml.SafeLoader):
    """PyYAML SafeLoader with the YAML 1.1 yes/no/on/off boolean aliases removed."""

    yaml_implicit_resolvers: dict[str | None, list[tuple[str, re.Pattern[str]]]] = {
        first_character: [
            (tag, pattern) for tag, pattern in resolvers if tag != _BOOL_TAG
        ]
        for first_character, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }


_YAML12_BOOL = re.compile(r"^(?:true|false)$", re.IGNORECASE)
for _first_character in "tTfF":
    _Yaml12SafeLoader.yaml_implicit_resolvers.setdefault(_first_character, []).append(
        (_BOOL_TAG, _YAML12_BOOL)
    )


class _WorkflowStep(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="allow")

    name: str | None = None
    run: str | None = None
    condition: str | None = Field(default=None, alias="if")
    timeout_minutes: int | None = Field(default=None, alias="timeout-minutes")


class _WorkflowService(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="allow")

    image: str


class _GatesJob(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="allow")

    services: dict[str, _WorkflowService]
    env: dict[str, str]
    steps: list[_WorkflowStep]


class _WorkflowJobs(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="allow")

    gates: _GatesJob


class _Workflow(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="allow")

    trigger: dict[str, None] = Field(alias="on")
    jobs: _WorkflowJobs


def _load_workflow() -> _Workflow:
    return _Workflow.model_validate(
        yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=_Yaml12SafeLoader)
    )


def _named_step(job: _GatesJob, name: str) -> _WorkflowStep:
    matches = [step for step in job.steps if step.name == name]
    assert len(matches) == 1, f"expected exactly one {name!r} step"
    return matches[0]


def _run_step(job: _GatesJob, command: str) -> _WorkflowStep:
    matches = [step for step in job.steps if step.run == command]
    assert len(matches) == 1, f"expected exactly one step running {command!r}"
    return matches[0]


def _bash_syntax_check(script: str) -> None:
    subprocess.run(
        ["bash", "-n"],
        input=script,
        text=True,
        capture_output=True,
        check=True,
    )


def test_ci_runs_locked_static_gates_before_starting_mongo() -> None:
    workflow = _load_workflow()
    assert workflow.trigger == {"push": None, "pull_request": None}

    job = workflow.jobs.gates
    assert job.services["redis"].image == "redis:7"
    assert "mongo" not in job.services

    sync = _run_step(job, "uv sync --locked")
    ruff = _named_step(job, "ruff")
    pyright = _named_step(job, "pyright")
    contract = _named_step(job, "CI workflow contract")
    start_mongo = _named_step(job, "Start Mongo replica set")
    pytest = _named_step(job, "pytest")
    stop_mongo = _named_step(job, "Stop Mongo replica set")

    assert ruff.run == "uv run ruff check src tests"
    assert pyright.run == "uv run pyright"
    assert contract.run == "uv run pytest tests/repository/test_ci_workflow.py -q"
    assert pytest.run == "uv run pytest -q"
    assert (
        job.steps.index(sync)
        < job.steps.index(ruff)
        < job.steps.index(pyright)
        < job.steps.index(contract)
        < job.steps.index(start_mongo)
        < job.steps.index(pytest)
        < job.steps.index(stop_mongo)
    )
    assert stop_mongo is job.steps[-1]


def test_ci_bootstraps_bounded_pinned_mongo_replica_set_and_always_cleans_up() -> None:
    job = _load_workflow().jobs.gates
    assert job.env["KOKORO_CI_MONGO_IMAGE"] == CANONICAL_MONGO_IMAGE
    assert job.env["KOKORO_MONGO_URL"] == (
        "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true"
    )

    start = _named_step(job, "Start Mongo replica set")
    assert start.timeout_minutes == 5
    assert start.run is not None
    for required in (
        "docker run --detach",
        '--name "${KOKORO_CI_MONGO_CONTAINER}"',
        "--publish 127.0.0.1:27017:27017",
        '"${KOKORO_CI_MONGO_IMAGE}"',
        "--replSet kokoro-rs",
        "--bind_ip_all",
        "mongosh --quiet --host 127.0.0.1 --eval",
        "db.runCommand({ ping: 1 })",
        "rs.status()",
        "NotYetInitialized",
        "rs.initiate",
        'host: "localhost:27017"',
        "AlreadyInitialized",
        "isWritablePrimary",
        "docker logs",
    ):
        assert required in start.run
    assert start.run.count("for attempt in $(seq 1 30)") == 2
    assert (
        start.run.index("db.runCommand({ ping: 1 })")
        < start.run.index("rs.initiate")
        < start.run.index("isWritablePrimary")
    )
    _bash_syntax_check(start.run)

    stop = _named_step(job, "Stop Mongo replica set")
    assert stop.condition == "always()"
    assert stop.timeout_minutes is None
    assert stop.run == (
        'docker rm --force "${KOKORO_CI_MONGO_CONTAINER}" >/dev/null 2>&1 || true'
    )
    _bash_syntax_check(stop.run)
