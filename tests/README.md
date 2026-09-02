# Test architecture

Tests are organized by verification boundary rather than kept in one flat directory:

```text
tests/
├── contract/       # public wire, DeepAgents/Swarm locks, architecture rules
├── unit/           # hermetic behavior grouped by GA module boundary
├── integration/    # Redis, PostgreSQL, Docker, MinIO, or filesystem-backed integration
├── e2e/            # complete worker/runtime paths
├── acceptance/     # Agent HTTP owner interface against PostgreSQL + Redis fixtures
└── support/        # shared fakes and typed third-party test builders
```

The default command is hermetic:

```bash
uv run pytest
```

The default command is the fast gate: it includes unit and contract tests and
excludes service-backed integration, e2e, and HTTP acceptance tests.

The service-backed gate is explicit and must run with reachable PostgreSQL and
Redis fixtures:

```bash
uv run pytest -o addopts='' tests/unit tests/contract tests/integration tests/e2e tests/acceptance
```

`conftest.py` derives explicit markers from `contract/`, `integration/`, `e2e/`,
and `acceptance/`. A unit file may contain a focused real-service case; using a
shared service fixture marks only that case as integration. New service-heavy
runtime suites belong under `integration/` instead of being added to a filename
allowlist. Contract tests are explicitly marked but remain in the default fast
gate; acceptance tests are explicitly marked and are reserved for the
service-backed gate.

The shared PostgreSQL/Redis fixtures fail loudly when a configured service is
unreachable. The HTTP acceptance suite also performs its own preflight, so a
service job cannot become green by collecting tests while silently lacking a
backend.

`support/local_fake.py` is a test-only deterministic model driver. It exercises the
real DeepAgents loop and tool/HITL paths without provider credentials; it is not part
of the production package and is not a worker configuration mode.
