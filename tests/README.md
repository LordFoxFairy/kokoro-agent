# Test architecture

Tests are organized by verification boundary rather than kept in one flat directory:

```text
tests/
├── contract/       # public wire, DeepAgents/Swarm locks, architecture rules
├── unit/           # hermetic behavior grouped by GA module boundary
├── integration/    # Redis, PostgreSQL, Docker, MinIO, or filesystem-backed integration
├── e2e/            # complete worker/runtime paths
└── support/        # shared fakes and typed third-party test builders
```

The default command is hermetic:

```bash
uv run pytest
```

Service-backed gates are explicit:

```bash
uv run pytest -m integration
uv run pytest -m e2e
```

`conftest.py` derives coarse markers from `integration/` and `e2e/`. A unit file
may contain a focused real-service case; using a shared service fixture marks only
that case as integration. New service-heavy suites belong under `integration/`
instead of being added to a filename allowlist.

`support/local_fake.py` is a test-only deterministic model driver. It exercises the
real DeepAgents loop and tool/HITL paths without provider credentials; it is not part
of the production package and is not a worker configuration mode.
