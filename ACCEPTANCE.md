# Agent acceptance

This document is the self-contained acceptance contract for the `kokoro-agent`
owner. It covers the Agent HTTP ingress, its PostgreSQL durable state, and its
Redis transport. The HTTP host and worker are separate processes; the host
admits and exposes evidence, while the worker executes admitted runs.

## Required fixtures

The acceptance gate requires reachable PostgreSQL and Redis instances. Missing
or unreachable services are test failures, not skipped tests.

```bash
export KOKORO_AGENT_DATABASE_URL=postgresql://127.0.0.1/postgres
export KOKORO_REDIS_URL=redis://127.0.0.1:6379/0
export KOKORO_AGENT_DATABASE_SCHEMA=kokoro_agent_acceptance
export KOKORO_INTERNAL_SECRET_AGENT=acceptance-internal-secret
```

The test suite creates a unique PostgreSQL schema per test and removes it after
the test. Redis stream names are run-scoped where applicable; the launch stream
uses unique run IDs.

## HTTP owner interface

Start the HTTP owner with the configured database, Redis, schema, and internal
secret:

```bash
uv run kokoro-agent-http
```

`GET /healthz` is the unauthenticated process liveness probe:

```json
{"status":"ok","service":"kokoro-agent"}
```

Every other route requires these headers when
`KOKORO_INTERNAL_SECRET_AGENT` is configured:

```text
x-kokoro-service: kokoro-bff
x-kokoro-internal-secret: <configured secret>
```

`GET /readyz` checks the Agent-owned PostgreSQL and Redis dependencies and
returns:

```json
{"status":"ready","service":"kokoro-agent"}
```

### Launch

`POST /v1/runs` accepts the Agent launch transport:

```json
{
  "request_id": "request-1",
  "run_id": "run-1",
  "session_id": "session-1",
  "feature_key": "chat",
  "execution_identity": {
    "tenant_ref": "tenant",
    "actor": {"kind": "user", "opaque_ref": "actor"},
    "subject": {"kind": "user", "opaque_ref": "subject"},
    "identity_assertion_ref": "assertion"
  },
  "message_id": "message-1",
  "content": "hello"
}
```

The owner writes the immutable dispatch fence to PostgreSQL before publishing
the worker envelope to Redis. A successful request returns `202` with
`data.run_id`, `data.session_id`, and `data.replayed`. Repeating an identical
launch is idempotent; reusing `run_id` with a different immutable body returns
`409` with `run_identity_conflict`.

### Run control and evidence

`POST /v1/runs/{run_id}/control` accepts `run.cancel`, `run.resume`, or
`run.steer` bodies. The body always includes `kind`, `session_id`, and
`decision_id`; `run.resume` also includes non-empty `decisions`, while
`run.steer` includes `message_id` and `content`. A valid control request returns
`202` and is published to the run-isolated Redis control stream.

`GET /v1/runs/{run_id}/events?after_seq=0&limit=200` returns a `200` business
envelope containing the filtered event list, `next_seq`, and `terminal`. An
unknown run returns `404` with `run_not_found`.

### Session projections

Both session routes require trusted identity headers:

```text
x-kokoro-tenant-ref: <tenant reference>
x-kokoro-subject-ref: <subject reference>
x-kokoro-actor-ref: <actor reference>
x-kokoro-identity-assertion-ref: <assertion reference>
```

Optional `x-kokoro-subject-kind` and `x-kokoro-actor-kind` values are `user`,
`project`, or `service`.

- `GET /v1/sessions/{session_id}/messages` returns the allowlisted durable chat
  messages and `next_seq`.
- `GET /v1/sessions/{session_id}/events` returns the allowlisted durable chat
  events, `next_seq`, and `watermark`.

Both routes derive the storage namespace from the trusted identity. A different
subject therefore sees an empty projection rather than another subject's data.

Business success responses use `{data,meta:{request_id}}`; errors use
`{error:{code,message},meta:{request_id}}`. The HTTP owner does not expose raw
Python, SQL, or Redis errors.

## Acceptance commands

The fast gate includes unit and contract tests and intentionally excludes
service-backed suites:

```bash
uv run pytest
```

The complete Agent acceptance gate explicitly removes the default exclusion and
runs unit, contract, integration, e2e, and HTTP acceptance tests against the
configured PostgreSQL and Redis fixtures:

```bash
uv run pytest -o addopts='' tests/unit tests/contract tests/integration tests/e2e tests/acceptance
```

The HTTP acceptance suite starts the real loopback HTTP host and verifies:

1. liveness and readiness responses;
2. authenticated launch, durable PostgreSQL admission, Redis publication, and
   idempotent retry;
3. stable authentication and validation errors;
4. run control and paginated evidence over real Redis/PostgreSQL state; and
5. identity-scoped session history and replay over real PostgreSQL state.

The gate is complete only when both service fixtures are reachable and pytest
reports no collection, setup, or test failure. A skipped test caused by a
missing PostgreSQL or Redis fixture is not an acceptance result.
