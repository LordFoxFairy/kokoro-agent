---
architectureIndex: 1
rootId: agent.worker
owners:
  - "@LordFoxFairy"
---

# worker — process assembly and durable run scheduling

## Responsibilities

The worker parses environment configuration once, builds process-scoped infrastructure, and
runs `RunSupervisor`. The supervisor owns request claiming, lease heartbeats, expired-run
adoption, control delivery, terminal arbitration, outbox replay, retention, and graceful drain.

## Non-responsibilities

The worker does not own sessions, Site policy, commercial admission, catalog management, model
pricing, browser SSE, or capability selection. It consumes an opaque `namespace` and the exact
runtime configuration supplied by Session.

## Process assembly

`main.py` constructs streams, checkpoint/ledger stores, the Platform Hub mTLS RPC client, personas,
toolbox, model gateway settings, subagent catalog, and delivery storage. It injects these into
`AssembleDeps` and starts `RunSupervisor.serve`. ADR-013 M0 deliberately excludes the legacy Mongo
memory store from this production composition; Mongo still owns checkpoints and the run ledger.

Skills and MCP definitions are never loaded from local YAML, environment JSON, shared Hub
databases, or seed directories. Each run calls the injected capability resolver once before
tool construction and receives a verified immutable execution assembly.

`kokoro-agent-worker --readiness` is an independent bounded exec probe. It concurrently proves a
writable Mongo replica set with a real transaction, the exact Redis stream/group/claim/ack feature
set, and authenticated HTTP/2 Connect application paths for Hub and Model Gateway. The deliberately
invalid RPC probes accept only each service's exact safe `INVALID_ARGUMENT` domain response and
cannot reach capability lookup, model execution, or billing.

## Runtime invariants

- Incoming messages pass `parse_inbound`; `RunRequest` tool names are validated immediately after
  that pure boundary parse and before dispatch claim, execution binding, Hub, or sandbox effects.
  Invalid catalog frames are quarantined and acknowledged without creating a run terminal.
  Scheduling dependencies are injected and do not read environment state.
- Durable claim precedes ACK; lease fencing prevents a stale worker from emitting a terminal.
- Resume, cancel, and steer use keep-first durable control records and terminal rechecks.
- Critical events keep stable outbox identity across retries and recovery.
- SIGTERM stops intake, drains bounded active work, and leaves remaining recovery to leases.
- Each heartbeat refreshes global retained output/evidence record gauges. Destructive terminal
  retention is startup-fenced at zero: config parsing and direct supervisor construction reject
  a nonzero run TTL with `DURABLE_OUTPUT_RETENTION_REQUIRES_CONSUMER_ACK` until Root/Session
  define a consumer ACK/tombstone protocol. The separate live events-stream TTL remains allowed.
- Capability resolution is fail-closed. A missing or invalid Hub assembly cannot fall back to
  local configuration.
- A non-empty historical `KOKORO_AGENT_MEMORY` setting fails process configuration. Legacy
  `save_memory`/`search_memory` names fail at the supervisor ingress before dispatch claim, Hub
  resolution, execution binding, or sandbox allocation; assembly retains the same defense.
- Model streaming is accepted only from Platform's private RPC. Agent verifies the invocation and
  attempt identity, every sequence/hash-chain link, the aggregate terminal result, and resumes a
  transient disconnect from the last verified sequence using the same immutable attempt.

## Public boundary

- `main.py`: process entry point and dependency wiring.
- `supervisor.py`: durable lifecycle state machine.
- `messages.py`: inbound contract parsing.

## Verification

Run `uv run pytest -q tests/test_assembly.py tests/test_invoke.py tests/test_control_inbox.py`,
then `uv run ruff check .` and `uv run pyright`. Infrastructure-backed recovery suites run only
after the code and boundary checks are clean.
