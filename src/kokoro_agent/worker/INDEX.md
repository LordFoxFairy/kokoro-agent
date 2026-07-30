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

`main.py` constructs streams, checkpoint/ledger/memory stores, the Platform Hub mTLS RPC client,
personas, toolbox, model gateway settings, subagent catalog, and delivery storage. It injects
these into `AssembleDeps` and starts `RunSupervisor.serve`.

Skills and MCP definitions are never loaded from local YAML, environment JSON, shared Hub
databases, or seed directories. Each run calls the injected capability resolver once before
tool construction and receives a verified immutable execution assembly.

## Runtime invariants

- Incoming messages pass `parse_inbound`; scheduling dependencies are injected and do not read
  environment state.
- Durable claim precedes ACK; lease fencing prevents a stale worker from emitting a terminal.
- Resume, cancel, and steer use keep-first durable control records and terminal rechecks.
- Critical events keep stable outbox identity across retries and recovery.
- SIGTERM stops intake, drains bounded active work, and leaves remaining recovery to leases.
- Capability resolution is fail-closed. A missing or invalid Hub assembly cannot fall back to
  local configuration.

## Public boundary

- `main.py`: process entry point and dependency wiring.
- `supervisor.py`: durable lifecycle state machine.
- `messages.py`: inbound contract parsing.

## Verification

Run `uv run pytest -q tests/test_assembly.py tests/test_invoke.py tests/test_control_inbox.py`,
then `uv run ruff check .` and `uv run pyright`. Infrastructure-backed recovery suites run only
after the code and boundary checks are clean.
