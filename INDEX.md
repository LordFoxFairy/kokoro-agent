---
architectureIndex: 1
rootId: service.agent
owners:
  - "@LordFoxFairy"
---

# Kokoro Agent service

## Responsibilities

Execute approved Agent runs with LangGraph/DeepAgents, tools, HITL, durable control, sandbox backends, and raw Agent event publication.

## Non-responsibilities

Agent does not own Site identity, accounts, pricing, plans, credit deduction, Session messages, browser transport, provider routing, provider credentials, or model usage settlement.

## Public boundary

The only deployable process entrypoints are `kokoro-agent-worker` (`kokoro_agent.worker.main`),
`kokoro-agent-evidence` (`kokoro_agent.evidence.main`), and `kokoro-agent-presentation`
(`kokoro_agent.presentation.main`). [`deployables.yaml`](deployables.yaml) is their child-owned
activation inventory and [`deployables.schema.json`](deployables.schema.json) closes its shape.
There is no compatibility CLI or implicit deployment entrypoint. `skills`, `hitl`, and
`presentation` bind their Python public surface with `__init__.py` re-exports; `execution` and
`worker` re-export nothing, so their public surface is the module-level symbols listed in their
component INDEX files.

Hub runtime consumption is implemented only by `kokoro_agent.hub.HubExecutionAssemblyClient` over mTLS ConnectRPC. Each run binds the exact `agent_catalog_ref`, ordered grants, streamed Skill artifacts, and MCP Authorization material; there is no compatibility CLI or Hub persistence access.

Platform Media consumption is implemented by the narrow `kokoro_agent.platform.MediaOperationPort`. The `create_image` tool exposes product intent only; run-scoped opaque grants and deterministic command/output identities are injected during assembly. Platform remains the operation journal and execution owner.

Product Memory is Platform-owned. Under ADR-013 M0, the legacy Mongo-backed `save_memory` and
`search_memory` implementations are not imported or composed by production Agent modules. A stale
`KOKORO_AGENT_MEMORY` process setting fails startup, and either legacy name in a Run catalog fails
before Hub resolution or sandbox allocation. Mongo remains the Agent checkpoint/ledger authority;
the isolated legacy store modules exist only for explicit non-production experiments and are not a
Product Memory compatibility path. A future production Memory tool requires the Root-generated
narrow `MemoryPort` and the coordinated M2 contract release.

## Callers and dependencies

Session submits durable run/control messages. Agent consumes opaque `namespace` and calls model/capability/storage adapters through declared boundaries. Production model calls use only the typed Platform Model Gateway ConnectRPC; Admission supplies an opaque authorization handle inside the sealed RunRequest.

## Data ownership and events

Agent owns execution checkpoints, run leases, control/outbox state, and raw Agent events. Session owns conversations and browser-facing projections.

## Runtime and security

Namespace is opaque and is the only GA isolation key. GA holds no provider credential: the Model Gateway mTLS client sends stable call identities plus the opaque authorization handle, while Platform resolves the authorized model and settles usage. Untrusted tool/artifact content is sandboxed and bounded.

The Hub runtime consumer emits only a closed success count and never emits caller credentials,
resolved values, response bodies, or exception details.

The interpreter is pinned by `.python-version` (3.11), matching `requires-python` and the Pyright `pythonVersion`. CI installs uv without a Python version input, so without this file `uv sync --locked` resolves whichever interpreter the runner offers and the same lock runs under different interpreters.

The production image uses a digest-pinned Python base, build-only pinned uv, and a non-editable
package. Its runtime stage contains no uv/pip/cache path, runs as `10001:10001`, writes bytecode
nowhere, and is compatible with a read-only root filesystem plus `/tmp` tmpfs. The inventory also
forbids privilege escalation, drops every Linux capability, requires RuntimeDefault seccomp, and
disables service-account token mounting. A generic image healthcheck is intentionally absent: each
entrypoint instead exposes a role-specific dependency-aware `--readiness` exec command; liveness
remains process-only.

## Idempotency, failure, and recovery

Run claims, leases, terminal fencing, control inboxes, and critical-event outboxes support duplicate delivery, worker loss, resume, and deterministic recovery.

Each LangGraph model task derives `logicalCallRef` from the durable checkpoint namespace and derives one `attemptRef` from that logical identity plus producer generation. GA makes one RPC call and never retries a provider effect. `outcome_unknown` permits only replay/reconciliation of that exact attempt.

## Extension rules and forbidden dependencies

Add runtime behavior through existing public packages and narrow protocols. Never import Platform/Web/Session source or add `siteId`, `userId`, `ownerId`, or `workspaceId` as a second identity axis.

## Current gotchas

GA core semantics are frozen for the current Platform/Web/Session program. Graph, checkpoint, terminal, control, and handoff changes require prior user alignment.

All three inventory entries are currently `activationAuthorized: false`, `runtimeTraffic: false`,
`launchReadiness: blocked`. Worker and Evidence remain blocked on a monotonic execution-owner lease
epoch plus the terminal/outbox/evidence proof gate. Dependency-aware readiness itself is available:
Worker proves Mongo transactions, Redis consumer primitives, and exact authenticated Hub/Model RPC
paths concurrently; Evidence and Presentation prove Mongo plus their real authenticated listener.
Root K8s Secret/readiness-client material remains a separate launch blocker until its manifest is
synchronized.
Presentation and the current Evidence V2 boundary also remain `contract-only` in the Root registry;
the Evidence process still serves V1 and is explicitly blocked on that version mismatch. An image
build or a live PID is not activation evidence and must not alter those flags.

Interaction Protocol V2 is a dormant foundation, not an active Agent mode. Agent derives only the four Agent-owned
application-request, interaction-owner, projection-event, and group-projection identities. Session-owned decision and
resume refs remain opaque wire values. The generated Protobuf mirrors are distributed, but activation remains fail-closed
until Root-equivalent CEL/protovalidate, the decision-group identity helper, successor proof authority, durable evidence
composition, and the release epoch are wired; the incomplete Pydantic pseudo-mirrors are intentionally not shipped.

The official Python AG-UI SDK is pinned to `ag-ui-protocol==0.1.19` at Root's exact upstream commit.
`kokoro_agent.presentation` converts real `RunEmitter` owner facts directly into frozen Root R1
`PresentationSubmission` values and commits canonical envelopes to the append-only Mongo delivery log
before raw live publication. `kokoro-agent-presentation` exposes persisted `DeliveryRecord` values
through the generated mTLS Connect service. This implemented provider does not make the Root
`contract-only` boundary active; inventory activation stays blocked until the boundary lifecycle and
Root deployment wiring close.

## Verification

Run `uv run ruff check .`, `uv run pyright`, and `uv run pytest` with required Redis/Mongo/MinIO
dependencies available. The release metadata gate is
`uv run pytest tests/repository/test_deployment_inventory.py -q`; the runtime image gate is
`docker build --target runtime --tag kokoro-agent:verification .`.
