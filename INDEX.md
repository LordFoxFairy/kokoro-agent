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

The process entrypoint is `kokoro_agent.worker.main`. `skills`, `hitl`, and the dormant internal
`presentation` candidate adapter bound their public surface with `__init__.py` re-exports; `execution`
and `worker` re-export nothing, so their public surface is the module-level symbols listed in their
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

The Hub compatibility consumer emits only a closed success count and never emits caller credentials, resolved values, response bodies, or exception details.

The interpreter is pinned by `.python-version` (3.11), matching `requires-python` and the Pyright `pythonVersion`. CI installs uv without a Python version input, so without this file `uv sync --locked` resolves whichever interpreter the runner offers and the same lock runs under different interpreters.

## Idempotency, failure, and recovery

Run claims, leases, terminal fencing, control inboxes, and critical-event outboxes support duplicate delivery, worker loss, resume, and deterministic recovery.

Each LangGraph model task derives `logicalCallRef` from the durable checkpoint namespace and derives one `attemptRef` from that logical identity plus producer generation. GA makes one RPC call and never retries a provider effect. `outcome_unknown` permits only replay/reconciliation of that exact attempt.

## Extension rules and forbidden dependencies

Add runtime behavior through existing public packages and narrow protocols. Never import Platform/Web/Session source or add `siteId`, `userId`, `ownerId`, or `workspaceId` as a second identity axis.

## Current gotchas

GA core semantics are frozen for the current Platform/Web/Session program. Graph, checkpoint, terminal, control, and handoff changes require prior user alignment.

Interaction Protocol V2 is a dormant foundation, not an active Agent mode. Agent derives only the four Agent-owned
application-request, interaction-owner, projection-event, and group-projection identities. Session-owned decision and
resume refs remain opaque wire values. The generated Protobuf mirrors are distributed, but activation remains fail-closed
until Root-equivalent CEL/protovalidate, the decision-group identity helper, successor proof authority, durable evidence
composition, and the release epoch are wired; the incomplete Pydantic pseudo-mirrors are intentionally not shipped.

The prelaunch V1 outbound hard cut carries `tool.awaiting_approval.owner_version`. The stable
tool/request id remains the owner; an Agent-private pause journal binds the exact LangGraph
checkpoint to the latest durable applied resume decision, appends immutable revisions, reuses the
exact revision on attach/restart replay, and binds each successor to its predecessor. LangGraph may
re-prompt by changing writes under the same checkpoint id, so checkpoint id alone is never treated
as revision authority; persisted/unapplied control cannot advance the journal.
The semantic critical key is owner + version, and V1 execution evidence copies the emitted version.
Root/Session/Web contract mirrors must be promoted atomically before this wire shape is activated.

The official Python AG-UI SDK is pinned to `ag-ui-protocol==0.1.19` at Root's exact upstream commit.
`kokoro_agent.presentation` constructs official models and then closes them into a frozen typed
Agent-to-Session candidate envelope. It has no transport and is not wired into `RunEmitter`.
`RUN_FINISHED` carries explicit official `success` and forbids `result`; Session must validate that
outcome before deliberately projecting the narrower browser event. Candidate JCS digest, route,
uint64 source ordinal, and canonical UTC millisecond time are recomputable identity inputs.
The existing raw contract has no message-start fact, so current automatic mapping is deliberately
limited to run start/success/error; Text/Activity awaits an atomic durable segment-transition source.

## Verification

Run `uv run ruff check .`, `uv run pyright`, and `uv run pytest` with required Redis/Mongo/MinIO dependencies available.
