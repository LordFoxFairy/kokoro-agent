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

The process entrypoint is `kokoro_agent.worker.main`. `skills` and `hitl` bound their public surface with `__init__.py` re-exports; `execution` and `worker` re-export nothing, so their public surface is the module-level symbols listed in their component INDEX files.

Root compatibility gates invoke `scripts/compat/hub_runtime_consumer.py` as the child-owned live Hub secret consumer. The command wraps the production `HubSecretResolver`; its argv carries only the Hub URL, namespace, handle, and expected SHA-256 digest, while the Agent caller secret remains environment-only.

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

## Verification

Run `uv run ruff check .`, `uv run pyright`, and `uv run pytest` with required Redis/Mongo/MinIO dependencies available.
