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

Agent does not own Site identity, accounts, pricing, plans, credit deduction, Session messages, browser transport, or provider administration.

## Public boundary

The process entrypoint is `kokoro_agent.worker.main`. `skills` and `hitl` bound their public surface with `__init__.py` re-exports; `execution` and `worker` re-export nothing, so their public surface is the module-level symbols listed in their component INDEX files.

## Callers and dependencies

Session submits durable run/control messages. Agent consumes opaque `namespace` and calls model/capability/storage adapters through declared boundaries.

## Data ownership and events

Agent owns execution checkpoints, run leases, control/outbox state, and raw Agent events. Session owns conversations and browser-facing projections.

## Runtime and security

Namespace is opaque and is the only GA isolation key. Provider credentials remain adapter-side and untrusted tool/artifact content is sandboxed and bounded.

The interpreter is pinned by `.python-version` (3.11), matching `requires-python` and the Pyright `pythonVersion`. CI installs uv without a Python version input, so without this file `uv sync --locked` resolves whichever interpreter the runner offers and the same lock runs under different interpreters.

## Idempotency, failure, and recovery

Run claims, leases, terminal fencing, control inboxes, and critical-event outboxes support duplicate delivery, worker loss, resume, and deterministic recovery.

## Extension rules and forbidden dependencies

Add runtime behavior through existing public packages and narrow protocols. Never import Platform/Web/Session source or add `siteId`, `userId`, `ownerId`, or `workspaceId` as a second identity axis.

## Current gotchas

GA core semantics are frozen for the current Platform/Web/Session program. Graph, checkpoint, terminal, control, and handoff changes require prior user alignment.

## Verification

Run `uv run ruff check .`, `uv run pyright`, and `uv run pytest` with required Redis/Mongo/MinIO dependencies available.
