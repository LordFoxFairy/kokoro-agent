---
architectureIndex: 1
rootId: agent.presentation
owners:
  - "@LordFoxFairy"
---

# presentation — production official AG-UI output boundary

## Responsibilities

Convert real `RunEmitter` owner facts into the Root-pinned official Python AG-UI models, close them
through Kokoro's strict profile, and commit an append-only Agent presentation log before live raw-event
publication. This is the only Agent-owned browser-presentation semantic output.

`runtime.py` owns the stateful source-batch projector and child application port:

- first text delta atomically creates `TEXT_MESSAGE_START + TEXT_MESSAGE_CONTENT`;
- a completion without deltas atomically creates START + optional CONTENT + END;
- terminal/failure closes every open message before RUN_FINISHED/RUN_ERROR;
- one source fact may create multiple candidates, each with a stable run-global ordinal, member source
  identity and candidate digest;
- tool, HITL, plan and subagent facts become closed, redacted `ACTIVITY_SNAPSHOT` values. Raw args,
  raw results, provider data, secrets, reasoning and subagent text never enter the presentation log;
- an opaque Agent thread ref is domain-separated from namespace + inbound thread identity. Agent never
  forwards a Session/browser thread identifier.

## Durability and delivery

Mongo commits source marker, full ordered candidate batch and next projection state in one transaction.
Replay must reproduce the exact batch or fail closed. Candidate sequence is independent from raw live
index, lifecycle durable sequence and execution-output sequence.

`RunEmitter` has one production mapping path: `plan_presentation_batch` inside the fenced owner-event
unit of work. `adapter.py` only closes and seals an already-planned official SDK event; it does not map
Agent facts. There is no stateless or compatibility mapper that can bypass projection state.

`AgentPresentationService` freezes a snapshot head on the first pull and pages only through that head.
The future Root Connect provider maps this child application shape without changing its semantics:

- `PullCandidateBatches` returns ordered candidate envelopes plus record/envelope digests and producer
  instance/generation fencing;
- `AcknowledgeCandidateAdmissions` advances only a contiguous prefix under expected-watermark CAS and
  an idempotent request/effect digest;
- `GetDeliveryStatus` exposes acknowledged-through/revision/quarantine;
- a permanent Session rejection is a typed quarantine at the first gap. It never advances the ACK
  watermark and cannot be represented as success.

No candidate is garbage-collected merely because it was pulled. GC remains forbidden until the typed
Session admission receipt has advanced the contiguous acknowledgement watermark.

## Boundary with execution evidence and Session

`PullDurableOutputRecords` remains non-browser execution evidence for audit, business projection and
recovery. Web/AG-UI must never consume it. Agent does not own Site/Session binding, public run/message
identity, browser cursor, durable Session projection, snapshot repair, SSE or HITL decision authority.

The compatibility CLI exercises the same strict builder but is not production activation. Production
activation is `RunSupervisor -> RunEmitter -> commit_owner_event -> Mongo presentation log`.

## Verification

Run `uv run pytest tests/test_agui_production_presentation.py tests/test_invoke.py tests/test_supervisor.py -q`,
then `uv run ruff check .` and `uv run pyright`.
