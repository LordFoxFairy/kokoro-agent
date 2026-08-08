---
architectureIndex: 1
rootId: agent.presentation
owners:
  - "@LordFoxFairy"
---

# presentation — Root R1 submission and delivery boundary

## Responsibilities

Convert real `RunEmitter` owner facts through the pinned official AG-UI SDK adapter into strict
`PresentationSubmission` values, then commit an append-only Agent delivery log before live raw-event
publication. This is the only Agent-owned browser-presentation semantic output.

`runtime.py` owns the stateful source planner and child delivery application port:

- the first submission has `eventOrdinal = "0"`, every later submission advances exactly one, and
  `deliverySeq = eventOrdinal + 1`;
- first text delta atomically creates `TEXT_MESSAGE_START + TEXT_MESSAGE_CONTENT`;
- completion without deltas atomically creates START + optional CONTENT + END;
- terminal/failure closes every open message before RUN_FINISHED/RUN_ERROR;
- tool, HITL, plan, and subagent facts become closed, redacted `ACTIVITY_SNAPSHOT` values;
- `PresentationOwnerState` advances its positive uint64 decimal `ownerVersion` only when the owner's
  semantic fingerprint changes. Replay emits no new Submission; identity/placement drift, time
  regression, terminal revival, and overflow fail closed;
- HITL groups retain private Agent ancestry only. Session owns public binding and receipt authority.

`adapter.py` is the sole official AG-UI SDK trust boundary. It closes an upstream SDK model and
directly seals the Root R1 `PresentationSubmission`; no intermediate envelope or conversion bridge
exists. `submission.py` owns the strict envelope, canonical UTC/JCS encoding, event digest, and
`presentation.submission:sha256:` identity.

## Durability and delivery

Mongo commits a source commit, its complete ordered Submission batch, and the next planner state in
one transaction. Each `DeliveryRecord` persists the canonical Submission bytes, digest, and identity.
The generated wire record is serialized once in that append transaction; provider pull returns the
persisted wire bytes and never rebuilds an envelope.

The unpublished baseline uses only these five collections:

- `agent_presentation_delivery_record`;
- `agent_presentation_source_commit`;
- `agent_presentation_planner_state`;
- `agent_presentation_delivery_state`;
- `agent_presentation_admission_command_receipt`.

Startup creates only the new indexes. There is no old-name read, dual write, alias, migration, or
automatic reinterpretation path.

`PresentationDeliveryService` and `PresentationConnectService` preserve frozen-head paging,
producer generation fencing, contiguous ACK, first-gap quarantine, replay identity, delivery-chain
digests, and terminal seals. Nothing is garbage-collected merely because it was pulled.

## Public surface and dependencies

`__init__.py` exports Submission construction and stable delivery models only. `provider.py` maps the
generated Connect service to the storage port. `RunSupervisor -> RunEmitter -> commit_owner_event`
remains the single production activation path. The Agent-local compatibility command uses the same
public `build_submission` adapter and is not a second implementation.

`PullDurableOutputRecords` remains non-browser execution evidence. Agent does not own Site/Session
binding, public run/message identity, browser cursor, durable Session projection, snapshot repair,
SSE, or HITL decision authority.

## Verification

Run `uv run pytest tests/test_presentation_submission.py tests/test_presentation_planner.py
tests/test_storage.py -q`, then `uv run ruff check .`, `uv run pyright`, and `uv run pytest`.
