---
architectureIndex: 1
rootId: agent.presentation
owners:
  - "@LordFoxFairy"
---

# presentation — dormant internal AG-UI candidate boundary

## Responsibilities

Construct the Root-pinned official Python AG-UI event models, serialize their canonical camelCase
typed event, validate it again through Kokoro's closed strict profile, and seal one immutable
Agent-internal candidate envelope with a deterministic RFC 8785 JCS digest and identity.

## Public boundary

`kokoro_agent.presentation` exports the profile pins, strict `AgentAguiCandidateRoute`,
`AgentAguiCandidateSource` and `AgentAguiEventCandidate`, `build_agui_candidate`,
`map_agent_event_candidates`, and the stable `CandidateProtocolError`.

## Ownership and activation

This package produces internal candidates only. Session owns durable presentation rows, run/message
bindings, browser projection, cursors, snapshot repair, SSE and cancellation/interruption rendering.
The package is dormant and has no transport, process composition, background task, or browser path.

The caller must supply an opaque durable `sourceEventRef`; the adapter never derives one from content.
`sourceOrdinal` is canonical uint64 decimal, `recordedAt` is canonical UTC milliseconds equal to the
official event timestamp, and route/source/digest are bound into candidate identity. `RUN_FINISHED`
always carries the official explicit success outcome and never carries `result`; Session must validate
and deliberately project that outcome before the narrower browser event shape.

Caller-supplied source models are untrusted even when their Python type is correct. The builder dumps and
strictly reconstructs the complete nested source before scope or identity work, and candidate models enable
instance revalidation, so `model_copy`/`model_construct` cannot bypass ordinal, identifier, time or route rules.

Candidate `RUN_STARTED` forbids `parentRunId`; Session owns run bindings and alone derives the browser
presentation parent. `sourceOrdinal` comes from `AgentEvent.index`, the RunEmitter-owned per-run sequence
that starts at zero and continues across attach/resume. It never comes from the independent optional,
one-based `durable_seq` assigned by the outbox.

The current raw Agent contract has no message-start fact and tool/subagent segments are not guaranteed
to have an admitted presentation message binding. Therefore `map_agent_event_candidates` maps only
statelessly complete run start/success/error facts today; all message/tool/activity source events map
to zero. The explicit builder still validates every allowed Text/Activity arm for the future atomic
durable source-batch/segment-transition adapter. Shipping CONTENT/END or ACTIVITY alone would create
an unusable sequence, so it is intentionally blocked rather than approximated.

## Closed profile

Allowed event types are RUN start/success/error, TEXT start/content/end and the eight registered safe
ACTIVITY snapshots. CUSTOM, raw/state/messages, native tool, thinking/reasoning/step/chunk families,
Artifact/Cost owner activities, `rawEvent`, `input`, `result` and arbitrary extras fail closed. The
official SDK uses `extra=allow`, so its model is construction authority, not the final trust boundary.

## Extension rules

Do not wire this package into `RunEmitter` until a reviewed durable source-batch integration can commit
zero-or-more candidates atomically. Do not add Site/account/subject/plan/price/provider/storage facts,
cursor/SSE fields, or import Session/Web/Platform source. New event/activity arms require a coordinated
Root profile revision and compatibility evidence.

## Verification

Run `uv run pytest tests/test_agui_presentation_candidate.py -q`, then repository Ruff and Pyright.
