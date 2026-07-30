# Execution evidence

This package is the Agent-owned reconciliation boundary for durable execution facts. It
derives typed V1 evidence from the same critical event payload. Each fact is one indexed
document in `agent_execution_evidence`; a Mongo transaction commits it with the run-local
outbox identity. Evidence never grows the run document and every read is a bounded indexed
query. It is not a second event log, UI projection, or business-identity store.

The same package also owns `agent_durable_output`: an append-only, per-run output sequence
and digest chain independent of lifecycle `durable_seq`. `RunEmitter` persists all typed
outputs for one source event in one transaction before live publication; stable event identity
plus output ordinal gives keep-first replay and conflict detection. Every row repeats the source
batch cardinality and ordinal, so a replay that adds, removes, or reorders drafts fails closed
instead of accepting a matching prefix. Terminal-owner CAS freezes
the output high watermark and digest. Long text is split on UTF-8 boundaries without loss; a
completed snapshot uses one replacement snapshot followed by delta chunks when necessary.
Reasoning, tool arguments/results, executable schemas, and local delivery metadata are
excluded structurally. Agent delivery produces only a safe `delivery.created` notice: a content
hash is not promoted or relabeled as an Artifact/ArtifactVersion authority.

`models.py` is the deterministic protobuf canonicalizer and 64 KiB evidence cap.
`service.py` implements the four generated ConnectRPC reads, including exclusive-cursor
output paging capped at 64 records. `server.py` builds the
HTTP/2-only mTLS listener, and `main.py` is the independently deployable provider process.

The storage adapter can delete output and evidence rows with an eligible run in one Mongo
transaction, but that destructive path is not operationally enabled. `KOKORO_RETENTION_RUN_TTL_S`
and direct supervisor construction accept zero only; a nonzero value fails startup with
`DURABLE_OUTPUT_RETENTION_REQUIRES_CONSUMER_ACK`. Retained output/evidence counts remain visible
as gauges. Root and Session must define a consumer ACK/tombstone contract before time-based
durable-output deletion can be enabled. The separate live events-stream TTL is non-authoritative
and remains configurable.

The caller trust bundle must pin only the client leaf certificates or dedicated issuer
chains used by `kokoro-session` and `kokoro-platform`; strict partial-chain TLS client
certificate verification is therefore the caller allowlist. Application headers are never
accepted as caller identity. Evidence records must never add site, project, user, namespace,
billing, or payment axes. New facts require a Root-owned protobuf/registry revision before
Agent implementation. Production Mongo must be a replica set or sharded topology because
cross-collection atomicity requires transactions; startup/runtime fails closed otherwise.

Current unresolved core blockers: durable-output and terminal writers do not yet carry a
monotonic execution-owner lease epoch, and failure/cancel terminal seal plus terminal outbox/
evidence are not one storage operation. Both changes alter frozen Agent lease/terminal semantics
and require explicit user alignment before implementation.

Source replay guarantee is intentionally narrower than graph replay. For non-semantic live
events, the persisted live index gives a stable source only across the window where output append
succeeds and live publish has not yet succeeded. Once live publish succeeds, a later framework
checkpoint replay has no persisted projection-event identity in the current Agent contract, so
this package does not claim deduplication for that window. The fail-closed completion design is a
Root-owned `projection_event_ref` carried by every output-producing projection: Agent must reject
missing refs, persist ref plus batch manifest, keep-first exact replays, and reject payload or
cardinality drift. Verification must crash-inject both before and after live publish. That
contract is required before broadening the guarantee; content hashing alone is invalid because
identical user-visible deltas can be legitimate repeated output.
