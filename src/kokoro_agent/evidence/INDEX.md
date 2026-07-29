# Execution evidence

This package is the Agent-owned reconciliation boundary for durable execution facts. It
derives typed V1 evidence from the same critical event payload. Each fact is one indexed
document in `agent_execution_evidence`; a Mongo transaction commits it with the run-local
outbox identity. Evidence never grows the run document and every read is a bounded indexed
query. It is not a second event log, UI projection, or business-identity store.

`models.py` is the deterministic protobuf canonicalizer and 64 KiB evidence cap.
`service.py` implements the three generated ConnectRPC reads. `server.py` builds the
HTTP/2-only mTLS listener, and `main.py` is the independently deployable provider process.

The caller trust bundle must pin only the client leaf certificates or dedicated issuer
chains used by `kokoro-session` and `kokoro-platform`; strict partial-chain TLS client
certificate verification is therefore the caller allowlist. Application headers are never
accepted as caller identity. Evidence records must never add site, project, user, namespace,
billing, or payment axes. New facts require a Root-owned protobuf/registry revision before
Agent implementation. Production Mongo must be a replica set or sharded topology because
cross-collection atomicity requires transactions; startup/runtime fails closed otherwise.
