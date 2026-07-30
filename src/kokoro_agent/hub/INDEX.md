---
architectureIndex: 1
rootId: agent.hub-consumer
owners:
  - "@LordFoxFairy"
---

# Hub consumer boundary

## Responsibility

Consume one exact frozen execution assembly per run from Hub over Agent-only mTLS ConnectRPC.
The client validates response order and set equality, recomputes the secret-free assembly digest,
downloads Skill artifacts through bounded server streams, and publishes immutable Skill/MCP inputs
to the Agent assembly layer.

## Forbidden dependencies

This package must not import Mongo, S3, Hub persistence schemas, or Platform source. Hub-owned
state is consumed only through the generated protobuf client. Never log response bodies,
Authorization values, TLS material, or remote exception details.

## Artifact safety

Each archive is limited to 32 MiB compressed; a run is limited to 64 MiB compressed and 128 MiB
unpacked. Archive extraction rejects traversal, links/devices, encryption, duplicate/case-folded
paths, invalid UTF-8, excessive depth, and suspicious compression ratios. Cache publication uses a
temporary sibling directory followed by atomic rename; a partial cache is never visible.
