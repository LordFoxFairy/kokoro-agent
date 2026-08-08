---
architectureIndex: 1
rootId: agent.security
owners:
  - "@LordFoxFairy"
---

# security — local secret boundary helpers

`read_secure_tls_material` is the single TLS file reader for Hub, Model Gateway, Media Runtime,
and readiness transports. It accepts a direct regular file or a Kubernetes AtomicWriter chain
only when every resolved target remains under the exposed file's mount parent. Reads are bounded,
the final file is opened with `O_NOFOLLOW`, and `lstat`/`fstat` identity must match.

AtomicWriter generations are immutable, so a `..data` rotation is observed on the next read. The
loader rejects mount escapes, excessive symlink chains, non-regular files, oversized content, and
group/world-writable material. It deliberately does not require host-style `0600`: Kubernetes
Secret volumes commonly expose read-only `0644`/`0440` files inside a single-user container.

Callers supply only a stable error code. Paths, bytes, and OS exception details must never cross
this boundary.
